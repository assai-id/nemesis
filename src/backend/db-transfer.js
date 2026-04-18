import fs from 'fs';
import path from 'path';
import readline from 'readline';
import Database from 'better-sqlite3';

const SQLITE_FILE_EXTENSIONS = new Set(['.sqlite', '.sqlite3', '.db']);
const SQL_DUMP_EXTENSIONS = new Set(['.sql']);
const SQL_IMPORT_BATCH_BYTES = 1024 * 1024;

function getTransferFileFormat(filePath) {
  const extension = path.extname(filePath).toLowerCase();

  if (SQLITE_FILE_EXTENSIONS.has(extension)) {
    return 'sqlite';
  }

  if (SQL_DUMP_EXTENSIONS.has(extension)) {
    return 'sql';
  }

  return null;
}

function isImportableDatabaseFile(filePath) {
  return Boolean(getTransferFileFormat(filePath));
}

function isSqliteTransferFile(filePath) {
  return getTransferFileFormat(filePath) === 'sqlite';
}

function normalizeSqlStatement(sql) {
  return String(sql).replace(/\r?\n/g, ' ').trim();
}

function escapeIdentifier(identifier) {
  return `"${String(identifier).replace(/"/g, '""')}"`;
}

function toBlobLiteral(buffer) {
  return `X'${buffer.toString('hex').toUpperCase()}'`;
}

function toTextExpression(value) {
  return `CAST(${toBlobLiteral(Buffer.from(value, 'utf8'))} AS TEXT)`;
}

function toSqlLiteral(value) {
  if (value === null) {
    return 'NULL';
  }

  if (Buffer.isBuffer(value)) {
    return toBlobLiteral(value);
  }

  if (typeof value === 'string') {
    return toTextExpression(value);
  }

  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      throw new Error(`Cannot export non-finite numeric value "${value}".`);
    }

    return Object.is(value, -0) ? '-0' : String(value);
  }

  if (typeof value === 'bigint') {
    return value.toString();
  }

  if (typeof value === 'boolean') {
    return value ? '1' : '0';
  }

  throw new Error(`Unsupported SQLite value type "${typeof value}" in SQL dump export.`);
}

function tableExists(db, tableName) {
  return Boolean(
    db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?").get(tableName)
  );
}

function removeSqliteArtifacts(filePath) {
  ['', '-shm', '-wal'].forEach((suffix) => {
    fs.rmSync(`${filePath}${suffix}`, { force: true });
  });
}

function waitForDrain(stream) {
  return new Promise((resolve, reject) => {
    function cleanup() {
      stream.off('drain', handleDrain);
      stream.off('error', handleError);
    }

    function handleDrain() {
      cleanup();
      resolve();
    }

    function handleError(error) {
      cleanup();
      reject(error);
    }

    stream.once('drain', handleDrain);
    stream.once('error', handleError);
  });
}

async function writeLine(stream, line) {
  if (!stream.write(`${line}\n`, 'utf8')) {
    await waitForDrain(stream);
  }
}

function closeStream(stream) {
  return new Promise((resolve, reject) => {
    function cleanup() {
      stream.off('finish', handleFinish);
      stream.off('error', handleError);
    }

    function handleFinish() {
      cleanup();
      resolve();
    }

    function handleError(error) {
      cleanup();
      reject(error);
    }

    stream.once('finish', handleFinish);
    stream.once('error', handleError);
    stream.end();
  });
}

function listSchemaObjects(db) {
  return db
    .prepare(
      `
        SELECT
          rowid AS schemaRowId,
          type,
          name,
          tbl_name AS tableName,
          sql
        FROM sqlite_master
        WHERE sql IS NOT NULL
          AND name NOT LIKE 'sqlite_%'
        ORDER BY
          CASE type
            WHEN 'table' THEN 0
            WHEN 'view' THEN 1
            WHEN 'index' THEN 2
            WHEN 'trigger' THEN 3
            ELSE 4
          END,
          schemaRowId
      `
    )
    .all();
}

async function exportTableData(db, tableName, outputStream) {
  const selectStatement = db.prepare(`SELECT * FROM ${escapeIdentifier(tableName)}`).raw();

  for (const row of selectStatement.iterate()) {
    const values = row.map((value) => toSqlLiteral(value)).join(', ');
    await writeLine(outputStream, `INSERT INTO ${escapeIdentifier(tableName)} VALUES(${values});`);
  }
}

async function exportSqliteSequence(db, outputStream) {
  if (!tableExists(db, 'sqlite_sequence')) {
    return;
  }

  const rows = db.prepare(`SELECT name, seq FROM "sqlite_sequence" ORDER BY name`).raw().all();

  if (!rows.length) {
    return;
  }

  await writeLine(outputStream, `DELETE FROM "sqlite_sequence";`);

  for (const row of rows) {
    const values = row.map((value) => toSqlLiteral(value)).join(', ');
    await writeLine(outputStream, `INSERT INTO "sqlite_sequence" VALUES(${values});`);
  }
}

async function exportDatabaseAsSql(sourcePath, outputPath) {
  const db = new Database(sourcePath, { readonly: true, fileMustExist: true });
  db.defaultSafeIntegers(true);

  const schemaObjects = listSchemaObjects(db);
  const tables = schemaObjects.filter((entry) => entry.type === 'table');
  const otherObjects = schemaObjects.filter((entry) => entry.type !== 'table');
  const outputStream = fs.createWriteStream(outputPath, { encoding: 'utf8' });

  try {
    await writeLine(outputStream, '-- indoaudit SQL text dump');
    await writeLine(outputStream, 'PRAGMA foreign_keys=OFF;');
    await writeLine(outputStream, 'BEGIN TRANSACTION;');

    for (const table of tables) {
      await writeLine(outputStream, `${normalizeSqlStatement(table.sql)};`);
      await exportTableData(db, table.name, outputStream);
    }

    await exportSqliteSequence(db, outputStream);

    for (const entry of otherObjects) {
      await writeLine(outputStream, `${normalizeSqlStatement(entry.sql)};`);
    }

    await writeLine(outputStream, 'COMMIT;');
    await closeStream(outputStream);
  } catch (error) {
    outputStream.destroy(error);
    fs.rmSync(outputPath, { force: true });
    throw error;
  } finally {
    db.close();
  }
}

async function importSqlDump(sourcePath, targetPath) {
  const targetDirectory = path.dirname(targetPath);
  const tempDbPath = path.join(targetDirectory, `.import-${Date.now()}-${process.pid}.sqlite`);
  const db = new Database(tempDbPath);

  let lineNumber = 0;
  let statements = [];
  let bufferedBytes = 0;
  let sqlApplied = false;

  function flushStatements() {
    if (!statements.length) {
      return;
    }

    db.exec(statements.join('\n'));
    statements = [];
    bufferedBytes = 0;
  }

  try {
    const inputStream = fs.createReadStream(sourcePath, { encoding: 'utf8' });
    const input = readline.createInterface({
      input: inputStream,
      crlfDelay: Infinity,
    });

    for await (let line of input) {
      lineNumber += 1;

      if (lineNumber === 1) {
        line = line.replace(/^\uFEFF/, '');
      }

      const trimmed = line.trim();

      if (!trimmed || trimmed.startsWith('--')) {
        continue;
      }

      statements.push(line);
      bufferedBytes += Buffer.byteLength(line, 'utf8') + 1;

      if (bufferedBytes >= SQL_IMPORT_BATCH_BYTES) {
        flushStatements();
      }
    }

    flushStatements();
    sqlApplied = true;
  } catch (error) {
    throw new Error(`Failed to import SQL dump near line ${lineNumber}: ${error.message}`);
  } finally {
    db.close();

    if (!sqlApplied) {
      removeSqliteArtifacts(tempDbPath);
    }
  }

  const importedDb = new Database(tempDbPath, { readonly: true, fileMustExist: true });

  try {
    await importedDb.backup(targetPath);
  } finally {
    importedDb.close();
    removeSqliteArtifacts(tempDbPath);
  }
}

export {
  exportDatabaseAsSql,
  getTransferFileFormat,
  importSqlDump,
  isImportableDatabaseFile,
  isSqliteTransferFile,
};
