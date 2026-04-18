import fs from "fs";
import path from "path";
import Database from "better-sqlite3";
import {  DB_PATH  } from "../src/backend/config.js";
import {  getTransferFileFormat, importSqlDump, isImportableDatabaseFile  } from "../src/backend/db-transfer.js";

function findLatestExportInDefaultFolder() {
  const exportDir = path.join(path.dirname(DB_PATH), "exports");

  if (!fs.existsSync(exportDir)) {
    return null;
  }

  const files = fs
    .readdirSync(exportDir, { withFileTypes: true })
    .filter((entry) => entry.isFile() && isImportableDatabaseFile(entry.name))
    .map((entry) => {
      const filePath = path.join(exportDir, entry.name);
      const stats = fs.statSync(filePath);

      return {
        filePath,
        modifiedAt: stats.mtimeMs,
      };
    })
    .sort((left, right) => right.modifiedAt - left.modifiedAt);

  return files.length ? files[0].filePath : null;
}

function resolveImportPath(args) {
  const inArgIndex = args.indexOf("--in");

  if (inArgIndex !== -1) {
    const fromArg = args[inArgIndex + 1];
    if (!fromArg) {
      throw new Error(
        'Missing value for "--in". Example: npm run db:import -- --in data/exports/my-db.sqlite or data/exports/my-db.sql'
      );
    }

    return path.isAbsolute(fromArg) ? fromArg : path.resolve(fromArg);
  }

  const fromEnv = process.env.DB_IMPORT_PATH;
  if (fromEnv) {
    return path.isAbsolute(fromEnv) ? fromEnv : path.resolve(fromEnv);
  }

  const latestExport = findLatestExportInDefaultFolder();
  if (latestExport) {
    return latestExport;
  }

  throw new Error("No import file provided and no exports found in data/exports.");
}

function assertSchema(dbPath) {
  const db = new Database(dbPath, { readonly: true });

  try {
    const hasPackages = db
      .prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='packages'")
      .get();

    if (!hasPackages) {
      throw new Error(`Imported DB at ${dbPath} does not contain expected table \"packages\".`);
    }
  } finally {
    db.close();
  }
}

async function main() {
  const sourcePath = resolveImportPath(process.argv.slice(2));
  const sourceFormat = getTransferFileFormat(sourcePath);

  if (!fs.existsSync(sourcePath)) {
    throw new Error(`Import file not found at ${sourcePath}.`);
  }

  if (!sourceFormat) {
    throw new Error(
      `Import file must be a SQLite backup (.sqlite, .sqlite3, .db) or SQL dump (.sql). Got: ${sourcePath}`
    );
  }

  const targetPath = path.resolve(DB_PATH);
  fs.mkdirSync(path.dirname(targetPath), { recursive: true });

  if (path.resolve(sourcePath) === targetPath) {
    throw new Error("Import source and target DB path are the same. Nothing to import.");
  }

  if (sourceFormat === "sql") {
    await importSqlDump(sourcePath, targetPath);
  } else {
    const sourceDb = new Database(sourcePath, { readonly: true });

    try {
      await sourceDb.backup(targetPath);
    } finally {
      sourceDb.close();
    }
  }

  assertSchema(targetPath);

  console.log("Database import completed.");
  console.log(`Import source: ${sourcePath}`);
  console.log(`Format: ${sourceFormat}`);
  console.log(`Runtime DB: ${targetPath}`);
  console.log("You can run backend directly without reseeding.");
}

main().catch((error) => {
  console.error(`DB import failed: ${error.message}`);
  console.error("If DB is locked, stop backend process first and retry.");
  process.exitCode = 1;
});
