import fs from 'node:fs';
import path from 'node:path';
import Database from 'better-sqlite3';
import { DATA_DIR, DB_PATH } from './config.js';

const SQLITE_EXTENSIONS = new Set(['.sqlite', '.sqlite3', '.db']);
const REQUIRED_SCHEMA_TABLES = ['packages', 'regions'];

function isSqliteFile(fileName) {
  const extension = path.extname(fileName).toLowerCase();
  return SQLITE_EXTENSIONS.has(extension);
}

function listExistingSqliteFiles(directoryPath) {
  if (!fs.existsSync(directoryPath)) {
    return [];
  }

  return fs
    .readdirSync(directoryPath, { withFileTypes: true })
    .filter((entry) => entry.isFile() && isSqliteFile(entry.name))
    .map((entry) => path.resolve(directoryPath, entry.name))
    .sort((left, right) => left.localeCompare(right));
}

function hasApplicationSchema(filePath) {
  let db;

  try {
    db = new Database(filePath, { readonly: true, fileMustExist: true });

    return REQUIRED_SCHEMA_TABLES.every((tableName) =>
      db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?").get(tableName)
    );
  } catch {
    return false;
  } finally {
    if (db) {
      db.close();
    }
  }
}

function resolveRuntimeDbPath() {
  const configuredPath = path.resolve(DB_PATH);
  const configuredFileName = path.basename(configuredPath).toLowerCase();
  const existingDatabases = listExistingSqliteFiles(DATA_DIR);

  if (!existingDatabases.length) {
    return configuredPath;
  }

  const schemaDatabases = existingDatabases.filter(hasApplicationSchema);
  const preferredDatabases = schemaDatabases.length ? schemaDatabases : existingDatabases;
  const configuredMatch = preferredDatabases.find(
    (filePath) => path.basename(filePath).toLowerCase() === configuredFileName
  );

  return configuredMatch || preferredDatabases[0];
}

function ensureDataDirectory() {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}

function openDatabase() {
  ensureDataDirectory();
  const runtimeDbPath = resolveRuntimeDbPath();

  const db = new Database(runtimeDbPath);
  db.pragma('journal_mode = WAL');
  db.pragma('foreign_keys = ON');

  // Performance pragmas. The packages DB is ~2.5 GB and the default
  // page cache (~8 MB) forces near-random I/O on every cold query, so
  // a single filter on a large region (e.g. Jakarta Pusat, ~72k pkgs)
  // can take 5+ seconds before the OS file cache fills.
  //   • mmap_size:  let SQLite map the DB into virtual memory; OS
  //                 page cache transparently absorbs read-heavy work.
  //   • cache_size: dedicate ~128 MB to SQLite's own page cache.
  //                 Negative value means kibibytes (so -131072 = 128 MiB).
  //   • temp_store: keep transient tables (sort, group-by) in RAM.
  //   • synchronous=NORMAL: WAL-safe + faster than FULL for reads.
  db.pragma('mmap_size = 2147483648');
  db.pragma('cache_size = -131072');
  db.pragma('temp_store = MEMORY');
  db.pragma('synchronous = NORMAL');

  // Covering index for the filter-by-(severity|owner_type|priority)
  // pattern that drives the regional/provincial detail tables. Without
  // it, every filtered count needs a heap row fetch per package id
  // returned by the package_regions join — on a 72 k-package region
  // (e.g. Jakarta Pusat) that's 5+ seconds of random I/O on cold cache.
  // With this index the planner runs an index-only scan: ~50 ms.
  // Idempotent; first build takes a few seconds, then it's a no-op.
  try {
    db.exec(
      'CREATE INDEX IF NOT EXISTS idx_packages_filter ' +
        'ON packages(id, severity, owner_type, is_priority)'
    );
  } catch {
    // Best-effort; if it fails (e.g. running against a stripped DB),
    // the queries still work — just slower.
  }

  // Warm up: touch the hottest indexes so the first user-facing
  // query doesn't pay the cold-cache penalty. These are cheap COUNTs
  // that scan covering indexes only — no rowid lookups.
  try {
    db.prepare('SELECT COUNT(*) FROM package_regions').get();
    db.prepare('SELECT COUNT(*) FROM package_provinces').get();
    db.prepare('SELECT COUNT(*) FROM packages WHERE severity IS NOT NULL').get();
    db.prepare('SELECT COUNT(*) FROM packages WHERE owner_type IS NOT NULL').get();
  } catch {
    // Best-effort warmup; ignore if a table happens to be missing.
  }

  return db;
}

export {
  DB_PATH,
  hasApplicationSchema,
  listExistingSqliteFiles,
  openDatabase,
  resolveRuntimeDbPath,
};
