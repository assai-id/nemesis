const fs = require("fs");
const path = require("path");
const { DATA_DIR, DB_PATH, PORT } = require("./config");
const { hasApplicationSchema, listExistingSqliteFiles, openDatabase, resolveRuntimeDbPath } = require("./db");
const { isImportableDatabaseFile } = require("./db-transfer");
const { ensureOwnerMetricsCompatibility, ensureRegionMetricsCompatibility } = require("./seed");
const { createApp } = require("./app");

const runtimeDbPath = resolveRuntimeDbPath();
const runtimeDbExisted = fs.existsSync(runtimeDbPath);
const db = openDatabase();

function findLatestSqliteFile(filePaths) {
  return filePaths
    .map((filePath) => ({
      filePath,
      modifiedAt: fs.statSync(filePath).mtimeMs,
    }))
    .sort((left, right) => right.modifiedAt - left.modifiedAt)[0]?.filePath;
}

function listTransferFiles(directoryPath) {
  if (!fs.existsSync(directoryPath)) {
    return [];
  }

  return fs
    .readdirSync(directoryPath, { withFileTypes: true })
    .filter((entry) => entry.isFile() && isImportableDatabaseFile(entry.name))
    .map((entry) => path.resolve(directoryPath, entry.name));
}

const hasSchema = db
  .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'regions'")
  .get();

if (!hasSchema) {
  const siblingDatabases = listExistingSqliteFiles(DATA_DIR).filter(
    (filePath) => path.resolve(filePath) !== path.resolve(runtimeDbPath)
  );
  const usableSiblingDatabases = siblingDatabases.filter(hasApplicationSchema);
  const exportDir = path.join(DATA_DIR, "exports");
  const exportCandidates = listTransferFiles(exportDir);
  const latestExport = findLatestSqliteFile(exportCandidates);

  console.error(`Audit dashboard schema was not found at ${runtimeDbPath}.`);

  if (!runtimeDbExisted && path.resolve(runtimeDbPath) === path.resolve(DB_PATH)) {
    console.error(`Startup created an empty SQLite file at ${runtimeDbPath} because the configured DB was missing.`);
  }

  if (usableSiblingDatabases.length) {
    console.error(`Found other SQLite files with the expected schema in ${DATA_DIR}:`);
    usableSiblingDatabases.forEach((filePath) => console.error(`- ${filePath}`));
    console.error(`Rename the desired file to ${path.basename(DB_PATH)} or set SQLITE_PATH to point to it.`);
  }

  if (latestExport) {
    console.error(`Database dump files inside ${exportDir} are not loaded automatically.`);
    console.error(`Import the latest export with: npm.cmd run db:import -- --in "${latestExport}"`);
  }

  console.error(`Run "npm.cmd run db:reset" inside backend/ if you want to rebuild the database from seed data.`);
  db.close();
  process.exit(1);
}

if (ensureRegionMetricsCompatibility(db)) {
  console.log("Region metrics schema was outdated. Rebuilt owner-scoped aggregates.");
}

if (ensureOwnerMetricsCompatibility(db)) {
  console.log("Owner metrics table was missing or outdated. Rebuilt national owner aggregates.");
}

db.exec("CREATE INDEX IF NOT EXISTS idx_packages_owner_lookup ON packages(owner_type, owner_name);");

const app = createApp(db);
const server = app.listen(PORT, () => {
  console.log(`Dashboard backend listening on http://127.0.0.1:${PORT}`);
  console.log(`SQLite database: ${runtimeDbPath}`);

  app.locals.bootstrapResponseCache
    .prime()
    .then(() => {
      console.log("Precomputed cached /api/bootstrap response.");
    })
    .catch((error) => {
      console.error("Failed to precompute cached /api/bootstrap response.");
      console.error(error);
    });
});

function shutdown(signal) {
  console.log(`${signal} received, shutting down...`);
  server.close(() => {
    db.close();
    process.exit(0);
  });

  setTimeout(() => {
    process.exit(1);
  }, 5000).unref();
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
