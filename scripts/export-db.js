import fs from "fs";
import path from "path";
import Database from "better-sqlite3";
import {  DATA_DIR  } from "../src/backend/config.js";
import {  exportDatabaseAsSql, getTransferFileFormat  } from "../src/backend/db-transfer.js";
import {  resolveRuntimeDbPath  } from "../src/backend/db.js";

function timestamp() {
  const now = new Date();
  const yyyy = String(now.getFullYear());
  const mm = String(now.getMonth() + 1).padStart(2, "0");
  const dd = String(now.getDate()).padStart(2, "0");
  const hh = String(now.getHours()).padStart(2, "0");
  const mi = String(now.getMinutes()).padStart(2, "0");
  const ss = String(now.getSeconds()).padStart(2, "0");

  return `${yyyy}${mm}${dd}-${hh}${mi}${ss}`;
}

function resolveRequestedFormat(args) {
  const formatArgIndex = args.indexOf("--format");

  if (formatArgIndex === -1) {
    return null;
  }

  const format = String(args[formatArgIndex + 1] || "").trim().toLowerCase();

  if (!format) {
    throw new Error('Missing value for "--format". Example: npm run db:export -- --format sql');
  }

  if (format !== "sqlite" && format !== "sql") {
    throw new Error(`Unsupported export format "${format}". Use "sqlite" or "sql".`);
  }

  return format;
}

function resolveOutputPath(args, requestedFormat) {
  const outArgIndex = args.indexOf("--out");

  if (outArgIndex !== -1) {
    const fromArg = args[outArgIndex + 1];
    if (!fromArg) {
      throw new Error(
        'Missing value for "--out". Example: npm run db:export -- --out data/exports/my-db.sqlite or data/exports/my-db.sql'
      );
    }

    return path.isAbsolute(fromArg) ? fromArg : path.resolve(fromArg);
  }

  const fromEnv = process.env.DB_EXPORT_PATH;
  if (fromEnv) {
    return path.isAbsolute(fromEnv) ? fromEnv : path.resolve(fromEnv);
  }

  const defaultExtension = requestedFormat === "sql" ? "sql" : "sqlite";
  return path.join(DATA_DIR, "exports", `dashboard-export-${timestamp()}.${defaultExtension}`);
}

function resolveOutputFormat(outputPath, requestedFormat) {
  const inferredFormat = getTransferFileFormat(outputPath);

  if (requestedFormat && inferredFormat && requestedFormat !== inferredFormat) {
    throw new Error(
      `Output path ${outputPath} does not match requested format "${requestedFormat}".`
    );
  }

  if (requestedFormat) {
    return requestedFormat;
  }

  return inferredFormat || "sqlite";
}

async function main() {
  const sourcePath = resolveRuntimeDbPath();

  if (!fs.existsSync(sourcePath)) {
    throw new Error(`Database source not found at ${sourcePath}. Seed first with \"npm run db:reset\".`);
  }

  const args = process.argv.slice(2);
  const requestedFormat = resolveRequestedFormat(args);
  const outputPath = resolveOutputPath(args, requestedFormat);
  const outputFormat = resolveOutputFormat(outputPath, requestedFormat);
  fs.mkdirSync(path.dirname(outputPath), { recursive: true });

  if (path.resolve(sourcePath) === path.resolve(outputPath)) {
    throw new Error("Output path cannot be the same as source database path.");
  }

  if (outputFormat === "sql") {
    await exportDatabaseAsSql(sourcePath, outputPath);
  } else {
    const sourceDb = new Database(sourcePath, { readonly: true });

    try {
      await sourceDb.backup(outputPath);
    } finally {
      sourceDb.close();
    }
  }

  console.log(`Database export completed.`);
  console.log(`Source: ${sourcePath}`);
  console.log(`Format: ${outputFormat}`);
  console.log(`Export: ${outputPath}`);
  console.log(`Restore command: npm run db:import -- --in \"${outputPath}\"`);
}

main().catch((error) => {
  console.error(`DB export failed: ${error.message}`);
  process.exitCode = 1;
});
