#!/usr/bin/env node
"use strict";

/**
 * score-all.js
 *
 * CLI script: batch-score all packages in the SQLite database and persist
 * results to the `anomaly_scores` table.
 *
 * Follows the project convention:
 *   - Uses openDatabase() from ./db (same as server.js)
 *   - Uses better-sqlite3 synchronous API throughout
 *   - Reads DB path from config / env (no hardcoded paths)
 *
 * Usage (from backend/ directory):
 *   node scripts/score-all.js
 *   node scripts/score-all.js --dry-run
 *   node scripts/score-all.js --reset --limit=5000
 *   node scripts/score-all.js --owner-type=PEMDA --owner-name="Dinas Pendidikan Kota X"
 *
 * Options:
 *   --dry-run          Print top results, do NOT write to DB
 *   --reset            Drop and recreate anomaly_scores before run
 *   --limit=N          Process only N packages (default: all)
 *   --owner-type=X     Filter by owner_type
 *   --owner-name=X     Filter by owner_name
 */

const path = require("path");

// Resolve paths relative to backend/src/ where db.js and config.js live
const SRC_DIR = path.resolve(__dirname, "../src");
const { openDatabase }        = require(path.join(SRC_DIR, "db"));
const { scoreBatch }          = require(path.join(SRC_DIR, "anomaly-engine"));

// ── Parse CLI args ────────────────────────────────────────────────────────────

const args = Object.fromEntries(
  process.argv.slice(2)
    .filter(a => a.startsWith("--"))
    .map(a => {
      const [k, v] = a.slice(2).split("=");
      return [k, v !== undefined ? v : true];
    })
);

const DRY_RUN    = !!args["dry-run"];
const RESET      = !!args["reset"];
const LIMIT      = args["limit"]      ? parseInt(args["limit"])      : null;
const OWNER_TYPE = (args["owner-type"] || "").trim();
const OWNER_NAME = (args["owner-name"] || "").trim();

// ── Setup anomaly_scores table ────────────────────────────────────────────────

function ensureAnomalyTable(db) {
  if (RESET) {
    db.exec("DROP TABLE IF EXISTS anomaly_scores;");
    console.log("✓ Dropped existing anomaly_scores table");
  }

  db.exec(`
    CREATE TABLE IF NOT EXISTS anomaly_scores (
      package_id    TEXT    PRIMARY KEY,
      owner_type    TEXT,
      owner_name    TEXT,
      package_name  TEXT,
      score         REAL    NOT NULL DEFAULT 0,
      label         TEXT    NOT NULL DEFAULT 'CLEAN',
      anomaly_count INTEGER NOT NULL DEFAULT 0,
      anomaly_types TEXT,
      computed_at   TEXT    NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_anomaly_label   ON anomaly_scores(label);
    CREATE INDEX IF NOT EXISTS idx_anomaly_score   ON anomaly_scores(score DESC);
    CREATE INDEX IF NOT EXISTS idx_anomaly_owner   ON anomaly_scores(owner_type, owner_name);
  `);
}

// ── Load packages ─────────────────────────────────────────────────────────────

function loadPackages(db) {
  let query = "SELECT * FROM packages WHERE 1=1";
  const params = [];

  if (OWNER_TYPE) { query += " AND owner_type = ?"; params.push(OWNER_TYPE); }
  if (OWNER_NAME) { query += " AND owner_name = ?"; params.push(OWNER_NAME); }
  if (LIMIT)      { query += ` LIMIT ${LIMIT}`; }

  return db.prepare(query).all(...params);
}

// ── Persist results ───────────────────────────────────────────────────────────

function persistResults(db, results) {
  const insert = db.prepare(`
    INSERT OR REPLACE INTO anomaly_scores
      (package_id, owner_type, owner_name, package_name,
       score, label, anomaly_count, anomaly_types, computed_at)
    VALUES
      (@package_id, @owner_type, @owner_name, @package_name,
       @score, @label, @anomaly_count, @anomaly_types, @computed_at)
  `);

  const now = new Date().toISOString();

  const insertMany = db.transaction(rows => {
    for (const r of rows) {
      insert.run({
        package_id:    r.package_id,
        owner_type:    r.owner_type   ?? null,
        owner_name:    r.owner_name   ?? null,
        package_name:  r.package_name ?? null,
        score:         r.score,
        label:         r.label,
        anomaly_count: r.anomaly_count,
        anomaly_types: JSON.stringify(r.anomalies.map(a => a.type)),
        computed_at:   now,
      });
    }
  });

  insertMany(results);
}

// ── Print preview (dry run) ───────────────────────────────────────────────────

function printPreview(results) {
  const top = results.slice(0, 25);
  console.log(`\n📋 Top ${top.length} by risk score:`);
  for (const [i, r] of top.entries()) {
    const bar = "█".repeat(Math.round(r.score * 20)).padEnd(20, "░");
    const pct = String(Math.round(r.score * 100)).padStart(3);
    console.log(`  ${String(i + 1).padStart(2)}. [${r.label.padEnd(6)}] ${pct}% ${bar}  ${r.package_id}`);
    for (const a of r.anomalies) {
      console.log(`       ↳ ${a.type}`);
    }
  }
  console.log("\n(dry run — nothing written to database)");
}

// ── Main ──────────────────────────────────────────────────────────────────────

function main() {
  console.log("🔍  Nemesis · Batch Anomaly Scorer");
  console.log("─".repeat(48));
  if (DRY_RUN)    console.log("  Mode     : DRY RUN");
  if (RESET)      console.log("  Reset    : yes");
  if (OWNER_TYPE) console.log(`  Filter   : owner_type = ${OWNER_TYPE}`);
  if (OWNER_NAME) console.log(`  Filter   : owner_name = ${OWNER_NAME}`);
  if (LIMIT)      console.log(`  Limit    : ${LIMIT} packages`);
  console.log("─".repeat(48));

  const db = openDatabase();

  if (!DRY_RUN) ensureAnomalyTable(db);

  const packages = loadPackages(db);
  console.log(`\n📦  Loaded ${packages.length.toLocaleString("id-ID")} packages`);

  if (!packages.length) {
    console.log("No packages found. Exiting.");
    db.close();
    return;
  }

  const t0      = Date.now();
  process.stdout.write("⚙️   Scoring...");
  const results = scoreBatch(packages);
  const elapsed = ((Date.now() - t0) / 1000).toFixed(2);
  console.log(` done in ${elapsed}s`);

  const counts = { HIGH: 0, MEDIUM: 0, LOW: 0, CLEAN: 0 };
  for (const r of results) counts[r.label] = (counts[r.label] ?? 0) + 1;

  console.log("\n📊  Results:");
  console.log(`    🔴 HIGH   : ${counts.HIGH.toLocaleString("id-ID")}`);
  console.log(`    🟡 MEDIUM : ${counts.MEDIUM.toLocaleString("id-ID")}`);
  console.log(`    🔵 LOW    : ${counts.LOW.toLocaleString("id-ID")}`);
  console.log(`    🟢 CLEAN  : ${counts.CLEAN.toLocaleString("id-ID")}`);

  if (DRY_RUN) {
    printPreview(results);
  } else {
    process.stdout.write("\n💾  Writing to anomaly_scores...");
    persistResults(db, results);
    console.log(` done. ${results.length.toLocaleString("id-ID")} records written.`);
  }

  db.close();
  console.log("\n✅  Completed.");
}

try {
  main();
} catch (err) {
  console.error("\n❌  Fatal:", err.message);
  process.exit(1);
}
