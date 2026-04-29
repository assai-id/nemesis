#!/usr/bin/env node
"use strict";

/**
 * scripts/top-risk.js
 *
 * CLI: print top-N risky packages from the existing risk_score column.
 * Useful for quick audit without opening the dashboard.
 *
 * Convention: uses openDatabase() from src/db — same as server.js.
 * No new columns written. Read-only.
 *
 * Usage (from backend/ directory):
 *   node scripts/top-risk.js
 *   node scripts/top-risk.js --limit=100
 *   node scripts/top-risk.js --severity=absurd
 *   node scripts/top-risk.js --owner-type=kabkota
 *   node scripts/top-risk.js --mencurigakan
 *   node scripts/top-risk.js --pemborosan
 *   node scripts/top-risk.js --out=report.json
 */

const path = require("path");
const fs   = require("fs");

const SRC_DIR      = path.resolve(__dirname, "../src");
const { openDatabase } = require(path.join(SRC_DIR, "db"));
const { getTopRiskyPackages, getSeverityDistribution, getMethodBreakdown } = require(path.join(SRC_DIR, "anomaly-repository"));

// ── Parse CLI args ────────────────────────────────────────────────────────────

const args = Object.fromEntries(
  process.argv.slice(2)
    .filter(a => a.startsWith("--"))
    .map(a => {
      const [k, v] = a.slice(2).split("=");
      return [k, v !== undefined ? v : true];
    })
);

const LIMIT       = Math.min(parseInt(args["limit"])      || 50, 500);
const SEVERITY    = args["severity"]    || "";
const OWNER_TYPE  = args["owner-type"]  || "";
const MENCURIGAKAN = !!args["mencurigakan"];
const PEMBOROSAN   = !!args["pemborosan"];
const OUT_FILE    = args["out"] || null;
const SHOW_STATS  = !!args["stats"];

// ── Formatters ────────────────────────────────────────────────────────────────

const IDR = v => new Intl.NumberFormat("id-ID", {
  style: "currency", currency: "IDR", maximumFractionDigits: 0,
}).format(v ?? 0);

const SEVERITY_ICON = { absurd: "🔴", high: "🟠", med: "🟡", low: "🔵" };
const BAR_WIDTH = 20;

function riskBar(score) {
  const filled = Math.round((score ?? 0) * BAR_WIDTH);
  return "█".repeat(filled) + "░".repeat(BAR_WIDTH - filled);
}

// ── Main ──────────────────────────────────────────────────────────────────────

function main() {
  console.log("🕵️  Nemesis · Top Risk Report");
  console.log("─".repeat(60));

  const db = openDatabase();

  // Stats mode: show distributions and exit
  if (SHOW_STATS) {
    const { data: severities } = getSeverityDistribution(db);
    const { data: methods    } = getMethodBreakdown(db, { ownerType: OWNER_TYPE });

    console.log("\n📊  Severity Distribution:");
    for (const s of severities) {
      const icon = SEVERITY_ICON[s.severity] ?? "⚪";
      console.log(`  ${icon} ${s.severity.padEnd(8)} │ ${String(s.totalPackages).padStart(6)} paket │ waste ${IDR(s.totalPotentialWaste)} │ avg risk ${s.avgRiskScore.toFixed(3)}`);
    }

    console.log("\n📋  Procurement Method Breakdown:");
    for (const m of methods) {
      console.log(`  ${(m.procurementMethod || "(kosong)").padEnd(30)} │ ${String(m.totalPackages).padStart(5)} paket │ mencurigakan: ${m.totalMencurigakan} │ pemborosan: ${m.totalPemborosan}`);
    }

    db.close();
    return;
  }

  // Main: top risky packages
  const query = {
    limit:        LIMIT,
    severity:     SEVERITY,
    ownerType:    OWNER_TYPE,
    mencurigakan: MENCURIGAKAN ? "1" : "0",
    pemborosan:   PEMBOROSAN   ? "1" : "0",
  };

  const { data, meta } = getTopRiskyPackages(db, query);

  // Active filters summary
  const activeFilters = Object.entries(meta.filters)
    .filter(([, v]) => v !== "" && v !== false)
    .map(([k, v]) => `${k}=${v}`)
    .join(", ");
  if (activeFilters) console.log(`  Filters  : ${activeFilters}`);
  console.log(`  Results  : ${data.length} packages`);
  console.log("─".repeat(60));

  if (!data.length) {
    console.log("  Tidak ada paket ditemukan.");
    db.close();
    return;
  }

  // Print table
  for (const [i, p] of data.entries()) {
    const icon = SEVERITY_ICON[p.audit.severity] ?? "⚪";
    const score = (p.audit.riskScore ?? 0).toFixed(3);
    const bar   = riskBar(p.audit.riskScore);
    const flags = [
      p.audit.isMencurigakan ? "⚠ mencurigakan" : "",
      p.audit.isPemborosan   ? "💸 pemborosan"   : "",
      p.meta.isPriority      ? "★ priority"      : "",
    ].filter(Boolean).join("  ");

    console.log(`\n  ${String(i + 1).padStart(3)}. ${icon} [${score}] ${bar}`);
    console.log(`       ${p.packageName ?? "(no name)"}`);
    console.log(`       ${p.ownerType} › ${p.ownerName}`);
    console.log(`       Budget: ${IDR(p.budget)}  │  Waste: ${IDR(p.audit.potentialWaste)}`);
    console.log(`       Metode: ${p.procurementMethod ?? "-"}  │  Severity: ${p.audit.severity}`);
    if (flags) console.log(`       ${flags}`);
    if (p.audit.reason) console.log(`       Reason: ${p.audit.reason.slice(0, 100)}`);
  }

  // JSON export
  if (OUT_FILE) {
    const outPath = path.resolve(OUT_FILE);
    fs.writeFileSync(outPath, JSON.stringify({ meta, data }, null, 2), "utf8");
    console.log(`\n💾  Exported ${data.length} records → ${outPath}`);
  }

  db.close();
  console.log("\n✅  Done.");
}

try {
  main();
} catch (err) {
  console.error("\n❌  Fatal:", err.message);
  process.exit(1);
}
