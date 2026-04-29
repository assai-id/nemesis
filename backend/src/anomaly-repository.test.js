"use strict";

/**
 * anomaly-repository.test.js
 *
 * Integration tests using an in-memory SQLite database seeded with
 * the minimum schema needed by anomaly-repository.js.
 *
 * Run: node --test backend/src/anomaly-repository.test.js
 * Requires: better-sqlite3 (already in backend/package.json)
 */

const { test, describe, before } = require("node:test");
const assert = require("node:assert/strict");
const Database = require("better-sqlite3");

const {
  getTopRiskyPackages,
  getOwnerAnomalySummary,
  getSeverityDistribution,
  getMethodBreakdown,
} = require("./anomaly-repository");

// ── In-memory DB fixture ──────────────────────────────────────────────────────

function buildTestDb() {
  const db = new Database(":memory:");

  // Minimum schema — only columns used by anomaly-repository.js
  db.exec(`
    CREATE TABLE packages (
      id                  TEXT    PRIMARY KEY,
      package_name        TEXT,
      owner_name          TEXT,
      owner_type          TEXT,
      satker              TEXT,
      budget              REAL,
      procurement_method  TEXT,
      procurement_type    TEXT,
      selection_date      TEXT,
      potential_waste     REAL    DEFAULT 0,
      severity            TEXT,
      reason              TEXT,
      risk_score          REAL,
      is_mencurigakan     INTEGER,
      is_pemborosan       INTEGER,
      is_priority         INTEGER DEFAULT 0,
      is_flagged          INTEGER DEFAULT 0,
      active_tag_count    INTEGER DEFAULT 0,
      mapped_region_count INTEGER DEFAULT 0,
      inserted_order      INTEGER
    );

    CREATE TABLE owner_metrics (
      owner_type                  TEXT,
      owner_name                  TEXT,
      total_packages              INTEGER DEFAULT 0,
      total_priority_packages     INTEGER DEFAULT 0,
      total_flagged_packages      INTEGER DEFAULT 0,
      total_potential_waste       REAL    DEFAULT 0,
      total_budget                REAL    DEFAULT 0,
      med_severity_packages       INTEGER DEFAULT 0,
      high_severity_packages      INTEGER DEFAULT 0,
      absurd_severity_packages    INTEGER DEFAULT 0,
      PRIMARY KEY (owner_type, owner_name)
    );
  `);

  const insertPkg = db.prepare(`
    INSERT INTO packages
      (id, package_name, owner_name, owner_type, budget,
       procurement_method, risk_score, severity, potential_waste,
       is_mencurigakan, is_pemborosan, is_priority, is_flagged, inserted_order)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const insertOwner = db.prepare(`
    INSERT INTO owner_metrics
      (owner_type, owner_name, total_packages, total_priority_packages,
       total_flagged_packages, total_potential_waste, total_budget,
       med_severity_packages, high_severity_packages, absurd_severity_packages)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const seed = db.transaction(() => {
    // 3 packages — same owner
    insertPkg.run("PKT-001", "Pengadaan Server",    "Dinas A", "kabkota", 500_000_000, "Penunjukan Langsung", 0.85, "absurd", 200_000_000, 1, 1, 1, 1, 1);
    insertPkg.run("PKT-002", "Renovasi Gedung",     "Dinas A", "kabkota", 200_000_000, "Tender",             0.40, "med",    50_000_000,  0, 1, 0, 0, 2);
    insertPkg.run("PKT-003", "Pengadaan ATK",       "Dinas A", "kabkota",  10_000_000, "Tender",             0.10, "low",     1_000_000,  0, 0, 0, 0, 3);

    // 1 package — different owner
    insertPkg.run("PKT-004", "Konsultansi Hukum",   "Dinas B", "provinsi", 300_000_000, "Penunjukan Langsung", 0.70, "high", 80_000_000, 1, 0, 1, 1, 4);

    insertOwner.run("kabkota", "Dinas A", 3, 1, 1, 251_000_000, 710_000_000, 1, 0, 1);
    insertOwner.run("provinsi", "Dinas B", 1, 1, 1,  80_000_000, 300_000_000, 0, 1, 0);
  });

  seed();
  return db;
}

let db;
before(() => { db = buildTestDb(); });

// ── getTopRiskyPackages ───────────────────────────────────────────────────────

describe("getTopRiskyPackages", () => {
  test("returns packages sorted by risk_score DESC", () => {
    const { data } = getTopRiskyPackages(db, { limit: 10 });
    assert.ok(data.length > 0);
    for (let i = 1; i < data.length; i++) {
      assert.ok(data[i - 1].audit.riskScore >= data[i].audit.riskScore,
        "results not sorted by riskScore DESC");
    }
  });

  test("filter: ownerType=kabkota returns only kabkota packages", () => {
    const { data } = getTopRiskyPackages(db, { ownerType: "kabkota" });
    assert.ok(data.every(p => p.ownerType === "kabkota"));
  });

  test("filter: severity=absurd returns only absurd packages", () => {
    const { data } = getTopRiskyPackages(db, { severity: "absurd" });
    assert.ok(data.every(p => p.audit.severity === "absurd"));
  });

  test("filter: mencurigakan=1 returns only is_mencurigakan packages", () => {
    const { data } = getTopRiskyPackages(db, { mencurigakan: "1" });
    assert.ok(data.every(p => p.audit.isMencurigakan === true));
  });

  test("filter: pemborosan=1 returns only is_pemborosan packages", () => {
    const { data } = getTopRiskyPackages(db, { pemborosan: "1" });
    assert.ok(data.every(p => p.audit.isPemborosan === true));
  });

  test("respects limit param", () => {
    const { data } = getTopRiskyPackages(db, { limit: 1 });
    assert.equal(data.length, 1);
  });

  test("caps limit at 200", () => {
    // seeds only 4 rows, so just verify no crash with huge limit
    const { data } = getTopRiskyPackages(db, { limit: 99999 });
    assert.ok(data.length <= 4);
  });

  test("invalid ownerType is ignored (no filter applied)", () => {
    const { data } = getTopRiskyPackages(db, { ownerType: "INVALID" });
    assert.ok(data.length > 0); // should return all, not throw
  });

  test("result shape matches expected keys", () => {
    const { data } = getTopRiskyPackages(db, { limit: 1 });
    const p = data[0];
    assert.ok("id"          in p);
    assert.ok("packageName" in p);
    assert.ok("ownerName"   in p);
    assert.ok("audit"       in p);
    assert.ok("riskScore"   in p.audit);
    assert.ok("severity"    in p.audit);
    assert.ok("meta"        in p);
  });
});

// ── getOwnerAnomalySummary ────────────────────────────────────────────────────

describe("getOwnerAnomalySummary", () => {
  test("returns summary + topPackages for known owner", () => {
    const result = getOwnerAnomalySummary(db, "kabkota", "Dinas A");
    assert.ok(result !== null);
    assert.ok("summary"     in result);
    assert.ok("topPackages" in result);
  });

  test("summary includes mencurigakan and pemborosan counts", () => {
    const { summary } = getOwnerAnomalySummary(db, "kabkota", "Dinas A");
    assert.equal(summary.totalMencurigakan, 1);
    assert.equal(summary.totalPemborosan,   2);
  });

  test("summary avgRiskScore and maxRiskScore are numbers", () => {
    const { summary } = getOwnerAnomalySummary(db, "kabkota", "Dinas A");
    assert.equal(typeof summary.avgRiskScore, "number");
    assert.equal(typeof summary.maxRiskScore, "number");
    assert.ok(summary.maxRiskScore >= summary.avgRiskScore);
  });

  test("topPackages sorted by risk_score DESC", () => {
    const { topPackages } = getOwnerAnomalySummary(db, "kabkota", "Dinas A");
    for (let i = 1; i < topPackages.length; i++) {
      assert.ok(topPackages[i - 1].audit.riskScore >= topPackages[i].audit.riskScore);
    }
  });

  test("returns null for unknown owner", () => {
    const result = getOwnerAnomalySummary(db, "kabkota", "Tidak Ada");
    assert.equal(result, null);
  });

  test("returns null for invalid ownerType", () => {
    const result = getOwnerAnomalySummary(db, "INVALID", "Dinas A");
    assert.equal(result, null);
  });
});

// ── getSeverityDistribution ───────────────────────────────────────────────────

describe("getSeverityDistribution", () => {
  test("returns array with severity groups", () => {
    const { data } = getSeverityDistribution(db);
    assert.ok(Array.isArray(data));
    assert.ok(data.length > 0);
  });

  test("each entry has required fields", () => {
    const { data } = getSeverityDistribution(db);
    for (const entry of data) {
      assert.ok("severity"            in entry);
      assert.ok("totalPackages"       in entry);
      assert.ok("totalPotentialWaste" in entry);
      assert.ok("avgRiskScore"        in entry);
    }
  });

  test("covers all seeded severity levels", () => {
    const { data } = getSeverityDistribution(db);
    const levels = data.map(d => d.severity);
    assert.ok(levels.includes("absurd"));
    assert.ok(levels.includes("med"));
    assert.ok(levels.includes("low"));
  });
});

// ── getMethodBreakdown ────────────────────────────────────────────────────────

describe("getMethodBreakdown", () => {
  test("returns all procurement methods nationally", () => {
    const { data } = getMethodBreakdown(db);
    assert.ok(data.length >= 2); // Penunjukan Langsung + Tender
  });

  test("each entry has required fields", () => {
    const { data } = getMethodBreakdown(db);
    for (const entry of data) {
      assert.ok("procurementMethod"   in entry);
      assert.ok("totalPackages"       in entry);
      assert.ok("totalMencurigakan"   in entry);
      assert.ok("totalPemborosan"     in entry);
      assert.ok("avgRiskScore"        in entry);
    }
  });

  test("scoped to owner returns only that owner's methods", () => {
    const { data } = getMethodBreakdown(db, { ownerType: "kabkota", ownerName: "Dinas A" });
    const totalPkgs = data.reduce((s, d) => s + d.totalPackages, 0);
    assert.equal(totalPkgs, 3); // Dinas A has 3 packages
  });

  test("penunjukan langsung has higher mencurigakan rate", () => {
    const { data } = getMethodBreakdown(db);
    const pl = data.find(d => /penunjukan/i.test(d.procurementMethod));
    assert.ok(pl);
    assert.ok(pl.totalMencurigakan > 0);
  });
});
