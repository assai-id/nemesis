"use strict";

/**
 * anomaly-engine.test.js
 *
 * Run: node --test backend/src/anomaly-engine.test.js
 * No extra deps — uses Node.js built-in test runner (v18+).
 */

const { test, describe } = require("node:test");
const assert = require("node:assert/strict");

const {
  scorePackage,
  scoreBatch,
  _detectors: {
    detectSingleSourceHighValue,
    detectRoundNumberPrice,
    detectDeadlineCramming,
    detectPriceOutlier,
    detectVendorConcentration,
  },
} = require("./anomaly-engine");

// ── Fixtures ──────────────────────────────────────────────────────────────────

/** Minimal clean package row matching `packages` table columns */
const base = {
  id:                 "PKT-001",
  name:               "Pengadaan Laptop Sekolah",
  owner_type:         "PEMDA",
  owner_name:         "Dinas Pendidikan Kota X",
  budget:             200_000_000,
  procurement_method: "Tender",
  provider_name:      "PT Maju Bersama",
  start_date:         "2024-01-01",
  end_date:           "2024-01-30",
};

// ── detectSingleSourceHighValue ───────────────────────────────────────────────

describe("detectSingleSourceHighValue", () => {
  test("flags penunjukan langsung above 500 juta", () => {
    const r = detectSingleSourceHighValue({
      ...base,
      procurement_method: "Penunjukan Langsung",
      budget: 600_000_000,
    });
    assert.ok(r, "expected a result");
    assert.equal(r.type, "SINGLE_SOURCE_HIGH_VALUE");
    assert.ok(r.score > 0 && r.score <= 1);
  });

  test("ignores regular tender above threshold", () => {
    assert.equal(detectSingleSourceHighValue({ ...base, budget: 600_000_000 }), null);
  });

  test("ignores penunjukan langsung below threshold", () => {
    assert.equal(
      detectSingleSourceHighValue({ ...base, procurement_method: "Penunjukan Langsung", budget: 100_000_000 }),
      null
    );
  });

  test("score scales with contract value", () => {
    const low  = detectSingleSourceHighValue({ ...base, procurement_method: "Penunjukan Langsung", budget: 500_000_001 });
    const high = detectSingleSourceHighValue({ ...base, procurement_method: "Penunjukan Langsung", budget: 5_000_000_000 });
    assert.ok(high.score > low.score);
  });
});

// ── detectRoundNumberPrice ────────────────────────────────────────────────────

describe("detectRoundNumberPrice", () => {
  test("flags exact billion", () => {
    const r = detectRoundNumberPrice({ ...base, budget: 1_000_000_000 });
    assert.ok(r);
    assert.equal(r.type, "ROUND_NUMBER_PRICE");
  });

  test("ignores non-round value", () => {
    assert.equal(detectRoundNumberPrice({ ...base, budget: 197_340_500 }), null);
  });

  test("ignores contracts below 10 juta", () => {
    assert.equal(detectRoundNumberPrice({ ...base, budget: 5_000_000 }), null);
  });
});

// ── detectDeadlineCramming ────────────────────────────────────────────────────

describe("detectDeadlineCramming", () => {
  function addDays(iso, n) {
    const d = new Date(iso);
    d.setDate(d.getDate() + n);
    return d.toISOString().split("T")[0];
  }

  test("flags 3-day window", () => {
    const today = "2024-06-01";
    const r = detectDeadlineCramming({ ...base, start_date: today, end_date: addDays(today, 3) });
    assert.ok(r);
    assert.equal(r.type, "DEADLINE_CRAMMING");
  });

  test("does not flag 30-day window", () => {
    // base fixture has 29-day window
    assert.equal(detectDeadlineCramming(base), null);
  });

  test("handles missing dates gracefully", () => {
    assert.equal(detectDeadlineCramming({ ...base, start_date: null }), null);
    assert.equal(detectDeadlineCramming({ ...base, end_date: undefined }), null);
  });
});

// ── detectPriceOutlier ────────────────────────────────────────────────────────

describe("detectPriceOutlier", () => {
  const peers = [100, 110, 105, 108, 102, 107].map(m => ({ ...base, budget: m * 1_000_000 }));

  test("flags statistical outlier (far above peers)", () => {
    const outlier = { ...base, budget: 500_000_000 };
    const r = detectPriceOutlier(outlier, peers);
    assert.ok(r);
    assert.equal(r.type, "PRICE_OUTLIER");
    assert.ok(r.evidence.z_score > 2.5);
  });

  test("does not flag normal contract in peer group", () => {
    assert.equal(detectPriceOutlier({ ...base, budget: 106_000_000 }, peers), null);
  });

  test("returns null with fewer than 5 peers", () => {
    assert.equal(detectPriceOutlier(base, peers.slice(0, 3)), null);
  });

  test("returns null with empty peers", () => {
    assert.equal(detectPriceOutlier(base, []), null);
  });
});

// ── detectVendorConcentration ─────────────────────────────────────────────────

describe("detectVendorConcentration", () => {
  const vendorStats = {
    "PT Dominan Jaya": { total_budget: 800_000_000, count: 8 },
    "CV Kecil":        { total_budget: 100_000_000, count: 2 },
    "UD Lain":         { total_budget: 100_000_000, count: 1 },
  };

  test("flags dominant vendor at 80% share", () => {
    const r = detectVendorConcentration(
      { ...base, provider_name: "PT Dominan Jaya" },
      vendorStats
    );
    assert.ok(r);
    assert.equal(r.type, "VENDOR_CONCENTRATION");
    assert.ok(r.evidence.vendor_share >= 0.7);
  });

  test("does not flag minority vendor", () => {
    assert.equal(
      detectVendorConcentration({ ...base, provider_name: "CV Kecil" }, vendorStats),
      null
    );
  });

  test("returns null with fewer than 3 vendors", () => {
    const small = { "A": { total_budget: 100, count: 1 }, "B": { total_budget: 10, count: 1 } };
    assert.equal(detectVendorConcentration(base, small), null);
  });

  test("returns null if provider_name missing", () => {
    assert.equal(detectVendorConcentration({ ...base, provider_name: null }, vendorStats), null);
  });
});

// ── scorePackage (composite) ──────────────────────────────────────────────────

describe("scorePackage", () => {
  test("clean package scores CLEAN", () => {
    const r = scorePackage(base);
    assert.equal(r.label, "CLEAN");
    assert.ok(r.score < 0.15);
    assert.equal(r.package_id, base.id);
  });

  test("penunjukan langsung high-value scores higher", () => {
    const suspicious = {
      ...base,
      procurement_method: "Penunjukan Langsung",
      budget: 2_000_000_000,
    };
    const r = scorePackage(suspicious);
    assert.ok(r.score > 0.15, `expected score > 0.15, got ${r.score}`);
    assert.ok(["MEDIUM", "HIGH"].includes(r.label));
  });

  test("result includes anomalies array", () => {
    const r = scorePackage(base);
    assert.ok(Array.isArray(r.anomalies));
  });

  test("round number triggers anomaly", () => {
    const r = scorePackage({ ...base, budget: 1_000_000_000 });
    assert.ok(r.anomalies.some(a => a.type === "ROUND_NUMBER_PRICE"));
  });
});

// ── scoreBatch ────────────────────────────────────────────────────────────────

describe("scoreBatch", () => {
  test("returns results sorted by score descending", () => {
    const packages = [
      { ...base, id: "A", procurement_method: "Penunjukan Langsung", budget: 2_000_000_000 },
      { ...base, id: "B" },
      { ...base, id: "C", procurement_method: "Penunjukan Langsung", budget: 600_000_000 },
    ];
    const results = scoreBatch(packages);
    assert.ok(results[0].score >= results[1].score);
    assert.ok(results[1].score >= results[2].score);
  });

  test("handles empty input", () => {
    assert.deepEqual(scoreBatch([]), []);
  });

  test("handles single package", () => {
    const results = scoreBatch([base]);
    assert.equal(results.length, 1);
    assert.equal(results[0].package_id, base.id);
  });
});
