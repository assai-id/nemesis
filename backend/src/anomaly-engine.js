"use strict";

/**
 * anomaly-engine.js
 *
 * Heuristic anomaly scoring for procurement packages.
 * Follows the repo's convention: db is passed in, queries are synchronous (better-sqlite3).
 * Table name: `packages` (matches existing schema).
 *
 * Usage (from anomaly-repository.js):
 *   const { scorePackage, scoreBatch } = require("./anomaly-engine");
 *   const result = scorePackage(row, peers);
 */

// ── Thresholds ──────────────────────────────────────────────────────────────

const THRESHOLDS = {
  HIGH_VALUE_IDR:         500_000_000,   // 500 juta
  ROUND_DIVISORS:         [1_000_000_000, 500_000_000, 100_000_000, 50_000_000],
  ROUND_TOLERANCE:        0.001,
  DEADLINE_DAYS_WARNING:  7,
  OUTLIER_ZSCORE:         2.5,
  CONCENTRATION_RATIO:    0.70,
  MIN_PEERS_FOR_OUTLIER:  5,
};

// ── Risk weights (must sum ≤ 1.0) ───────────────────────────────────────────

const WEIGHTS = {
  SINGLE_SOURCE_HIGH_VALUE: 0.30,
  PRICE_OUTLIER:            0.25,
  VENDOR_CONCENTRATION:     0.20,
  DEADLINE_CRAMMING:        0.15,
  ROUND_NUMBER_PRICE:       0.10,
};

// ── Individual detectors ─────────────────────────────────────────────────────

/**
 * Penunjukan langsung above 500 juta — bypasses open competition entirely.
 * @param {Object} pkg - row from `packages` table
 * @returns {{ type, score, description, evidence }|null}
 */
function detectSingleSourceHighValue(pkg) {
  const isSingleSource = /penunjukan\s*langsung/i.test(pkg.procurement_method ?? "");
  const value = pkg.budget ?? 0;
  if (!isSingleSource || value < THRESHOLDS.HIGH_VALUE_IDR) return null;

  const score = Math.min(value / (THRESHOLDS.HIGH_VALUE_IDR * 10), 1.0);
  return {
    type: "SINGLE_SOURCE_HIGH_VALUE",
    score,
    description: `Penunjukan langsung senilai ${formatIDR(value)} — melewati kompetisi terbuka.`,
    evidence: { procurement_method: pkg.procurement_method, budget: value },
  };
}

/**
 * Budget that is a suspiciously round number (exact multiple of 50 juta+).
 * Real cost estimates are never perfectly round.
 * @param {Object} pkg
 * @returns {{ type, score, description, evidence }|null}
 */
function detectRoundNumberPrice(pkg) {
  const value = pkg.budget ?? 0;
  if (value < 10_000_000) return null;

  for (const div of THRESHOLDS.ROUND_DIVISORS) {
    if (value % div === 0 || (value % div) / value < THRESHOLDS.ROUND_TOLERANCE) {
      return {
        type: "ROUND_NUMBER_PRICE",
        score: 0.4,
        description: `Nilai anggaran persis ${formatIDR(value)} — angka bulat sempurna tanpa estimasi biaya nyata.`,
        evidence: { budget: value, divisor: div },
      };
    }
  }
  return null;
}

/**
 * Contract announced with fewer than DEADLINE_DAYS_WARNING days until close.
 * Short windows de facto exclude most vendors.
 * @param {Object} pkg
 * @returns {{ type, score, description, evidence }|null}
 */
function detectDeadlineCramming(pkg) {
  if (!pkg.start_date || !pkg.end_date) return null;

  const start = new Date(pkg.start_date);
  const end   = new Date(pkg.end_date);
  const days  = (end - start) / 86_400_000;

  if (isNaN(days) || days < 0 || days >= THRESHOLDS.DEADLINE_DAYS_WARNING) return null;

  const score = Math.max(0, 1 - (days / THRESHOLDS.DEADLINE_DAYS_WARNING));
  return {
    type: "DEADLINE_CRAMMING",
    score,
    description: `Jendela penawaran hanya ${Math.round(days)} hari — terlalu sempit untuk persaingan sehat.`,
    evidence: { start_date: pkg.start_date, end_date: pkg.end_date, days_available: Math.round(days) },
  };
}

/**
 * Budget is a statistical outlier (Z-score) compared to peer packages
 * in the same owner_type + owner_name group.
 * @param {Object}   pkg
 * @param {Object[]} peers  - packages in the same owner group
 * @returns {{ type, score, description, evidence }|null}
 */
function detectPriceOutlier(pkg, peers) {
  if (!peers || peers.length < THRESHOLDS.MIN_PEERS_FOR_OUTLIER) return null;

  const values = peers.map(p => p.budget).filter(v => v > 0);
  if (values.length < THRESHOLDS.MIN_PEERS_FOR_OUTLIER) return null;

  const mean    = values.reduce((a, b) => a + b, 0) / values.length;
  const variance = values.reduce((s, v) => s + (v - mean) ** 2, 0) / values.length;
  const stddev  = Math.sqrt(variance);
  if (stddev === 0) return null;

  const z = Math.abs((pkg.budget - mean) / stddev);
  if (z < THRESHOLDS.OUTLIER_ZSCORE) return null;

  const dir = pkg.budget > mean ? "di atas" : "di bawah";
  return {
    type: "PRICE_OUTLIER",
    score: Math.min(z / 10, 1.0),
    description: `Nilai anggaran ${dir} rata-rata kelompok sebesar ${z.toFixed(1)} standar deviasi.`,
    evidence: {
      budget: pkg.budget,
      peer_mean: Math.round(mean),
      peer_stddev: Math.round(stddev),
      z_score: parseFloat(z.toFixed(2)),
      peer_count: peers.length,
    },
  };
}

/**
 * One vendor wins an outsized share of the owner's total procurement spend.
 * @param {Object} pkg
 * @param {Object} vendorStats  - { [provider_name]: { total_budget, count } }
 * @returns {{ type, score, description, evidence }|null}
 */
function detectVendorConcentration(pkg, vendorStats) {
  if (!vendorStats || Object.keys(vendorStats).length < 3) return null;
  if (!pkg.provider_name) return null;

  const totalSpend  = Object.values(vendorStats).reduce((s, v) => s + v.total_budget, 0);
  const vendorSpend = vendorStats[pkg.provider_name]?.total_budget ?? 0;
  if (totalSpend === 0) return null;

  const ratio = vendorSpend / totalSpend;
  if (ratio < THRESHOLDS.CONCENTRATION_RATIO) return null;

  return {
    type: "VENDOR_CONCENTRATION",
    score: ratio,
    description: `${pkg.provider_name} menguasai ${(ratio * 100).toFixed(1)}% belanja pemilik ini — potensi monopoli de facto.`,
    evidence: {
      provider_name: pkg.provider_name,
      vendor_share: parseFloat(ratio.toFixed(3)),
      vendor_spend: vendorSpend,
      total_spend: totalSpend,
    },
  };
}

// ── Composite scorer ─────────────────────────────────────────────────────────

/**
 * Score a single package row.
 *
 * @param {Object}   pkg
 * @param {Object}   ctx
 * @param {Object[]} ctx.peers        - peer packages in same owner group
 * @param {Object}   ctx.vendorStats  - aggregated vendor data for the owner
 * @returns {{ package_id, score, label, anomaly_count, anomalies }}
 */
function scorePackage(pkg, ctx = {}) {
  const { peers = [], vendorStats = {} } = ctx;

  const detections = [
    detectSingleSourceHighValue(pkg),
    detectRoundNumberPrice(pkg),
    detectDeadlineCramming(pkg),
    detectPriceOutlier(pkg, peers),
    detectVendorConcentration(pkg, vendorStats),
  ].filter(Boolean);

  let composite = 0;
  for (const d of detections) {
    composite += d.score * (WEIGHTS[d.type] ?? 0.1);
  }
  composite = Math.min(composite, 1.0);

  const label =
    composite >= 0.70 ? "HIGH"   :
    composite >= 0.40 ? "MEDIUM" :
    composite >= 0.15 ? "LOW"    : "CLEAN";

  return {
    package_id:    pkg.id,
    owner_type:    pkg.owner_type,
    owner_name:    pkg.owner_name,
    package_name:  pkg.name,
    score:         parseFloat(composite.toFixed(4)),
    label,
    anomaly_count: detections.length,
    anomalies:     detections,
  };
}

/**
 * Score an array of packages, auto-building peer groups and vendor stats by owner.
 * @param {Object[]} packages
 * @returns {Array} sorted by score descending
 */
function scoreBatch(packages) {
  // Build peer groups: owner_type + owner_name
  const peerGroups = {};
  for (const p of packages) {
    const key = `${p.owner_type}::${p.owner_name}`;
    (peerGroups[key] = peerGroups[key] ?? []).push(p);
  }

  // Build vendor stats per owner
  const ownerVendorStats = {};
  for (const p of packages) {
    const key = `${p.owner_type}::${p.owner_name}`;
    if (!ownerVendorStats[key]) ownerVendorStats[key] = {};
    const stats = ownerVendorStats[key];
    if (!stats[p.provider_name]) stats[p.provider_name] = { total_budget: 0, count: 0 };
    stats[p.provider_name].total_budget += p.budget ?? 0;
    stats[p.provider_name].count        += 1;
  }

  return packages
    .map(p => {
      const key = `${p.owner_type}::${p.owner_name}`;
      return scorePackage(p, {
        peers:       peerGroups[key] ?? [],
        vendorStats: ownerVendorStats[key] ?? {},
      });
    })
    .sort((a, b) => b.score - a.score);
}

// ── Util ─────────────────────────────────────────────────────────────────────

function formatIDR(value) {
  return new Intl.NumberFormat("id-ID", {
    style: "currency", currency: "IDR", maximumFractionDigits: 0,
  }).format(value);
}

// ── Exports ──────────────────────────────────────────────────────────────────

module.exports = {
  scorePackage,
  scoreBatch,
  THRESHOLDS,
  WEIGHTS,
  _detectors: {
    detectSingleSourceHighValue,
    detectRoundNumberPrice,
    detectDeadlineCramming,
    detectPriceOutlier,
    detectVendorConcentration,
  },
};
