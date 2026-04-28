"use strict";

/**
 * anomaly-repository.js
 *
 * Query layer for anomaly scoring. Mirrors the pattern of dashboard-repository.js:
 * - db is passed in (not a singleton)
 * - all queries are synchronous (better-sqlite3)
 * - returns plain JS objects ready for res.json()
 *
 * Consumed by: app.js (new /api/anomaly/* routes)
 */

const { scorePackage, scoreBatch } = require("./anomaly-engine");

// ── Prepared statement cache ──────────────────────────────────────────────────
// Statements are prepared once per db instance for performance.

const stmtCache = new WeakMap();

function getStmts(db) {
  if (stmtCache.has(db)) return stmtCache.get(db);

  const stmts = {
    getPackageById: db.prepare(`
      SELECT * FROM packages WHERE id = ? LIMIT 1
    `),

    getPackagesByOwner: db.prepare(`
      SELECT * FROM packages
      WHERE owner_type = ? AND owner_name = ?
    `),

    getVendorStatsByOwner: db.prepare(`
      SELECT
        provider_name,
        COUNT(*)          AS count,
        SUM(budget)       AS total_budget
      FROM packages
      WHERE owner_type = ? AND owner_name = ?
        AND provider_name IS NOT NULL
      GROUP BY provider_name
    `),

    getTopRiskyPackages: db.prepare(`
      SELECT p.*
      FROM packages p
      WHERE (:ownerType = '' OR p.owner_type = :ownerType)
        AND (:ownerName = '' OR p.owner_name = :ownerName)
        AND (:method    = '' OR p.procurement_method LIKE '%' || :method || '%')
      LIMIT :limit
    `),
  };

  stmtCache.set(db, stmts);
  return stmts;
}

// ── Repository functions ──────────────────────────────────────────────────────

/**
 * Score a single package by ID.
 * Fetches peers from the same owner for outlier detection.
 *
 * @param {import('better-sqlite3').Database} db
 * @param {string|number} packageId
 * @returns {{ data: Object }|null}
 */
function getPackageAnomalyScore(db, packageId) {
  const stmts = getStmts(db);

  const pkg = stmts.getPackageById.get(packageId);
  if (!pkg) return null;

  const peers = stmts.getPackagesByOwner.all(pkg.owner_type, pkg.owner_name);

  const vendorRows = stmts.getVendorStatsByOwner.all(pkg.owner_type, pkg.owner_name);
  const vendorStats = Object.fromEntries(
    vendorRows.map(r => [r.provider_name, { total_budget: r.total_budget, count: r.count }])
  );

  const result = scorePackage(pkg, { peers, vendorStats });
  return { data: result };
}

/**
 * Score all packages for a given owner and return sorted results.
 *
 * @param {import('better-sqlite3').Database} db
 * @param {string} ownerType
 * @param {string} ownerName
 * @returns {{ data: Object[], meta: Object }}
 */
function getOwnerAnomalyScores(db, ownerType, ownerName) {
  const stmts = getStmts(db);

  const packages   = stmts.getPackagesByOwner.all(ownerType, ownerName);
  const vendorRows = stmts.getVendorStatsByOwner.all(ownerType, ownerName);

  const vendorStats = Object.fromEntries(
    vendorRows.map(r => [r.provider_name, { total_budget: r.total_budget, count: r.count }])
  );

  const results = packages
    .map(p => scorePackage(p, { peers: packages, vendorStats }))
    .sort((a, b) => b.score - a.score);

  const meta = {
    total:  results.length,
    high:   results.filter(r => r.label === "HIGH").length,
    medium: results.filter(r => r.label === "MEDIUM").length,
    low:    results.filter(r => r.label === "LOW").length,
    clean:  results.filter(r => r.label === "CLEAN").length,
  };

  return { data: results, meta };
}

/**
 * Fetch and score a slice of packages across all owners (for the global risk dashboard).
 * Scoring is done in-memory without full peer groups — use for overview/listing only.
 *
 * @param {import('better-sqlite3').Database} db
 * @param {Object} query
 * @param {string} [query.ownerType]
 * @param {string} [query.ownerName]
 * @param {string} [query.method]      - partial match on procurement_method
 * @param {string} [query.label]       - filter results by risk label
 * @param {number} [query.limit=50]
 * @returns {{ data: Object[], meta: Object }}
 */
function getTopRiskyPackages(db, query = {}) {
  const stmts = getStmts(db);

  const limit     = Math.min(parseInt(query.limit) || 50, 200);
  const ownerType = (query.ownerType || "").trim();
  const ownerName = (query.ownerName || "").trim();
  const method    = (query.method    || "").trim();

  const packages = stmts.getTopRiskyPackages.all({
    ownerType, ownerName, method, limit: limit * 4, // over-fetch for label filtering
  });

  const scored = scoreBatch(packages);

  const filtered = query.label
    ? scored.filter(r => r.label === query.label.toUpperCase())
    : scored;

  const page = filtered.slice(0, limit);

  return {
    data: page,
    meta: {
      returned:    page.length,
      label_filter: query.label || null,
      high:   scored.filter(r => r.label === "HIGH").length,
      medium: scored.filter(r => r.label === "MEDIUM").length,
      low:    scored.filter(r => r.label === "LOW").length,
      clean:  scored.filter(r => r.label === "CLEAN").length,
    },
  };
}

module.exports = {
  getPackageAnomalyScore,
  getOwnerAnomalyScores,
  getTopRiskyPackages,
};
