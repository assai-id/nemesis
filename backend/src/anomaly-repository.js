"use strict";

/**
 * anomaly-repository.js
 *
 * Query layer for anomaly analysis endpoints.
 *
 * Schema facts (from dashboard-repository.js):
 *   packages columns used here:
 *     id, package_name, owner_name, owner_type, satker,
 *     budget, procurement_method, procurement_type,
 *     risk_score, severity, potential_waste, reason,
 *     is_mencurigakan, is_pemborosan, is_priority, is_flagged,
 *     selection_date, inserted_order, active_tag_count
 *
 *   owner_metrics columns:
 *     owner_type, owner_name,
 *     total_packages, total_priority_packages, total_flagged_packages,
 *     total_potential_waste, total_budget,
 *     med_severity_packages, high_severity_packages, absurd_severity_packages
 *
 * Conventions (same as dashboard-repository.js):
 *   - db passed in, never a singleton
 *   - better-sqlite3 sync API only
 *   - returns plain JS objects for res.json()
 *   - prepared statements cached per db instance via WeakMap
 */

// ── Shared constants (mirrors dashboard-repository.js) ────────────────────────

const VALID_OWNER_TYPES = ["kabkota", "provinsi", "central", "other"];
const VALID_SEVERITIES  = ["low", "med", "high", "absurd"];

// ── Statement cache (one per db instance) ────────────────────────────────────

const stmtCache = new WeakMap();

function getStmts(db) {
  if (stmtCache.has(db)) return stmtCache.get(db);

  const stmts = {

    // Top packages by risk_score across the whole dataset
    // Supports optional filters: ownerType, severity, is_mencurigakan, is_pemborosan
    topRiskyPackages: db.prepare(`
      SELECT
        packages.id,
        packages.package_name,
        packages.owner_name,
        packages.owner_type,
        packages.satker,
        packages.budget,
        packages.procurement_method,
        packages.procurement_type,
        packages.selection_date,
        packages.risk_score,
        packages.severity,
        packages.potential_waste,
        packages.reason,
        packages.is_mencurigakan,
        packages.is_pemborosan,
        packages.is_priority,
        packages.is_flagged,
        packages.active_tag_count,
        packages.mapped_region_count
      FROM packages
      WHERE
        (:ownerType = ''  OR packages.owner_type = :ownerType)
        AND (:severity = '' OR packages.severity  = :severity)
        AND (:mencurigakan = 0 OR packages.is_mencurigakan = 1)
        AND (:pemborosan   = 0 OR packages.is_pemborosan   = 1)
        AND (:priorityOnly = 0 OR packages.is_priority     = 1)
        AND packages.risk_score IS NOT NULL
      ORDER BY
        packages.risk_score    DESC,
        packages.potential_waste DESC,
        packages.inserted_order ASC
      LIMIT :limit
    `),

    // Aggregate breakdown for a given owner
    ownerAnomalySummary: db.prepare(`
      SELECT
        owner_metrics.owner_type,
        owner_metrics.owner_name,
        owner_metrics.total_packages,
        owner_metrics.total_priority_packages,
        owner_metrics.total_flagged_packages,
        owner_metrics.total_potential_waste,
        owner_metrics.total_budget,
        owner_metrics.med_severity_packages,
        owner_metrics.high_severity_packages,
        owner_metrics.absurd_severity_packages,
        COALESCE(extra.total_mencurigakan, 0) AS total_mencurigakan,
        COALESCE(extra.total_pemborosan,   0) AS total_pemborosan,
        COALESCE(extra.avg_risk_score,     0) AS avg_risk_score,
        COALESCE(extra.max_risk_score,     0) AS max_risk_score
      FROM owner_metrics
      LEFT JOIN (
        SELECT
          owner_type,
          owner_name,
          SUM(is_mencurigakan) AS total_mencurigakan,
          SUM(is_pemborosan)   AS total_pemborosan,
          ROUND(AVG(risk_score), 4) AS avg_risk_score,
          MAX(risk_score)           AS max_risk_score
        FROM packages
        WHERE owner_type = ? AND owner_name = ?
      ) extra ON extra.owner_type = owner_metrics.owner_type
             AND extra.owner_name = owner_metrics.owner_name
      WHERE owner_metrics.owner_type = ?
        AND owner_metrics.owner_name = ?
    `),

    // Top packages within one owner, sorted by risk
    ownerTopPackages: db.prepare(`
      SELECT
        packages.id,
        packages.package_name,
        packages.owner_name,
        packages.owner_type,
        packages.satker,
        packages.budget,
        packages.procurement_method,
        packages.procurement_type,
        packages.selection_date,
        packages.risk_score,
        packages.severity,
        packages.potential_waste,
        packages.reason,
        packages.is_mencurigakan,
        packages.is_pemborosan,
        packages.is_priority,
        packages.is_flagged,
        packages.active_tag_count,
        packages.mapped_region_count
      FROM packages
      WHERE packages.owner_type = ?
        AND packages.owner_name = ?
        AND (:severity     = '' OR packages.severity       = :severity)
        AND (:mencurigakan = 0  OR packages.is_mencurigakan = 1)
        AND (:pemborosan   = 0  OR packages.is_pemborosan   = 1)
      ORDER BY
        packages.risk_score     DESC,
        packages.potential_waste DESC,
        packages.inserted_order  ASC
      LIMIT :limit
    `),

    // Severity distribution across the entire dataset
    globalSeverityDistribution: db.prepare(`
      SELECT
        severity,
        COUNT(*) AS total,
        ROUND(SUM(potential_waste), 2) AS total_potential_waste,
        ROUND(AVG(risk_score), 4)      AS avg_risk_score
      FROM packages
      WHERE severity IS NOT NULL
      GROUP BY severity
      ORDER BY avg_risk_score DESC
    `),

    // Procurement method breakdown — useful for detecting penunjukan langsung dominance
    methodBreakdown: db.prepare(`
      SELECT
        procurement_method,
        COUNT(*)                              AS total_packages,
        ROUND(SUM(COALESCE(budget, 0)), 2)   AS total_budget,
        ROUND(SUM(potential_waste), 2)        AS total_potential_waste,
        SUM(is_mencurigakan)                  AS total_mencurigakan,
        SUM(is_pemborosan)                    AS total_pemborosan,
        ROUND(AVG(risk_score), 4)             AS avg_risk_score
      FROM packages
      WHERE (:ownerType = '' OR owner_type = :ownerType)
        AND (:ownerName = '' OR owner_name = :ownerName)
        AND procurement_method IS NOT NULL
      GROUP BY procurement_method
      ORDER BY total_potential_waste DESC
    `),
  };

  stmtCache.set(db, stmts);
  return stmts;
}

// ── Row mappers ───────────────────────────────────────────────────────────────

function mapRiskyPackageRow(row) {
  return {
    id:                row.id,
    packageName:       row.package_name,
    ownerName:         row.owner_name,
    ownerType:         row.owner_type,
    satker:            row.satker,
    budget:            row.budget,
    procurementMethod: row.procurement_method,
    procurementType:   row.procurement_type,
    selectionDate:     row.selection_date,
    audit: {
      riskScore:       row.risk_score,
      severity:        row.severity,
      potentialWaste:  row.potential_waste,
      reason:          row.reason,
      isMencurigakan:  row.is_mencurigakan === null ? null : Boolean(row.is_mencurigakan),
      isPemborosan:    row.is_pemborosan   === null ? null : Boolean(row.is_pemborosan),
    },
    meta: {
      isPriority:       Boolean(row.is_priority),
      isFlagged:        Boolean(row.is_flagged),
      activeTagCount:   row.active_tag_count,
      mappedRegionCount: row.mapped_region_count,
    },
  };
}

function mapOwnerSummaryRow(row) {
  return {
    ownerType:              row.owner_type,
    ownerName:              row.owner_name,
    totalPackages:          row.total_packages,
    totalPriorityPackages:  row.total_priority_packages,
    totalFlaggedPackages:   row.total_flagged_packages,
    totalPotentialWaste:    row.total_potential_waste,
    totalBudget:            row.total_budget,
    totalMencurigakan:      row.total_mencurigakan,
    totalPemborosan:        row.total_pemborosan,
    avgRiskScore:           Number((row.avg_risk_score || 0).toFixed(4)),
    maxRiskScore:           row.max_risk_score,
    severityCounts: {
      med:    row.med_severity_packages,
      high:   row.high_severity_packages,
      absurd: row.absurd_severity_packages,
    },
  };
}

// ── Repository functions ──────────────────────────────────────────────────────

/**
 * Top risky packages nationally, sorted by risk_score DESC.
 *
 * @param {import('better-sqlite3').Database} db
 * @param {Object} query
 * @param {string}  [query.ownerType]     - filter by owner_type
 * @param {string}  [query.severity]      - filter by severity (low|med|high|absurd)
 * @param {boolean} [query.mencurigakan]  - only is_mencurigakan = 1
 * @param {boolean} [query.pemborosan]    - only is_pemborosan = 1
 * @param {boolean} [query.priorityOnly]  - only is_priority = 1
 * @param {number}  [query.limit=50]      - max results (capped at 200)
 * @returns {{ data: Object[], meta: Object }}
 */
function getTopRiskyPackages(db, query = {}) {
  const stmts = getStmts(db);

  const ownerType    = VALID_OWNER_TYPES.includes((query.ownerType || "").trim()) ? query.ownerType.trim() : "";
  const severity     = VALID_SEVERITIES.includes((query.severity   || "").trim()) ? query.severity.trim()  : "";
  const mencurigakan = query.mencurigakan === "1" || query.mencurigakan === true ? 1 : 0;
  const pemborosan   = query.pemborosan   === "1" || query.pemborosan   === true ? 1 : 0;
  const priorityOnly = query.priorityOnly === "1" || query.priorityOnly === true ? 1 : 0;
  const limit        = Math.min(parseInt(query.limit) || 50, 200);

  const rows = stmts.topRiskyPackages.all({
    ownerType, severity, mencurigakan, pemborosan, priorityOnly, limit,
  });

  return {
    data: rows.map(mapRiskyPackageRow),
    meta: {
      returned:    rows.length,
      filters: { ownerType, severity, mencurigakan: !!mencurigakan, pemborosan: !!pemborosan, priorityOnly: !!priorityOnly },
    },
  };
}

/**
 * Anomaly summary + top risky packages for a specific owner.
 *
 * @param {import('better-sqlite3').Database} db
 * @param {string} ownerType
 * @param {string} ownerName
 * @param {Object} [query]
 * @param {string}  [query.severity]
 * @param {boolean} [query.mencurigakan]
 * @param {boolean} [query.pemborosan]
 * @param {number}  [query.limit=20]
 * @returns {{ summary: Object, topPackages: Object[] }|null}
 */
function getOwnerAnomalySummary(db, ownerType, ownerName, query = {}) {
  if (!VALID_OWNER_TYPES.includes(ownerType)) return null;

  const stmts = getStmts(db);

  const summaryRow = stmts.ownerAnomalySummary.get(ownerType, ownerName, ownerType, ownerName);
  if (!summaryRow) return null;

  const severity     = VALID_SEVERITIES.includes((query.severity || "").trim()) ? query.severity.trim() : "";
  const mencurigakan = query.mencurigakan === "1" || query.mencurigakan === true ? 1 : 0;
  const pemborosan   = query.pemborosan   === "1" || query.pemborosan   === true ? 1 : 0;
  const limit        = Math.min(parseInt(query.limit) || 20, 100);

  const packageRows = stmts.ownerTopPackages.all(
    ownerType, ownerName,
    { severity, mencurigakan, pemborosan, limit }
  );

  return {
    summary:     mapOwnerSummaryRow(summaryRow),
    topPackages: packageRows.map(mapRiskyPackageRow),
  };
}

/**
 * Severity distribution nationally.
 *
 * @param {import('better-sqlite3').Database} db
 * @returns {{ data: Object[] }}
 */
function getSeverityDistribution(db) {
  const stmts = getStmts(db);
  const rows  = stmts.globalSeverityDistribution.all();

  return {
    data: rows.map(r => ({
      severity:           r.severity,
      totalPackages:      r.total,
      totalPotentialWaste: r.total_potential_waste,
      avgRiskScore:       r.avg_risk_score,
    })),
  };
}

/**
 * Procurement method breakdown — optionally scoped to one owner.
 *
 * @param {import('better-sqlite3').Database} db
 * @param {Object} [query]
 * @param {string}  [query.ownerType]
 * @param {string}  [query.ownerName]
 * @returns {{ data: Object[] }}
 */
function getMethodBreakdown(db, query = {}) {
  const stmts    = getStmts(db);
  const ownerType = VALID_OWNER_TYPES.includes((query.ownerType || "").trim()) ? query.ownerType.trim() : "";
  const ownerName = (query.ownerName || "").trim();

  const rows = stmts.methodBreakdown.all({ ownerType, ownerName });

  return {
    data: rows.map(r => ({
      procurementMethod:   r.procurement_method,
      totalPackages:       r.total_packages,
      totalBudget:         r.total_budget,
      totalPotentialWaste: r.total_potential_waste,
      totalMencurigakan:   r.total_mencurigakan,
      totalPemborosan:     r.total_pemborosan,
      avgRiskScore:        r.avg_risk_score,
    })),
  };
}

module.exports = {
  getTopRiskyPackages,
  getOwnerAnomalySummary,
  getSeverityDistribution,
  getMethodBreakdown,
};
