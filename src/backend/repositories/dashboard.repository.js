export const VALID_OWNER_TYPES = ['kabkota', 'provinsi', 'central', 'other'];
export const VALID_SEVERITIES = ['low', 'med', 'high', 'absurd'];

export function escapeLikePattern(value) {
  return String(value).replace(/[\\%_]/g, (match) => `\\${match}`);
}

export function getJsonAsset(db, key, fallback) {
  const row = db.prepare('SELECT json FROM assets WHERE key = ?').get(key);
  return row ? JSON.parse(row.json) : fallback;
}

export function getNationalSummary(db) {
  return db
    .prepare(
      `
      SELECT
        COUNT(*) AS total_packages,
        COALESCE(SUM(is_priority), 0) AS total_priority_packages,
        COALESCE(ROUND(SUM(potential_waste), 2), 0) AS total_potential_waste,
        COALESCE(SUM(COALESCE(budget, 0)), 0) AS total_budget,
        COALESCE(SUM(CASE WHEN mapped_region_count = 0 THEN 1 ELSE 0 END), 0) AS unmapped_packages,
        COALESCE(SUM(CASE WHEN mapped_region_count > 1 THEN 1 ELSE 0 END), 0) AS multi_location_packages
      FROM packages
    `
    )
    .get();
}

export function getRegionRows(db) {
  return db
    .prepare(
      `
      SELECT
        regions.region_key,
        regions.code,
        regions.province_name,
        regions.region_name,
        regions.region_type,
        regions.display_name,
        region_metrics.total_packages,
        region_metrics.total_priority_packages,
        region_metrics.total_flagged_packages,
        region_metrics.total_potential_waste,
        region_metrics.total_budget,
        region_metrics.avg_risk_score,
        region_metrics.max_risk_score,
        region_metrics.central_packages,
        region_metrics.provincial_packages,
        region_metrics.local_packages,
        region_metrics.other_packages,
        region_metrics.central_priority_packages,
        region_metrics.provincial_priority_packages,
        region_metrics.local_priority_packages,
        region_metrics.other_priority_packages,
        region_metrics.central_potential_waste,
        region_metrics.provincial_potential_waste,
        region_metrics.local_potential_waste,
        region_metrics.other_potential_waste,
        region_metrics.central_budget,
        region_metrics.provincial_budget,
        region_metrics.local_budget,
        region_metrics.other_budget,
        region_metrics.med_severity_packages,
        region_metrics.high_severity_packages,
        region_metrics.absurd_severity_packages
      FROM regions
      INNER JOIN region_metrics ON region_metrics.region_key = regions.region_key
      ORDER BY
        region_metrics.total_potential_waste DESC,
        region_metrics.total_priority_packages DESC,
        region_metrics.total_packages DESC,
        regions.display_name ASC
    `
    )
    .all();
}

export function getProvinceRows(db) {
  return db
    .prepare(
      `
      SELECT
        provinces.province_key,
        provinces.code,
        provinces.province_name,
        provinces.display_name,
        province_metrics.total_packages,
        province_metrics.total_priority_packages,
        province_metrics.total_flagged_packages,
        province_metrics.total_potential_waste,
        province_metrics.total_budget,
        province_metrics.avg_risk_score,
        province_metrics.max_risk_score,
        province_metrics.med_severity_packages,
        province_metrics.high_severity_packages,
        province_metrics.absurd_severity_packages
      FROM provinces
      INNER JOIN province_metrics ON province_metrics.province_key = provinces.province_key
      ORDER BY
        province_metrics.total_potential_waste DESC,
        province_metrics.total_priority_packages DESC,
        province_metrics.total_packages DESC,
        provinces.display_name ASC
    `
    )
    .all();
}

export function getOwnerRows(db, ownerType) {
  return db
    .prepare(
      `
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
        owner_metrics.absurd_severity_packages
      FROM owner_metrics
      WHERE owner_metrics.owner_type = ?
      ORDER BY
        owner_metrics.total_potential_waste DESC,
        owner_metrics.total_priority_packages DESC,
        owner_metrics.total_packages DESC,
        owner_metrics.owner_name ASC
    `
    )
    .all(ownerType);
}

export function buildPackagesWhereClause(scopeColumn, scopeKey, query, options = {}) {
  const clauses = [`${scopeColumn} = ?`];
  const params = [scopeKey];

  if (query.search) {
    const searchValue = `%${escapeLikePattern(query.search)}%`;
    clauses.push(
      "(packages.package_name LIKE ? ESCAPE '\\' OR packages.owner_name LIKE ? ESCAPE '\\' OR COALESCE(packages.satker, '') LIKE ? ESCAPE '\\')"
    );
    params.push(searchValue, searchValue, searchValue);
  }

  if (options.forcedOwnerType) {
    clauses.push('packages.owner_type = ?');
    params.push(options.forcedOwnerType);
  } else if (VALID_OWNER_TYPES.includes(query.ownerType)) {
    clauses.push('packages.owner_type = ?');
    params.push(query.ownerType);
  }

  if (options.allowSeverity !== false && VALID_SEVERITIES.includes(query.severity)) {
    clauses.push('packages.severity = ?');
    params.push(query.severity);
  }

  if (query.priorityOnly) {
    clauses.push('packages.is_priority = 1');
  }

  return {
    sql: clauses.join(' AND '),
    params,
  };
}

export function buildOwnerPackagesWhereClause(ownerType, ownerName, query) {
  const clauses = ['packages.owner_type = ?', 'packages.owner_name = ?'];
  const params = [ownerType, ownerName];

  if (query.search) {
    const searchValue = `%${escapeLikePattern(query.search)}%`;
    clauses.push(
      "(packages.package_name LIKE ? ESCAPE '\\' OR packages.owner_name LIKE ? ESCAPE '\\' OR COALESCE(packages.satker, '') LIKE ? ESCAPE '\\')"
    );
    params.push(searchValue, searchValue, searchValue);
  }

  if (VALID_SEVERITIES.includes(query.severity)) {
    clauses.push('packages.severity = ?');
    params.push(query.severity);
  }

  if (query.priorityOnly) {
    clauses.push('packages.is_priority = 1');
  }

  return {
    sql: clauses.join(' AND '),
    params,
  };
}

export function queryPackagesPageRaw(
  db,
  scopeTable,
  scopeColumn,
  scopeKey,
  normalizedQuery,
  options = {}
) {
  const whereClause = buildPackagesWhereClause(scopeColumn, scopeKey, normalizedQuery, options);
  const countRow = db
    .prepare(
      `
      SELECT COUNT(*) AS total
      FROM ${scopeTable}
      INNER JOIN packages ON packages.id = ${scopeTable}.package_id
      WHERE ${whereClause.sql}
    `
    )
    .get(...whereClause.params);
  const totalItems = countRow.total || 0;
  const totalPages = totalItems ? Math.ceil(totalItems / normalizedQuery.pageSize) : 1;
  const page = Math.min(normalizedQuery.page, totalPages);
  const offset = (page - 1) * normalizedQuery.pageSize;
  const rows = db
    .prepare(
      `
      SELECT
        packages.id,
        packages.source_id,
        packages.schema_version,
        packages.owner_name,
        packages.owner_type,
        packages.satker,
        packages.package_name,
        packages.location_raw,
        packages.budget,
        packages.funding_source,
        packages.procurement_type,
        packages.procurement_method,
        packages.selection_date,
        packages.potential_waste,
        packages.severity,
        packages.reason,
        packages.is_mencurigakan,
        packages.is_pemborosan,
        packages.risk_score,
        packages.active_tag_count,
        packages.is_priority,
        packages.is_flagged,
        packages.mapped_region_count
      FROM ${scopeTable}
      INNER JOIN packages ON packages.id = ${scopeTable}.package_id
      WHERE ${whereClause.sql}
      ORDER BY
        packages.is_priority DESC,
        packages.potential_waste DESC,
        packages.risk_score DESC,
        COALESCE(packages.budget, 0) DESC,
        packages.inserted_order ASC
      LIMIT ? OFFSET ?
    `
    )
    .all(...whereClause.params, normalizedQuery.pageSize, offset);

  return {
    totalItems,
    page,
    pageSize: normalizedQuery.pageSize,
    totalPages,
    rows,
  };
}

export function queryOwnerPackagesPageRaw(db, ownerType, ownerName, normalizedQuery) {
  const whereClause = buildOwnerPackagesWhereClause(ownerType, ownerName, normalizedQuery);
  const countRow = db
    .prepare(
      `
      SELECT COUNT(*) AS total
      FROM packages
      WHERE ${whereClause.sql}
    `
    )
    .get(...whereClause.params);
  const totalItems = countRow.total || 0;
  const totalPages = totalItems ? Math.ceil(totalItems / normalizedQuery.pageSize) : 1;
  const page = Math.min(normalizedQuery.page, totalPages);
  const offset = (page - 1) * normalizedQuery.pageSize;
  const rows = db
    .prepare(
      `
      SELECT
        packages.id,
        packages.source_id,
        packages.schema_version,
        packages.owner_name,
        packages.owner_type,
        packages.satker,
        packages.package_name,
        packages.location_raw,
        packages.budget,
        packages.funding_source,
        packages.procurement_type,
        packages.procurement_method,
        packages.selection_date,
        packages.potential_waste,
        packages.severity,
        packages.reason,
        packages.is_mencurigakan,
        packages.is_pemborosan,
        packages.risk_score,
        packages.active_tag_count,
        packages.is_priority,
        packages.is_flagged,
        packages.mapped_region_count
      FROM packages
      WHERE ${whereClause.sql}
      ORDER BY
        packages.is_priority DESC,
        packages.potential_waste DESC,
        packages.risk_score DESC,
        COALESCE(packages.budget, 0) DESC,
        packages.inserted_order ASC
      LIMIT ? OFFSET ?
    `
    )
    .all(...whereClause.params, normalizedQuery.pageSize, offset);

  return {
    totalItems,
    page,
    pageSize: normalizedQuery.pageSize,
    totalPages,
    rows,
  };
}
