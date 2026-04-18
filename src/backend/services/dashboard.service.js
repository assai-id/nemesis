import {
  getJsonAsset,
  getNationalSummary,
  getRegionRows,
  getProvinceRows,
  getOwnerRows,
  queryPackagesPageRaw,
  queryOwnerPackagesPageRaw,
  VALID_OWNER_TYPES,
  VALID_SEVERITIES,
} from '../repositories/dashboard.repository.js';
import { DEFAULT_REGION_PAGE_SIZE, MAX_REGION_PAGE_SIZE } from '../config.js';

const LEGEND_COLORS = ['#7b86a3', '#b5a882', '#d4a999', '#8b7332', '#a83c2e'];
const OWNER_METRIC_DEFINITIONS = [
  {
    key: 'central',
    countField: 'central_packages',
    priorityField: 'central_priority_packages',
    wasteField: 'central_potential_waste',
    budgetField: 'central_budget',
  },
  {
    key: 'provinsi',
    countField: 'provincial_packages',
    priorityField: 'provincial_priority_packages',
    wasteField: 'provincial_potential_waste',
    budgetField: 'provincial_budget',
  },
  {
    key: 'kabkota',
    countField: 'local_packages',
    priorityField: 'local_priority_packages',
    wasteField: 'local_potential_waste',
    budgetField: 'local_budget',
  },
  {
    key: 'other',
    countField: 'other_packages',
    priorityField: 'other_priority_packages',
    wasteField: 'other_potential_waste',
    budgetField: 'other_budget',
  },
];

function clampInteger(value, defaultValue, minimum, maximum) {
  const parsed = Number.parseInt(value, 10);

  if (!Number.isFinite(parsed)) {
    return defaultValue;
  }

  return Math.min(Math.max(parsed, minimum), maximum);
}

function parseBooleanQuery(value) {
  if (value === undefined || value === null || value === '') {
    return false;
  }

  if (typeof value === 'boolean') {
    return value;
  }

  const normalized = String(value).trim().toLowerCase();
  return ['1', 'true', 'yes', 'ya'].includes(normalized);
}

function dominantOwnerType(row) {
  const counts = [
    { key: 'central', value: row.central_packages || 0 },
    { key: 'provinsi', value: row.provincial_packages || 0 },
    { key: 'kabkota', value: row.local_packages || 0 },
    { key: 'other', value: row.other_packages || 0 },
  ].sort((left, right) => right.value - left.value);

  return counts[0].value > 0 ? counts[0].key : null;
}

function buildOwnerMetrics(row) {
  return OWNER_METRIC_DEFINITIONS.reduce((metrics, definition) => {
    metrics[definition.key] = {
      totalPackages: row[definition.countField] || 0,
      totalPriorityPackages: row[definition.priorityField] || 0,
      totalPotentialWaste: row[definition.wasteField] || 0,
      totalBudget: row[definition.budgetField] || 0,
    };

    return metrics;
  }, {});
}

function buildProvinceOwnerMetrics(row) {
  return {
    central: {
      totalPackages: 0,
      totalPriorityPackages: 0,
      totalPotentialWaste: 0,
      totalBudget: 0,
    },
    provinsi: {
      totalPackages: row.total_packages || 0,
      totalPriorityPackages: row.total_priority_packages || 0,
      totalPotentialWaste: row.total_potential_waste || 0,
      totalBudget: row.total_budget || 0,
    },
    kabkota: {
      totalPackages: 0,
      totalPriorityPackages: 0,
      totalPotentialWaste: 0,
      totalBudget: 0,
    },
    other: {
      totalPackages: 0,
      totalPriorityPackages: 0,
      totalPotentialWaste: 0,
      totalBudget: 0,
    },
  };
}

function mapOwnerRow(row) {
  return {
    ownerType: row.owner_type,
    ownerName: row.owner_name,
    totalPackages: row.total_packages,
    totalPriorityPackages: row.total_priority_packages,
    totalFlaggedPackages: row.total_flagged_packages,
    totalPotentialWaste: row.total_potential_waste,
    totalBudget: row.total_budget,
    severityCounts: {
      med: row.med_severity_packages,
      high: row.high_severity_packages,
      absurd: row.absurd_severity_packages,
    },
  };
}

function mapRegionRow(row) {
  return {
    regionKey: row.region_key,
    code: row.code,
    provinceName: row.province_name,
    regionName: row.region_name,
    regionType: row.region_type,
    displayName: row.display_name,
    totalPackages: row.total_packages,
    totalPriorityPackages: row.total_priority_packages,
    totalFlaggedPackages: row.total_flagged_packages,
    totalPotentialWaste: row.total_potential_waste,
    totalBudget: row.total_budget,
    avgRiskScore: Number((row.avg_risk_score || 0).toFixed(2)),
    maxRiskScore: row.max_risk_score,
    ownerMix: {
      central: row.central_packages,
      provinsi: row.provincial_packages,
      kabkota: row.local_packages,
      other: row.other_packages,
    },
    ownerMetrics: buildOwnerMetrics(row),
    severityCounts: {
      med: row.med_severity_packages,
      high: row.high_severity_packages,
      absurd: row.absurd_severity_packages,
    },
    dominantOwnerType: dominantOwnerType(row),
  };
}

function mapProvinceRow(row) {
  return {
    provinceKey: row.province_key,
    code: row.code,
    provinceName: row.province_name,
    regionName: row.province_name,
    regionType: 'Provinsi',
    displayName: row.display_name,
    totalPackages: row.total_packages,
    totalPriorityPackages: row.total_priority_packages,
    totalFlaggedPackages: row.total_flagged_packages,
    totalPotentialWaste: row.total_potential_waste,
    totalBudget: row.total_budget,
    avgRiskScore: Number((row.avg_risk_score || 0).toFixed(2)),
    maxRiskScore: row.max_risk_score,
    ownerMix: {
      central: 0,
      provinsi: row.total_packages,
      kabkota: 0,
      other: 0,
    },
    ownerMetrics: buildProvinceOwnerMetrics(row),
    severityCounts: {
      med: row.med_severity_packages,
      high: row.high_severity_packages,
      absurd: row.absurd_severity_packages,
    },
    dominantOwnerType: row.total_packages > 0 ? 'provinsi' : null,
  };
}

function buildLegend(values) {
  const positiveValues = values.filter((value) => value > 0).sort((left, right) => left - right);
  const ranges = [];

  if (!positiveValues.length) {
    return {
      zeroColor: '#243155',
      ranges,
    };
  }

  const quantiles = [0.2, 0.4, 0.6, 0.8, 1].map((ratio) => {
    const index = Math.min(
      positiveValues.length - 1,
      Math.floor((positiveValues.length - 1) * ratio)
    );
    return positiveValues[index];
  });

  let minimum = positiveValues[0];

  for (let index = 0; index < quantiles.length; index += 1) {
    const maximum = quantiles[index];

    if (maximum < minimum) {
      continue;
    }

    if (ranges.length && maximum === ranges[ranges.length - 1].max) {
      continue;
    }

    ranges.push({
      key: `band-${index + 1}`,
      color: LEGEND_COLORS[Math.min(index, LEGEND_COLORS.length - 1)],
      min: minimum,
      max: maximum,
    });

    minimum = maximum + 0.01;
  }

  return {
    zeroColor: '#243155',
    ranges,
  };
}

function normalizeScopedPackageQuery(requestQuery, options = {}) {
  return {
    page: clampInteger(requestQuery.page, 1, 1, Number.MAX_SAFE_INTEGER),
    pageSize: clampInteger(requestQuery.pageSize, DEFAULT_REGION_PAGE_SIZE, 1, MAX_REGION_PAGE_SIZE),
    search: (requestQuery.search || '').trim(),
    ownerType: options.allowOwnerType === false ? '' : (requestQuery.ownerType || '').trim(),
    severity: options.allowSeverity === false ? '' : (requestQuery.severity || '').trim(),
    priorityOnly: parseBooleanQuery(requestQuery.priorityOnly),
  };
}

function mapPackageRow(row) {
  return {
    id: row.id,
    sourceId: row.source_id,
    packageName: row.package_name,
    ownerName: row.owner_name,
    ownerType: row.owner_type,
    satker: row.satker,
    locationRaw: row.location_raw,
    budget: row.budget,
    fundingSource: row.funding_source,
    procurementType: row.procurement_type,
    procurementMethod: row.procurement_method,
    selectionDate: row.selection_date,
    audit: {
      schemaVersion: row.schema_version,
      severity: row.severity,
      potensiPemborosan: row.potential_waste,
      reason: row.reason,
      flags: {
        isMencurigakan: row.is_mencurigakan === null ? null : Boolean(row.is_mencurigakan),
        isPemborosan: row.is_pemborosan === null ? null : Boolean(row.is_pemborosan),
      },
    },
    meta: {
      isPriority: Boolean(row.is_priority),
      isFlagged: Boolean(row.is_flagged),
      riskScore: row.risk_score,
      activeTagCount: row.active_tag_count,
      mappedRegionCount: row.mapped_region_count,
    },
  };
}

function queryPackagesPage(db, scopeTable, scopeColumn, scopeKey, normalizedQuery, options = {}) {
  const result = queryPackagesPageRaw(
    db,
    scopeTable,
    scopeColumn,
    scopeKey,
    normalizedQuery,
    options
  );
  result.rows = result.rows.map(mapPackageRow);
  return result;
}

function queryOwnerPackagesPage(db, ownerType, ownerName, normalizedQuery) {
  const result = queryOwnerPackagesPageRaw(db, ownerType, ownerName, normalizedQuery);
  result.rows = result.rows.map(mapPackageRow);
  return result;
}

function getBootstrapPayload(db) {
  const summaryRow = getNationalSummary(db);
  const regions = getRegionRows(db).map(mapRegionRow);
  const provinces = getProvinceRows(db).map(mapProvinceRow);
  const centralOwners = getOwnerRows(db, 'central').map(mapOwnerRow);

  return {
    summary: {
      totalPackages: summaryRow.total_packages || 0,
      totalPriorityPackages: summaryRow.total_priority_packages || 0,
      totalPotentialWaste: summaryRow.total_potential_waste || 0,
      totalBudget: summaryRow.total_budget || 0,
      unmappedPackages: summaryRow.unmapped_packages || 0,
      multiLocationPackages: summaryRow.multi_location_packages || 0,
    },
    legend: buildLegend(regions.map((region) => region.totalPotentialWaste)),
    geo: getJsonAsset(db, 'audit_geojson', { type: 'FeatureCollection', features: [] }),
    regions,
    provinceView: {
      legend: buildLegend(provinces.map((province) => province.totalPotentialWaste)),
      geo: getJsonAsset(db, 'audit_province_geojson', { type: 'FeatureCollection', features: [] }),
      provinces,
    },
    ownerLists: {
      central: centralOwners,
    },
  };
}

function getRegionPackages(db, regionKey, requestQuery) {
  const regionRow = db
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
      WHERE regions.region_key = ?
    `
    )
    .get(regionKey);

  if (!regionRow) {
    return null;
  }

  const normalizedQuery = normalizeScopedPackageQuery(requestQuery);
  const pageResult = queryPackagesPage(
    db,
    'package_regions',
    'package_regions.region_key',
    regionKey,
    normalizedQuery
  );

  return {
    region: mapRegionRow(regionRow),
    summary: {
      totalItems: pageResult.totalItems,
      filteredItems: pageResult.totalItems,
    },
    pagination: {
      page: pageResult.page,
      pageSize: pageResult.pageSize,
      totalItems: pageResult.totalItems,
      totalPages: pageResult.totalPages,
    },
    filters: {
      search: normalizedQuery.search,
      ownerType: normalizedQuery.ownerType,
      severity: normalizedQuery.severity,
      priorityOnly: normalizedQuery.priorityOnly,
    },
    items: pageResult.rows,
  };
}

function getProvincePackages(db, provinceKey, requestQuery) {
  const provinceRow = db
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
      WHERE provinces.province_key = ?
    `
    )
    .get(provinceKey);

  if (!provinceRow) {
    return null;
  }

  const normalizedQuery = normalizeScopedPackageQuery(requestQuery, {
    allowOwnerType: false,
  });
  const pageResult = queryPackagesPage(
    db,
    'package_provinces',
    'package_provinces.province_key',
    provinceKey,
    normalizedQuery,
    {
      forcedOwnerType: 'provinsi',
    }
  );

  return {
    province: mapProvinceRow(provinceRow),
    summary: {
      totalItems: pageResult.totalItems,
      filteredItems: pageResult.totalItems,
    },
    pagination: {
      page: pageResult.page,
      pageSize: pageResult.pageSize,
      totalItems: pageResult.totalItems,
      totalPages: pageResult.totalPages,
    },
    filters: {
      search: normalizedQuery.search,
      severity: normalizedQuery.severity,
      priorityOnly: normalizedQuery.priorityOnly,
    },
    items: pageResult.rows,
  };
}

function getOwnerPackages(db, requestQuery) {
  const ownerType = (requestQuery.ownerType || '').trim();
  const ownerName = (requestQuery.ownerName || '').trim();

  if (!VALID_OWNER_TYPES.includes(ownerType) || !ownerName) {
    return null;
  }

  const ownerRow = db
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
        AND owner_metrics.owner_name = ?
    `
    )
    .get(ownerType, ownerName);

  if (!ownerRow) {
    return null;
  }

  const normalizedQuery = normalizeScopedPackageQuery(requestQuery, {
    allowOwnerType: false,
  });
  const pageResult = queryOwnerPackagesPage(db, ownerType, ownerName, normalizedQuery);

  return {
    owner: mapOwnerRow(ownerRow),
    summary: {
      totalItems: pageResult.totalItems,
      filteredItems: pageResult.totalItems,
    },
    pagination: {
      page: pageResult.page,
      pageSize: pageResult.pageSize,
      totalItems: pageResult.totalItems,
      totalPages: pageResult.totalPages,
    },
    filters: {
      search: normalizedQuery.search,
      severity: normalizedQuery.severity,
      priorityOnly: normalizedQuery.priorityOnly,
    },
    items: pageResult.rows,
  };
}

export { getBootstrapPayload, getOwnerPackages, getRegionPackages, getProvincePackages };
