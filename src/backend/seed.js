import fs from 'fs';
import path from 'path';
import { StringDecoder } from 'string_decoder';
import {
  AUDIT_DATASET_DIR,
  AUDIT_DATASET_YEAR,
  GEO_ROOT_PATH,
  GEOJSON_PATH,
  PROVINCE_GEOJSON_PATH,
} from './config.js';

const SEVERITY_SCORES = {
  low: 1,
  med: 2,
  high: 3,
  absurd: 4,
};

const PROVINCE_KEY_ALIASES = {
  'daerah khusus ibukota jakarta': 'jakartaraya',
  'dki jakarta': 'jakartaraya',
  'jakarta raya': 'jakartaraya',
  'daerah istimewa yogyakarta': 'yogyakarta',
  'di yogyakarta': 'yogyakarta',
  'bangka belitung': 'bangkabelitung',
  'kep bangka belitung': 'bangkabelitung',
  'kepulauan bangka belitung': 'bangkabelitung',
  'kep riau': 'kepulauanriau',
};

const PROVINCE_DISPLAY_ALIASES = {
  'Jakarta Raya': 'DKI Jakarta',
  Yogyakarta: 'DI Yogyakarta',
  'Daerah Istimewa Yogyakarta': 'DI Yogyakarta',
  'Bangka Belitung': 'Kepulauan Bangka Belitung',
};

const REGION_KEY_ALIASES = {
  'adm kepulauan seribu': 'kepulauanseribu',
  'adm kepulauanseribu': 'kepulauanseribu',
  'karang asem': 'karangasem',
  'kepulauan siau tagulandang biaro': 'siautagulandangbiaro',
  'kep seribu': 'kepulauanseribu',
  'bukit tinggi': 'bukittinggi',
  'kota sorong': 'sorong',
  'pangkal pinang': 'pangkalpinang',
  'pangkajene kepulauan': 'pangkajenedankepulauan',
  'penajem paser utara': 'penajampaserutara',
  'tanjung jabung barat': 'tanjungjabungb',
  'tanjung jabung timur': 'tanjungjabungt',
  'tanjung pinang': 'tanjungpinang',
  'tebing tinggi': 'tebingtinggi',
  terenggalek: 'trenggalek',
};

const REGION_DISPLAY_ALIASES = {
  bukittinggi: 'Bukit Tinggi',
  kepulauanseribu: 'Kepulauan Seribu',
  pangkalpinang: 'Pangkal Pinang',
  pangkajenedankepulauan: 'Pangkajene dan Kepulauan',
  tanjungjabungb: 'Tanjung Jabung Barat',
  tanjungjabungt: 'Tanjung Jabung Timur',
  tanjungpinang: 'Tanjung Pinang',
  tebingtinggi: 'Tebing Tinggi',
};

const OWNER_TYPE_ALIASES = {
  central: 'central',
  instansipusat: 'central',
  kementerianlembaga: 'central',
  provinsi: 'provinsi',
  pemprov: 'provinsi',
  kabkota: 'kabkota',
  kabupatenkota: 'kabkota',
  pemkot: 'kabkota',
  pemkab: 'kabkota',
  other: 'other',
  others: 'other',
  lainnya: 'other',
};

const REGION_OWNER_METRIC_COLUMNS = [
  {
    ownerType: 'central',
    countColumn: 'central_packages',
    priorityColumn: 'central_priority_packages',
    wasteColumn: 'central_potential_waste',
    budgetColumn: 'central_budget',
  },
  {
    ownerType: 'provinsi',
    countColumn: 'provincial_packages',
    priorityColumn: 'provincial_priority_packages',
    wasteColumn: 'provincial_potential_waste',
    budgetColumn: 'provincial_budget',
  },
  {
    ownerType: 'kabkota',
    countColumn: 'local_packages',
    priorityColumn: 'local_priority_packages',
    wasteColumn: 'local_potential_waste',
    budgetColumn: 'local_budget',
  },
  {
    ownerType: 'other',
    countColumn: 'other_packages',
    priorityColumn: 'other_priority_packages',
    wasteColumn: 'other_potential_waste',
    budgetColumn: 'other_budget',
  },
];

const REQUIRED_REGION_METRICS_COLUMNS = REGION_OWNER_METRIC_COLUMNS.flatMap((definition) => [
  definition.countColumn,
  definition.priorityColumn,
  definition.wasteColumn,
  definition.budgetColumn,
]);

const REGION_METRICS_TABLE_SQL = `
    CREATE TABLE region_metrics (
      region_key TEXT PRIMARY KEY,
      total_packages INTEGER NOT NULL,
      total_priority_packages INTEGER NOT NULL,
      total_flagged_packages INTEGER NOT NULL,
      total_potential_waste REAL NOT NULL,
      total_budget INTEGER NOT NULL,
      avg_risk_score REAL NOT NULL,
      max_risk_score INTEGER NOT NULL,
      central_packages INTEGER NOT NULL,
      provincial_packages INTEGER NOT NULL,
      local_packages INTEGER NOT NULL,
      other_packages INTEGER NOT NULL,
      central_priority_packages INTEGER NOT NULL,
      provincial_priority_packages INTEGER NOT NULL,
      local_priority_packages INTEGER NOT NULL,
      other_priority_packages INTEGER NOT NULL,
      central_potential_waste REAL NOT NULL,
      provincial_potential_waste REAL NOT NULL,
      local_potential_waste REAL NOT NULL,
      other_potential_waste REAL NOT NULL,
      central_budget INTEGER NOT NULL,
      provincial_budget INTEGER NOT NULL,
      local_budget INTEGER NOT NULL,
      other_budget INTEGER NOT NULL,
      med_severity_packages INTEGER NOT NULL,
      high_severity_packages INTEGER NOT NULL,
      absurd_severity_packages INTEGER NOT NULL,
      FOREIGN KEY (region_key) REFERENCES regions(region_key) ON DELETE CASCADE
    );
`;

const PROVINCE_METRICS_TABLE_SQL = `
    CREATE TABLE province_metrics (
      province_key TEXT PRIMARY KEY,
      total_packages INTEGER NOT NULL,
      total_priority_packages INTEGER NOT NULL,
      total_flagged_packages INTEGER NOT NULL,
      total_potential_waste REAL NOT NULL,
      total_budget INTEGER NOT NULL,
      avg_risk_score REAL NOT NULL,
      max_risk_score INTEGER NOT NULL,
      med_severity_packages INTEGER NOT NULL,
      high_severity_packages INTEGER NOT NULL,
      absurd_severity_packages INTEGER NOT NULL,
      FOREIGN KEY (province_key) REFERENCES provinces(province_key) ON DELETE CASCADE
    );
`;

const REQUIRED_OWNER_METRICS_COLUMNS = [
  'owner_type',
  'owner_name',
  'total_packages',
  'total_priority_packages',
  'total_flagged_packages',
  'total_potential_waste',
  'total_budget',
  'med_severity_packages',
  'high_severity_packages',
  'absurd_severity_packages',
];

const OWNER_METRICS_TABLE_SQL = `
    CREATE TABLE owner_metrics (
      owner_type TEXT NOT NULL,
      owner_name TEXT NOT NULL,
      total_packages INTEGER NOT NULL,
      total_priority_packages INTEGER NOT NULL,
      total_flagged_packages INTEGER NOT NULL,
      total_potential_waste REAL NOT NULL,
      total_budget INTEGER NOT NULL,
      med_severity_packages INTEGER NOT NULL,
      high_severity_packages INTEGER NOT NULL,
      absurd_severity_packages INTEGER NOT NULL,
      PRIMARY KEY (owner_type, owner_name)
    );
`;

const UNKNOWN_OWNER_NAMES = new Set([
  '',
  '-',
  'n a',
  'na',
  'none',
  'null',
  'tanpa lembaga',
  'tidak diketahui',
  'unknown',
]);
const SKIPPED_GEO_DIRECTORY_FILES = new Set(['none.geojson']);
const DISTRICT_GEO_MAX_RING_POINTS = 220;
const PROVINCE_GEO_MAX_RING_POINTS = 120;
const JSONL_READ_BUFFER_SIZE = 256 * 1024;
const RELATION_INSERT_BATCH_SIZE = 2000;
const LOCATION_CACHE_MAX_SIZE = 100000;

function cleanText(value) {
  if (value === null || value === undefined) {
    return null;
  }

  const text = String(value).trim();
  return text || null;
}

function toComparableWords(value) {
  return String(value || '')
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function toComparableSlug(value) {
  return toComparableWords(value).replace(/\s+/g, '');
}

function slugify(value) {
  return (
    String(value || '')
      .normalize('NFKD')
      .replace(/[\u0300-\u036f]/g, '')
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-+|-+$/g, '') || 'item'
  );
}

function parseBoolean(value) {
  if (value === null || value === undefined || value === '') {
    return null;
  }

  if (typeof value === 'boolean') {
    return value;
  }

  if (typeof value === 'number') {
    return value !== 0;
  }

  const normalized = String(value).trim().toLowerCase();

  if (['1', 'true', 'yes', 'ya'].includes(normalized)) {
    return true;
  }

  if (['0', 'false', 'no', 'tidak'].includes(normalized)) {
    return false;
  }

  return null;
}

function parseInteger(value) {
  if (value === null || value === undefined || value === '') {
    return null;
  }

  if (typeof value === 'number' && Number.isFinite(value)) {
    return Math.round(value);
  }

  const normalized = String(value).trim().replace(/\./g, '').replace(/,/g, '');

  if (!normalized) {
    return null;
  }

  const parsed = Number.parseFloat(normalized);
  return Number.isFinite(parsed) ? Math.round(parsed) : null;
}

function parseAmount(value, budget) {
  if (value === null || value === undefined || value === '') {
    return 0;
  }

  const parsed = Number.parseFloat(String(value).trim().replace(/\./g, '').replace(/,/g, ''));
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return 0;
  }

  if (Number.isFinite(budget) && budget !== null) {
    return Math.min(parsed, budget);
  }

  return parsed;
}

function normalizeSeverity(value) {
  if (typeof value === 'boolean') {
    return value ? 'med' : 'low';
  }

  const text = cleanText(value);
  if (!text) {
    return 'low';
  }

  const normalized = text.toLowerCase();
  if (normalized === 'high') {
    return 'high';
  }

  if (normalized === 'absurd') {
    return 'absurd';
  }

  if (normalized === 'med' || normalized === 'medium') {
    return 'med';
  }

  return 'low';
}

function sanitizeReason(value) {
  const text = cleanText(value);
  return text ? text.slice(0, 1000) : null;
}

function inferOwnerType(ownerName) {
  const normalized = toComparableWords(ownerName);

  if (!normalized || UNKNOWN_OWNER_NAMES.has(normalized)) {
    return 'other';
  }

  if (
    normalized.startsWith('kab ') ||
    normalized.startsWith('kabupaten ') ||
    normalized.startsWith('kota ') ||
    normalized.startsWith('pemkab ') ||
    normalized.startsWith('pemerintah kabupaten ') ||
    normalized.startsWith('pemkot ') ||
    normalized.startsWith('pemerintah kota ')
  ) {
    return 'kabkota';
  }

  if (
    normalized.startsWith('provinsi ') ||
    normalized.startsWith('pemprov ') ||
    normalized.startsWith('pemerintah provinsi ')
  ) {
    return 'provinsi';
  }

  return 'central';
}

function normalizeOwnerType(value, ownerName) {
  const normalized = toComparableSlug(value);

  if (normalized && OWNER_TYPE_ALIASES[normalized]) {
    return OWNER_TYPE_ALIASES[normalized];
  }

  return inferOwnerType(ownerName);
}

function normalizeProvinceKey(value) {
  const normalized = toComparableWords(value);
  return PROVINCE_KEY_ALIASES[normalized] || toComparableSlug(normalized);
}

function normalizeProvinceDisplayName(value) {
  const text = cleanText(value);
  return text ? PROVINCE_DISPLAY_ALIASES[text] || text : 'Tidak diketahui';
}

function normalizeRegionType(value) {
  const normalized = toComparableWords(value);
  return normalized.startsWith('kab') ? 'Kabupaten' : 'Kota';
}

function normalizeRegionKey(value) {
  const normalized = toComparableWords(value)
    .replace(/^kabupaten\s+/, '')
    .replace(/^kab\s+/, '')
    .replace(/^kota\s+/, '')
    .replace(/^adm\.?\s+/, '')
    .trim();

  return REGION_KEY_ALIASES[normalized] || toComparableSlug(normalized);
}

function normalizeRegionDisplayName(value, regionType) {
  const cleaned = cleanText(value) || 'Tidak diketahui';
  const withoutPrefix = cleaned
    .replace(/^Kabupaten\s+/i, '')
    .replace(/^Kab\.\s+/i, '')
    .replace(/^Kota\s+/i, '')
    .replace(/^Adm\.?\s+/i, '')
    .trim();
  const key = normalizeRegionKey(withoutPrefix);

  return REGION_DISPLAY_ALIASES[key] || withoutPrefix;
}

function buildLocationLookupKey(provinceName, regionName, regionType) {
  return `${normalizeProvinceKey(provinceName)}|${normalizeRegionKey(regionName)}|${toComparableSlug(regionType)}`;
}

function buildProvinceLookupKey(provinceName) {
  return normalizeProvinceKey(provinceName);
}

function buildRegionOnlyLookupKey(regionName, regionType) {
  return `${normalizeRegionKey(regionName)}|${toComparableSlug(regionType)}`;
}

function buildRegionDisplayName(regionName, regionType) {
  return `${regionType === 'Kota' ? 'Kota' : 'Kab.'} ${regionName}`;
}

function roundPoint(point) {
  return point.slice(0, 2).map((value) => Number(Number(value).toFixed(4)));
}

function samePoint(left, right) {
  return (
    Array.isArray(left) && Array.isArray(right) && left[0] === right[0] && left[1] === right[1]
  );
}

function simplifyRing(ring, maxPoints) {
  if (!Array.isArray(ring) || ring.length <= 10) {
    return ring.map(roundPoint);
  }

  const roundedRing = ring.map(roundPoint);
  const isClosed = samePoint(roundedRing[0], roundedRing[roundedRing.length - 1]);
  const openRing = isClosed ? roundedRing.slice(0, -1) : roundedRing.slice();
  const step = Math.max(1, Math.ceil(openRing.length / maxPoints));
  const simplified = [];

  for (let index = 0; index < openRing.length; index += step) {
    simplified.push(openRing[index]);
  }

  const lastPoint = openRing[openRing.length - 1];

  if (!samePoint(simplified[simplified.length - 1], lastPoint)) {
    simplified.push(lastPoint);
  }

  if (simplified.length < 4) {
    return roundedRing;
  }

  if (isClosed && !samePoint(simplified[0], simplified[simplified.length - 1])) {
    simplified.push(simplified[0].slice());
  }

  return simplified;
}

function simplifyGeometry(geometry, maxRingPoints) {
  if (!geometry || !geometry.type || !Array.isArray(geometry.coordinates)) {
    return geometry;
  }

  if (geometry.type === 'Polygon') {
    return {
      ...geometry,
      coordinates: geometry.coordinates.map((ring) => simplifyRing(ring, maxRingPoints)),
    };
  }

  if (geometry.type === 'MultiPolygon') {
    return {
      ...geometry,
      coordinates: geometry.coordinates.map((polygon) =>
        polygon.map((ring) => simplifyRing(ring, maxRingPoints))
      ),
    };
  }

  return geometry;
}

function parseGeoJsonFile(filePath) {
  const payload = JSON.parse(fs.readFileSync(filePath, 'utf8'));

  if (!payload || payload.type !== 'FeatureCollection' || !Array.isArray(payload.features)) {
    throw new Error(`GeoJSON asset at "${filePath}" is invalid.`);
  }

  return payload;
}

function listGeoDirectoryFiles(directoryPath) {
  return fs
    .readdirSync(directoryPath, { withFileTypes: true })
    .filter((entry) => entry.isFile() && entry.name.toLowerCase().endsWith('.geojson'))
    .map((entry) => path.resolve(directoryPath, entry.name))
    .sort((left, right) => left.localeCompare(right));
}

function resolveLegacyFallbackGeoPath(directoryPath) {
  const fallbackPath = path.resolve(directoryPath, '..', 'indonesia-kabkota-simple.geojson');
  return fs.existsSync(fallbackPath) ? fallbackPath : null;
}

function assertGeoFeature(feature, sourcePath, index) {
  if (
    !feature ||
    feature.type !== 'Feature' ||
    !feature.geometry ||
    !feature.properties ||
    typeof feature.properties !== 'object'
  ) {
    throw new Error(
      `GeoJSON asset at "${sourcePath}" contains an invalid feature at index ${index}.`
    );
  }
}

function loadGeoSource() {
  if (!fs.existsSync(GEO_ROOT_PATH)) {
    throw new Error(`Geo root folder was not found at "${GEO_ROOT_PATH}".`);
  }

  if (!fs.existsSync(GEOJSON_PATH)) {
    throw new Error(`GeoJSON asset was not found at "${GEOJSON_PATH}".`);
  }

  const stats = fs.statSync(GEOJSON_PATH);

  if (!stats.isDirectory()) {
    throw new Error(
      `District geo source at "${GEOJSON_PATH}" must be a directory under "${GEO_ROOT_PATH}".`
    );
  }

  return {
    kind: 'district-directory',
    sourcePath: GEOJSON_PATH,
  };
}

function loadProvinceGeoSource() {
  if (!fs.existsSync(GEO_ROOT_PATH)) {
    throw new Error(`Geo root folder was not found at "${GEO_ROOT_PATH}".`);
  }

  if (!fs.existsSync(PROVINCE_GEOJSON_PATH)) {
    throw new Error(`Province GeoJSON asset was not found at "${PROVINCE_GEOJSON_PATH}".`);
  }

  const stats = fs.statSync(PROVINCE_GEOJSON_PATH);

  if (!stats.isDirectory()) {
    throw new Error(`Province geo source at "${PROVINCE_GEOJSON_PATH}" must be a directory.`);
  }

  return {
    kind: 'province-directory',
    sourcePath: PROVINCE_GEOJSON_PATH,
  };
}

function parseCsv(text) {
  const rows = [];
  let row = [];
  let field = '';
  let inQuotes = false;

  for (let index = 0; index < text.length; index += 1) {
    const character = text[index];

    if (inQuotes) {
      if (character === '"') {
        if (text[index + 1] === '"') {
          field += '"';
          index += 1;
        } else {
          inQuotes = false;
        }
      } else {
        field += character;
      }

      continue;
    }

    if (character === '"') {
      inQuotes = true;
      continue;
    }

    if (character === ',') {
      row.push(field);
      field = '';
      continue;
    }

    if (character === '\n') {
      row.push(field);
      rows.push(row);
      row = [];
      field = '';
      continue;
    }

    if (character !== '\r') {
      field += character;
    }
  }

  if (field || row.length) {
    row.push(field);
    rows.push(row);
  }

  if (!rows.length) {
    return [];
  }

  const headers = rows[0];

  return rows
    .slice(1)
    .filter((currentRow) => currentRow.some((value) => value !== ''))
    .map((currentRow) => {
      const record = {};

      headers.forEach((header, columnIndex) => {
        record[header] = currentRow[columnIndex] ?? '';
      });

      return record;
    });
}

function escapeRegExp(text) {
  return String(text).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function listDatasetPartFiles(extension) {
  if (!AUDIT_DATASET_DIR || !fs.existsSync(AUDIT_DATASET_DIR)) {
    return [];
  }

  const year = String(AUDIT_DATASET_YEAR || '').trim();
  if (!year) {
    return [];
  }

  const matcher = new RegExp(`^year-${escapeRegExp(year)}\\.part-(\\d{5})\\.${extension}$`, 'i');

  return fs
    .readdirSync(AUDIT_DATASET_DIR, { withFileTypes: true })
    .filter((entry) => entry.isFile())
    .map((entry) => {
      const match = entry.name.match(matcher);

      if (!match) {
        return null;
      }

      return {
        partNumber: Number.parseInt(match[1], 10),
        filePath: path.resolve(AUDIT_DATASET_DIR, entry.name),
      };
    })
    .filter(Boolean)
    .sort((left, right) => left.partNumber - right.partNumber)
    .map((entry) => entry.filePath);
}

function datasetSourcePath(format) {
  return path.resolve(AUDIT_DATASET_DIR, `year-${AUDIT_DATASET_YEAR}.part-*.${format}`);
}

function selectAuditSource() {
  if (!fs.existsSync(AUDIT_DATASET_DIR)) {
    throw new Error(`Dataset folder was not found at "${AUDIT_DATASET_DIR}".`);
  }

  const datasetJsonlFiles = listDatasetPartFiles('jsonl');
  if (datasetJsonlFiles.length) {
    return {
      sourceFormat: 'jsonl',
      sourcePath: datasetSourcePath('jsonl'),
      sourceFiles: datasetJsonlFiles,
    };
  }

  const datasetCsvFiles = listDatasetPartFiles('csv');
  if (datasetCsvFiles.length) {
    return {
      sourceFormat: 'csv',
      sourcePath: datasetSourcePath('csv'),
      sourceFiles: datasetCsvFiles,
    };
  }

  throw new Error(
    `Audit source was not found in dataset folder "${AUDIT_DATASET_DIR}" for year "${AUDIT_DATASET_YEAR}". Expected files like "year-${AUDIT_DATASET_YEAR}.part-00001.jsonl" or ".csv".`
  );
}

function forEachAuditRow(source, onRow) {
  if (source.sourceFormat === 'jsonl') {
    for (const filePath of source.sourceFiles) {
      forEachJsonlRow(filePath, onRow);
    }

    return;
  }

  for (const filePath of source.sourceFiles) {
    const rows = parseCsv(fs.readFileSync(filePath, 'utf8'));

    for (const row of rows) {
      onRow(row);
    }
  }
}

function forEachJsonlRow(filePath, onRow) {
  const fd = fs.openSync(filePath, 'r');
  const buffer = Buffer.allocUnsafe(JSONL_READ_BUFFER_SIZE);
  const decoder = new StringDecoder('utf8');
  let lineNumber = 0;
  let pending = '';

  const processLine = (rawLine) => {
    lineNumber += 1;

    let line = rawLine;
    if (line.endsWith('\r')) {
      line = line.slice(0, -1);
    }

    line = line.trim();
    if (!line) {
      return;
    }

    try {
      onRow(JSON.parse(line));
    } catch (error) {
      throw new Error(
        `Failed to parse JSONL at "${filePath}" line ${lineNumber}: ${error.message}`
      );
    }
  };

  try {
    while (true) {
      const bytesRead = fs.readSync(fd, buffer, 0, buffer.length, null);

      if (!bytesRead) {
        break;
      }

      const chunk = pending + decoder.write(buffer.subarray(0, bytesRead));
      const lines = chunk.split('\n');
      pending = lines.pop() || '';

      for (const rawLine of lines) {
        processLine(rawLine);
      }
    }

    const rest = pending + decoder.end();
    if (rest) {
      processLine(rest);
    }
  } finally {
    fs.closeSync(fd);
  }
}

function loadAuditRows() {
  return selectAuditSource();
}

function getTagValue(row, key) {
  if (
    row &&
    typeof row === 'object' &&
    row.tags &&
    typeof row.tags === 'object' &&
    key in row.tags
  ) {
    return row.tags[key];
  }

  return row ? row[`tags.${key}`] : undefined;
}

function inferSchemaVersion(row) {
  if (
    row.reason !== undefined ||
    getTagValue(row, 'isMencurigakan') !== undefined ||
    getTagValue(row, 'isPemborosan') !== undefined ||
    getTagValue(row, 'isInappropriateUse') !== undefined
  ) {
    return 'analyze_v2';
  }

  return 'analyze_legacy';
}

function normalizeAuditRow(row, index) {
  const schemaVersion = inferSchemaVersion(row);
  const flags = {
    isMencurigakan: parseBoolean(getTagValue(row, 'isMencurigakan')),
    isPemborosan: parseBoolean(getTagValue(row, 'isPemborosan')),
  };
  const budget = parseInteger(row.pagu);
  const severity = normalizeSeverity(
    getTagValue(row, 'isInappropriateUse') ?? getTagValue(row, 'isInappropriate')
  );
  const reason = sanitizeReason(row.reason ?? getTagValue(row, 'inappropriateReason'));
  const potentialWaste = parseAmount(row.potensiPemborosan, budget);
  const riskScore =
    (flags.isMencurigakan ? 1 : 0) + (flags.isPemborosan ? 1 : 0) + SEVERITY_SCORES[severity];
  const activeTagCount =
    (flags.isMencurigakan ? 1 : 0) +
    (flags.isPemborosan ? 1 : 0) +
    (severity === 'med' || severity === 'high' || severity === 'absurd' ? 1 : 0);
  const ownerName = cleanText(row.lembaga) || 'Tanpa lembaga';
  const ownerType = normalizeOwnerType(row.ownerType ?? row.owner_type, ownerName);
  const sourceId = cleanText(row.id) || `row-${index + 1}`;

  return {
    id: String(sourceId),
    source_id: parseInteger(row.id),
    schema_version: schemaVersion,
    owner_name: ownerName,
    owner_type: ownerType,
    satker: cleanText(row.satker),
    package_name: cleanText(row.paket) || `Paket ${sourceId}`,
    procurement_type: cleanText(row.jenisPengadaan),
    procurement_method: cleanText(row.metode),
    location_raw: cleanText(row.lokasi) || '',
    budget,
    selection_date: cleanText(row.pemilihanDate),
    funding_source: cleanText(row.sumberDana),
    is_umkm: parseBoolean(row.isUMKM) ? 1 : 0,
    within_country: parseBoolean(row.dalamNegeri) ? 1 : 0,
    volume: cleanText(row.volumePekerjaan),
    work_description: cleanText(row.uraianPekerjaan),
    specification: cleanText(row.spesifikasiPekerjaan),
    potential_waste: Number(potentialWaste.toFixed(2)),
    severity,
    reason,
    is_mencurigakan: flags.isMencurigakan === null ? null : flags.isMencurigakan ? 1 : 0,
    is_pemborosan: flags.isPemborosan === null ? null : flags.isPemborosan ? 1 : 0,
    risk_score: riskScore,
    active_tag_count: activeTagCount,
    is_priority: potentialWaste > 0 || riskScore >= 2 ? 1 : 0,
    is_flagged: activeTagCount > 0 ? 1 : 0,
    mapped_region_count: 0,
    inserted_order: index + 1,
  };
}

function splitLocationSegments(locationRaw) {
  return String(locationRaw || '')
    .split('|')
    .map((segment) => segment.trim())
    .filter(Boolean);
}

function parseLocationSegment(segment) {
  let value = String(segment || '')
    .replace(/\s+/g, ' ')
    .trim();

  if (!value || value === 'LAINNYA, Luar Indonesia') {
    return null;
  }

  value = value
    .replace(/\(Kab\)$/i, '(Kab.)')
    .replace(/\(Kab\)/i, '(Kab.)')
    .replace(/\(Kota\.\)/i, '(Kota)')
    .replace(/([^\s])\((Kab\.|Kab|Kota)\)/gi, '$1 ($2)');

  let match = value.match(/^(.+),\s+(.+)\s+\((Kab\.|Kota)\)$/i);
  if (match) {
    return {
      provinceName: match[1].trim(),
      regionName: match[2].trim(),
      regionType: match[3].toLowerCase().startsWith('kab') ? 'Kabupaten' : 'Kota',
    };
  }

  match = value.match(/^(.+),\s+Kabupaten\s+(.+)$/i);
  if (match) {
    return {
      provinceName: match[1].trim(),
      regionName: match[2].trim(),
      regionType: 'Kabupaten',
    };
  }

  match = value.match(/^(.+),\s+Kota\s+(.+)$/i);
  if (match) {
    return {
      provinceName: match[1].trim(),
      regionName: match[2].trim(),
      regionType: 'Kota',
    };
  }

  return null;
}

function createLegacyRegionRecord(feature, index) {
  const provinceName = normalizeProvinceDisplayName(feature.properties.NAME_1);
  const regionType = normalizeRegionType(feature.properties.TYPE_2);
  const regionName = normalizeRegionDisplayName(feature.properties.NAME_2, regionType);
  const gid = cleanText(feature.properties.GID_2);
  const regionKey = gid
    ? `gid-${slugify(gid)}`
    : `region-${slugify(`${provinceName}-${regionType}-${regionName}`)}`;

  return {
    region_key: regionKey,
    code: cleanText(feature.properties.CC_2) || cleanText(feature.properties.GID_2),
    province_name: provinceName,
    region_name: regionName,
    region_type: regionType,
    display_name: buildRegionDisplayName(regionName, regionType),
    feature_index: index,
    lookup_key: buildLocationLookupKey(provinceName, regionName, regionType),
  };
}

function normalizeDistrictRegionType(value) {
  const normalized = toComparableWords(value);
  return normalized.startsWith('kota') ? 'Kota' : 'Kabupaten';
}

function createDistrictRegionRecord(feature, index) {
  const provinceName = normalizeProvinceDisplayName(feature.properties.WADMPR);
  const regionType = normalizeDistrictRegionType(feature.properties.WADMKK);
  const regionName = normalizeRegionDisplayName(feature.properties.WADMKK, regionType);

  return {
    region_key: `region-${slugify(`${provinceName}-${regionType}-${regionName}`)}`,
    code: cleanText(feature.properties.OBJECTID),
    province_name: provinceName,
    region_name: regionName,
    region_type: regionType,
    display_name: buildRegionDisplayName(regionName, regionType),
    feature_index: index,
    lookup_key: buildLocationLookupKey(provinceName, regionName, regionType),
  };
}

function createProvinceRecord(feature, index) {
  const provinceName = normalizeProvinceDisplayName(feature.properties.WADMPR);

  return {
    province_key: `province-${slugify(provinceName)}`,
    code: cleanText(feature.properties.OBJECTID),
    province_name: provinceName,
    display_name: provinceName,
    feature_index: index,
    lookup_key: buildProvinceLookupKey(provinceName),
  };
}

function buildGeoFeature(record, geometry) {
  return {
    type: 'Feature',
    geometry: simplifyGeometry(geometry, DISTRICT_GEO_MAX_RING_POINTS),
    properties: {
      regionKey: record.region_key,
      code: record.code,
      provinceName: record.province_name,
      regionName: record.region_name,
      regionType: record.region_type,
      displayName: record.display_name,
    },
  };
}

function buildProvinceGeoFeature(record, geometry) {
  return {
    type: 'Feature',
    geometry: simplifyGeometry(geometry, PROVINCE_GEO_MAX_RING_POINTS),
    properties: {
      provinceKey: record.province_key,
      code: record.code,
      provinceName: record.province_name,
      displayName: record.display_name,
      regionType: 'Provinsi',
    },
  };
}

function buildLegacyGeoRegistry(filePath) {
  const rawGeoJson = parseGeoJsonFile(filePath);
  const lookup = new Map();
  const regions = [];
  const features = rawGeoJson.features.map((feature, index) => {
    assertGeoFeature(feature, filePath, index);

    const record = createLegacyRegionRecord(feature, index);

    lookup.set(record.lookup_key, record);
    regions.push(record);

    return buildGeoFeature(record, feature.geometry);
  });

  return {
    mode: 'legacy-file',
    sourcePath: filePath,
    sourceFiles: [filePath],
    usedSourceFiles: [filePath],
    skippedFiles: [],
    geoJson: {
      type: 'FeatureCollection',
      features,
    },
    regions,
    lookup,
  };
}

function buildLegacyGeometryIndex(filePath) {
  const rawGeoJson = parseGeoJsonFile(filePath);
  const exactGeometries = new Map();
  const regionOnlyGeometries = new Map();
  const ambiguousRegionOnlyKeys = new Set();

  rawGeoJson.features.forEach((feature, index) => {
    assertGeoFeature(feature, filePath, index);

    const record = createLegacyRegionRecord(feature, index);
    const regionOnlyKey = buildRegionOnlyLookupKey(record.region_name, record.region_type);

    exactGeometries.set(record.lookup_key, feature.geometry);

    if (ambiguousRegionOnlyKeys.has(regionOnlyKey)) {
      return;
    }

    if (regionOnlyGeometries.has(regionOnlyKey)) {
      regionOnlyGeometries.delete(regionOnlyKey);
      ambiguousRegionOnlyKeys.add(regionOnlyKey);
      return;
    }

    regionOnlyGeometries.set(regionOnlyKey, feature.geometry);
  });

  return {
    sourcePath: filePath,
    exactGeometries,
    regionOnlyGeometries,
  };
}

function selectDistrictGeometrySet(record, payload, legacyGeometryIndex) {
  const rawGeometries = payload.features.map((feature) => feature.geometry);

  if (payload.features.length > 1 || !legacyGeometryIndex) {
    return {
      source: 'district-raw',
      geometries: rawGeometries,
    };
  }

  const exactGeometry = legacyGeometryIndex.exactGeometries.get(record.lookup_key);

  if (exactGeometry) {
    return {
      source: 'legacy-exact',
      geometries: [exactGeometry],
    };
  }

  const regionOnlyGeometry = legacyGeometryIndex.regionOnlyGeometries.get(
    buildRegionOnlyLookupKey(record.region_name, record.region_type)
  );

  if (regionOnlyGeometry) {
    return {
      source: 'legacy-region-only',
      geometries: [regionOnlyGeometry],
    };
  }

  return {
    source: 'district-raw',
    geometries: rawGeometries,
  };
}

function buildDistrictDirectoryGeoRegistry(directoryPath) {
  const sourceFiles = listGeoDirectoryFiles(directoryPath);
  const legacyFallbackGeoPath = resolveLegacyFallbackGeoPath(directoryPath);
  const legacyGeometryIndex = legacyFallbackGeoPath
    ? buildLegacyGeometryIndex(legacyFallbackGeoPath)
    : null;
  const usedSourceFiles = [];
  const skippedFiles = [];
  const lookup = new Map();
  const regions = [];
  const features = [];
  const geometrySourceCounts = {
    'district-raw': 0,
    'legacy-exact': 0,
    'legacy-region-only': 0,
  };

  for (const filePath of sourceFiles) {
    const fileName = path.basename(filePath);

    if (SKIPPED_GEO_DIRECTORY_FILES.has(fileName.toLowerCase())) {
      skippedFiles.push({
        fileName,
        reason: 'reserved-file',
      });
      continue;
    }

    const payload = parseGeoJsonFile(filePath);

    if (!payload.features.length) {
      skippedFiles.push({
        fileName,
        reason: 'empty-feature-collection',
      });
      continue;
    }

    assertGeoFeature(payload.features[0], filePath, 0);

    const record = createDistrictRegionRecord(payload.features[0], features.length);

    if (lookup.has(record.lookup_key)) {
      throw new Error(
        `Duplicate geo region lookup key "${record.lookup_key}" found in "${filePath}".`
      );
    }

    lookup.set(record.lookup_key, record);
    regions.push(record);
    usedSourceFiles.push(filePath);

    payload.features.forEach((feature, index) => {
      assertGeoFeature(feature, filePath, index);
    });

    const geometrySet = selectDistrictGeometrySet(record, payload, legacyGeometryIndex);
    geometrySourceCounts[geometrySet.source] += 1;

    geometrySet.geometries.forEach((geometry) => {
      features.push(buildGeoFeature(record, geometry));
    });
  }

  return {
    mode: 'district-directory',
    sourcePath: directoryPath,
    sourceFiles,
    usedSourceFiles,
    skippedFiles,
    legacyFallbackGeoPath,
    geometrySourceCounts,
    geoJson: {
      type: 'FeatureCollection',
      features,
    },
    regions,
    lookup,
  };
}

function loadGeoRegistry() {
  const geoSource = loadGeoSource();

  if (geoSource.kind === 'legacy-file') {
    return buildLegacyGeoRegistry(geoSource.sourcePath);
  }

  return buildDistrictDirectoryGeoRegistry(geoSource.sourcePath);
}

function buildProvinceGeoRegistry(directoryPath) {
  const sourceFiles = listGeoDirectoryFiles(directoryPath);
  const usedSourceFiles = [];
  const skippedFiles = [];
  const lookup = new Map();
  const provinces = [];
  const features = [];

  for (const filePath of sourceFiles) {
    const fileName = path.basename(filePath);
    const payload = parseGeoJsonFile(filePath);

    if (!payload.features.length) {
      skippedFiles.push({
        fileName,
        reason: 'empty-feature-collection',
      });
      continue;
    }

    assertGeoFeature(payload.features[0], filePath, 0);

    const record = createProvinceRecord(payload.features[0], features.length);

    if (lookup.has(record.lookup_key)) {
      throw new Error(
        `Duplicate province lookup key "${record.lookup_key}" found in "${filePath}".`
      );
    }

    lookup.set(record.lookup_key, record);
    provinces.push(record);
    usedSourceFiles.push(filePath);

    payload.features.forEach((feature, index) => {
      assertGeoFeature(feature, filePath, index);
      features.push(buildProvinceGeoFeature(record, feature.geometry));
    });
  }

  return {
    mode: 'province-directory',
    sourcePath: directoryPath,
    sourceFiles,
    usedSourceFiles,
    skippedFiles,
    geoJson: {
      type: 'FeatureCollection',
      features,
    },
    provinces,
    lookup,
  };
}

function loadProvinceGeoRegistry() {
  const geoSource = loadProvinceGeoSource();
  return buildProvinceGeoRegistry(geoSource.sourcePath);
}

function resolveRegionKeys(locationRaw, lookup) {
  const resolvedKeys = new Set();

  for (const segment of splitLocationSegments(locationRaw)) {
    const parsed = parseLocationSegment(segment);

    if (!parsed) {
      continue;
    }

    const lookupKey = buildLocationLookupKey(
      parsed.provinceName,
      parsed.regionName,
      parsed.regionType
    );
    const region = lookup.get(lookupKey);

    if (region) {
      resolvedKeys.add(region.region_key);
    }
  }

  return [...resolvedKeys];
}

function resolveProvinceKeys(locationRaw, provinceLookup, regionLookup) {
  const resolvedKeys = new Set();

  for (const segment of splitLocationSegments(locationRaw)) {
    const parsed = parseLocationSegment(segment);

    if (!parsed) {
      continue;
    }

    const province = provinceLookup.get(buildProvinceLookupKey(parsed.provinceName));

    if (province) {
      resolvedKeys.add(province.province_key);
      continue;
    }

    if (!regionLookup) {
      continue;
    }

    const region = regionLookup.get(
      buildLocationLookupKey(parsed.provinceName, parsed.regionName, parsed.regionType)
    );

    if (!region) {
      continue;
    }

    const provinceFromRegion = provinceLookup.get(buildProvinceLookupKey(region.province_name));

    if (provinceFromRegion) {
      resolvedKeys.add(provinceFromRegion.province_key);
    }
  }

  return [...resolvedKeys];
}

function createLocationResolver(lookup, provinceLookup, regionLookup) {
  const cache = new Map();

  return (locationRaw) => {
    const cacheKey = String(locationRaw || '');
    const cached = cache.get(cacheKey);

    if (cached) {
      return cached;
    }

    const regionKeys = resolveRegionKeys(cacheKey, lookup);
    const provinceKeys = resolveProvinceKeys(cacheKey, provinceLookup, regionLookup);
    const resolved = {
      regionKeys,
      provinceKeys,
    };

    if (cache.size >= LOCATION_CACHE_MAX_SIZE) {
      cache.clear();
    }

    cache.set(cacheKey, resolved);
    return resolved;
  };
}

function createRelationBulkInserter(db, tableName, leftColumn, rightColumn) {
  const statementByChunkSize = new Map();

  return (pairs) => {
    if (!pairs.length) {
      return;
    }

    let offset = 0;

    while (offset < pairs.length) {
      const chunkSize = Math.min(RELATION_INSERT_BATCH_SIZE, pairs.length - offset);
      let statement = statementByChunkSize.get(chunkSize);

      if (!statement) {
        const placeholders = new Array(chunkSize).fill('(?, ?)').join(', ');
        statement = db.prepare(`
          INSERT INTO ${tableName} (${leftColumn}, ${rightColumn})
          VALUES ${placeholders}
        `);
        statementByChunkSize.set(chunkSize, statement);
      }

      const params = [];

      for (let index = offset; index < offset + chunkSize; index += 1) {
        params.push(pairs[index].left, pairs[index].right);
      }

      statement.run(...params);
      offset += chunkSize;
    }

    pairs.length = 0;
  };
}

function createSchema(db) {
  db.exec(`
    DROP TABLE IF EXISTS owner_metrics;
    DROP TABLE IF EXISTS province_metrics;
    DROP TABLE IF EXISTS region_metrics;
    DROP TABLE IF EXISTS package_provinces;
    DROP TABLE IF EXISTS package_regions;
    DROP TABLE IF EXISTS packages;
    DROP TABLE IF EXISTS provinces;
    DROP TABLE IF EXISTS regions;
    DROP TABLE IF EXISTS assets;

    CREATE TABLE assets (
      key TEXT PRIMARY KEY,
      json TEXT NOT NULL
    );

    CREATE TABLE regions (
      region_key TEXT PRIMARY KEY,
      code TEXT,
      province_name TEXT NOT NULL,
      region_name TEXT NOT NULL,
      region_type TEXT NOT NULL,
      display_name TEXT NOT NULL,
      feature_index INTEGER NOT NULL
    );

    CREATE TABLE provinces (
      province_key TEXT PRIMARY KEY,
      code TEXT,
      province_name TEXT NOT NULL,
      display_name TEXT NOT NULL,
      feature_index INTEGER NOT NULL
    );

    CREATE TABLE packages (
      id TEXT PRIMARY KEY,
      source_id INTEGER,
      schema_version TEXT NOT NULL,
      owner_name TEXT NOT NULL,
      owner_type TEXT NOT NULL,
      satker TEXT,
      package_name TEXT NOT NULL,
      procurement_type TEXT,
      procurement_method TEXT,
      location_raw TEXT NOT NULL,
      budget INTEGER,
      selection_date TEXT,
      funding_source TEXT,
      is_umkm INTEGER NOT NULL,
      within_country INTEGER NOT NULL,
      volume TEXT,
      work_description TEXT,
      specification TEXT,
      potential_waste REAL NOT NULL,
      severity TEXT NOT NULL,
      reason TEXT,
      is_mencurigakan INTEGER,
      is_pemborosan INTEGER,
      risk_score INTEGER NOT NULL,
      active_tag_count INTEGER NOT NULL,
      is_priority INTEGER NOT NULL,
      is_flagged INTEGER NOT NULL,
      mapped_region_count INTEGER NOT NULL,
      inserted_order INTEGER NOT NULL
    );

    CREATE TABLE package_regions (
      package_id TEXT NOT NULL,
      region_key TEXT NOT NULL,
      PRIMARY KEY (package_id, region_key),
      FOREIGN KEY (package_id) REFERENCES packages(id) ON DELETE CASCADE,
      FOREIGN KEY (region_key) REFERENCES regions(region_key) ON DELETE CASCADE
    );

    CREATE TABLE package_provinces (
      package_id TEXT NOT NULL,
      province_key TEXT NOT NULL,
      PRIMARY KEY (package_id, province_key),
      FOREIGN KEY (package_id) REFERENCES packages(id) ON DELETE CASCADE,
      FOREIGN KEY (province_key) REFERENCES provinces(province_key) ON DELETE CASCADE
    );
  `);

  db.exec(REGION_METRICS_TABLE_SQL);
  db.exec(PROVINCE_METRICS_TABLE_SQL);
  db.exec(OWNER_METRICS_TABLE_SQL);
}

function createIndexes(db) {
  db.exec(`
    CREATE INDEX idx_packages_priority_order ON packages(is_priority, potential_waste DESC, risk_score DESC);
    CREATE INDEX idx_packages_owner_type ON packages(owner_type);
    CREATE INDEX idx_packages_owner_lookup ON packages(owner_type, owner_name);
    CREATE INDEX idx_packages_severity ON packages(severity);
    CREATE INDEX idx_package_regions_region ON package_regions(region_key, package_id);
    CREATE INDEX idx_package_provinces_province ON package_provinces(province_key, package_id);
  `);
}

function materializeRegionMetrics(db) {
  db.exec(`
    INSERT INTO region_metrics (
      region_key,
      total_packages,
      total_priority_packages,
      total_flagged_packages,
      total_potential_waste,
      total_budget,
      avg_risk_score,
      max_risk_score,
      central_packages,
      provincial_packages,
      local_packages,
      other_packages,
      central_priority_packages,
      provincial_priority_packages,
      local_priority_packages,
      other_priority_packages,
      central_potential_waste,
      provincial_potential_waste,
      local_potential_waste,
      other_potential_waste,
      central_budget,
      provincial_budget,
      local_budget,
      other_budget,
      med_severity_packages,
      high_severity_packages,
      absurd_severity_packages
    )
    SELECT
      regions.region_key,
      COUNT(package_regions.package_id) AS total_packages,
      COALESCE(SUM(packages.is_priority), 0) AS total_priority_packages,
      COALESCE(SUM(packages.is_flagged), 0) AS total_flagged_packages,
      COALESCE(ROUND(SUM(packages.potential_waste), 2), 0) AS total_potential_waste,
      COALESCE(SUM(COALESCE(packages.budget, 0)), 0) AS total_budget,
      COALESCE(AVG(packages.risk_score), 0) AS avg_risk_score,
      COALESCE(MAX(packages.risk_score), 0) AS max_risk_score,
      COALESCE(SUM(CASE WHEN packages.owner_type = 'central' THEN 1 ELSE 0 END), 0) AS central_packages,
      COALESCE(SUM(CASE WHEN packages.owner_type = 'provinsi' THEN 1 ELSE 0 END), 0) AS provincial_packages,
      COALESCE(SUM(CASE WHEN packages.owner_type = 'kabkota' THEN 1 ELSE 0 END), 0) AS local_packages,
      COALESCE(SUM(CASE WHEN packages.owner_type = 'other' THEN 1 ELSE 0 END), 0) AS other_packages,
      COALESCE(SUM(CASE WHEN packages.owner_type = 'central' THEN packages.is_priority ELSE 0 END), 0) AS central_priority_packages,
      COALESCE(SUM(CASE WHEN packages.owner_type = 'provinsi' THEN packages.is_priority ELSE 0 END), 0) AS provincial_priority_packages,
      COALESCE(SUM(CASE WHEN packages.owner_type = 'kabkota' THEN packages.is_priority ELSE 0 END), 0) AS local_priority_packages,
      COALESCE(SUM(CASE WHEN packages.owner_type = 'other' THEN packages.is_priority ELSE 0 END), 0) AS other_priority_packages,
      COALESCE(
        ROUND(SUM(CASE WHEN packages.owner_type = 'central' THEN packages.potential_waste ELSE 0 END), 2),
        0
      ) AS central_potential_waste,
      COALESCE(
        ROUND(SUM(CASE WHEN packages.owner_type = 'provinsi' THEN packages.potential_waste ELSE 0 END), 2),
        0
      ) AS provincial_potential_waste,
      COALESCE(
        ROUND(SUM(CASE WHEN packages.owner_type = 'kabkota' THEN packages.potential_waste ELSE 0 END), 2),
        0
      ) AS local_potential_waste,
      COALESCE(
        ROUND(SUM(CASE WHEN packages.owner_type = 'other' THEN packages.potential_waste ELSE 0 END), 2),
        0
      ) AS other_potential_waste,
      COALESCE(
        SUM(CASE WHEN packages.owner_type = 'central' THEN COALESCE(packages.budget, 0) ELSE 0 END),
        0
      ) AS central_budget,
      COALESCE(
        SUM(CASE WHEN packages.owner_type = 'provinsi' THEN COALESCE(packages.budget, 0) ELSE 0 END),
        0
      ) AS provincial_budget,
      COALESCE(
        SUM(CASE WHEN packages.owner_type = 'kabkota' THEN COALESCE(packages.budget, 0) ELSE 0 END),
        0
      ) AS local_budget,
      COALESCE(
        SUM(CASE WHEN packages.owner_type = 'other' THEN COALESCE(packages.budget, 0) ELSE 0 END),
        0
      ) AS other_budget,
      COALESCE(SUM(CASE WHEN packages.severity = 'med' THEN 1 ELSE 0 END), 0) AS med_severity_packages,
      COALESCE(SUM(CASE WHEN packages.severity = 'high' THEN 1 ELSE 0 END), 0) AS high_severity_packages,
      COALESCE(SUM(CASE WHEN packages.severity = 'absurd' THEN 1 ELSE 0 END), 0) AS absurd_severity_packages
    FROM regions
    LEFT JOIN package_regions ON package_regions.region_key = regions.region_key
    LEFT JOIN packages ON packages.id = package_regions.package_id
    GROUP BY regions.region_key
  `);
}

function materializeProvinceMetrics(db) {
  db.exec(`
    INSERT INTO province_metrics (
      province_key,
      total_packages,
      total_priority_packages,
      total_flagged_packages,
      total_potential_waste,
      total_budget,
      avg_risk_score,
      max_risk_score,
      med_severity_packages,
      high_severity_packages,
      absurd_severity_packages
    )
    SELECT
      provinces.province_key,
      COALESCE(SUM(CASE WHEN packages.owner_type = 'provinsi' THEN 1 ELSE 0 END), 0) AS total_packages,
      COALESCE(SUM(CASE WHEN packages.owner_type = 'provinsi' THEN packages.is_priority ELSE 0 END), 0) AS total_priority_packages,
      COALESCE(SUM(CASE WHEN packages.owner_type = 'provinsi' THEN packages.is_flagged ELSE 0 END), 0) AS total_flagged_packages,
      COALESCE(
        ROUND(SUM(CASE WHEN packages.owner_type = 'provinsi' THEN packages.potential_waste ELSE 0 END), 2),
        0
      ) AS total_potential_waste,
      COALESCE(
        SUM(CASE WHEN packages.owner_type = 'provinsi' THEN COALESCE(packages.budget, 0) ELSE 0 END),
        0
      ) AS total_budget,
      COALESCE(AVG(CASE WHEN packages.owner_type = 'provinsi' THEN packages.risk_score END), 0) AS avg_risk_score,
      COALESCE(MAX(CASE WHEN packages.owner_type = 'provinsi' THEN packages.risk_score END), 0) AS max_risk_score,
      COALESCE(SUM(CASE WHEN packages.owner_type = 'provinsi' AND packages.severity = 'med' THEN 1 ELSE 0 END), 0) AS med_severity_packages,
      COALESCE(SUM(CASE WHEN packages.owner_type = 'provinsi' AND packages.severity = 'high' THEN 1 ELSE 0 END), 0) AS high_severity_packages,
      COALESCE(SUM(CASE WHEN packages.owner_type = 'provinsi' AND packages.severity = 'absurd' THEN 1 ELSE 0 END), 0) AS absurd_severity_packages
    FROM provinces
    LEFT JOIN package_provinces ON package_provinces.province_key = provinces.province_key
    LEFT JOIN packages ON packages.id = package_provinces.package_id
    GROUP BY provinces.province_key
  `);
}

function materializeOwnerMetrics(db) {
  db.exec(`
    INSERT INTO owner_metrics (
      owner_type,
      owner_name,
      total_packages,
      total_priority_packages,
      total_flagged_packages,
      total_potential_waste,
      total_budget,
      med_severity_packages,
      high_severity_packages,
      absurd_severity_packages
    )
    SELECT
      packages.owner_type,
      packages.owner_name,
      COUNT(*) AS total_packages,
      COALESCE(SUM(packages.is_priority), 0) AS total_priority_packages,
      COALESCE(SUM(packages.is_flagged), 0) AS total_flagged_packages,
      COALESCE(ROUND(SUM(packages.potential_waste), 2), 0) AS total_potential_waste,
      COALESCE(SUM(COALESCE(packages.budget, 0)), 0) AS total_budget,
      COALESCE(SUM(CASE WHEN packages.severity = 'med' THEN 1 ELSE 0 END), 0) AS med_severity_packages,
      COALESCE(SUM(CASE WHEN packages.severity = 'high' THEN 1 ELSE 0 END), 0) AS high_severity_packages,
      COALESCE(SUM(CASE WHEN packages.severity = 'absurd' THEN 1 ELSE 0 END), 0) AS absurd_severity_packages
    FROM packages
    GROUP BY packages.owner_type, packages.owner_name
  `);
}

function listTableColumns(db, tableName) {
  return new Set(
    db
      .prepare(`PRAGMA table_info(${tableName})`)
      .all()
      .map((row) => row.name)
  );
}

function ensureRegionMetricsCompatibility(db) {
  const hasRegionMetricsTable = Boolean(
    db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'region_metrics'").get()
  );
  const columnNames = hasRegionMetricsTable ? listTableColumns(db, 'region_metrics') : new Set();
  const needsRebuild =
    !hasRegionMetricsTable ||
    REQUIRED_REGION_METRICS_COLUMNS.some((columnName) => !columnNames.has(columnName));

  if (!needsRebuild) {
    return false;
  }

  db.transaction(() => {
    db.exec('DROP TABLE IF EXISTS region_metrics;');
    db.exec(REGION_METRICS_TABLE_SQL);
    materializeRegionMetrics(db);
  })();

  return true;
}

function ensureOwnerMetricsCompatibility(db) {
  const hasOwnerMetricsTable = Boolean(
    db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'owner_metrics'").get()
  );
  const columnNames = hasOwnerMetricsTable ? listTableColumns(db, 'owner_metrics') : new Set();
  const needsRebuild =
    !hasOwnerMetricsTable ||
    REQUIRED_OWNER_METRICS_COLUMNS.some((columnName) => !columnNames.has(columnName));

  if (!needsRebuild) {
    return false;
  }

  db.transaction(() => {
    db.exec('DROP TABLE IF EXISTS owner_metrics;');
    db.exec(OWNER_METRICS_TABLE_SQL);
    materializeOwnerMetrics(db);
  })();

  return true;
}

function seedDatabase(db) {
  const auditSource = loadAuditRows();
  const { sourceFormat, sourcePath, sourceFiles } = auditSource;
  const {
    mode: geoSourceMode,
    sourcePath: geoSourcePath,
    sourceFiles: geoSourceFiles,
    usedSourceFiles: usedGeoSourceFiles,
    skippedFiles: skippedGeoFiles,
    legacyFallbackGeoPath,
    geometrySourceCounts,
    geoJson,
    regions,
    lookup,
  } = loadGeoRegistry();
  const {
    sourcePath: provinceGeoSourcePath,
    sourceFiles: provinceGeoSourceFiles,
    usedSourceFiles: usedProvinceGeoSourceFiles,
    skippedFiles: skippedProvinceGeoFiles,
    geoJson: provinceGeoJson,
    provinces,
    lookup: provinceLookup,
  } = loadProvinceGeoRegistry();
  const resolveLocation = createLocationResolver(lookup, provinceLookup, lookup);
  let packageCount = 0;
  let unmappedPackageCount = 0;
  let multiLocationPackageCount = 0;

  const insertAsset = db.prepare('INSERT INTO assets (key, json) VALUES (?, ?)');
  const insertRegion = db.prepare(`
    INSERT INTO regions (
      region_key, code, province_name, region_name, region_type, display_name, feature_index
    ) VALUES (
      @region_key, @code, @province_name, @region_name, @region_type, @display_name, @feature_index
    )
  `);
  const insertProvince = db.prepare(`
    INSERT INTO provinces (
      province_key, code, province_name, display_name, feature_index
    ) VALUES (
      @province_key, @code, @province_name, @display_name, @feature_index
    )
  `);
  const insertPackage = db.prepare(`
    INSERT INTO packages (
      id, source_id, schema_version, owner_name, owner_type, satker, package_name,
      procurement_type, procurement_method, location_raw, budget, selection_date,
      funding_source, is_umkm, within_country, volume, work_description, specification,
      potential_waste, severity, reason, is_mencurigakan, is_pemborosan, risk_score,
      active_tag_count, is_priority, is_flagged, mapped_region_count, inserted_order
    ) VALUES (
      @id, @source_id, @schema_version, @owner_name, @owner_type, @satker, @package_name,
      @procurement_type, @procurement_method, @location_raw, @budget, @selection_date,
      @funding_source, @is_umkm, @within_country, @volume, @work_description, @specification,
      @potential_waste, @severity, @reason, @is_mencurigakan, @is_pemborosan, @risk_score,
      @active_tag_count, @is_priority, @is_flagged, @mapped_region_count, @inserted_order
    )
  `);
  const flushPackageRegions = createRelationBulkInserter(
    db,
    'package_regions',
    'package_id',
    'region_key'
  );
  const flushPackageProvinces = createRelationBulkInserter(
    db,
    'package_provinces',
    'package_id',
    'province_key'
  );
  const pendingPackageRegions = [];
  const pendingPackageProvinces = [];

  db.transaction(() => {
    insertAsset.run('audit_geojson', JSON.stringify(geoJson));
    insertAsset.run('audit_province_geojson', JSON.stringify(provinceGeoJson));

    for (const region of regions) {
      insertRegion.run(region);
    }

    for (const province of provinces) {
      insertProvince.run(province);
    }

    forEachAuditRow(auditSource, (row) => {
      const record = normalizeAuditRow(row, packageCount);
      const { regionKeys, provinceKeys } = resolveLocation(record.location_raw);

      packageCount += 1;
      record.mapped_region_count = regionKeys.length;

      if (!regionKeys.length) {
        unmappedPackageCount += 1;
      } else if (regionKeys.length > 1) {
        multiLocationPackageCount += 1;
      }

      insertPackage.run(record);

      for (const regionKey of regionKeys) {
        pendingPackageRegions.push({
          left: record.id,
          right: regionKey,
        });
      }

      for (const provinceKey of provinceKeys) {
        pendingPackageProvinces.push({
          left: record.id,
          right: provinceKey,
        });
      }

      if (pendingPackageRegions.length >= RELATION_INSERT_BATCH_SIZE) {
        flushPackageRegions(pendingPackageRegions);
      }

      if (pendingPackageProvinces.length >= RELATION_INSERT_BATCH_SIZE) {
        flushPackageProvinces(pendingPackageProvinces);
      }
    });

    flushPackageRegions(pendingPackageRegions);
    flushPackageProvinces(pendingPackageProvinces);

    insertAsset.run(
      'audit_metadata',
      JSON.stringify({
        importedAt: new Date().toISOString(),
        sourceFormat,
        sourcePath,
        sourceFiles,
        totalSourceFiles: sourceFiles.length,
        geoSourceMode,
        geoSourcePath,
        geoSourceFiles,
        totalGeoSourceFiles: geoSourceFiles.length,
        usedGeoSourceFiles,
        totalGeoUsedSourceFiles: usedGeoSourceFiles.length,
        skippedGeoFiles,
        provinceGeoSourcePath,
        provinceGeoSourceFiles,
        totalProvinceGeoSourceFiles: provinceGeoSourceFiles.length,
        usedProvinceGeoSourceFiles,
        totalProvinceGeoUsedSourceFiles: usedProvinceGeoSourceFiles.length,
        skippedProvinceGeoFiles,
        legacyFallbackGeoPath,
        geometrySourceCounts,
        totalRows: packageCount,
        totalRegions: regions.length,
        totalGeoFeatures: geoJson.features.length,
        totalProvinces: provinces.length,
        totalProvinceGeoFeatures: provinceGeoJson.features.length,
        unmappedPackageCount,
        multiLocationPackageCount,
      })
    );

    materializeRegionMetrics(db);
    materializeProvinceMetrics(db);
    materializeOwnerMetrics(db);
    createIndexes(db);
  })();

  return {
    assetCount: 3,
    regionCount: regions.length,
    provinceCount: provinces.length,
    packageCount,
    mappedPackageCount: packageCount - unmappedPackageCount,
    unmappedPackageCount,
    multiLocationPackageCount,
    sourceFormat,
    sourcePath,
    sourceFileCount: sourceFiles.length,
    geoSourceMode,
    geoSourcePath,
    geoFeatureCount: geoJson.features.length,
    provinceGeoSourcePath,
    provinceGeoFeatureCount: provinceGeoJson.features.length,
    geometrySourceCounts,
  };
}

export {
  createSchema,
  ensureOwnerMetricsCompatibility,
  ensureRegionMetricsCompatibility,
  seedDatabase,
};
