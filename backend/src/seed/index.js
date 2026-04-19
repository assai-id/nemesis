/**
 * Main seed module — schema creation, data parsing, metrics, and seeding orchestration.
 * Re-exports the same public API as the original seed.js.
 */

const fs = require("fs");
const { StringDecoder } = require("string_decoder");
const { AUDIT_DATASET_DIR, AUDIT_DATASET_YEAR } = require("../config");
const {
  SEVERITY_SCORES,
  cleanText,
  slugify,
  parseBoolean,
  parseInteger,
  parseAmount,
  normalizeSeverity,
  sanitizeReason,
  normalizeOwnerType,
  splitLocationSegments,
} = require("./normalize");
const {
  loadGeoRegistry,
  loadProvinceGeoRegistry,
  createLocationResolver,
} = require("./geo");

const RELATION_INSERT_BATCH_SIZE = 2000;

const REGION_OWNER_METRIC_COLUMNS = [
  {
    ownerType: "central",
    countColumn: "central_packages",
    priorityColumn: "central_priority_packages",
    wasteColumn: "central_potential_waste",
    budgetColumn: "central_budget",
  },
  {
    ownerType: "provinsi",
    countColumn: "provincial_packages",
    priorityColumn: "provincial_priority_packages",
    wasteColumn: "provincial_potential_waste",
    budgetColumn: "provincial_budget",
  },
  {
    ownerType: "kabkota",
    countColumn: "local_packages",
    priorityColumn: "local_priority_packages",
    wasteColumn: "local_potential_waste",
    budgetColumn: "local_budget",
  },
  {
    ownerType: "other",
    countColumn: "other_packages",
    priorityColumn: "other_priority_packages",
    wasteColumn: "other_potential_waste",
    budgetColumn: "other_budget",
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
  "owner_type",
  "owner_name",
  "total_packages",
  "total_priority_packages",
  "total_flagged_packages",
  "total_potential_waste",
  "total_budget",
  "med_severity_packages",
  "high_severity_packages",
  "absurd_severity_packages",
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

const JSONL_READ_BUFFER_SIZE = 256 * 1024;

// --- Data source loading ---

function escapeRegExp(text) {
  return String(text).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function listDatasetPartFiles(extension) {
  if (!AUDIT_DATASET_DIR || !fs.existsSync(AUDIT_DATASET_DIR)) {
    return [];
  }

  const year = String(AUDIT_DATASET_YEAR || "").trim();
  if (!year) {
    return [];
  }

  const matcher = new RegExp(`^year-${escapeRegExp(year)}\\.part-(\\d{5})\\.${extension}$`, "i");

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
        filePath: require("path").resolve(AUDIT_DATASET_DIR, entry.name),
      };
    })
    .filter(Boolean)
    .sort((left, right) => left.partNumber - right.partNumber)
    .map((entry) => entry.filePath);
}

function datasetSourcePath(format) {
  return require("path").resolve(AUDIT_DATASET_DIR, `year-${AUDIT_DATASET_YEAR}.part-*.${format}`);
}

function selectAuditSource() {
  if (!fs.existsSync(AUDIT_DATASET_DIR)) {
    throw new Error(`Dataset folder was not found at "${AUDIT_DATASET_DIR}".`);
  }

  const datasetJsonlFiles = listDatasetPartFiles("jsonl");
  if (datasetJsonlFiles.length) {
    return {
      sourceFormat: "jsonl",
      sourcePath: datasetSourcePath("jsonl"),
      sourceFiles: datasetJsonlFiles,
    };
  }

  const datasetCsvFiles = listDatasetPartFiles("csv");
  if (datasetCsvFiles.length) {
    return {
      sourceFormat: "csv",
      sourcePath: datasetSourcePath("csv"),
      sourceFiles: datasetCsvFiles,
    };
  }

  throw new Error(
    `Audit source was not found in dataset folder "${AUDIT_DATASET_DIR}" for year "${AUDIT_DATASET_YEAR}". Expected files like "year-${AUDIT_DATASET_YEAR}.part-00001.jsonl" or ".csv".`
  );
}

function parseCsv(text) {
  const rows = [];
  let row = [];
  let field = "";
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
    } else if (character === '"') {
      inQuotes = true;
    } else if (character === ",") {
      row.push(field);
      field = "";
    } else if (character === "\r" && text[index + 1] === "\n") {
      row.push(field);
      field = "";
      rows.push(row);
      row = [];
      index += 1;
    } else if (character === "\n") {
      row.push(field);
      field = "";
      rows.push(row);
      row = [];
    } else {
      field += character;
    }
  }

  if (field || row.length) {
    row.push(field);
    rows.push(row);
  }

  return rows;
}

function forEachAuditRow(source, onRow) {
  if (source.sourceFormat === "jsonl") {
    for (const filePath of source.sourceFiles) {
      forEachJsonlRow(filePath, onRow);
    }

    return;
  }

  for (const filePath of source.sourceFiles) {
    const rows = parseCsv(fs.readFileSync(filePath, "utf8"));

    for (const row of rows) {
      onRow(row);
    }
  }
}

function forEachJsonlRow(filePath, onRow) {
  const fd = fs.openSync(filePath, "r");
  const buffer = Buffer.allocUnsafe(JSONL_READ_BUFFER_SIZE);
  const decoder = new StringDecoder("utf8");
  let lineNumber = 0;
  let pending = "";

  const processLine = (rawLine) => {
    lineNumber += 1;

    let line = rawLine;
    if (line.endsWith("\r")) {
      line = line.slice(0, -1);
    }

    line = line.trim();
    if (!line) {
      return;
    }

    try {
      onRow(JSON.parse(line));
    } catch (error) {
      throw new Error(`Failed to parse JSONL at "${filePath}" line ${lineNumber}: ${error.message}`);
    }
  };

  try {
    while (true) {
      const bytesRead = fs.readSync(fd, buffer, 0, buffer.length, null);

      if (!bytesRead) {
        break;
      }

      const chunk = pending + decoder.write(buffer.subarray(0, bytesRead));
      const lines = chunk.split("\n");
      pending = lines.pop() || "";

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
  if (row && typeof row === "object" && row.tags && typeof row.tags === "object" && key in row.tags) {
    return row.tags[key];
  }

  return row ? row[`tags.${key}`] : undefined;
}

function inferSchemaVersion(row) {
  if (
    row.reason !== undefined ||
    getTagValue(row, "isMencurigakan") !== undefined ||
    getTagValue(row, "isPemborosan") !== undefined ||
    getTagValue(row, "isInappropriateUse") !== undefined
  ) {
    return "analyze_v2";
  }

  return "analyze_legacy";
}

function normalizeAuditRow(row, index) {
  const schemaVersion = inferSchemaVersion(row);
  const flags = {
    isMencurigakan: parseBoolean(getTagValue(row, "isMencurigakan")),
    isPemborosan: parseBoolean(getTagValue(row, "isPemborosan")),
  };
  const budget = parseInteger(row.pagu);
  const severity = normalizeSeverity(
    getTagValue(row, "isInappropriateUse") ?? getTagValue(row, "isInappropriate")
  );
  const reason = sanitizeReason(row.reason ?? getTagValue(row, "inappropriateReason"));
  const potentialWaste = parseAmount(row.potensiPemborosan, budget);
  const riskScore =
    (flags.isMencurigakan ? 1 : 0) +
    (flags.isPemborosan ? 1 : 0) +
    SEVERITY_SCORES[severity];
  const activeTagCount =
    (flags.isMencurigakan ? 1 : 0) +
    (flags.isPemborosan ? 1 : 0) +
    (severity === "med" || severity === "high" || severity === "absurd" ? 1 : 0);
  const ownerName = cleanText(row.lembaga) || "Tanpa lembaga";
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
    location_raw: cleanText(row.lokasi) || "",
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

// --- Bulk insert helper ---

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
        const placeholders = new Array(chunkSize).fill("(?, ?)").join(", ");
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

// --- Schema ---

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

// --- Metrics materialization ---

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

// --- Compatibility ---

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
  const columnNames = hasRegionMetricsTable ? listTableColumns(db, "region_metrics") : new Set();
  const needsRebuild =
    !hasRegionMetricsTable || REQUIRED_REGION_METRICS_COLUMNS.some((columnName) => !columnNames.has(columnName));

  if (!needsRebuild) {
    return false;
  }

  db.transaction(() => {
    db.exec("DROP TABLE IF EXISTS region_metrics;");
    db.exec(REGION_METRICS_TABLE_SQL);
    materializeRegionMetrics(db);
  })();

  return true;
}

function ensureOwnerMetricsCompatibility(db) {
  const hasOwnerMetricsTable = Boolean(
    db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'owner_metrics'").get()
  );
  const columnNames = hasOwnerMetricsTable ? listTableColumns(db, "owner_metrics") : new Set();
  const needsRebuild =
    !hasOwnerMetricsTable || REQUIRED_OWNER_METRICS_COLUMNS.some((columnName) => !columnNames.has(columnName));

  if (!needsRebuild) {
    return false;
  }

  db.transaction(() => {
    db.exec("DROP TABLE IF EXISTS owner_metrics;");
    db.exec(OWNER_METRICS_TABLE_SQL);
    materializeOwnerMetrics(db);
  })();

  return true;
}

// --- Main seed function ---

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

  const insertAsset = db.prepare("INSERT INTO assets (key, json) VALUES (?, ?)");
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
  const flushPackageRegions = createRelationBulkInserter(db, "package_regions", "package_id", "region_key");
  const flushPackageProvinces = createRelationBulkInserter(db, "package_provinces", "package_id", "province_key");
  const pendingPackageRegions = [];
  const pendingPackageProvinces = [];

  db.transaction(() => {
    insertAsset.run("audit_geojson", JSON.stringify(geoJson));
    insertAsset.run("audit_province_geojson", JSON.stringify(provinceGeoJson));

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
      "audit_metadata",
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

module.exports = {
  createSchema,
  ensureOwnerMetricsCompatibility,
  ensureRegionMetricsCompatibility,
  seedDatabase,
};
