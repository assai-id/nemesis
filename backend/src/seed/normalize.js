/**
 * Text normalization, slugify, province/region key mapping utilities.
 * Extracted from seed.js for maintainability.
 */

const SEVERITY_SCORES = {
  low: 1,
  med: 2,
  high: 3,
  absurd: 4,
};

const PROVINCE_KEY_ALIASES = {
  "daerah khusus ibukota jakarta": "jakartaraya",
  "dki jakarta": "jakartaraya",
  "jakarta raya": "jakartaraya",
  "daerah istimewa yogyakarta": "yogyakarta",
  "di yogyakarta": "yogyakarta",
  "bangka belitung": "bangkabelitung",
  "kep bangka belitung": "bangkabelitung",
  "kepulauan bangka belitung": "bangkabelitung",
  "kep riau": "kepulauanriau",
};

const PROVINCE_DISPLAY_ALIASES = {
  "Jakarta Raya": "DKI Jakarta",
  Yogyakarta: "DI Yogyakarta",
  "Daerah Istimewa Yogyakarta": "DI Yogyakarta",
  "Bangka Belitung": "Kepulauan Bangka Belitung",
};

const REGION_KEY_ALIASES = {
  "adm kepulauan seribu": "kepulauanseribu",
  "adm kepulauanseribu": "kepulauanseribu",
  "karang asem": "karangasem",
  "kepulauan siau tagulandang biaro": "siautagulandangbiaro",
  "kep seribu": "kepulauanseribu",
  "bukit tinggi": "bukittinggi",
  "kota sorong": "sorong",
  "pangkal pinang": "pangkalpinang",
  "pangkajene kepulauan": "pangkajenedankepulauan",
  "penajem paser utara": "penajampaserutara",
  "tanjung jabung barat": "tanjungjabungb",
  "tanjung jabung timur": "tanjungjabungt",
  "tanjung pinang": "tanjungpinang",
  "tebing tinggi": "tebingtinggi",
  terenggalek: "trenggalek",
};

const REGION_DISPLAY_ALIASES = {
  bukittinggi: "Bukit Tinggi",
  kepulauanseribu: "Kepulauan Seribu",
  pangkalpinang: "Pangkal Pinang",
  pangkajenedankepulauan: "Pangkajene dan Kepulauan",
  tanjungjabungb: "Tanjung Jabung Barat",
  tanjungjabungt: "Tanjung Jabung Timur",
  tanjungpinang: "Tanjung Pinang",
  tebingtinggi: "Tebing Tinggi",
};

const OWNER_TYPE_ALIASES = {
  central: "central",
  instansipusat: "central",
  kementerianlembaga: "central",
  provinsi: "provinsi",
  pemprov: "provinsi",
  kabkota: "kabkota",
  kabupatenkota: "kabkota",
  pemkot: "kabkota",
  pemkab: "kabkota",
  other: "other",
  others: "other",
  lainnya: "other",
};

const UNKNOWN_OWNER_NAMES = new Set(["", "-", "n a", "na", "none", "null", "tanpa lembaga", "tidak diketahui", "unknown"]);

function cleanText(value) {
  if (value === null || value === undefined) {
    return null;
  }

  const text = String(value).trim();
  return text || null;
}

function toComparableWords(value) {
  return String(value || "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function toComparableSlug(value) {
  return toComparableWords(value).replace(/\s+/g, "");
}

function slugify(value) {
  return String(value || "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "") || "item";
}

function parseBoolean(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }

  if (typeof value === "boolean") {
    return value;
  }

  if (typeof value === "number") {
    return value !== 0;
  }

  const normalized = String(value).trim().toLowerCase();

  if (["1", "true", "yes", "ya"].includes(normalized)) {
    return true;
  }

  if (["0", "false", "no", "tidak"].includes(normalized)) {
    return false;
  }

  return null;
}

function parseInteger(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }

  if (typeof value === "number" && Number.isFinite(value)) {
    return Math.round(value);
  }

  const normalized = String(value).trim().replace(/\./g, "").replace(/,/g, "");

  if (!normalized) {
    return null;
  }

  const parsed = Number.parseFloat(normalized);
  return Number.isFinite(parsed) ? Math.round(parsed) : null;
}

function parseAmount(value, budget) {
  if (value === null || value === undefined || value === "") {
    return 0;
  }

  const parsed = Number.parseFloat(String(value).trim().replace(/\./g, "").replace(/,/g, ""));
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return 0;
  }

  if (Number.isFinite(budget) && budget !== null) {
    return Math.min(parsed, budget);
  }

  return parsed;
}

function normalizeSeverity(value) {
  if (typeof value === "boolean") {
    return value ? "med" : "low";
  }

  const text = cleanText(value);
  if (!text) {
    return "low";
  }

  const normalized = text.toLowerCase();
  if (normalized === "high") {
    return "high";
  }

  if (normalized === "absurd") {
    return "absurd";
  }

  if (normalized === "med" || normalized === "medium") {
    return "med";
  }

  return "low";
}

function sanitizeReason(value) {
  const text = cleanText(value);
  return text ? text.slice(0, 1000) : null;
}

function inferOwnerType(ownerName) {
  const normalized = toComparableWords(ownerName);

  if (!normalized || UNKNOWN_OWNER_NAMES.has(normalized)) {
    return "other";
  }

  if (
    normalized.startsWith("kab ") ||
    normalized.startsWith("kabupaten ") ||
    normalized.startsWith("kota ") ||
    normalized.startsWith("pemkab ") ||
    normalized.startsWith("pemerintah kabupaten ") ||
    normalized.startsWith("pemkot ") ||
    normalized.startsWith("pemerintah kota ")
  ) {
    return "kabkota";
  }

  if (
    normalized.startsWith("provinsi ") ||
    normalized.startsWith("pemprov ") ||
    normalized.startsWith("pemerintah provinsi ")
  ) {
    return "provinsi";
  }

  return "central";
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
  return text ? PROVINCE_DISPLAY_ALIASES[text] || text : "Tidak diketahui";
}

function normalizeRegionType(value) {
  const normalized = toComparableWords(value);
  return normalized.startsWith("kab") ? "Kabupaten" : "Kota";
}

function normalizeRegionKey(value) {
  const normalized = toComparableWords(value)
    .replace(/^kabupaten\s+/, "")
    .replace(/^kab\s+/, "")
    .replace(/^kota\s+/, "")
    .replace(/^adm\.?\s+/, "")
    .trim();

  return REGION_KEY_ALIASES[normalized] || toComparableSlug(normalized);
}

function normalizeDistrictRegionType(value) {
  const normalized = toComparableWords(value);
  return normalized.startsWith("kota") ? "Kota" : "Kabupaten";
}

function normalizeRegionDisplayName(value, regionType) {
  const cleaned = cleanText(value) || "Tidak diketahui";
  const withoutPrefix = cleaned
    .replace(/^Kabupaten\s+/i, "")
    .replace(/^Kab\.\s+/i, "")
    .replace(/^Kota\s+/i, "")
    .replace(/^Adm\.?\s+/i, "")
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
  return `${regionType === "Kota" ? "Kota" : "Kab."} ${regionName}`;
}

function splitLocationSegments(locationRaw) {
  return String(locationRaw || "")
    .split("|")
    .map((segment) => segment.trim())
    .filter(Boolean);
}

function parseLocationSegment(segment) {
  let value = String(segment || "").replace(/\s+/g, " ").trim();

  if (!value || value === "LAINNYA, Luar Indonesia") {
    return null;
  }

  value = value
    .replace(/\(Kab\)$/i, "(Kab.)")
    .replace(/\(Kab\)/i, "(Kab.)")
    .replace(/\(Kota\.\)/i, "(Kota)")
    .replace(/([^\s])\((Kab\.|Kab|Kota)\)/gi, "$1 ($2)");

  let match = value.match(/^(.+),\s+(.+)\s+\((Kab\.|Kota)\)$/i);
  if (match) {
    return {
      provinceName: match[1].trim(),
      regionName: match[2].trim(),
      regionType: match[3].toLowerCase().startsWith("kab") ? "Kabupaten" : "Kota",
    };
  }

  match = value.match(/^(.+),\s+Kabupaten\s+(.+)$/i);
  if (match) {
    return {
      provinceName: match[1].trim(),
      regionName: match[2].trim(),
      regionType: "Kabupaten",
    };
  }

  match = value.match(/^(.+),\s+Kota\s+(.+)$/i);
  if (match) {
    return {
      provinceName: match[1].trim(),
      regionName: match[2].trim(),
      regionType: "Kota",
    };
  }

  return null;
}

module.exports = {
  SEVERITY_SCORES,
  PROVINCE_KEY_ALIASES,
  PROVINCE_DISPLAY_ALIASES,
  REGION_KEY_ALIASES,
  REGION_DISPLAY_ALIASES,
  OWNER_TYPE_ALIASES,
  UNKNOWN_OWNER_NAMES,
  cleanText,
  toComparableWords,
  toComparableSlug,
  slugify,
  parseBoolean,
  parseInteger,
  parseAmount,
  normalizeSeverity,
  sanitizeReason,
  inferOwnerType,
  normalizeOwnerType,
  normalizeProvinceKey,
  normalizeProvinceDisplayName,
  normalizeRegionType,
  normalizeDistrictRegionType,
  normalizeRegionKey,
  normalizeRegionDisplayName,
  buildLocationLookupKey,
  buildProvinceLookupKey,
  buildRegionOnlyLookupKey,
  buildRegionDisplayName,
  splitLocationSegments,
  parseLocationSegment,
};
