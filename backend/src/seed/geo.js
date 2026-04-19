/**
 * GeoJSON loading, simplification, and registry building.
 * Extracted from seed.js for maintainability.
 */

const fs = require("fs");
const path = require("path");
const {
  GEO_ROOT_PATH,
  GEOJSON_PATH,
  PROVINCE_GEOJSON_PATH,
} = require("../config");
const {
  cleanText,
  slugify,
  normalizeProvinceDisplayName,
  normalizeRegionType,
  normalizeRegionDisplayName,
  normalizeDistrictRegionType,
  buildLocationLookupKey,
  buildProvinceLookupKey,
  buildRegionOnlyLookupKey,
  buildRegionDisplayName,
} = require("./normalize");

const SKIPPED_GEO_DIRECTORY_FILES = new Set(["none.geojson"]);
const DISTRICT_GEO_MAX_RING_POINTS = 220;
const PROVINCE_GEO_MAX_RING_POINTS = 120;
const LOCATION_CACHE_MAX_SIZE = 100000;

function roundPoint(point) {
  return point.slice(0, 2).map((value) => Number(Number(value).toFixed(4)));
}

function samePoint(left, right) {
  return Array.isArray(left) && Array.isArray(right) && left[0] === right[0] && left[1] === right[1];
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

  if (geometry.type === "Polygon") {
    return {
      ...geometry,
      coordinates: geometry.coordinates.map((ring) => simplifyRing(ring, maxRingPoints)),
    };
  }

  if (geometry.type === "MultiPolygon") {
    return {
      ...geometry,
      coordinates: geometry.coordinates.map((polygon) => polygon.map((ring) => simplifyRing(ring, maxRingPoints))),
    };
  }

  return geometry;
}

function parseGeoJsonFile(filePath) {
  const payload = JSON.parse(fs.readFileSync(filePath, "utf8"));

  if (!payload || payload.type !== "FeatureCollection" || !Array.isArray(payload.features)) {
    throw new Error(`GeoJSON asset at "${filePath}" is invalid.`);
  }

  return payload;
}

function listGeoDirectoryFiles(directoryPath) {
  return fs
    .readdirSync(directoryPath, { withFileTypes: true })
    .filter((entry) => entry.isFile() && entry.name.toLowerCase().endsWith(".geojson"))
    .map((entry) => path.resolve(directoryPath, entry.name))
    .sort((left, right) => left.localeCompare(right));
}

function resolveLegacyFallbackGeoPath(directoryPath) {
  const fallbackPath = path.resolve(directoryPath, "..", "indonesia-kabkota-simple.geojson");
  return fs.existsSync(fallbackPath) ? fallbackPath : null;
}

function assertGeoFeature(feature, sourcePath, index) {
  if (
    !feature ||
    feature.type !== "Feature" ||
    !feature.geometry ||
    !feature.properties ||
    typeof feature.properties !== "object"
  ) {
    throw new Error(`GeoJSON asset at "${sourcePath}" contains an invalid feature at index ${index}.`);
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
    kind: "district-directory",
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
    kind: "province-directory",
    sourcePath: PROVINCE_GEOJSON_PATH,
  };
}

function createLegacyRegionRecord(feature, index) {
  const provinceName = normalizeProvinceDisplayName(feature.properties.NAME_1);
  const regionType = normalizeRegionType(feature.properties.TYPE_2);
  const regionName = normalizeRegionDisplayName(feature.properties.NAME_2, regionType);
  const gid = cleanText(feature.properties.GID_2);
  const regionKey = gid ? `gid-${slugify(gid)}` : `region-${slugify(`${provinceName}-${regionType}-${regionName}`)}`;

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
    type: "Feature",
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
    type: "Feature",
    geometry: simplifyGeometry(geometry, PROVINCE_GEO_MAX_RING_POINTS),
    properties: {
      provinceKey: record.province_key,
      code: record.code,
      provinceName: record.province_name,
      displayName: record.display_name,
      regionType: "Provinsi",
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
    mode: "legacy-file",
    sourcePath: filePath,
    sourceFiles: [filePath],
    usedSourceFiles: [filePath],
    skippedFiles: [],
    geoJson: {
      type: "FeatureCollection",
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
      source: "district-raw",
      geometries: rawGeometries,
    };
  }

  const exactGeometry = legacyGeometryIndex.exactGeometries.get(record.lookup_key);

  if (exactGeometry) {
    return {
      source: "legacy-exact",
      geometries: [exactGeometry],
    };
  }

  const regionOnlyGeometry = legacyGeometryIndex.regionOnlyGeometries.get(
    buildRegionOnlyLookupKey(record.region_name, record.region_type)
  );

  if (regionOnlyGeometry) {
    return {
      source: "legacy-region-only",
      geometries: [regionOnlyGeometry],
    };
  }

  return {
    source: "district-raw",
    geometries: rawGeometries,
  };
}

function buildDistrictDirectoryGeoRegistry(directoryPath) {
  const sourceFiles = listGeoDirectoryFiles(directoryPath);
  const legacyFallbackGeoPath = resolveLegacyFallbackGeoPath(directoryPath);
  const legacyGeometryIndex = legacyFallbackGeoPath ? buildLegacyGeometryIndex(legacyFallbackGeoPath) : null;
  const usedSourceFiles = [];
  const skippedFiles = [];
  const lookup = new Map();
  const regions = [];
  const features = [];
  const geometrySourceCounts = {
    "district-raw": 0,
    "legacy-exact": 0,
    "legacy-region-only": 0,
  };

  for (const filePath of sourceFiles) {
    const fileName = path.basename(filePath);

    if (SKIPPED_GEO_DIRECTORY_FILES.has(fileName.toLowerCase())) {
      skippedFiles.push({
        fileName,
        reason: "reserved-file",
      });
      continue;
    }

    const payload = parseGeoJsonFile(filePath);

    if (!payload.features.length) {
      skippedFiles.push({
        fileName,
        reason: "empty-feature-collection",
      });
      continue;
    }

    assertGeoFeature(payload.features[0], filePath, 0);

    const record = createDistrictRegionRecord(payload.features[0], features.length);

    if (lookup.has(record.lookup_key)) {
      throw new Error(`Duplicate geo region lookup key "${record.lookup_key}" found in "${filePath}".`);
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
    mode: "district-directory",
    sourcePath: directoryPath,
    sourceFiles,
    usedSourceFiles,
    skippedFiles,
    legacyFallbackGeoPath,
    geometrySourceCounts,
    geoJson: {
      type: "FeatureCollection",
      features,
    },
    regions,
    lookup,
  };
}

function loadGeoRegistry() {
  const geoSource = loadGeoSource();

  if (geoSource.kind === "legacy-file") {
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
        reason: "empty-feature-collection",
      });
      continue;
    }

    assertGeoFeature(payload.features[0], filePath, 0);

    const record = createProvinceRecord(payload.features[0], features.length);

    if (lookup.has(record.lookup_key)) {
      throw new Error(`Duplicate province lookup key "${record.lookup_key}" found in "${filePath}".`);
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
    mode: "province-directory",
    sourcePath: directoryPath,
    sourceFiles,
    usedSourceFiles,
    skippedFiles,
    geoJson: {
      type: "FeatureCollection",
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

    const lookupKey = buildLocationLookupKey(parsed.provinceName, parsed.regionName, parsed.regionType);
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

    const region = regionLookup.get(buildLocationLookupKey(parsed.provinceName, parsed.regionName, parsed.regionType));

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
    const cacheKey = String(locationRaw || "");
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

module.exports = {
  SKIPPED_GEO_DIRECTORY_FILES,
  DISTRICT_GEO_MAX_RING_POINTS,
  PROVINCE_GEO_MAX_RING_POINTS,
  LOCATION_CACHE_MAX_SIZE,
  parseGeoJsonFile,
  listGeoDirectoryFiles,
  loadGeoRegistry,
  loadProvinceGeoRegistry,
  createLocationResolver,
};
