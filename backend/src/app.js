const express = require("express");
const cors = require("cors");
const compression = require("compression");
const crypto = require("crypto");
const { CORS_ORIGIN } = require("./config");
const { getBootstrapPayload, getOwnerPackages, getRegionPackages, getProvincePackages, streamGeoJsonAsset, getRegionPackagesCsv, getProvincePackagesCsv, getOwnerPackagesCsv } = require("./dashboard-repository");

// ─── In-memory cache ──────────────────────────────────────────────────────────
let bootstrapCache = null;
let bootstrapEtag = null;

function getBootstrapCached(db) {
  if (!bootstrapCache) {
    bootstrapCache = getBootstrapPayload(db);
    bootstrapEtag = `"${crypto.createHash("md5").update(JSON.stringify(bootstrapCache)).digest("hex")}"`;
  }
  return { payload: bootstrapCache, etag: bootstrapEtag };
}

function resolveCorsOrigin() {
  if (CORS_ORIGIN === "*") {
    return "*";
  }
  return CORS_ORIGIN.split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

// ─── CSV helpers ──────────────────────────────────────────────────────────────
const CSV_HEADERS = [
  "ID", "Nama Paket", "Pemilik", "Jenis Pemilik", "Satker",
  "Lokasi", "Pagu (Rp)", "Severity", "Potensi Pemborosan (Rp)",
  "Risk Score", "Prioritas", "Alasan",
];

function escapeCsv(value) {
  if (value === null || value === undefined) return "";
  const str = String(value);
  if (str.includes(",") || str.includes('"') || str.includes("\n")) {
    return `"${str.replace(/"/g, '""')}"`;
  }
  return str;
}

function packagesToCsv(items) {
  const header = CSV_HEADERS.map(escapeCsv).join(",");
  const rows = items.map((pkg) =>
    [
      pkg.id,
      pkg.packageName,
      pkg.ownerName,
      pkg.ownerType,
      pkg.satker || "",
      pkg.locationRaw,
      pkg.budget || 0,
      pkg.audit.severity,
      pkg.audit.potensiPemborosan || 0,
      pkg.meta.riskScore,
      pkg.meta.isPriority ? "Ya" : "Tidak",
      (pkg.audit.reason || "").replace(/\n/g, " "),
    ].map(escapeCsv).join(",")
  );
  return [header, ...rows].join("\r\n");
}

function createApp(db) {
  const app = express();

  app.use(cors({ origin: resolveCorsOrigin() }));
  app.use(compression()); // ✅ Gzip semua response JSON
  app.use(express.json());

  // ─── Health ───────────────────────────────────────────────────────────────
  app.get("/api/health", (_req, res) => {
    res.json({ status: "ok" });
  });

  // ─── Bootstrap (with ETag + In-memory cache) ──────────────────────────────
  app.get("/api/bootstrap", (req, res) => {
    const { payload, etag } = getBootstrapCached(db);

    res.setHeader("ETag", etag);
    res.setHeader("Cache-Control", "public, max-age=60, stale-while-revalidate=120");

    // Kirim 304 Not Modified jika browser punya versi yang sama
    if (req.headers["if-none-match"] === etag) {
      return res.status(304).end();
    }

    res.json(payload);
  });

  // ─── GeoJSON streaming ────────────────────────────────────────────────────
  app.get("/api/geo/regions", (_req, res) => {
    res.setHeader("Cache-Control", "public, max-age=3600");
    streamGeoJsonAsset(db, res, "audit_geojson");
  });

  app.get("/api/geo/provinces", (_req, res) => {
    res.setHeader("Cache-Control", "public, max-age=3600");
    streamGeoJsonAsset(db, res, "audit_province_geojson");
  });

  // ─── Region packages ──────────────────────────────────────────────────────
  app.get("/api/regions/:regionKey/packages", (req, res) => {
    const payload = getRegionPackages(db, req.params.regionKey, req.query);
    if (!payload) return res.status(404).json({ error: "Region not found" });
    res.json(payload);
  });

  // ─── Region packages CSV export ───────────────────────────────────────────
  app.get("/api/regions/:regionKey/packages.csv", (req, res) => {
    const payload = getRegionPackages(db, req.params.regionKey, { ...req.query, page: 1, pageSize: 9999 });
    if (!payload) return res.status(404).json({ error: "Region not found" });

    const filename = `audit-${req.params.regionKey}.csv`;
    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader("Content-Disposition", `attachment; filename="${filename}"`);
    res.send("\uFEFF" + packagesToCsv(payload.items)); // BOM for Excel UTF-8
  });

  // ─── Province packages ────────────────────────────────────────────────────
  app.get("/api/provinces/:provinceKey/packages", (req, res) => {
    const payload = getProvincePackages(db, req.params.provinceKey, req.query);
    if (!payload) return res.status(404).json({ error: "Province not found" });
    res.json(payload);
  });

  // ─── Province packages CSV export ─────────────────────────────────────────
  app.get("/api/provinces/:provinceKey/packages.csv", (req, res) => {
    const payload = getProvincePackages(db, req.params.provinceKey, { ...req.query, page: 1, pageSize: 9999 });
    if (!payload) return res.status(404).json({ error: "Province not found" });

    const filename = `audit-prov-${req.params.provinceKey}.csv`;
    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader("Content-Disposition", `attachment; filename="${filename}"`);
    res.send("\uFEFF" + packagesToCsv(payload.items));
  });

  // ─── Owner packages ───────────────────────────────────────────────────────
  app.get("/api/owners/packages", (req, res) => {
    const ownerType = (req.query.ownerType || "").trim();
    const ownerName = (req.query.ownerName || "").trim();
    if (!ownerType || !ownerName) {
      return res.status(400).json({ error: "ownerType and ownerName are required" });
    }
    const payload = getOwnerPackages(db, req.query);
    if (!payload) return res.status(404).json({ error: "Owner not found" });
    res.json(payload);
  });

  // ─── Owner packages CSV export ────────────────────────────────────────────
  app.get("/api/owners/packages.csv", (req, res) => {
    const ownerType = (req.query.ownerType || "").trim();
    const ownerName = (req.query.ownerName || "").trim();
    if (!ownerType || !ownerName) {
      return res.status(400).json({ error: "ownerType and ownerName are required" });
    }
    const payload = getOwnerPackages(db, { ...req.query, page: 1, pageSize: 9999 });
    if (!payload) return res.status(404).json({ error: "Owner not found" });

    const filename = `audit-${ownerType}-${ownerName.slice(0, 40).replace(/[^a-z0-9]/gi, "-")}.csv`;
    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader("Content-Disposition", `attachment; filename="${filename}"`);
    res.send("\uFEFF" + packagesToCsv(payload.items));
  });

  // ─── Error handler ────────────────────────────────────────────────────────
  app.use((err, _req, res, _next) => {
    console.error(err);
    res.status(500).json({ error: "Internal server error" });
  });

  return app;
}

module.exports = { createApp };
