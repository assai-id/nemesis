"use strict";

/**
 * app.js  (modified)
 *
 * Changes from original:
 *   + import anomaly-repository
 *   + 3 new GET routes under /api/anomaly/*
 *
 * Everything else is identical to the original file.
 * Diff is intentionally minimal to ease review.
 */

const express = require("express");
const cors = require("cors");
const { CORS_ORIGIN } = require("./config");
const {
  getBootstrapPayload,
  getOwnerPackages,
  getRegionPackages,
  getProvincePackages,
} = require("./dashboard-repository");

// ── NEW ────────────────────────────────────────────────────────────────────────
const {
  getPackageAnomalyScore,
  getOwnerAnomalyScores,
  getTopRiskyPackages,
} = require("./anomaly-repository");
// ──────────────────────────────────────────────────────────────────────────────

function resolveCorsOrigin() {
  if (CORS_ORIGIN === "*") return "*";
  return CORS_ORIGIN.split(",").map((item) => item.trim()).filter(Boolean);
}

function createApp(db) {
  const app = express();

  app.use(cors({ origin: resolveCorsOrigin() }));
  app.use(express.json());

  // ── Existing routes (unchanged) ────────────────────────────────────────────

  app.get("/api/health", (_req, res) => {
    res.json({ status: "ok" });
  });

  app.get("/api/bootstrap", (_req, res) => {
    res.json(getBootstrapPayload(db));
  });

  app.get("/api/regions/:regionKey/packages", (req, res) => {
    const payload = getRegionPackages(db, req.params.regionKey, req.query);
    if (!payload) { res.status(404).json({ error: "Region not found" }); return; }
    res.json(payload);
  });

  app.get("/api/provinces/:provinceKey/packages", (req, res) => {
    const payload = getProvincePackages(db, req.params.provinceKey, req.query);
    if (!payload) { res.status(404).json({ error: "Province not found" }); return; }
    res.json(payload);
  });

  app.get("/api/owners/packages", (req, res) => {
    const ownerType = (req.query.ownerType || "").trim();
    const ownerName = (req.query.ownerName || "").trim();
    if (!ownerType || !ownerName) {
      res.status(400).json({ error: "ownerType and ownerName are required" });
      return;
    }
    const payload = getOwnerPackages(db, req.query);
    if (!payload) { res.status(404).json({ error: "Owner not found" }); return; }
    res.json(payload);
  });

  // ── NEW: Anomaly routes ────────────────────────────────────────────────────

  /**
   * GET /api/anomaly/packages/:packageId
   * Score a single package by ID.
   *
   * Response: { data: { package_id, score, label, anomaly_count, anomalies[] } }
   */
  app.get("/api/anomaly/packages/:packageId", (req, res) => {
    const result = getPackageAnomalyScore(db, req.params.packageId);
    if (!result) { res.status(404).json({ error: "Package not found" }); return; }
    res.json(result);
  });

  /**
   * GET /api/anomaly/owners/packages?ownerType=&ownerName=
   * Score all packages for a specific owner, sorted by risk score.
   *
   * Response: { data: [...], meta: { total, high, medium, low, clean } }
   */
  app.get("/api/anomaly/owners/packages", (req, res) => {
    const ownerType = (req.query.ownerType || "").trim();
    const ownerName = (req.query.ownerName || "").trim();
    if (!ownerType || !ownerName) {
      res.status(400).json({ error: "ownerType and ownerName are required" });
      return;
    }
    const result = getOwnerAnomalyScores(db, ownerType, ownerName);
    res.json(result);
  });

  /**
   * GET /api/anomaly/top?label=HIGH&ownerType=&ownerName=&method=&limit=50
   * Top risky packages across owners, optionally filtered.
   *
   * Query params:
   *   label      - filter by risk label (HIGH | MEDIUM | LOW | CLEAN)
   *   ownerType  - filter by owner type
   *   ownerName  - filter by owner name (partial match)
   *   method     - filter by procurement method (partial match)
   *   limit      - max results (default 50, max 200)
   *
   * Response: { data: [...], meta: { returned, high, medium, low, clean } }
   */
  app.get("/api/anomaly/top", (req, res) => {
    const result = getTopRiskyPackages(db, req.query);
    res.json(result);
  });

  // ── Error handler (unchanged) ──────────────────────────────────────────────

  app.use((err, _req, res, _next) => {
    console.error(err);
    res.status(500).json({ error: "Internal server error" });
  });

  return app;
}

module.exports = { createApp };
