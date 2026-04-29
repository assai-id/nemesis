"use strict";                                                                                                                   /**                                                              * app.js  (modified — minimal diff)                             *
 * Added:                                                        *   + import anomaly-repository (4 functions)                   *   + 4 new GET routes under /api/anomaly/*                     *                                                               * Original routes and logic: unchanged.                         */                                                             
const express = require("express");                             const cors = require("cors");                                   const { CORS_ORIGIN } = require("./config");                    const {                                                           getBootstrapPayload,                                            getOwnerPackages,                                               getRegionPackages,
  getProvincePackages,
} = require("./dashboard-repository");

// ── NEW ───────────────────────────────────────────────────────────────────────
const {
  getTopRiskyPackages,
  getOwnerAnomalySummary,
  getSeverityDistribution,
  getMethodBreakdown,
} = require("./anomaly-repository");
// ─────────────────────────────────────────────────────────────────────────────

function resolveCorsOrigin() {
  if (CORS_ORIGIN === "*") return "*";
  return CORS_ORIGIN.split(",").map((item) => item.trim()).filter(Boolean);
}

function createApp(db) {
  const app = express();

  app.use(cors({ origin: resolveCorsOrigin() }));
  app.use(express.json());

  // ── Existing routes (unchanged) ───────────────────────────────────────────

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

  // ── NEW: Anomaly routes ───────────────────────────────────────────────────

  /**
   * GET /api/anomaly/top
   *
   * Top risky packages nationally, sorted by risk_score DESC.
   * Leverages the existing `risk_score` and `severity` columns — no re-scoring.
   *
   * Query params:
   *   ownerType    - filter: kabkota | provinsi | central | other
   *   severity     - filter: low | med | high | absurd
   *   mencurigakan - filter: 1 = only is_mencurigakan packages
   *   pemborosan   - filter: 1 = only is_pemborosan packages
   *   priorityOnly - filter: 1 = only is_priority packages
   *   limit        - max results (default 50, max 200)
   *
   * Response: { data: [...], meta: { returned, filters } }
   */
  app.get("/api/anomaly/top", (req, res) => {
    res.json(getTopRiskyPackages(db, req.query));
  });

  /**
   * GET /api/anomaly/owners/summary?ownerType=&ownerName=
   *
   * Anomaly breakdown for a specific owner:
   * aggregated stats (mencurigakan count, pemborosan count, avg/max risk_score)
   * + top risky packages within that owner.
   *
   * Query params (optional filters on topPackages):
   *   severity, mencurigakan, pemborosan, limit (default 20)
   *
   * Response: { summary: {...}, topPackages: [...] }
   */
  app.get("/api/anomaly/owners/summary", (req, res) => {
    const ownerType = (req.query.ownerType || "").trim();
    const ownerName = (req.query.ownerName || "").trim();

    if (!ownerType || !ownerName) {
      res.status(400).json({ error: "ownerType and ownerName are required" });
      return;
    }

    const result = getOwnerAnomalySummary(db, ownerType, ownerName, req.query);
    if (!result) { res.status(404).json({ error: "Owner not found" }); return; }
    res.json(result);
  });

  /**
   * GET /api/anomaly/severity
   *
   * National severity distribution:
   * count, total potential waste, and avg risk_score per severity level.
   *
   * Response: { data: [{ severity, totalPackages, totalPotentialWaste, avgRiskScore }] }
   */
  app.get("/api/anomaly/severity", (_req, res) => {
    res.json(getSeverityDistribution(db));
  });

  /**
   * GET /api/anomaly/methods?ownerType=&ownerName=
   *
   * Procurement method breakdown with anomaly counts.
   * Useful for spotting penunjukan langsung dominance within an owner.
   *
   * Query params (optional scope):
   *   ownerType, ownerName
   *
   * Response: { data: [{ procurementMethod, totalPackages, totalBudget,
   *                       totalPotentialWaste, totalMencurigakan,
   *                       totalPemborosan, avgRiskScore }] }
   */
  app.get("/api/anomaly/methods", (req, res) => {
    res.json(getMethodBreakdown(db, req.query));
  });

  // ── Error handler (unchanged) ─────────────────────────────────────────────

  app.use((err, _req, res, _next) => {
    console.error(err);
    res.status(500).json({ error: "Internal server error" });
  });

  return app;
}

module.exports = { createApp };
