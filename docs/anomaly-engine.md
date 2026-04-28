# Anomaly Engine — Technical Documentation

This document explains the design, heuristics, and integration points of the anomaly detection layer added in this PR.

---

## Overview

The anomaly engine scores procurement packages using a set of deterministic heuristics. It operates entirely in-process — no external API calls, no model inference. This makes it auditable, fast, and runnable offline.

The engine is intentionally layered:

```
app.js  →  anomaly-repository.js  →  anomaly-engine.js
              (SQL queries)            (pure scoring logic)
```

This mirrors the existing pattern: `app.js → dashboard-repository.js`.

---

## Files Added

| File | Location | Purpose |
|------|----------|---------|
| `anomaly-engine.js` | `backend/src/` | Pure scoring functions, no DB dependency |
| `anomaly-repository.js` | `backend/src/` | SQL queries + wires engine to DB |
| `app.js` | `backend/src/` | +3 new routes under `/api/anomaly/*` |
| `anomaly-engine.test.js` | `backend/src/` | Unit tests (Node built-in runner) |
| `score-all.js` | `backend/scripts/` | CLI batch scorer |

---

## Heuristics

### 1. Single Source High Value (`SINGLE_SOURCE_HIGH_VALUE`)
**Weight: 30%**

Flags packages using *Penunjukan Langsung* (direct appointment) with a budget above Rp 500 juta. Direct appointments bypass open competition entirely — this combination is the single strongest indicator of procurement irregularity in SIRUP data.

Score scales linearly with contract value up to 10× the threshold.

### 2. Price Outlier (`PRICE_OUTLIER`)
**Weight: 25%**

Uses Z-score analysis against peer packages in the same `owner_type + owner_name` group. A Z-score above 2.5 (roughly top/bottom 0.6% of the distribution) triggers this detector. Requires at least 5 peers to activate — avoids false positives on small agencies.

### 3. Vendor Concentration (`VENDOR_CONCENTRATION`)
**Weight: 20%**

Flags packages where a single vendor controls ≥70% of an owner's total procurement spend. Computed from all packages in the same owner group. Requires at least 3 distinct vendors in the group to avoid false positives on new or very small agencies.

### 4. Deadline Cramming (`DEADLINE_CRAMMING`)
**Weight: 15%**

Flags packages with fewer than 7 days between `start_date` and `end_date`. Short windows de facto exclude most vendors and concentrate awards to those with advance knowledge of the tender.

Score increases as the window shrinks toward zero.

### 5. Round Number Price (`ROUND_NUMBER_PRICE`)
**Weight: 10%**

Flags budgets that are exact multiples of Rp 50 juta or larger round numbers (100 juta, 500 juta, 1 miliar). Real cost estimates from engineering or market surveys are never perfectly round — suspiciously round numbers suggest the budget was set to match a preferred vendor's quote.

Minimum value: Rp 10 juta (ignores petty cash).

---

## Composite Score

Each detector produces a sub-score (0.0–1.0). The composite is a weighted sum, capped at 1.0:

```
composite = Σ (detector_score × weight)
```

Risk labels:
| Score range | Label |
|-------------|-------|
| ≥ 0.70 | HIGH |
| ≥ 0.40 | MEDIUM |
| ≥ 0.15 | LOW |
| < 0.15 | CLEAN |

---

## API Endpoints

### `GET /api/anomaly/packages/:packageId`
Score a single package.

```json
{
  "data": {
    "package_id": "PKT-001",
    "score": 0.4250,
    "label": "MEDIUM",
    "anomaly_count": 2,
    "anomalies": [
      {
        "type": "SINGLE_SOURCE_HIGH_VALUE",
        "score": 0.12,
        "description": "Penunjukan langsung senilai Rp600.000.000 — melewati kompetisi terbuka.",
        "evidence": { "procurement_method": "Penunjukan Langsung", "budget": 600000000 }
      }
    ]
  }
}
```

### `GET /api/anomaly/owners/packages?ownerType=PEMDA&ownerName=Dinas+X`
All packages for a specific owner, sorted by risk score descending.

```json
{
  "data": [ ... ],
  "meta": { "total": 42, "high": 3, "medium": 8, "low": 11, "clean": 20 }
}
```

### `GET /api/anomaly/top?label=HIGH&limit=50`
Top risky packages across all owners.

Query params: `label`, `ownerType`, `ownerName`, `method`, `limit` (max 200).

---

## Running Tests

```bash
cd backend
node --test src/anomaly-engine.test.js
```

No additional dependencies needed (Node.js 18+ required).

---

## Running the Batch Scorer

Pre-compute scores and persist them to `anomaly_scores` table:

```bash
cd backend

# Dry run — see results without writing
node scripts/score-all.js --dry-run

# Full run
node scripts/score-all.js

# Reset table and re-score
node scripts/score-all.js --reset

# Score only one agency
node scripts/score-all.js --owner-type=PEMDA --owner-name="Dinas Pendidikan Kota X"
```

---

## Design Decisions

**Why no async?**
`better-sqlite3` is synchronous by design. Mixing sync/async here would add complexity with no benefit since Node.js single-thread handles this fine for read workloads.

**Why not cache scores in DB by default?**
The three `/api/anomaly/*` routes compute scores on the fly so they always reflect the latest data. For production scale, run `score-all.js` on a schedule and add a `/api/anomaly/top` route that reads from `anomaly_scores` directly.

**Why not use the GPT-analyzed SIRUP dataset labels?**
The AI-analyzed labels are not yet exposed via the existing API. This engine runs on raw package fields only, making it usable immediately with any SIRUP SQLite dump.

---

## Limitations & Future Work

- Peer group is currently `owner_type + owner_name` — a finer grouping by `procurement_type` or budget range would reduce false positives in heterogeneous agencies.
- `ROUND_NUMBER_PRICE` is a weak signal and should never be the sole basis for flagging a package.
- Vendor deduplication via NPWP is not yet implemented — the same vendor under slightly different name spellings counts as different vendors.
- The scoring weights are currently hand-tuned. Calibration against known corrupt contracts from investigative datasets would significantly improve precision.
