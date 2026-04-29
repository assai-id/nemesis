# Anomaly API — Documentation

Added in this PR: 4 new read-only endpoints under `/api/anomaly/*`.

All endpoints leverage **existing columns** in the `packages` table:  
`risk_score`, `severity`, `potential_waste`, `is_mencurigakan`, `is_pemborosan`.  
No new scoring logic is introduced — the endpoints surface what's already computed.

---

## Endpoints

### `GET /api/anomaly/top`

Top risky packages nationally, sorted by `risk_score DESC`.

**Query params** (all optional):

| Param | Type | Description |
|-------|------|-------------|
| `ownerType` | string | Filter: `kabkota` \| `provinsi` \| `central` \| `other` |
| `severity` | string | Filter: `low` \| `med` \| `high` \| `absurd` |
| `mencurigakan` | `1` | Only packages where `is_mencurigakan = 1` |
| `pemborosan` | `1` | Only packages where `is_pemborosan = 1` |
| `priorityOnly` | `1` | Only packages where `is_priority = 1` |
| `limit` | number | Max results, default `50`, max `200` |

**Example:**
```
GET /api/anomaly/top?severity=absurd&ownerType=kabkota&limit=10
```

**Response:**
```json
{
  "data": [
    {
      "id": "PKT-001",
      "packageName": "Pengadaan Server",
      "ownerName": "Dinas Pendidikan Kota X",
      "ownerType": "kabkota",
      "satker": null,
      "budget": 500000000,
      "procurementMethod": "Penunjukan Langsung",
      "procurementType": "Barang",
      "selectionDate": "2024-03-15",
      "audit": {
        "riskScore": 0.85,
        "severity": "absurd",
        "potentialWaste": 200000000,
        "reason": "...",
        "isMencurigakan": true,
        "isPemborosan": true
      },
      "meta": {
        "isPriority": true,
        "isFlagged": true,
        "activeTagCount": 3,
        "mappedRegionCount": 1
      }
    }
  ],
  "meta": {
    "returned": 1,
    "filters": {
      "ownerType": "kabkota",
      "severity": "absurd",
      "mencurigakan": false,
      "pemborosan": false,
      "priorityOnly": false
    }
  }
}
```

---

### `GET /api/anomaly/owners/summary?ownerType=&ownerName=`

Anomaly breakdown for a specific owner: aggregated stats + top packages.

**Required params:** `ownerType`, `ownerName`

**Optional filters on `topPackages`:** `severity`, `mencurigakan`, `pemborosan`, `limit` (default `20`)

**Response:**
```json
{
  "summary": {
    "ownerType": "kabkota",
    "ownerName": "Dinas Pendidikan Kota X",
    "totalPackages": 42,
    "totalPriorityPackages": 8,
    "totalFlaggedPackages": 5,
    "totalPotentialWaste": 1200000000,
    "totalBudget": 8000000000,
    "totalMencurigakan": 3,
    "totalPemborosan": 7,
    "avgRiskScore": 0.3412,
    "maxRiskScore": 0.91,
    "severityCounts": { "med": 12, "high": 4, "absurd": 1 }
  },
  "topPackages": [ ... ]
}
```

---

### `GET /api/anomaly/severity`

National severity distribution.

**Response:**
```json
{
  "data": [
    { "severity": "absurd", "totalPackages": 1240, "totalPotentialWaste": 8200000000, "avgRiskScore": 0.82 },
    { "severity": "high",   "totalPackages": 4310, "totalPotentialWaste": 3100000000, "avgRiskScore": 0.61 },
    { "severity": "med",    "totalPackages": 9800, "totalPotentialWaste": 1200000000, "avgRiskScore": 0.38 },
    { "severity": "low",    "totalPackages": 42000,"totalPotentialWaste":  400000000, "avgRiskScore": 0.12 }
  ]
}
```

---

### `GET /api/anomaly/methods?ownerType=&ownerName=`

Procurement method breakdown with anomaly counts. Optionally scoped to one owner.

**Response:**
```json
{
  "data": [
    {
      "procurementMethod": "Penunjukan Langsung",
      "totalPackages": 3200,
      "totalBudget": 12000000000,
      "totalPotentialWaste": 4800000000,
      "totalMencurigakan": 1840,
      "totalPemborosan": 2100,
      "avgRiskScore": 0.72
    },
    {
      "procurementMethod": "Tender",
      "totalPackages": 18000,
      "totalBudget": 45000000000,
      "totalPotentialWaste": 3200000000,
      "totalMencurigakan": 420,
      "totalPemborosan": 1800,
      "avgRiskScore": 0.28
    }
  ]
}
```

---

## CLI Tool

Quick audit from terminal without opening the dashboard:

```bash
cd backend

# Top 50 by risk_score (default)
node scripts/top-risk.js

# Only absurd severity
node scripts/top-risk.js --severity=absurd

# Only kabkota, mencurigakan, export JSON
node scripts/top-risk.js --owner-type=kabkota --mencurigakan --out=report.json

# Show severity distribution + method breakdown
node scripts/top-risk.js --stats

# Limit results
node scripts/top-risk.js --limit=100
```

---

## Running Tests

```bash
cd backend
node --test src/anomaly-repository.test.js
```

Uses an in-memory SQLite fixture — no real database needed.

---

## Design Notes

**Why not add new scoring?**  
The repo already has `risk_score`, `severity`, `potential_waste`, `is_mencurigakan`, and `is_pemborosan` computed during the audit pipeline. Adding a parallel scoring system would create conflicting signals and confuse the frontend. These endpoints expose the existing signals through a queryable API instead.

**Why a separate `anomaly-repository.js`?**  
Same reason `dashboard-repository.js` exists — keeps `app.js` clean and makes the query layer independently testable.

**Why `WeakMap` for statement cache?**  
`better-sqlite3` recommends preparing statements once. `WeakMap` ties the cache lifetime to the `db` instance automatically — no manual cleanup needed.
