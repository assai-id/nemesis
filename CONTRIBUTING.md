# Contributing to Nemesis

> Nemesis surfaces procurement anomalies from SIRUP data.  
> **End the vampire ball.**

---

## Dev Setup

```bash
# Clone fork
git clone https://github.com/<your-username>/nemesis.git

# Install backend deps
cd nemesis/backend && npm install

# Place database (download link in README)
mv dashboard.sqlite data/dashboard.sqlite

# Start backend
npm start
# → http://127.0.0.1:3000

# Serve frontend (separate terminal)
cd ../frontend && python3 -m http.server 8080
# → http://127.0.0.1:8080
```

---

## Architecture

```
backend/src/
  app.js                    ← Express routes (entry: createApp(db))
  server.js                 ← Startup, DB validation, index creation
  db.js                     ← openDatabase(), resolveRuntimeDbPath()
  config.js                 ← Env vars
  dashboard-repository.js   ← Query layer: regions, provinces, owners
  anomaly-repository.js     ← Query layer: risk/anomaly endpoints (new)

backend/scripts/
  top-risk.js               ← CLI audit tool (new)

frontend/
  index.html                ← No build step. Plain HTML/CSS/JS.
```

**Key conventions:**

```js
// ✅ db is always passed in
function getTopRiskyPackages(db, query) { ... }

// ✅ better-sqlite3 is synchronous — no async/await on queries
const rows = db.prepare("SELECT ...").all(param);

// ✅ Prepared statements cached per db instance via WeakMap
const stmtCache = new WeakMap();
function getStmts(db) {
  if (stmtCache.has(db)) return stmtCache.get(db);
  const stmts = { myQuery: db.prepare("SELECT ...") };
  stmtCache.set(db, stmts);
  return stmts;
}

// ✅ VALID_OWNER_TYPES = ["kabkota", "provinsi", "central", "other"]
// ✅ VALID_SEVERITIES  = ["low", "med", "high", "absurd"]
// ✅ Table: packages   (not contracts, not orders)
```

---

## Schema Quick Reference

Key `packages` columns for anomaly work:

| Column | Type | Notes |
|--------|------|-------|
| `risk_score` | REAL | 0.0–1.0, computed by audit pipeline |
| `severity` | TEXT | `low` \| `med` \| `high` \| `absurd` |
| `potential_waste` | REAL | IDR, computed |
| `is_mencurigakan` | INTEGER | 0/1/null |
| `is_pemborosan` | INTEGER | 0/1/null |
| `is_priority` | INTEGER | 0/1 |
| `is_flagged` | INTEGER | 0/1 |
| `owner_type` | TEXT | `kabkota` \| `provinsi` \| `central` \| `other` |
| `owner_name` | TEXT | Name of procuring unit |
| `procurement_method` | TEXT | e.g. `Penunjukan Langsung`, `Tender` |
| `budget` | REAL | IDR |

---

## Running Tests

```bash
cd backend
node --test src/anomaly-repository.test.js
```

Uses in-memory SQLite — no real database file needed.

---

## Commit Convention

```
feat: add /api/anomaly/top endpoint
fix: handle null risk_score in top-risk CLI
docs: add anomaly API documentation
test: add integration tests for anomaly-repository
refactor: extract method breakdown query to anomaly-repository
```

---

## PR Checklist

- [ ] Tested locally with the real SIRUP database
- [ ] No hardcoded paths or credentials
- [ ] New query functions documented in `docs/anomaly-api.md`
- [ ] Commits follow the convention above
- [ ] PR description explains *why*, not just *what*

---

## Reporting a Data Finding

Found a suspicious contract? Open a **Data Anomaly** issue — these directly inform what the platform highlights.

*Nemesis is a project of Abil Sudarman School of Artificial Intelligence (assai.id).*
