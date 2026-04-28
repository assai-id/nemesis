# Contributing to Nemesis

> Nemesis surfaces procurement anomalies from SIRUP data to citizens, journalists, and policymakers.  
> **End the vampire ball.**

---

## Quick Start

```bash
# 1. Fork and clone
git clone https://github.com/<your-username>/nemesis.git
cd nemesis/backend

# 2. Install dependencies
npm install

# 3. Place the database (download from README)
mv dashboard.sqlite backend/data/dashboard.sqlite

# 4. Start backend
npm start
# → http://127.0.0.1:3000

# 5. Serve frontend (separate terminal)
cd ../frontend
python3 -m http.server 8080
# → http://127.0.0.1:8080
```

---

## Project Structure

```
nemesis/
├── backend/
│   ├── src/
│   │   ├── app.js                    # Express routes
│   │   ├── server.js                 # Startup & DB validation
│   │   ├── db.js                     # SQLite connection helpers
│   │   ├── config.js                 # Env config
│   │   ├── dashboard-repository.js   # Query layer (existing)
│   │   ├── anomaly-engine.js         # Heuristic scoring (new)
│   │   └── anomaly-repository.js     # Anomaly query layer (new)
│   ├── scripts/
│   │   └── score-all.js              # CLI batch scorer (new)
│   ├── seed/geo/
│   ├── data/                         # .gitignored — place .sqlite here
│   ├── .env.example
│   └── package.json
├── frontend/
│   └── index.html                    # No build step — plain HTML/CSS/JS
├── docs/
│   └── anomaly-engine.md
└── CONTRIBUTING.md
```

---

## Contribution Areas

### High Priority
- **Anomaly detection** — new heuristics, better peer grouping, weight calibration
- **API endpoints** — filtering, pagination, export
- **Frontend** — visualize anomaly scores, risk dashboard panel

### Good First Issues
- Documentation improvements
- UI accessibility (ARIA, contrast)
- Tests for `anomaly-repository.js` (requires SQLite fixture)
- i18n for UI strings (Bahasa Indonesia)

---

## Conventions

**Pattern: db is always passed in**
```js
// ✅ correct — matches existing codebase pattern
function getOwnerAnomalyScores(db, ownerType, ownerName) { ... }

// ❌ wrong — no singleton db modules
const db = require("./db-singleton");
```

**Queries: synchronous (better-sqlite3)**
```js
// ✅ correct
const rows = db.prepare("SELECT * FROM packages WHERE owner_type = ?").all(ownerType);

// ❌ wrong — no async/await on DB calls
const rows = await db.query(...);
```

**Table name: `packages`** (not `contracts`)

**Commit convention:**
```
feat: add vendor concentration detector
fix: correct Z-score calculation for small peer groups
docs: add anomaly-engine technical documentation
test: add unit tests for deadline cramming detector
```

---

## Running Tests

```bash
cd backend
node --test src/anomaly-engine.test.js
```

No additional test dependencies needed (Node.js ≥ 18).

---

## Pull Request Checklist

- [ ] Tested locally with the SIRUP SQLite database
- [ ] No hardcoded file paths or credentials
- [ ] New anomaly heuristics documented in `docs/anomaly-engine.md`
- [ ] Commits follow the convention above
- [ ] PR description explains *why*, not just *what*

---

## Reporting Data Findings

Found a suspicious contract in the SIRUP data?  
Open an issue using the **Data Anomaly** template — these are especially valuable and may directly inform new detection heuristics.

---

*Nemesis is a project of Abil Sudarman School of Artificial Intelligence (assai.id).*
