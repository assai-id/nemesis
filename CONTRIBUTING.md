# Contributing to Nemesis

> **Nemesis** is an open investigative platform surfacing procurement anomalies from SIRUP data.  
> We welcome contributors who care about transparency, data integrity, and civic technology.

---

## Table of Contents

- [How to Contribute](#how-to-contribute)
- [Project Structure](#project-structure)
- [Development Setup](#development-setup)
- [Contribution Areas](#contribution-areas)
- [Code Standards](#code-standards)
- [Pull Request Process](#pull-request-process)
- [Issue Reporting](#issue-reporting)
- [Code of Conduct](#code-of-conduct)

---

## How to Contribute

1. **Fork** the repository
2. **Clone** your fork: `git clone https://github.com/<your-username>/nemesis.git`
3. **Create a branch**: `git checkout -b feat/your-feature-name`
4. **Make changes** and commit with clear messages (see [Commit Convention](#commit-convention))
5. **Push** to your fork and open a **Pull Request**

---

## Project Structure

```
nemesis/
├── backend/
│   ├── src/            # Node.js API server
│   ├── scripts/        # Data ingestion & seeding scripts
│   ├── seed/geo/       # Geographic data seeds
│   ├── data/           # SQLite database (not committed)
│   └── package.json
├── frontend/
│   ├── index.html      # Main entry point
│   ├── components/     # Reusable HTML/JS components
│   └── assets/         # Static assets
├── docs/               # Documentation
└── .github/            # Issue templates & workflows
```

---

## Development Setup

### Requirements

- Node.js >= 18
- SQLite3
- Git

### Steps

```bash
# 1. Clone and install backend dependencies
git clone https://github.com/assai-id/nemesis.git
cd nemesis/backend
npm install

# 2. Download and prepare the SIRUP dataset
#    See README.md for dataset download links

# 3. Place the database
mv dashboard.sqlite backend/data/dashboard.sqlite

# 4. Start the backend
node src/index.js
# → Runs on http://127.0.0.1:3000

# 5. Serve the frontend (in another terminal)
cd ../frontend
npx serve . -l 8080
# → Open http://127.0.0.1:8080
```

---

## Contribution Areas

Below are the highest-priority areas where contributions are needed:

### 🔴 Critical (High Impact)

| Area | Description |
|------|-------------|
| **Anomaly detection logic** | Improve or add new heuristics for detecting procurement irregularities (price outliers, single-source patterns, suspicious timing) |
| **Data pipeline** | Scripts to automate SIRUP dataset download, parsing, and SQLite ingestion |
| **API endpoints** | New endpoints for filtering, aggregation, and export |

### 🟡 Medium Priority

| Area | Description |
|------|-------------|
| **Frontend visualization** | New chart types, better map integration for regional data |
| **Search & filtering** | Full-text search improvements, multi-field filter UI |
| **Export features** | CSV/PDF export for anomaly reports |
| **Mobile responsiveness** | Improve layout for mobile and low-res screens |

### 🟢 Good First Issues

| Area | Description |
|------|-------------|
| **Documentation** | Improve README, add API docs, write data dictionary |
| **UI polish** | Fix layout bugs, improve accessibility (ARIA labels, contrast) |
| **Test coverage** | Write integration tests for API endpoints |
| **i18n** | Bahasa Indonesia localization for UI strings |

---

## Code Standards

### JavaScript (Backend & Frontend)

- Use **ES Modules** where possible
- Keep functions small and single-purpose
- Avoid external dependencies unless necessary (the frontend has zero build step — keep it that way)
- Comment non-obvious business logic, especially anomaly detection rules

### SQL / SQLite

- All queries must be **parameterized** — no string concatenation
- Index columns used in WHERE clauses
- Document the schema changes in `docs/schema.md`

### Commit Convention

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
feat: add price outlier detection for single-vendor contracts
fix: correct pagination offset in /api/contracts endpoint
docs: add data dictionary for SIRUP fields
refactor: split anomaly scoring into separate module
perf: add index on satuan_kerja column
```

---

## Pull Request Process

1. Ensure your PR targets the `main` branch
2. Fill in the PR template completely
3. Link any related issues with `Closes #<issue-number>`
4. Keep PRs focused — one feature or fix per PR
5. PRs adding anomaly detection logic **must** include a brief explanation of the detection rationale

### PR Checklist

- [ ] Code follows project style
- [ ] No hardcoded credentials or absolute paths
- [ ] Tested locally with the SIRUP dataset
- [ ] Documentation updated if needed
- [ ] Commit messages follow convention

---

## Issue Reporting

Use the issue templates in `.github/ISSUE_TEMPLATE/`:

- **Bug Report** — for broken functionality
- **Feature Request** — for new capabilities
- **Data Anomaly** — for reporting suspicious procurement patterns found in the data (these are especially valuable!)

When reporting a data anomaly, include:
- The contract/package ID from SIRUP
- Why you believe it's anomalous
- Any supporting data or references

---

## Code of Conduct

This project exists to serve the public interest. Contributors are expected to:

- Be respectful and constructive
- Base claims on data, not speculation
- Protect the privacy of individuals not involved in procurement decisions
- Not weaponize the platform against legitimate actors

Violations may result in removal from the project.

---

## Questions?

Open a GitHub Discussion or reach out via the project's contact channels listed in the README.

**End the vampire ball.**
