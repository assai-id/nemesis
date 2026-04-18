# Nemesis
Nemesis is the public-facing investigative interface as the result of Operation Diponegoro, initiated by Abil Sudarman School of Artificial Intelligence. We ingest millions of rows of procurement data, surface anomalies, and make the findings legible to citizens, journalists, and policymakers.

Live dashboard: https://assai.id/nemesis


> End the vampire ball.


## Release Status

| Asset | Status | ETA |
|-------|--------|-----|
| Fine-tuned model | 🟡 In progress | ? |
| Scraping & Analyze source code | 🟡 In progress | ? |



Stay tuned.

## Downloads

### Dataset

[Download SIRUP raw jsonl dataset (analyzed by GPT-5.4)](https://contenflowstorage.blob.core.windows.net/shared/gpt-5.4-analyzed-sirup.zip?sp=r&st=2026-04-16T12:00:08Z&se=2029-04-16T20:15:08Z&spr=https&sv=2025-11-05&sr=b&sig=m%2FATynnnZq5gSdP8xWWw2ew41EMJZz09fDQRwpbWolk%3D)

[Download SIRUP dataset SQL Ver (analyzed by GPT-5.4-mini)](https://contenflowstorage.blob.core.windows.net/shared/datasets/dashboard.sql?sp=r&st=2026-04-16T12:16:15Z&se=2029-04-16T20:31:15Z&spr=https&sv=2025-11-05&sr=b&sig=sKPH9uazyLcYcSwhARcEwVSG%2FTld9VnGJgZ2mOZIxrA%3D)


### Model
Stay tuned

### 1. Prepare the Database

You have two options for initializing the database:

**Option A: Build from Raw Dataset (Recommended)**
If you downloaded the raw `jsonl` dataset, place the unzipped files inside a folder named `dataset/` at the project root. Then dynamically compile the database:
```bash
npm run db:reset
```

**Option B: Import the Pre-Analyzed V1 SQL Dump**
If you downloaded the `dashboard.sql` plain-text dump instead, you MUST compile it securely into the V2 SQLite binary format! Place `dashboard.sql` inside the `data/` folder and run:
```bash
# 1. Delete any auto-generated corrupt binaries
rm -f data/dashboard.sqlite

# 2. Compile the 4.4GB text dump heavily into a binary SQLite database
sqlite3 data/dashboard.sqlite < data/dashboard.sql

# 3. Patch the legacy V1 data to strictly support V2.0.0 analytics
sqlite3 data/dashboard.sqlite < data/patch-v1-to-v2.sql
```

### 2. Run the Application

The frontend and backend have been unified into a seamless, high-performance Vite orchestrator. You no longer need to jump between directories or run Python servers!

For Production:
```bash
npm install
npm run build && npm run start
```
For Development:
```bash
npm run dev
```

The unified orchestrated server will boot automatically. Open your browser to the local URL explicitly printed in your terminal (usually `http://127.0.0.1:$PORT`).

## Notes
- There is no manual frontend build step required during development. Vite cleanly orchestrates HMR under the hood.
- To physically compile a final production bundle and boot it, run `npm run build` followed by `npm run start`.

## Environment

Configuration is securely loaded from the `.env` file located in the project root.

Copy from example:
```bash
cp .env.example .env
```
