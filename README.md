# Nemesis
assai.id/nemesis
> End the vampire ball.

Mohon bersabar, file-file sedang dalam proses finalisasi.

## Release Status

| Asset | Status | ETA |
|-------|--------|-----|
| Dashboard URL | 🟡 In progress | 17 April |
| SQL / SQLite / Raw Dataset Files | 🟡 In progress | 18 April |

Stay tuned.

## Downloads

### Raw Dataset (Analyzed by GPT-5.4)

[Download SIRUP dataset (analyzed)](https://contenflowstorage.blob.core.windows.net/shared/gpt-5.4-analyzed-sirup.zip?sp=r&st=2026-04-16T12:00:08Z&se=2029-04-16T20:15:08Z&spr=https&sv=2025-11-05&sr=b&sig=m%2FATynnnZq5gSdP8xWWw2ew41EMJZz09fDQRwpbWolk%3D)

### 1. Prepare the Database

Download and unzip the raw dataset, then convert it to SQLite:

```bash
# Unzip the dataset
unzip gpt-5.4-analyzed-sirup.zip

# Convert to SQLite (adjust based on source format)
# Place the resulting .sqlite file at the expected path:
mv dashboard.sqlite backend/data/dashboard.sqlite
```

The backend expects the database at `backend/data/dashboard.sqlite`.

### 2. Run the Application

> ⚠️ Note: This is a time-limited SAS URL that expires on 16 April 2026 at 20:15 UTC. Request a refreshed link if access has lapsed.



