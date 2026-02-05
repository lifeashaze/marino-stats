# Marino Stats Poller

Polls the Marino occupancy API every 10 minutes via GitHub Actions and stores
only `LocationName`, `LastCount`, and `LastUpdatedDateAndTime` in Turso.

## Setup

1. Create a Turso database (or use an existing one).
2. Add GitHub Actions secrets in this repo:
   - `TURSO_DATABASE_URL`
   - `TURSO_AUTH_TOKEN`
3. (Optional) Trigger the workflow manually from the Actions tab to test.

## Data model

Table: `facility_counts`

- `location_name` (TEXT)
- `last_count` (INTEGER)
- `last_updated_at` (TEXT)

Primary key: `(location_name, last_updated_at)`

## Local run (optional)

```bash
export TURSO_DATABASE_URL="..."
export TURSO_AUTH_TOKEN="..."
npm install
npm run poll
```
# marino-stats
