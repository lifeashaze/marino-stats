# Marino Stats Poller

Polls the Marino occupancy API every 10 minutes using a Cloudflare Worker
Cron Trigger and stores location snapshots in Turso.

## Setup (Cloudflare Workers)

1. Create a Turso database (or use an existing one).
2. Create a Cloudflare Worker and set secrets:
   - `TURSO_DATABASE_URL` (or `TURSO_URL`)
   - `TURSO_AUTH_TOKEN`
   - Example: `npx wrangler secret put TURSO_DATABASE_URL`
   - Example: `npx wrangler secret put TURSO_AUTH_TOKEN`
3. Deploy the worker:
   - `npm install`
   - `npm run deploy`

## Data model

Table: `locations`

- `location_id` (INTEGER, primary key)
- `location_name` (TEXT)
- `facility_name` (TEXT, optional)

Table: `location_counts`

- `location_id` (INTEGER)
- `last_count` (INTEGER)
- `last_updated_at` (TEXT)
- `fetched_at` (TEXT)

Primary key: `(location_id, fetched_at)`

## Notes

- The schedule is configured in `wrangler.toml` as `*/10 * * * *` (UTC).

## Local run (optional)

Create a `.dev.vars` file:

```bash
TURSO_DATABASE_URL=... # or TURSO_URL
TURSO_AUTH_TOKEN=...
```

Then:

```bash
npm install
npm run dev
```
