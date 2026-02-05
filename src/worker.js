import { createClient } from "@libsql/client/web";

const DEFAULT_API_URL =
  "https://goboardapi.azurewebsites.net/api/FacilityCount/GetCountsByAccount?AccountAPIKey=2a2be0d8-df10-4a48-bedd-b3bc0cd628e7";

function requireValue(key, value) {
  if (!value) {
    throw new Error(`Missing required environment variable: ${key}`);
  }
}

function toRow(item) {
  const locationId = Number(item?.LocationId);
  const locationName = item?.LocationName?.trim();
  const facilityName = item?.FacilityName?.trim() || null;
  const lastCount = Number(item?.LastCount);
  const lastUpdated = item?.LastUpdatedDateAndTime;

  if (!Number.isFinite(locationId)) return null;
  if (!locationName) return null;
  if (!Number.isFinite(lastCount)) return null;
  if (!lastUpdated) return null;

  return {
    locationId,
    locationName,
    facilityName,
    lastCount,
    lastUpdated
  };
}

async function fetchJson(url) {
  const res = await fetch(url, {
    headers: {
      "user-agent": "marino-stats-worker/1.0"
    }
  });

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`API request failed: ${res.status} ${res.statusText} ${text}`);
  }

  return res.json();
}

async function ensureSchema(client) {
  await client.execute(
    "CREATE TABLE IF NOT EXISTS locations (location_id INTEGER PRIMARY KEY, location_name TEXT NOT NULL, facility_name TEXT)"
  );
  await client.execute(
    "CREATE TABLE IF NOT EXISTS location_counts (location_id INTEGER NOT NULL, last_count INTEGER NOT NULL, last_updated_at TEXT NOT NULL, fetched_at TEXT NOT NULL, PRIMARY KEY (location_id, fetched_at))"
  );
}

async function upsertLocations(client, rows) {
  const statements = rows.map((row) => ({
    sql: "INSERT INTO locations (location_id, location_name, facility_name) VALUES (?, ?, ?) ON CONFLICT(location_id) DO UPDATE SET location_name = excluded.location_name, facility_name = excluded.facility_name",
    args: [row.locationId, row.locationName, row.facilityName]
  }));

  if (statements.length > 0) {
    await client.batch(statements);
  }
}

async function insertCounts(client, rows, fetchedAt) {
  const statements = rows.map((row) => ({
    sql: "INSERT OR IGNORE INTO location_counts (location_id, last_count, last_updated_at, fetched_at) VALUES (?, ?, ?, ?)",
    args: [row.locationId, row.lastCount, row.lastUpdated, fetchedAt]
  }));

  if (statements.length === 0) {
    return { inserted: 0, received: 0 };
  }

  const results = await client.batch(statements);
  const inserted = results.reduce((sum, r) => sum + (r.rowsAffected || 0), 0);
  return { inserted, received: statements.length };
}

export default {
  async scheduled(_event, env, ctx) {
    ctx.waitUntil(run(env));
  }
};

async function run(env) {
  const tursoUrl = env.TURSO_DATABASE_URL || env.TURSO_URL;
  const tursoToken = env.TURSO_AUTH_TOKEN;

  requireValue("TURSO_DATABASE_URL (or TURSO_URL)", tursoUrl);
  requireValue("TURSO_AUTH_TOKEN", tursoToken);

  const apiUrl = env.API_URL || DEFAULT_API_URL;
  const fetchedAt = new Date().toISOString();

  const data = await fetchJson(apiUrl);
  if (!Array.isArray(data)) {
    throw new Error("Unexpected API response: expected an array");
  }

  const rows = data.map(toRow).filter(Boolean);
  if (rows.length === 0) {
    console.log("No valid rows to insert.");
    return;
  }

  const client = createClient({
    url: tursoUrl,
    authToken: tursoToken
  });

  await ensureSchema(client);
  await upsertLocations(client, rows);
  const { inserted, received } = await insertCounts(client, rows, fetchedAt);

  console.log(`Inserted ${inserted} rows (received ${received}).`);
}
