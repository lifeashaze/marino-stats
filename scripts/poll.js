import { createClient } from "@libsql/client";

const API_URL =
  process.env.API_URL ||
  "https://goboardapi.azurewebsites.net/api/FacilityCount/GetCountsByAccount?AccountAPIKey=2a2be0d8-df10-4a48-bedd-b3bc0cd628e7";

const TURSO_DATABASE_URL = process.env.TURSO_DATABASE_URL;
const TURSO_AUTH_TOKEN = process.env.TURSO_AUTH_TOKEN;

const JITTER_MAX_SECONDS = Number.parseInt(
  process.env.JITTER_MAX_SECONDS || "0",
  10
);

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function requireEnv(name, value) {
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
}

async function fetchJson(url) {
  const res = await fetch(url, {
    headers: {
      "user-agent": "marino-stats-poller/1.0"
    }
  });

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`API request failed: ${res.status} ${res.statusText} ${text}`);
  }

  return res.json();
}

function toRow(item) {
  const locationName = item?.LocationName;
  const lastCount = item?.LastCount;
  const lastUpdated = item?.LastUpdatedDateAndTime;

  if (!locationName || !lastUpdated) {
    return null;
  }

  const parsedCount = Number.isFinite(Number(lastCount))
    ? Number(lastCount)
    : null;

  if (parsedCount === null) {
    return null;
  }

  return {
    locationName,
    lastCount: parsedCount,
    lastUpdated
  };
}

async function main() {
  requireEnv("TURSO_DATABASE_URL", TURSO_DATABASE_URL);
  requireEnv("TURSO_AUTH_TOKEN", TURSO_AUTH_TOKEN);

  if (JITTER_MAX_SECONDS > 0) {
    const delay = Math.floor(Math.random() * JITTER_MAX_SECONDS * 1000);
    await sleep(delay);
  }

  const data = await fetchJson(API_URL);

  if (!Array.isArray(data)) {
    throw new Error("Unexpected API response: expected an array");
  }

  const rows = data.map(toRow).filter(Boolean);

  if (rows.length === 0) {
    console.log("No valid rows to insert.");
    return;
  }

  const fetchedAt = new Date().toISOString();

  const client = createClient({
    url: TURSO_DATABASE_URL,
    authToken: TURSO_AUTH_TOKEN
  });

  await client.execute(
    "CREATE TABLE IF NOT EXISTS facility_counts (location_name TEXT NOT NULL, last_count INTEGER NOT NULL, last_updated_at TEXT NOT NULL, fetched_at TEXT NOT NULL, PRIMARY KEY (location_name, last_updated_at))"
  );

  const tableInfo = await client.execute("PRAGMA table_info(facility_counts)");
  const hasFetchedAt = tableInfo.rows.some((row) => row.name === "fetched_at");
  if (!hasFetchedAt) {
    await client.execute("ALTER TABLE facility_counts ADD COLUMN fetched_at TEXT");
  }

  const statements = rows.map((row) => ({
    sql: "INSERT OR IGNORE INTO facility_counts (location_name, last_count, last_updated_at, fetched_at) VALUES (?, ?, ?, ?)",
    args: [row.locationName, row.lastCount, row.lastUpdated, fetchedAt]
  }));

  const results = await client.batch(statements);
  const inserted = results.reduce((sum, r) => sum + (r.rowsAffected || 0), 0);

  console.log(`Inserted ${inserted} rows (received ${rows.length}).`);
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
