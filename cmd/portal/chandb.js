// Shared browser-side runtime for every chandb page.
//
// The query engine runs here, in the visitor's tab — the server is a static
// file host. boot() brings DuckDB-WASM up, registers each published Parquet
// partition by URL, and exposes one view per dataset.
//
// Both pages import this rather than duplicating the setup, so a change to
// how datasets are registered lands everywhere at once.

import * as duckdb from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.32.0/+esm';

export async function boot() {
  const bundles = duckdb.getJsDelivrBundles();
  const bundle = await duckdb.selectBundle(bundles);

  // The worker is built from a blob: browsers refuse to load a cross-origin
  // script directly as a Worker.
  const workerURL = URL.createObjectURL(new Blob(
    [`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' }));

  const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), new Worker(workerURL));
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  URL.revokeObjectURL(workerURL);

  const conn = await db.connect();
  const sets = await (await fetch('/api/datasets')).json();

  const available = new Set();
  let bytes = 0, files = 0;

  for (const ds of sets) {
    const names = [];
    for (const url of ds.files) {
      const name = url.replace(/^\/data\//, '').replace(/\//g, '_');
      // HTTP protocol means range requests: a selective query fetches only
      // the row groups and column chunks it touches, not whole files.
      await db.registerFileURL(name, new URL(url, location.href).href,
                               duckdb.DuckDBDataProtocol.HTTP, false);
      names.push(name);
    }
    const list = names.map(n => `'${n}'`).join(', ');
    // Dataset names come from directory names on our own disk; the server
    // only ever exposes the gold tree.
    await conn.query(`CREATE OR REPLACE VIEW ${ds.name} AS
                      SELECT * FROM read_parquet([${list}])`);
    available.add(ds.name);
    bytes += ds.bytes;
    files += ds.files.length;
  }

  return { db, conn, available, bytes, files };
}

// columnsOf returns the column names of a view as a Set.
//
// The pages ship inside the container image; the Parquet they read is rebuilt
// independently by the pipeline. The two are therefore ALWAYS briefly out of
// step on deploy, and a page that assumes a column exists breaks for everyone
// until gold catches up. Ask first, then build SQL to match.
export async function columnsOf(conn, table) {
  const res = await conn.query(`DESCRIBE SELECT * FROM ${table}`);
  return new Set(res.toArray().map(r => r.toJSON().column_name));
}

// ---------------------------------------------------------------------------
// Time formatting
//
// Parquet stores epoch seconds. That is what you need when writing a query and
// useless when reading a departure board, hence the toggle on both pages.
// ---------------------------------------------------------------------------

export const INSTANT_COLS = new Set([
  'arrival_time', 'departure_time', 'last_seen', 'feed_timestamp',
  'vehicle_timestamp',
]);

export const DURATION_COLS = new Set([
  'seconds_away', 'lead_time_s', 'arrival_delay', 'departure_delay',
  'avg_seconds', 'seconds', 'avg_dwell_s', 'late_seconds',
  'avg_late_s', 'worst_late_s',
]);

export function fmtDuration(sec) {
  const n = Number(sec);
  if (!Number.isFinite(n)) return String(sec);
  const sign = n < 0 ? '-' : '';
  const a = Math.abs(Math.trunc(n));
  if (a < 60) return `${sign}${a}s`;
  const m = Math.floor(a / 60);
  if (a < 3600) {
    const rest = a % 60;
    return rest ? `${sign}${m}m ${rest}s` : `${sign}${m}m`;
  }
  return `${sign}${Math.floor(a / 3600)}h ${Math.floor((a % 3600) / 60)}m`;
}

export function fmtInstant(sec) {
  const n = Number(sec);
  if (!Number.isFinite(n) || n <= 0) return String(sec);
  return new Date(n * 1000).toLocaleTimeString();
}

// fmtEta turns a signed second count into board language.
export function fmtEta(sec) {
  const n = Number(sec);
  if (!Number.isFinite(n)) return String(sec);
  if (n <= -60) return `${fmtDuration(-n)} ago`;
  if (n < 60) return 'due';
  return `in ${fmtDuration(n)}`;
}

// GTFS route_type as published, for lakes whose gold predates vehicle_type.
export function typeFromRouteType(n) {
  return ({ 0: 'Light Rail', 1: 'Subway', 2: 'Commuter Rail', 3: 'Bus',
            4: 'Ferry', 5: 'Cable Tram', 6: 'Aerial Lift', 7: 'Funicular',
            11: 'Trolleybus', 12: 'Monorail' })[Number(n)];
}

// GTFS route_type, decoded for display.
export function vehicleIcon(type) {
  switch (type) {
    case 'Subway': return '\u{1F687}';
    case 'Light Rail': return '\u{1F68B}';
    case 'Commuter Rail': return '\u{1F686}';
    case 'Bus': return '\u{1F68C}';
    case 'Ferry': return '\u{26F4}';
    default: return '\u{1F68F}';
  }
}
