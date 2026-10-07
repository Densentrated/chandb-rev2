// Shared browser-side runtime for every chandb page.
//
// The query engine runs here, in the visitor's tab — the server is a static
// file host. boot() brings DuckDB-WASM up, registers each published Parquet
// partition by URL, and exposes one view per dataset.
//
// Both pages import this rather than duplicating the setup, so a change to
// how datasets are registered lands everywhere at once.

import * as duckdb from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.32.0/+esm';

// boot(onProgress) brings the engine up.
//
// The DuckDB-WASM bundle is ~34MB. That is a one-time, browser-cached cost,
// but it is long enough on a slow link that a silent page looks hung — so
// instantiate reports download progress and every caller surfaces it.
export async function boot(onProgress) {
  // Phase timings, so a slow start says WHICH part was slow. The engine
  // bundle is ~34MB and dominates a cold load; dataset registration is a
  // handful of HTTP range requests. Without this split, every slow boot looks
  // identical and is diagnosed by guesswork.
  const t = { start: performance.now() };

  const bundles = duckdb.getJsDelivrBundles();
  const bundle = await duckdb.selectBundle(bundles);

  // The worker is built from a blob: browsers refuse to load a cross-origin
  // script directly as a Worker.
  const workerURL = URL.createObjectURL(new Blob(
    [`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' }));

  const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), new Worker(workerURL));
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker, (p) => {
    // Absent when the bundle comes from cache, so guard rather than assume.
    if (onProgress && p && p.bytesTotal) {
      onProgress(p.bytesLoaded, p.bytesTotal);
    }
  });
  URL.revokeObjectURL(workerURL);

  t.engine = performance.now();

  const conn = await db.connect();
  if (onProgress) onProgress(-1, -1);   // engine up; registering datasets now
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

  t.datasets = performance.now();
  const timings = {
    engineMs:   Math.round(t.engine - t.start),
    datasetsMs: Math.round(t.datasets - t.engine),
  };
  console.info('chandb boot', timings);

  return { db, conn, available, bytes, files, timings };
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
// DuckDB-WASM returns every BIGINT column as a JS BigInt. BigInt does not mix
// with Number in arithmetic, and — the subtle one — Array.sort coerces its
// comparator's return value to a Number, so a comparator that subtracts two
// BigInts throws "Cannot convert a BigInt value to a number".
//
// Epoch seconds and durations are far inside Number's safe integer range, so
// normalise on the way out of a query rather than defending at each use site.
export function num(v) {
  return typeof v === 'bigint' ? Number(v) : v;
}

// rowsOf converts an Arrow result to plain objects with BigInts flattened.
export function rowsOf(res) {
  return res.toArray().map(r => {
    const o = r.toJSON();
    for (const k of Object.keys(o)) o[k] = num(o[k]);
    return o;
  });
}

export function fmtMB(n) { return (n / 1048576).toFixed(1); }

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
