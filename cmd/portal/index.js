import { boot, columnsOf, INSTANT_COLS, DURATION_COLS, fmtInstant, fmtDuration, fmtMB } from '/chandb.js';

const $ = (id) => document.getElementById(id);
const statusEl = $('status'), sqlEl = $('sql'), runEl = $('run'),
      outEl = $('out'), timingEl = $('timing');

const say = (msg, isErr = false) => {
  statusEl.textContent = msg;
  statusEl.classList.toggle('err', isErr);
  statusEl.setAttribute('aria-busy', !isErr && !msg.startsWith('ready') && !msg.startsWith('No ') ? 'true' : 'false');
};

// Each example declares the table — and optionally the columns — it needs.
// Buttons are hidden unless the published data can actually satisfy them.
//
// This matters because the page and the Parquet deploy independently: the page
// ships in the container image, gold is rebuilt by the pipeline on its own
// cycle. A button referencing a column gold does not have yet is a guaranteed
// Binder Error for whoever clicks it.
const EXAMPLES = [
  { table: 'mbta_vehicle_positions', label: 'Where are the trains now?', sql:
`SELECT route_short_name, trip_headsign, current_status,
       round(latitude, 4)  AS lat,
       round(longitude, 4) AS lon,
       stop_name
FROM mbta_vehicle_positions
WHERE feed_time = (SELECT max(feed_time) FROM mbta_vehicle_positions)
ORDER BY route_type, route_short_name
LIMIT 200;` },

  { table: 'bart_stop_events', label: 'Which trains were late?', sql:
`-- lead_time_s is how long before the predicted arrival the final forecast
-- was made. Small = trustworthy. Filter on it; don't trust every row equally.
SELECT route_long_name, stop_name, trip_headsign,
       arrival_delay AS late_seconds,
       lead_time_s, observations
FROM bart_stop_events
WHERE lead_time_s BETWEEN -300 AND 900
  AND arrival_delay IS NOT NULL
ORDER BY arrival_delay DESC
LIMIT 50;` },

  { table: 'bart_stop_events', label: 'Lateness by hour', sql:
`SELECT hour(last_seen_time)                            AS hour_utc,
       count(*)                                        AS stop_events,
       round(avg(arrival_delay))                       AS avg_late_s,
       max(arrival_delay)                              AS worst_late_s,
       round(100.0 * count(*) FILTER (WHERE arrival_delay > 300)
             / count(*), 1)                            AS pct_over_5min
FROM bart_stop_events
WHERE lead_time_s BETWEEN -300 AND 900
GROUP BY 1
ORDER BY 1;` },

  { table: 'bart_stop_events', label: 'Time between stations', sql:
`-- Predicted runtime for each consecutive pair of stops on a trip.
WITH seg AS (
  SELECT route_long_name, stop_name, arrival_time,
         arrival_time - lag(arrival_time)
           OVER (PARTITION BY trip_id ORDER BY arrival_time) AS seconds,
         lag(stop_name)
           OVER (PARTITION BY trip_id ORDER BY arrival_time) AS from_stop
  FROM bart_stop_events
  WHERE stop_name IS NOT NULL
)
SELECT route_long_name, from_stop, stop_name AS to_stop,
       round(avg(seconds)) AS avg_seconds,
       count(*)            AS trips
FROM seg
WHERE seconds BETWEEN 30 AND 1800
GROUP BY 1, 2, 3
ORDER BY trips DESC
LIMIT 50;` },

  { table: 'bart_stop_events', label: 'Dwell time per station', sql:
`SELECT stop_name,
       round(avg(departure_time - arrival_time)) AS avg_dwell_s,
       count(*)                                  AS n
FROM bart_stop_events
WHERE departure_time IS NOT NULL AND arrival_time IS NOT NULL
  AND departure_time - arrival_time BETWEEN 0 AND 600
GROUP BY 1
HAVING count(*) > 2
ORDER BY avg_dwell_s DESC
LIMIT 40;` },

  { table: 'bart_stop_events', label: 'How good is the data?', sql:
`-- At a 30-minute sampling interval most final predictions are stale.
-- Sampling faster moves rows up this table without growing it.
SELECT CASE
         WHEN lead_time_s <=  120 THEN 'under 2 min  (reliable)'
         WHEN lead_time_s <=  600 THEN '2-10 min'
         WHEN lead_time_s <= 1800 THEN '10-30 min'
         ELSE                          'over 30 min (stale)'
       END        AS forecast_age,
       count(*)   AS stop_events
FROM bart_stop_events
WHERE lead_time_s IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC;` },

  { table: 'septa_vehicle_positions', label: 'SEPTA right now', sql:
`-- Bus, metro and trolley. Regional Rail is septarail_vehicle_positions.
SELECT vehicle_type, route_short_name, trip_headsign,
       round(latitude, 4)  AS lat,
       round(longitude, 4) AS lon,
       stop_name
FROM septa_vehicle_positions
WHERE feed_time = (SELECT max(feed_time) FROM septa_vehicle_positions)
ORDER BY route_type, route_short_name
LIMIT 200;` },

  { table: 'mbta_vehicle_positions', label: 'Vehicle types',
    needs: ['vehicle_type', 'source'], sql:
`-- vehicle_type is GTFS route_type decoded; source records which feed the
-- row was ingested from, so unioned tables stay traceable.
SELECT source, vehicle_type,
       count(DISTINCT route_id)   AS routes,
       count(DISTINCT vehicle_id) AS vehicles,
       count(*)                   AS observations
FROM mbta_vehicle_positions
GROUP BY 1, 2
ORDER BY observations DESC;` },

  { table: 'mbta_vehicle_positions', label: 'Columns', sql:
`DESCRIBE SELECT * FROM mbta_vehicle_positions;` },

  { table: 'mbta_vehicle_positions', label: 'Busiest MBTA routes', sql:
`SELECT route_short_name, route_long_name,
       count(DISTINCT vehicle_id) AS vehicles,
       count(*)                   AS observations
FROM mbta_vehicle_positions
WHERE route_short_name IS NOT NULL
GROUP BY 1, 2
ORDER BY vehicles DESC
LIMIT 25;` },

  { table: 'broadband_tract_coverage', label: 'Measured vs unmeasured tracts', sql:
`-- A tract with no speed test is UNMEASURED, not unserved. Counting it as a
-- zero is the single easiest way to misread this dataset, so the two are
-- separated here rather than averaged together.
SELECT metro,
       count(*)                                     AS tracts,
       count(*) FILTER (WHERE NOT unmeasured)       AS measured,
       count(*) FILTER (WHERE unmeasured)           AS no_test_at_all,
       sum(tests)                                   AS tests,
       round(median(down_mbps_weighted), 1)         AS median_tract_mbps
FROM broadband_tract_coverage
GROUP BY 1
ORDER BY 1;` },

  { table: 'broadband_tract_measured', label: 'Slowest measured tracts', sql:
`-- "Slow" here means slow MEASUREMENTS. It is not evidence that service is
-- unavailable: people may simply be buying cheap plans. The tests column is
-- the confidence signal — a tract resting on 12 tests is not comparable to
-- one resting on 2,000, so it is selected alongside and never hidden.
SELECT tract_name,
       round(down_mbps_weighted, 1)        AS down_mbps,
       round(down_mbps_median, 1)          AS tile_median_mbps,
       tests,
       round(100 * share_tests_over_100, 1) AS pct_tests_over_100,
       CASE WHEN tests >= 1000 THEN 'strong'
            WHEN tests >=  200 THEN 'moderate'
            ELSE                    'thin — treat with care'
       END                                  AS evidence
FROM broadband_tract_measured
ORDER BY down_mbps_weighted
LIMIT 40;` },

  { table: 'broadband_tract_measured', label: 'Does the number rest on few tiles?', sql:
`-- down_mbps_weighted is test-weighted; down_mbps_median is the unweighted
-- median across the ~600m tiles in the tract. A large gap means one busy tile
-- is carrying the headline figure for the whole tract.
SELECT tract_name, tiles, tests,
       round(down_mbps_weighted, 1)                    AS weighted,
       round(down_mbps_median, 1)                      AS tile_median,
       round(down_mbps_weighted - down_mbps_median, 1) AS gap
FROM broadband_tract_measured
WHERE down_mbps_median > 0
ORDER BY abs(down_mbps_weighted - down_mbps_median) DESC
LIMIT 40;` },

  { table: 'broadband_tract_coverage', label: 'Where is measurement thinnest?', sql:
`-- tiles_per_km2 is measurement density, not service density. A dense urban
-- tract with almost no tiles is a measurement gap; a sparse one may just be
-- empty land, which is why land_m2 is shown next to it.
SELECT tract_name, metro,
       round(land_m2 / 1000000.0, 2)  AS land_km2,
       tiles, tests,
       round(tiles_per_km2, 2)        AS tiles_per_km2,
       unmeasured
FROM broadband_tract_coverage
ORDER BY unmeasured DESC, tiles_per_km2
LIMIT 40;` },
];

let available = new Set();
let columns = new Map();   // table -> Set of column names

// 'human' renders local clock times and durations; 'raw' shows the epoch
// seconds the Parquet actually stores, which is what you need when writing
// a query against them.
let timeMode = 'human';

function renderExamples() {
  const box = $('examples');
  box.replaceChildren();
  for (const ex of EXAMPLES) {
    if (!available.has(ex.table)) continue;
    const cols = columns.get(ex.table);
    if (ex.needs && cols && !ex.needs.every(c => cols.has(c))) continue;
    const b = document.createElement('button');
    b.textContent = ex.label;
    b.onclick = () => { sqlEl.value = ex.sql; run(); };
    box.appendChild(b);
  }
}

let conn;

async function init() {
  try {
    const ctx = await boot((loaded, total) => {
      say(loaded < 0
        ? 'engine ready — registering datasets…'
        : `downloading DuckDB engine… ${fmtMB(loaded)} / ${fmtMB(total)} MB (one-time, then cached)`);
    });
    conn = ctx.conn;
    for (const name of ctx.available) {
      available.add(name);
      columns.set(name, await columnsOf(ctx.conn, name));
    }
    if (!available.size) {
      say('No gold data published yet. The pipeline writes it on its next cycle.', true);
      runEl.disabled = true;
      return;
    }
    renderExamples();

    const mb = (ctx.bytes / 1048576).toFixed(1);
    say(`ready — ${[...available].join(', ')} · ${ctx.files} partition(s), ${mb} MB`
      + ` · engine ${(ctx.timings.engineMs / 1000).toFixed(1)}s, data ${(ctx.timings.datasetsMs / 1000).toFixed(1)}s`);
    runEl.disabled = false;
    statusEl.setAttribute('aria-busy', 'false');
    sqlEl.value = (EXAMPLES.find(e => available.has(e.table)) || EXAMPLES[0]).sql;
    await run();
  } catch (e) {
    say('failed to start: ' + e, true);
    console.error(e);
  }
}

function cell(v, col) {
  const td = document.createElement('td');
  if (v === null || v === undefined) { td.textContent = 'NULL'; td.className = 'null'; return td; }
  if (typeof v === 'bigint' || typeof v === 'number') {
    td.className = 'num';
    if (timeMode === 'human' && INSTANT_COLS.has(col)) td.textContent = fmtInstant(v);
    else if (timeMode === 'human' && DURATION_COLS.has(col)) td.textContent = fmtDuration(v);
    else td.textContent = v.toString();
    return td;
  }
  // textContent, never innerHTML: route and stop names are upstream data and
  // must never be able to inject markup into this page.
  td.textContent = String(v);
  return td;
}

async function run() {
  if (!conn) return;
  runEl.disabled = true;
  timingEl.textContent = '';
  const t0 = performance.now();
  try {
    const res = await conn.query(sqlEl.value);
    const rows = res.toArray();
    const cols = res.schema.fields.map(f => f.name);

    outEl.replaceChildren();
    const thead = outEl.createTHead().insertRow();
    for (const c of cols) {
      const th = document.createElement('th');
      th.textContent = c;
      thead.appendChild(th);
    }
    const tbody = outEl.createTBody();
    if (!rows.length) {
      const tr = tbody.insertRow();
      const td = tr.insertCell();
      td.colSpan = cols.length || 1;
      td.className = 'empty';
      td.textContent = 'The query returned no rows.';
    }
    for (const r of rows) {
      const tr = tbody.insertRow();
      const o = r.toJSON();
      for (const c of cols) tr.appendChild(cell(o[c], c));
    }
    timingEl.textContent =
      `${rows.length} row(s) in ${(performance.now() - t0).toFixed(0)} ms`;
    statusEl.classList.remove('err');
  } catch (e) {
    say(String(e), true);
  } finally {
    runEl.disabled = false;
  }
}

$('tmode').onclick = (e) => {
  timeMode = timeMode === 'human' ? 'raw' : 'human';
  e.target.textContent = timeMode === 'human'
    ? 'Times: human-readable' : 'Times: epoch seconds';
  run();
};

runEl.onclick = run;
sqlEl.addEventListener('keydown', (e) => {
  if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') { e.preventDefault(); run(); }
});

renderExamples();
init();
