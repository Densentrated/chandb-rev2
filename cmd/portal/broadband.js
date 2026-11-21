// /broadband — what the speed tests say, and how much they are worth.
//
// The honesty constraint drives every rendering decision in this file, so it is
// stated once here rather than repeated at each use site:
//
//   1. A measured speed is NOT availability. Ookla records what somebody got on
//      the plan they already bought. Nothing on this page may let a reader slide
//      from "low number" to "bad internet here", so the word "measured" is
//      attached to every figure and the claimed-coverage caveat sits ABOVE the
//      data in broadband.html rather than under it.
//   2. An unmeasured tract is not a slow tract. It is never drawn as 0, never
//      drawn as blank, and never drawn from the speed ramp. "No data" is always
//      faint ink plus a dashed or hollow edge — a shape, not a value — so the
//      two cannot be confused even at a glance.
//   3. `tests` is the only confidence signal this dataset carries, and it varies
//      by two orders of magnitude between tracts. Every figure is shown with its
//      evidence tier, thin-evidence rows are hatched and dimmed, and one whole
//      panel exists purely to show how far a sparsely-tested tract can wander.
//
// All ~2,000 tract rows are pulled once and filtered in memory. That is a few
// hundred kilobytes, and it makes the metro and evidence controls instant
// instead of a round trip through DuckDB for every click.

import { boot, columnsOf, rowsOf, fmtMB } from '/chandb.js';

const $ = (id) => document.getElementById(id);
const statusEl = $('status');

const say = (text, error = false) => {
  statusEl.textContent = text;
  statusEl.classList.toggle('err', error);
  statusEl.setAttribute('aria-busy', error ? 'false' : 'true');
};

const COVERAGE = 'broadband_tract_coverage';
const MEASURED = 'broadband_tract_measured';

// Mirrors the :root tokens in portal.css. A canvas takes colour strings, not
// custom properties, so these are duplicated deliberately — if the palette
// moves there, it has to move here too.
//
// The assignment below is an invariant, not a preference, and the verify
// harness reads pixels back against it:
//
//   saturated ramp colours  →  a measured tract, and nothing else
//   INK.dim (bright, grey)  →  "no data": the hollow rings and the threshold
//                              annotations, never a value on the speed axis
//   INK.line / INK.faint    →  graticule and axis furniture
//
// So annotation deliberately does NOT use --accent here, even where it would
// look good: an accent-coloured benchmark line is indistinguishable from
// plotted data both to a reader skimming and to a pixel check.
const INK = {
  panel: '#101014',   // --bg-raise, matches the .chart / .tmap background
  line: '#22222a',    // --line
  faint: '#52677a',   // --ink-faint
  dim: '#91a6b8',     // --ink-dim
};

// Evidence tiers. The cuts are judgement rather than statistics: across a
// published quarter the typical tract carries a few hundred tests, so 200
// separates "a neighbourhood sample" from "a handful of households", and 1,000
// marks a tract dense enough that one unusual connection cannot move the mean.
// They are deliberately coarse — the point is to stop an unqualified number
// sitting next to a well-supported one, not to imply a confidence interval.
const TIERS = [
  { id: 'strong', label: 'Strong', min: 1000, short: '1,000+ tests' },
  { id: 'moderate', label: 'Moderate', min: 200, short: '200–999 tests' },
  { id: 'thin', label: 'Thin', min: 1, short: 'under 200 tests' },
];

// null means unmeasured, which is a different kind of thing from "thin" and is
// never folded into the weakest tier.
const tierOf = (tests) => TIERS.find(t => Number(tests) >= t.min) ?? null;

// Bin edges in Mbps. 100 is the FCC's current benchmark for "broadband" and 25
// is the one it replaced; both stay as edges so the chart can be read against
// the thresholds policy actually uses rather than against round numbers.
const BINS = [0, 25, 50, 100, 200, 400, 800, Infinity];
const BENCHMARK = 100;

// The top of the colour ramp. Fixed rather than data-derived so that a tract
// keeps its colour when the metro filter changes — a dot that turns from
// magenta to cyan because the selection changed would be a lie.
const SPEED_FULL = 1000;

const METRO_LABELS = { boston: 'Boston', bayarea: 'Bay Area' };

// Slow is magenta, fast is cyan — the same hsl(190..330 88% 64%) family /maps
// uses, so the two pages read as one system. Unmeasured never enters this
// function; it has no ratio to pass in.
function speedColor(mbps, alpha = 1) {
  const ratio = Math.max(0, Math.min(1, Number(mbps) / SPEED_FULL));
  return `hsl(${Math.round(330 - ratio * 140)} 88% 64% / ${alpha})`;
}

const fmtInt = (n) => Number(n).toLocaleString();
const fmtMbps = (v) => (v === null || v === undefined || !Number.isFinite(Number(v)))
  ? null : Number(v).toFixed(Number(v) >= 100 ? 0 : 1);
const fmtPct = (f) => (f === null || f === undefined || !Number.isFinite(Number(f)))
  ? null : `${(Number(f) * 100).toFixed(Number(f) >= 0.1 ? 0 : 1)}%`;

// ---------------------------------------------------------------- the query
//
// Optional enrichments. FCC claimed coverage and ACS demographics need
// credentials that do not exist yet, and the page and the Parquet deploy
// independently anyway, so each enrichment declares what it would contribute
// and simply drops out when its table — or an individual column — is absent.
// index.html does the same thing with its `needs` list; the alternative is a
// Binder Error for every visitor until gold catches up.
const ENRICHMENTS = [
  {
    table: MEASURED, key: 'geoid',
    columns: ['down_mbps_median', 'up_mbps_weighted', 'latency_ms_weighted',
      'share_tests_over_100', 'share_tests_over_25',
      'down_mbps_min', 'down_mbps_max', 'src_year', 'src_quarter'],
  },
  // Not in gold yet — see broadbandTables() in cmd/pipeline/broadband.go. Listed
  // so the columns appear the day the credentials land, with no change here.
  {
    table: 'broadband_tract_claimed', key: 'geoid',
    columns: ['claimed_fiber_share', 'claimed_100_20_share'],
  },
  {
    table: 'broadband_tract_demographics', key: 'geoid',
    columns: ['median_household_income', 'households', 'broadband_subscription_share'],
  },
];

const BASE_COLUMNS = ['geoid', 'tract_name', 'metro', 'state_fips', 'county_fips',
  'centroid_lat', 'centroid_lon', 'land_m2', 'tiles', 'tests',
  'down_mbps_weighted', 'unmeasured', 'tiles_per_km2'];

async function buildQuery(conn, available) {
  const base = await columnsOf(conn, COVERAGE);
  const picked = BASE_COLUMNS.filter(c => base.has(c));
  const select = picked.map(c => `c.${c}`);
  const have = new Set(picked);

  let n = 0;
  const joins = [];
  for (const e of ENRICHMENTS) {
    if (!available.has(e.table)) continue;
    const cols = await columnsOf(conn, e.table);
    if (!cols.has(e.key)) continue;
    const take = e.columns.filter(c => cols.has(c) && !have.has(c));
    if (!take.length) continue;
    const alias = `e${n++}`;
    joins.push(`LEFT JOIN ${e.table} ${alias} ON ${alias}.${e.key} = c.${e.key}`);
    for (const c of take) { select.push(`${alias}.${c}`); have.add(c); }
  }
  return {
    sql: `SELECT ${select.join(', ')} FROM ${COVERAGE} c ${joins.join(' ')}`,
    columns: have,
  };
}

// ------------------------------------------------------------------- canvas

// surface prepares a host element's canvas at device resolution and paints the
// panel background, so every drawing routine starts from a known state.
function surface(host) {
  const canvas = document.createElement('canvas');
  host.replaceChildren(canvas);
  const w = host.clientWidth || 440, h = host.clientHeight || 300;
  const scale = window.devicePixelRatio || 1;
  canvas.width = Math.max(1, Math.round(w * scale));
  canvas.height = Math.max(1, Math.round(h * scale));
  const ctx = canvas.getContext('2d');
  ctx.scale(scale, scale);
  ctx.fillStyle = INK.panel;
  ctx.fillRect(0, 0, w, h);
  ctx.font = '10px ui-monospace, monospace';
  return { ctx, w, h };
}

// niceMax rounds an axis top up to 1/2/5 × a power of ten so the tick labels
// are readable numbers rather than whatever the maximum row happened to be.
function niceMax(v) {
  if (!Number.isFinite(v) || v <= 0) return 1;
  const pow = 10 ** Math.floor(Math.log10(v));
  for (const step of [1, 2, 2.5, 5, 10]) {
    if (v <= step * pow) return step * pow;
  }
  return 10 * pow;
}

// drawEvidenceChart plots tests (log x) against measured speed (linear y).
//
// This panel is the low-n argument made visual rather than asserted in prose:
// the left edge fans out across the whole speed axis because almost nothing was
// measured there, and the right edge converges. A reader who only looks at the
// ranking table can still see here why its top rows deserve less weight.
function drawEvidenceChart(host, rows) {
  const { ctx, w, h } = surface(host);
  const plotted = rows.filter(r => !r.unmeasured && r.down_mbps_weighted !== null
    && Number(r.tests) > 0);
  if (!plotted.length) {
    ctx.fillStyle = INK.faint;
    ctx.fillText('No measured tracts in this selection.', 12, 20);
    return 0;
  }

  // T leaves a clear row for the "Mbps" unit above the topmost tick label, and
  // B leaves one between the decade labels and the axis caption — stacked, the
  // two read as a single string ("1,000 tests") that means nothing.
  const L = 48, R = 14, T = 22, B = 34;
  const tests = plotted.map(r => Math.max(1, Number(r.tests)));
  let lo = Math.log10(Math.min(...tests)), hi = Math.log10(Math.max(...tests));
  // A selection whose tracts all carry a similar test count would otherwise
  // collapse the x domain to a single point and divide by zero.
  if (hi - lo < 1) { const c = (lo + hi) / 2; lo = c - 0.6; hi = c + 0.6; }
  const top = niceMax(Math.max(...plotted.map(r => Number(r.down_mbps_weighted))));

  const px = (t) => L + (Math.log10(Math.max(1, t)) - lo) / (hi - lo) * (w - L - R);
  const py = (v) => T + (1 - Math.min(1, Number(v) / top)) * (h - T - B);

  ctx.strokeStyle = INK.line;
  ctx.lineWidth = 1;
  ctx.fillStyle = INK.faint;
  for (let d = Math.ceil(lo); d <= hi; d++) {
    const x = px(10 ** d);
    ctx.beginPath(); ctx.moveTo(x, T); ctx.lineTo(x, h - B); ctx.stroke();
    ctx.fillText(fmtInt(10 ** d), x + 3, h - B + 13);
  }
  for (let i = 0; i <= 4; i++) {
    const v = top * i / 4, y = py(v);
    ctx.beginPath(); ctx.moveTo(L, y); ctx.lineTo(w - R, y); ctx.stroke();
    ctx.fillText(String(Math.round(v)), 6, y + 3);
  }
  const caption = 'tests run (log scale)';
  ctx.fillText(caption, w - R - ctx.measureText(caption).width, h - 5);
  ctx.fillText('Mbps', 6, T - 11);

  // The evidence tiers as vertical rules, and the FCC benchmark as a
  // horizontal one: the two thresholds a reader should judge a dot against.
  ctx.setLineDash([4, 3]);
  ctx.strokeStyle = INK.dim;
  for (const t of [TIERS[1].min, TIERS[0].min]) {
    const x = px(t);
    if (x < L || x > w - R) continue;
    ctx.beginPath(); ctx.moveTo(x, T); ctx.lineTo(x, h - B); ctx.stroke();
    ctx.fillStyle = INK.dim;
    // Flip the label to the inside of the rule near the right edge; a clipped
    // "1,000 te" is worse than no label at all.
    const text = `${fmtInt(t)} tests`;
    const tw = ctx.measureText(text).width;
    ctx.fillText(text, x + 5 + tw > w - R ? x - 5 - tw : x + 5, T + 11);
  }
  if (BENCHMARK <= top) {
    const y = py(BENCHMARK);
    ctx.beginPath(); ctx.moveTo(L, y); ctx.lineTo(w - R, y); ctx.stroke();
    ctx.fillStyle = INK.dim;
    ctx.fillText(`${BENCHMARK} Mbps benchmark`, L + 6, y - 4);
  }
  ctx.setLineDash([]);

  for (const r of plotted) {
    const x = px(Number(r.tests)), y = py(r.down_mbps_weighted);
    if (!Number.isFinite(x) || !Number.isFinite(y)) continue;
    // Thin evidence is drawn hollow. A hollow dot reads as "provisional"
    // without needing a legend, and it keeps the solid dots legible.
    const thin = Number(r.tests) < TIERS[1].min;
    ctx.beginPath();
    ctx.arc(x, y, thin ? 3 : 3.4, 0, Math.PI * 2);
    if (thin) {
      ctx.strokeStyle = speedColor(r.down_mbps_weighted, 0.85);
      ctx.lineWidth = 1.3;
      ctx.stroke();
    } else {
      ctx.fillStyle = speedColor(r.down_mbps_weighted, 0.85);
      ctx.fill();
    }
  }
  ctx.strokeStyle = INK.line;
  ctx.lineWidth = 1;
  ctx.strokeRect(0.5, 0.5, w - 1, h - 1);
  return plotted.length;
}

// gridStep picks a graticule interval that yields a handful of lines over the
// span, whether that span is a county or a nine-county metro.
function gridStep(span) {
  for (const step of [0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1]) {
    if (span / step <= 6) return step;
  }
  return 2;
}

// drawTractMap plots tract centroids for one metro.
//
// Centroids rather than polygons because gold publishes centroid_lat/lon and
// not geometry_wkt — a real choropleth needs the boundary column added to the
// gold table first. The encoding is chosen so the two failure modes of this
// domain cannot be confused: a measured tract is a FILLED dot on the speed
// ramp, an unmeasured tract is a HOLLOW faint ring with no ramp colour at all.
function drawTractMap(host, rows) {
  const { ctx, w, h } = surface(host);
  const located = rows.filter(r =>
    Number.isFinite(Number(r.centroid_lat)) && Number.isFinite(Number(r.centroid_lon)));
  if (!located.length) {
    ctx.fillStyle = INK.faint;
    ctx.fillText('No tracts in this selection.', 12, 20);
    return { drawn: 0, unmeasured: 0 };
  }

  const lats = located.map(r => Number(r.centroid_lat));
  const lons = located.map(r => Number(r.centroid_lon));
  let [minLat, maxLat] = [Math.min(...lats), Math.max(...lats)];
  let [minLon, maxLon] = [Math.min(...lons), Math.max(...lons)];
  // Tract centroids can share a longitude exactly (the fixtures do), which
  // would collapse the projection onto a line and divide by zero.
  const pad = (a, b, floor) => {
    const span = Math.max(b - a, floor), mid = (a + b) / 2;
    return [mid - span * 0.6, mid + span * 0.6];
  };
  [minLon, maxLon] = pad(minLon, maxLon, 0.02);
  [minLat, maxLat] = pad(minLat, maxLat, 0.02);

  const project = (lon, lat) => [
    ((lon - minLon) / (maxLon - minLon)) * w,
    ((maxLat - lat) / (maxLat - minLat)) * h,
  ];

  ctx.strokeStyle = INK.line;
  ctx.lineWidth = 1;
  ctx.fillStyle = INK.faint;
  const lonStep = gridStep(maxLon - minLon), latStep = gridStep(maxLat - minLat);
  for (let lon = Math.ceil(minLon / lonStep) * lonStep; lon <= maxLon; lon += lonStep) {
    const [x] = project(lon, minLat);
    ctx.beginPath(); ctx.moveTo(x, 0); ctx.lineTo(x, h); ctx.stroke();
    ctx.fillText(`${lon.toFixed(2)}°`, x + 4, h - 7);
  }
  for (let lat = Math.ceil(minLat / latStep) * latStep; lat <= maxLat; lat += latStep) {
    const [, y] = project(minLon, lat);
    ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(w, y); ctx.stroke();
    ctx.fillText(`${lat.toFixed(2)}°`, 5, Math.max(11, y - 4));
  }

  const maxTests = Math.max(1, ...located.map(r => Number(r.tests) || 0));
  let unmeasured = 0;
  // Unmeasured rings are drawn FIRST so a dense cluster of measured dots can
  // never bury them, and in INK.dim rather than INK.faint so they are plainly
  // visible rather than apologetic. The ring is hollow and off the speed ramp
  // entirely: there is no colour it could share with a slow tract.
  for (const r of located) {
    if (!r.unmeasured && r.down_mbps_weighted !== null) continue;
    unmeasured++;
    const [x, y] = project(Number(r.centroid_lon), Number(r.centroid_lat));
    ctx.strokeStyle = INK.dim;
    ctx.lineWidth = 1.5;
    ctx.beginPath(); ctx.arc(x, y, 5.5, 0, Math.PI * 2); ctx.stroke();
  }
  for (const r of located) {
    if (r.unmeasured || r.down_mbps_weighted === null) continue;
    const [x, y] = project(Number(r.centroid_lon), Number(r.centroid_lat));
    const radius = 2.5 + 4 * Math.sqrt((Number(r.tests) || 0) / maxTests);
    ctx.fillStyle = speedColor(r.down_mbps_weighted, 0.9);
    ctx.beginPath(); ctx.arc(x, y, radius, 0, Math.PI * 2); ctx.fill();
  }

  ctx.strokeStyle = INK.line;
  ctx.lineWidth = 1;
  ctx.strokeRect(0.5, 0.5, w - 1, h - 1);
  return { drawn: located.length, unmeasured };
}

// ------------------------------------------------------------------ the page

let rows = [];
let columns = new Set();
let metroFilter = null;      // null = every metro
let evidenceFloor = 0;       // minimum tests for a tract to enter the table
let quarterNote = '';
let sizeNote = '';

const visible = () => rows.filter(r => !metroFilter || r.metro === metroFilter);

function button(label, on, onclick) {
  const b = document.createElement('button');
  b.type = 'button';
  b.textContent = label;
  b.className = on ? 'on' : '';
  b.setAttribute('aria-pressed', String(!!on));
  b.onclick = onclick;
  return b;
}

function renderMetroFilter() {
  const box = $('metros');
  const metros = [...new Set(rows.map(r => r.metro).filter(Boolean))].sort();
  const label = box.firstElementChild;          // the "Metro" caption, kept
  box.replaceChildren(...(label ? [label] : []));
  if (metros.length < 2) return;
  box.appendChild(button('All', metroFilter === null,
    () => { metroFilter = null; renderAll(); }));
  for (const m of metros) {
    box.appendChild(button(METRO_LABELS[m] ?? m, metroFilter === m,
      () => { metroFilter = m; renderAll(); }));
  }
}

function renderEvidenceFilter() {
  const box = $('evidence');
  const label = box.firstElementChild;          // the "Evidence floor" caption
  box.replaceChildren(...(label ? [label] : []));
  const options = [
    ['Any', 0],
    [`${fmtInt(TIERS[1].min)}+ tests`, TIERS[1].min],
    [`${fmtInt(TIERS[0].min)}+ tests`, TIERS[0].min],
  ];
  for (const [label, min] of options) {
    box.appendChild(button(label, evidenceFloor === min,
      () => { evidenceFloor = min; renderEvidenceFilter(); renderTable(); }));
  }
}

function renderLedger() {
  const set = visible();
  const measured = set.filter(r => !r.unmeasured);
  const unmeasured = set.length - measured.length;
  const tests = measured.reduce((a, r) => a + (Number(r.tests) || 0), 0);

  $('tile-tracts').querySelector('.n').textContent = fmtInt(set.length);
  $('tile-measured').querySelector('.n').textContent = fmtInt(measured.length);
  // Rendered as a count of tracts with a "no test" label rather than as a
  // percentage of anything, so it cannot be read as a score.
  $('tile-unmeasured').querySelector('.n').textContent = fmtInt(unmeasured);
  $('tile-tests').querySelector('.n').textContent = fmtInt(tests);

  const thin = measured.filter(r => Number(r.tests) < TIERS[1].min).length;
  $('tile-tests').querySelector('.note').textContent = thin
    ? `Total tests behind this page. ${fmtInt(thin)} tract(s) rest on fewer than ${fmtInt(TIERS[1].min)}.`
    : 'Total tests behind every number on this page.';
}

function renderDistribution() {
  const set = visible();
  const measured = set.filter(r => !r.unmeasured && r.down_mbps_weighted !== null);
  const unmeasured = set.length - measured.length;

  // One bucket per bin, each split by evidence tier. Splitting rather than
  // totalling is the whole point: it shows WHERE the thin evidence sits, which
  // a single-colour histogram hides completely.
  const buckets = BINS.slice(0, -1).map((lo, i) => ({
    lo, hi: BINS[i + 1], counts: Object.fromEntries(TIERS.map(t => [t.id, 0])), total: 0,
  }));
  for (const r of measured) {
    const v = Number(r.down_mbps_weighted);
    const b = buckets.find(x => v >= x.lo && v < x.hi) ?? buckets[buckets.length - 1];
    const tier = tierOf(r.tests) ?? TIERS[TIERS.length - 1];
    b.counts[tier.id]++;
    b.total++;
  }
  const peak = Math.max(1, ...buckets.map(b => b.total));

  const box = $('dist');
  box.replaceChildren();
  for (const b of buckets) {
    const row = document.createElement('div');
    row.className = 'dist-row';

    const label = document.createElement('span');
    label.className = 'label';
    label.textContent = b.hi === Infinity
      ? `${fmtInt(b.lo)}+ Mbps`
      : `${fmtInt(b.lo)}–${fmtInt(b.hi)}`;
    if (b.lo === BENCHMARK) label.classList.add('dist-bench');

    const track = document.createElement('div');
    track.className = 'bar-track';
    const bar = document.createElement('div');
    bar.className = 'bar';
    bar.style.width = `${(b.total / peak) * 100}%`;
    for (const tier of TIERS) {
      const n = b.counts[tier.id];
      if (!n) continue;
      const seg = document.createElement('span');
      seg.className = `seg ${tier.id}`;
      seg.style.width = `${(n / b.total) * 100}%`;
      seg.title = `${fmtInt(n)} tract(s), ${tier.label.toLowerCase()} evidence (${tier.short})`;
      bar.appendChild(seg);
    }
    track.appendChild(bar);

    const n = document.createElement('span');
    n.className = 'n';
    n.textContent = fmtInt(b.total);

    row.append(label, track, n);
    box.appendChild(row);
  }

  // Deliberately detached from the bars above. An unmeasured tract has no
  // position on a speed axis, so putting it in a leftmost "0" bin would be the
  // exact misreading this page exists to prevent.
  const note = $('dist-unmeasured');
  note.textContent = unmeasured
    ? `Plus ${fmtInt(unmeasured)} tract(s) with no speed test — off this axis entirely, because "unmeasured" is not a speed. Listed in full at the foot of the page.`
    : 'Every tract in this selection carries at least one speed test.';

  const legend = $('dist-legend');
  legend.replaceChildren();
  for (const tier of TIERS) {
    const item = document.createElement('span');
    const swatch = document.createElement('i');
    swatch.className = `swatch seg ${tier.id}`;
    const text = document.createElement('span');
    text.textContent = `${tier.label} · ${tier.short}`;
    item.append(swatch, text);
    legend.appendChild(item);
  }
  const nodata = document.createElement('span');
  const ring = document.createElement('i');
  ring.className = 'swatch nodata-swatch';
  const ringText = document.createElement('span');
  ringText.textContent = 'No test · unmeasured';
  nodata.append(ring, ringText);
  legend.appendChild(nodata);

  $('dist-count').textContent = `${fmtInt(measured.length)} measured tract(s)`;
}

function renderCharts() {
  const set = visible();
  const plotted = drawEvidenceChart($('chart-evidence'), set);
  $('chart-count').textContent = plotted
    ? `${fmtInt(plotted)} tract(s) plotted`
    : 'Nothing measured to plot';

  for (const metro of Object.keys(METRO_LABELS)) {
    const host = $(`tracts-${metro}`);
    if (!host) continue;
    // The maps are per metro by definition, so they ignore the metro filter
    // and always show their own tracts.
    const stats = drawTractMap(host, rows.filter(r => r.metro === metro));
    $(`tracts-${metro}-count`).textContent = stats.drawn
      ? `${fmtInt(stats.drawn)} tract(s) · ${fmtInt(stats.unmeasured)} with no test`
      : 'No tracts published';
  }
}

function tierChip(tests) {
  const tier = tierOf(tests);
  const chip = document.createElement('span');
  chip.className = `tier ${tier ? tier.id : 'none'}`;
  chip.textContent = tier ? tier.label : 'No test';
  chip.title = tier ? tier.short : 'no speed test in this tract';
  return chip;
}

function textCell(tr, text, cls) {
  const td = tr.insertCell();
  // textContent, never innerHTML: tract names are upstream Census data.
  td.textContent = text;
  if (cls) td.className = cls;
  return td;
}

const TABLE_MAX = 60;

function renderTable() {
  const set = visible();
  const measured = set.filter(r => !r.unmeasured && r.down_mbps_weighted !== null);
  const eligible = measured.filter(r => Number(r.tests) >= evidenceFloor);
  const excluded = measured.length - eligible.length;
  const ordered = [...eligible].sort(
    (a, b) => Number(a.down_mbps_weighted) - Number(b.down_mbps_weighted));

  const hasMedian = columns.has('down_mbps_median');
  const hasShare = columns.has('share_tests_over_100');
  const headers = [['Tract'], ['Metro'], ['Measured down', true]];
  if (hasMedian) headers.push(['Tile median', true], ['Gap (Mbps)', true]);
  if (hasShare) headers.push(['Tests ≥100 Mbps', true]);
  headers.push(['Tests', true], ['Evidence']);

  const table = $('table');
  const caption = $('table-caption');
  table.replaceChildren(caption);

  const head = table.createTHead().insertRow();
  for (const [label, numeric] of headers) {
    const th = document.createElement('th');
    th.textContent = label;
    // A right-aligned column under a left-aligned heading is unreadable at a
    // glance; the eye has nothing to run down.
    if (numeric) th.className = 'num';
    head.appendChild(th);
  }

  const body = table.createTBody();
  for (const r of ordered.slice(0, TABLE_MAX)) {
    const tr = body.insertRow();
    const tier = tierOf(r.tests);
    // Thin-evidence rows are dimmed in place rather than dropped: hiding them
    // would make the ranking look more certain than it is, and showing them at
    // full weight would put a four-test tract at the top of a funding story.
    if (tier && tier.id === 'thin') tr.className = 'thin';

    textCell(tr, r.tract_name ?? r.geoid);
    textCell(tr, METRO_LABELS[r.metro] ?? r.metro ?? '—');
    textCell(tr, `${fmtMbps(r.down_mbps_weighted)} Mbps`, 'num');
    if (hasMedian) {
      const median = fmtMbps(r.down_mbps_median);
      textCell(tr, median === null ? '—' : `${median} Mbps`, 'num');
      const gap = (r.down_mbps_median === null || r.down_mbps_median === undefined)
        ? null : Number(r.down_mbps_weighted) - Number(r.down_mbps_median);
      textCell(tr, gap === null ? '—' : `${gap > 0 ? '+' : ''}${gap.toFixed(0)}`, 'num');
    }
    if (hasShare) {
      const share = fmtPct(r.share_tests_over_100);
      textCell(tr, share ?? '—', 'num');
    }
    textCell(tr, fmtInt(r.tests), 'num');
    tr.insertCell().appendChild(tierChip(r.tests));
  }

  if (!ordered.length) {
    const tr = body.insertRow();
    const td = tr.insertCell();
    td.colSpan = headers.length;
    td.className = 'empty';
    td.textContent = evidenceFloor
      ? `No tract in this selection carries ${fmtInt(evidenceFloor)} or more tests. That is a statement about the evidence, not about the service.`
      : 'No measured tracts in this selection.';
  }

  const unmeasured = set.length - measured.length;
  const parts = [
    `${fmtInt(Math.min(ordered.length, TABLE_MAX))} of ${fmtInt(ordered.length)} measured tract(s), slowest first`,
  ];
  if (excluded) parts.push(`${fmtInt(excluded)} held back by the evidence floor`);
  if (unmeasured) parts.push(`${fmtInt(unmeasured)} tract(s) have no test at all and are listed separately, never ranked`);
  caption.textContent = `${parts.join(' · ')}.`;
}

function renderUnmeasured() {
  const set = visible().filter(r => r.unmeasured || r.down_mbps_weighted === null);
  const box = $('unmeasured-list');
  box.replaceChildren();
  $('unmeasured-count').textContent = set.length
    ? `${fmtInt(set.length)} tract(s)`
    : 'None in this selection';
  if (!set.length) {
    const none = document.createElement('div');
    none.textContent = 'Every tract in this selection carries at least one speed test.';
    box.appendChild(none);
    return;
  }
  const ordered = [...set].sort((a, b) =>
    String(a.metro ?? '').localeCompare(String(b.metro ?? ''))
    || String(a.tract_name ?? a.geoid).localeCompare(String(b.tract_name ?? b.geoid)));
  for (const r of ordered.slice(0, 150)) {
    const item = document.createElement('div');
    item.className = 'nodata-row';
    item.textContent = `${r.tract_name ?? r.geoid} · ${METRO_LABELS[r.metro] ?? r.metro ?? '—'} · no test`;
    box.appendChild(item);
  }
  if (ordered.length > 150) {
    const more = document.createElement('div');
    more.textContent = `+ ${fmtInt(ordered.length - 150)} more`;
    box.appendChild(more);
  }
}

function renderAll() {
  renderMetroFilter();
  renderLedger();
  renderDistribution();
  renderCharts();
  renderTable();
  renderUnmeasured();

  const set = visible();
  const measured = set.filter(r => !r.unmeasured).length;
  const tests = set.reduce((a, r) => a + (Number(r.tests) || 0), 0);
  const where = metroFilter ? `${METRO_LABELS[metroFilter] ?? metroFilter} · ` : '';
  say(`Ready · ${where}${fmtInt(set.length)} tracts · ${fmtInt(measured)} measured`
    + ` · ${fmtInt(set.length - measured)} with no test · ${fmtInt(tests)} speed tests`
    + `${quarterNote} · measured speed, not claimed coverage${sizeNote}`);
  statusEl.setAttribute('aria-busy', 'false');
}

function unavailable(message) {
  say(message, true);
  for (const id of ['dist-count', 'chart-count', 'unmeasured-count',
    'tracts-boston-count', 'tracts-bayarea-count']) {
    const el = $(id);
    if (el) el.textContent = 'Unavailable';
  }
  $('table-caption').textContent = message;
}

async function run() {
  try {
    const ctx = await boot((loaded, total) => {
      if (total > 0) say(`Loading query engine… ${fmtMB(loaded)} / ${fmtMB(total)} MB (one-time, then cached)`);
      else if (loaded < 0) say('Query engine ready; registering datasets…');
    });
    if (!ctx.available.has(COVERAGE)) {
      unavailable(`No ${COVERAGE} table is published yet. The pipeline writes it once tract geometry and Ookla tiles have both landed.`);
      return;
    }

    const query = await buildQuery(ctx.conn, ctx.available);
    columns = query.columns;
    rows = rowsOf(await ctx.conn.query(query.sql));
    if (!rows.length) {
      unavailable('The broadband tables are published but empty.');
      return;
    }

    // Provenance belongs next to the figures: Ookla publishes quarterly, so a
    // reader needs to know which quarter they are looking at before comparing
    // anything to a number they have elsewhere.
    const stamped = rows.find(r => r.src_year && r.src_quarter);
    if (stamped) quarterNote = ` · Ookla Q${stamped.src_quarter} ${stamped.src_year}`;

    sizeNote = ` · ${fmtMB(ctx.bytes)} MB of Parquet`;
    renderEvidenceFilter();
    renderAll();
  } catch (error) {
    unavailable(`Unable to load broadband measurements: ${error.message}`);
    console.error(error);
  }
}

// Canvases are sized from their host's layout box, so a resize has to redraw
// them or they stay stretched at the old resolution.
let resizeTimer = null;
window.addEventListener('resize', () => {
  if (!rows.length) return;
  clearTimeout(resizeTimer);
  resizeTimer = setTimeout(renderCharts, 150);
});

run();
