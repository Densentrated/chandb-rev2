import { boot, columnsOf, fmtEta, fmtDuration, vehicleIcon, typeFromRouteType, fmtMB, rowsOf } from '/chandb.js';

const $ = (id) => document.getElementById(id);
const statusEl = $('status'), boardEl = $('board'), filterEl = $('filter'),
      countsEl = $('counts'), clearEl = $('clear'), detailEl = $('detail'),
      timelineEl = $('timeline'), detailClose = $('detail-close');

const say = (msg, isErr = false) => {
  statusEl.textContent = msg;
  statusEl.classList.toggle('err', isErr);
  statusEl.setAttribute('aria-busy', !isErr && !msg.includes('station') && !msg.includes('showing') ? 'true' : 'false');
};

let conn, rows = [], human = true, bootNote = '';
let agencies = [], agencyFilter = null;   // null = show every agency
let types = [], typeFilter = null;        // null = show every vehicle type
let selectedStation = null, previousFocus = null;

// Rail first: a rider looking for the Orange Line should not have to scroll
// past four hundred bus stops to find it.
const TYPE_ORDER = ['Subway', 'Light Rail', 'Commuter Rail', 'Ferry', 'Trolleybus', 'Bus'];

// Anything not listed sorts last. indexOf's -1 would otherwise rank an
// unknown mode AHEAD of the subway.
const typeRank = (ty) => {
  const i = TYPE_ORDER.indexOf(ty);
  return i < 0 ? TYPE_ORDER.length : i;
};

function renderTypeFilter() {
  const box = $('types');
  box.replaceChildren();
  if (types.length < 2) return;

  for (const ty of [null, ...types]) {
    const b = document.createElement('button');
    b.textContent = ty === null ? 'All modes' : ty;
    b.className = typeFilter === ty ? 'on' : '';
    b.onclick = () => { typeFilter = ty; renderTypeFilter(); render(); };
    box.appendChild(b);
  }
}

function renderAgencyFilter() {
  const box = $('agencies');
  box.replaceChildren();
  if (agencies.length < 2) return;        // pointless with a single agency

  for (const a of [null, ...agencies]) {
    const b = document.createElement('button');
    b.textContent = a === null ? 'All' : a.toUpperCase();
    b.className = agencyFilter === a ? 'on' : '';
    b.onclick = () => { agencyFilter = a; renderAgencyFilter(); render(); };
    box.appendChild(b);
  }
}

// "now" is the newest timestamp in the data, not the browser clock. Snapshots
// are up to one ingest interval old, so counting down from wall-clock time
// would show every departure as long gone.
// One SELECT per agency, UNIONed.
//
// "now" is computed PER TABLE, not once across the union: each agency's feed
// has its own latest timestamp, and sharing one would make the agency that
// happened to be fetched a minute earlier look uniformly late.
const agencySQL = (table, cols) => `
SELECT '${table.split('_')[0]}' AS agency,
       stop_name,
       coalesce(route_short_name, route_long_name, route_id) AS route,
       ${cols.has('vehicle_type') ? 'vehicle_type' : 'NULL AS vehicle_type'},
       ${cols.has('route_type')   ? 'route_type'   : 'NULL AS route_type'},
       trip_headsign,
       arrival_time,
       ${cols.has('departure_time') ? 'departure_time' : 'NULL AS departure_time'},
       ${cols.has('arrival_delay') ? 'arrival_delay' : 'NULL AS arrival_delay'},
       ${cols.has('departure_delay') ? 'departure_delay' : 'NULL AS departure_delay'},
       coalesce(${cols.has('departure_time') ? 'departure_time' : 'NULL'}, arrival_time) -
         (SELECT max(last_seen) FROM ${table}) AS seconds_away
FROM ${table}
WHERE stop_name    IS NOT NULL
  AND coalesce(${cols.has('departure_time') ? 'departure_time' : 'NULL'}, arrival_time) IS NOT NULL
  AND coalesce(${cols.has('departure_time') ? 'departure_time' : 'NULL'}, arrival_time)
      BETWEEN (SELECT max(last_seen) FROM ${table}) - 600
          AND (SELECT max(last_seen) FROM ${table}) + 3600`;

// MBTA alone publishes ~5,800 stops. Rendering a card for every one is tens
// of thousands of DOM nodes and a visibly janky page, so the board shows a
// window and the filter box is how you narrow it.
const MAX_STATIONS = 60;

function delayText(d) {
  const delay = d.departure_delay ?? d.arrival_delay;
  if (delay === null || delay === undefined || !Number.isFinite(Number(delay))) return 'Delay unavailable';
  const n = Number(delay);
  if (Math.abs(n) < 30) return 'On time';
  return n > 0 ? `${fmtDuration(n)} late` : `${fmtDuration(-n)} early`;
}

function showDetail(station, focus) {
  selectedStation = station;
  previousFocus = focus || document.activeElement;
  const deps = rows.filter(d => d.agency === station.agency && d.stop_name === station.stop_name &&
    (!agencyFilter || d.agency === agencyFilter) && (!typeFilter || d.kind === typeFilter))
    .sort((a, b) => (a.seconds_away - b.seconds_away));
  $('detail-title').textContent = station.stop_name;
  $('detail-summary').textContent = `${station.agency.toUpperCase()} · ${deps.length} available departures`;
  timelineEl.replaceChildren();
  if (!deps.length) {
    timelineEl.innerHTML = '<div class="empty">No departures match the current filters. Try clearing a filter or refresh the snapshot.</div>';
  } else {
    const times = deps.map(d => Number(d.seconds_away)).filter(Number.isFinite);
    const min = Math.min(...times, 0), max = Math.max(...times, 3600), span = Math.max(max - min, 3600);
    const ticks = document.createElement('div'); ticks.className = 'timeline-grid';
    for (let i = 0; i < 6; i++) {
      const tick = document.createElement('span'); tick.className = 'tick';
      tick.textContent = human ? fmtEta(min + span * i / 5) : String(Math.round(min + span * i / 5));
      ticks.appendChild(tick);
    }
    const items = document.createElement('div'); items.className = 'timeline-items';
    for (const [i, d] of deps.entries()) {
      const item = document.createElement('div'); item.className = 'timeline-item';
      item.style.top = `${30 + (i % 3) * 78}px`;
      item.style.left = `${Math.max(3, Math.min(97, ((d.seconds_away - min) / span) * 100))}%`;
      const dot = document.createElement('div'); dot.className = 'timeline-dot';
      const card = document.createElement('div'); card.className = 'timeline-card';
      const time = document.createElement('strong');
      time.textContent = human ? fmtEta(d.seconds_away) : String(d.departure_time ?? d.arrival_time);
      const mode = document.createElement('span'); mode.className = 'mode';
      mode.textContent = `${vehicleIcon(d.kind)} ${d.route ?? '?'} · ${d.kind}`;
      const destination = document.createElement('div'); destination.className = 'destination';
      destination.textContent = `to ${d.trip_headsign || 'Destination unavailable'}`;
      const delay = document.createElement('div'); delay.className = 'delay' + (delayText(d) === 'On time' ? ' good' : '');
      delay.textContent = delayText(d);
      card.append(time, mode, destination, delay); item.append(dot, card); items.appendChild(item);
    }
    const legend = document.createElement('div'); legend.className = 'legend';
    legend.textContent = 'Earlier departures are left; each marker shows route, mode, destination and reported delay.';
    timelineEl.append(ticks, items, legend);
  }
  boardEl.hidden = true; detailEl.hidden = false; filterEl.setAttribute('aria-hidden', 'true');
  detailClose.focus();
}

function closeDetail() {
  selectedStation = null; detailEl.hidden = true; boardEl.hidden = false; filterEl.removeAttribute('aria-hidden');
  if (previousFocus && document.contains(previousFocus)) previousFocus.focus();
}

function render() {
  const q = filterEl.value.trim().toLowerCase();
  const byStop = new Map();

  for (const r of rows) {
    if (agencyFilter && r.agency !== agencyFilter) continue;
    if (typeFilter && r.kind !== typeFilter) continue;
    if (q && !r.stop_name.toLowerCase().includes(q)) continue;
    // Keyed by agency too: "Civic Center" exists in more than one transit
    // system, and merging them into one card would be a lie.
    const key = r.agency + '\u0000' + r.stop_name;
    if (!byStop.has(key)) byStop.set(key, []);
    const list = byStop.get(key);
    if (list.length < 6) list.push(r); // a board shows the next few, not all
  }

  // A departure board is ordered by what leaves soonest. Sorting by name put
  // every numerically-named bus stop first and pushed Downtown Crossing to
  // position 1893 — past the render cap, so no rail ever appeared.
  const ordered = [...byStop.values()].sort((a, b) => {
    const rank = (l) => typeRank(l[0].kind);
    return (rank(a) - rank(b)) || (a[0].seconds_away - b[0].seconds_away);
  });

  const total = ordered.length;
  boardEl.replaceChildren();
  clearEl.hidden = !(q || agencyFilter || typeFilter);
  countsEl.textContent = total ? `${Math.min(total, MAX_STATIONS)} of ${total} stations shown` : '';
  if (!total) {
    say(q ? `No stations matching "${filterEl.value}".`
          : 'No upcoming arrivals in the latest snapshot.');
    return;
  }

  let drawn = 0;
  for (const deps of ordered) {
    if (drawn++ >= MAX_STATIONS) break;
    const card = document.createElement('section');
    card.className = 'station';
    const trigger = document.createElement('button');
    trigger.className = 'station-trigger';
    trigger.type = 'button';
    trigger.setAttribute('aria-label', `Show all departures from ${deps[0].stop_name}`);
    trigger.onclick = () => showDetail(deps[0], trigger);

    const h = document.createElement('h2');
    h.textContent = deps[0].stop_name;    // textContent: upstream names, never innerHTML
    const badge = document.createElement('span');
    badge.className = 'agency';
    badge.textContent = deps[0].agency.toUpperCase();
    h.appendChild(badge);
    trigger.appendChild(h);

    for (const d of deps) {
      const row = document.createElement('div');
      row.className = 'dep';

      const ico = document.createElement('span');
      // vehicle_type was added to gold later than this page; fall back to the
      // raw GTFS route_type so older partitions still show the right icon.
      ico.textContent = vehicleIcon(d.kind);

      const mid = document.createElement('span');
      const rt = document.createElement('span');
      rt.className = 'rt';
      rt.textContent = d.route ?? '?';
      const to = document.createElement('div');
      to.className = 'to';
      to.textContent = d.trip_headsign ?? '';
      mid.append(rt, to);

      const eta = document.createElement('span');
      eta.className = 'eta' + (d.seconds_away < -60 ? ' past' : '');
      eta.textContent = human
        ? fmtEta(d.seconds_away)
        : String(d.arrival_time);         // raw epoch seconds, as stored

      row.append(ico, mid, eta);
      trigger.appendChild(row);
    }
    card.appendChild(trigger);
    boardEl.appendChild(card);
  }

  const who = (agencyFilter ? agencyFilter.toUpperCase()
                            : agencies.map(a => a.toUpperCase()).join(' + '))
            + (typeFilter ? ` · ${typeFilter}` : '');
  say(total > MAX_STATIONS
    ? `${who} · showing ${MAX_STATIONS} of ${total} stations — type in the filter to narrow`
    : `${who} · ${total} station(s) — queried in your browser${bootNote}`);
}

async function load() {
  $('reload').disabled = true;
  boardEl.innerHTML = '<div class="empty">Loading arrivals…</div>';
  const tables = [...available].filter(t => t.endsWith('_stop_events')).sort();
  if (!tables.length) {
    say('No stop_events table published yet. The pipeline writes it on its next cycle.', true);
    $('reload').disabled = false;
    return;
  }

  const parts = [];
  for (const t of tables) {
    parts.push(agencySQL(t, await columnsOf(conn, t)));
  }
  const res = await conn.query(
    parts.join('\nUNION ALL\n') + '\nORDER BY agency, stop_name, arrival_time');

  rows = rowsOf(res).map(o => {
    // vehicle_type postdates some gold partitions; derive it from the raw
    // GTFS route_type when it is absent so filtering works either way.
    o.kind = o.vehicle_type ?? typeFromRouteType(o.route_type) ?? 'Other';
    return o;
  });
  agencies = [...new Set(rows.map(r => r.agency))].sort();
  types = [...new Set(rows.map(r => r.kind))]
    .sort((a, b) => typeRank(a) - typeRank(b));
  renderAgencyFilter();
  renderTypeFilter();
  render();
  $('reload').disabled = false;
}

let available;
try {
  const ctx = await boot((loaded, total) => {
      say(loaded < 0
        ? 'engine ready — registering datasets…'
        : `downloading DuckDB engine… ${fmtMB(loaded)} / ${fmtMB(total)} MB (one-time, then cached)`);
    });
  conn = ctx.conn;
  available = ctx.available;
  bootNote = ` · engine ${(ctx.timings.engineMs / 1000).toFixed(1)}s,`
           + ` data ${(ctx.timings.datasetsMs / 1000).toFixed(1)}s`;
  await load();
} catch (e) {
  say('failed to start: ' + e, true);
  console.error(e);
}

filterEl.addEventListener('input', render);
detailClose.onclick = closeDetail;
document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape' && !detailEl.hidden) closeDetail();
});
clearEl.onclick = () => {
  filterEl.value = '';
  agencyFilter = null;
  typeFilter = null;
  renderAgencyFilter();
  renderTypeFilter();
  render();
  filterEl.focus();
};
$('reload').onclick = () => load().catch(e => {
  say('Could not refresh arrivals: ' + e, true);
  $('reload').disabled = false;
});
$('tmode').onclick = (e) => {
  human = !human;
  e.target.textContent = human ? 'Times: human-readable' : 'Times: epoch seconds';
  render();
};
