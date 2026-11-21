import { boot, columnsOf, rowsOf, fmtMB } from '/chandb.js';

const $ = id => document.getElementById(id);
const status = $('status');
const say = (text, error = false) => {
  status.textContent = text;
  status.classList.toggle('err', error);
  status.setAttribute('aria-busy', error ? 'false' : 'true');
};
const configs = [
  { id: 'world', bounds: [-180, 180, -60, 75], agencies: null },
  { id: 'boston', bounds: [-72.5, -70.5, 41.5, 43], agencies: ['mbta'] },
  { id: 'bay', bounds: [-123, -121, 37, 39], agencies: ['bart'] },
  { id: 'philadelphia', bounds: [-76, -74.5, 39.3, 40.5], agencies: ['septa', 'septarail'] },
];

function makeMap(config) {
  const host = document.getElementById(`map-${config.id}`);
  const canvas = document.createElement('canvas');
  host.replaceChildren(canvas);
  return { canvas, bounds: config.bounds };
}

// Alpha is a parameter, not a suffix. The hex trick (`${color}cc`) only works
// on #rrggbb; appending it to a CSS Color 4 hsl() yields "hsl(...)cc", which
// addColorStop rejects outright — and run()'s catch turns that into a blank
// page rather than a stack trace.
function heatColor(ratio, alpha = 1) {
  return `hsl(${190 + Math.round(ratio * 140)} 88% 64% / ${alpha})`;
}

function addActivity(config, map, rows) {
  const selected = config.agencies ? rows.filter(row => config.agencies.includes(row.agency)) : rows;
  const bounds = config.bounds;
  const max = Math.max(...selected.map(row => Number(row.observations)), 1);
  const ctx = map.canvas.getContext('2d');
  const width = map.canvas.clientWidth, height = map.canvas.clientHeight;
  const scale = window.devicePixelRatio || 1;
  map.canvas.width = width * scale;
  map.canvas.height = height * scale;
  ctx.scale(scale, scale);
  ctx.fillStyle = '#101014';
  ctx.fillRect(0, 0, width, height);

  const project = (lon, lat) => [
    ((lon - bounds[0]) / (bounds[1] - bounds[0])) * width,
    ((bounds[3] - lat) / (bounds[3] - bounds[2])) * height,
  ];
  ctx.strokeStyle = '#22222a';
  ctx.lineWidth = 1;
  ctx.font = '10px ui-monospace, monospace';
  ctx.fillStyle = '#52677a';
  const lonStep = bounds[1] - bounds[0] > 100 ? 60 : 0.5;
  const latStep = bounds[1] - bounds[0] > 100 ? 30 : 0.5;
  for (let lon = Math.ceil(bounds[0] / lonStep) * lonStep; lon <= bounds[1]; lon += lonStep) {
    const [x] = project(lon, bounds[2]);
    ctx.beginPath(); ctx.moveTo(x, 0); ctx.lineTo(x, height); ctx.stroke();
    ctx.fillText(`${lon}°`, x + 5, height - 8);
  }
  for (let lat = Math.ceil(bounds[2] / latStep) * latStep; lat <= bounds[3]; lat += latStep) {
    const [, y] = project(bounds[0], lat);
    ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(width, y); ctx.stroke();
    ctx.fillText(`${lat}°`, 6, Math.max(12, y - 5));
  }
  ctx.strokeStyle = '#4ad5ff';
  ctx.strokeRect(0.5, 0.5, width - 1, height - 1);

  for (const row of selected) {
    const lat = Number(row.latitude), lon = Number(row.longitude);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;
    const ratio = Math.sqrt(Number(row.observations) / max);
    const [x, y] = project(lon, lat);
    if (x < 0 || x > width || y < 0 || y > height) continue;
    const radius = 3 + ratio * 12;
    const color = heatColor(ratio);
    const glow = ctx.createRadialGradient(x, y, 0, x, y, radius * 2.8);
    glow.addColorStop(0, heatColor(ratio, 0.8));
    glow.addColorStop(1, heatColor(ratio, 0));
    ctx.fillStyle = glow;
    ctx.beginPath(); ctx.arc(x, y, radius * 2.8, 0, Math.PI * 2); ctx.fill();
    ctx.fillStyle = color;
    ctx.beginPath(); ctx.arc(x, y, radius, 0, Math.PI * 2); ctx.fill();
  }
  $(`count-${config.id}`).textContent = selected.length
    ? `${selected.length.toLocaleString()} activity cells`
    : 'No positions published';
}

async function run() {
  try {
    const result = await boot((loaded, total) => {
      if (total > 0) say(`Loading query engine… ${Math.round(loaded / total * 100)}%`);
      else if (loaded < 0) say('Query engine ready; registering datasets…');
    });
    const tables = [];
    for (const table of result.available) {
      if (!table.endsWith('_vehicle_positions')) continue;
      const cols = await columnsOf(result.conn, table);
      if (['agency', 'latitude', 'longitude', 'feed_timestamp'].every(c => cols.has(c))) {
        tables.push(table);
      }
    }
    if (!tables.length) throw new Error('No vehicle-position datasets are published yet');
    const sql = tables.map(table => `
      SELECT agency,
             floor(latitude * 100) / 100 AS latitude,
             floor(longitude * 100) / 100 AS longitude,
             count(*) AS observations
      FROM ${table}
      WHERE feed_timestamp >= (SELECT max(feed_timestamp) FROM ${table}) - 86400
      GROUP BY agency, latitude, longitude
    `).join(' UNION ALL ');
    const rows = rowsOf(await result.conn.query(sql));
    const maps = new Map(configs.map(config => [config.id, { config, map: makeMap(config) }]));
    for (const entry of maps.values()) addActivity(entry.config, entry.map, rows);
    say(`Ready · ${rows.length.toLocaleString()} activity cells · latest 24 hours · ${fmtMB(result.bytes)} MB`);
    status.setAttribute('aria-busy', 'false');
  } catch (error) {
    say(`Unable to load train positions: ${error.message}`, true);
    for (const config of configs) $(`count-${config.id}`).textContent = 'Unavailable';
  }
}
run();
