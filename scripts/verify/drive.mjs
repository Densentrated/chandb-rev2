// Step 2, tier 2: drive the three pages in a real browser, like a user.
//
// Everything interesting in this app happens after the HTML arrives. The
// server is a static file host; DuckDB-WASM, the catalog fetch, the dataset
// registration, every query and every pixel on /maps happen in the tab. A test
// that only asserts on HTTP responses would pass against a completely broken
// frontend, so this tier clicks the buttons and reads what rendered.
//
// One browser context for all three pages, deliberately: the DuckDB-WASM
// bundle is ~34MB from jsDelivr and a shared HTTP cache means it is downloaded
// once per run rather than three times. That is the difference between a
// two-minute browser tier and one that trips its own wall-clock cap.
//
// Failure classification is the other half of the job. A jsDelivr hiccup and a
// broken boot() look identical from the outside — both leave the status bar
// busy forever — so the two are told apart by what the page was doing when the
// clock ran out and whether the CDN requests themselves failed. Calling a CDN
// outage a frontend regression sends the next agent hunting a bug that is not
// there.

import { appendFileSync, mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

const BASE = process.env.PORTAL_BASE || 'http://127.0.0.1:8099';
const RUN_DIR = process.env.RUN_DIR;
const CHECKS_TSV = process.env.CHECKS_TSV;

const EXIT_PASS = 0, EXIT_FAIL = 1, EXIT_INFRA = 2;

// A page still busy after this long is a hard failure, not patience.
const BUSY_TIMEOUT_MS = 90_000;
// Escape hatch for the whole tier. On exhaustion we report rather than hang.
const TIER_BUDGET_MS = 10 * 60_000;
const DEADLINE = Date.now() + TIER_BUDGET_MS;

const CDN_HOST = 'cdn.jsdelivr.net';
const MAP_IDS = ['world', 'boston', 'bay', 'philadelphia'];

const consoleLog = [];
const networkLog = [];
const results = [];
let infra = null;   // { detail, remedy } — set once, short-circuits the rest

function record(id, status, detail, artifact = '-') {
  results.push({ id, status, detail: String(detail), artifact });
  if (CHECKS_TSV) {
    appendFileSync(CHECKS_TSV,
      [id, status, String(detail).replace(/[\t\n]/g, ' '), artifact].join('\t') + '\n');
  }
  console.log(`  ${status.padEnd(5)} ${id}: ${detail}`);
}

function markInfra(detail, remedy) {
  if (!infra) infra = { detail, remedy };
  record('browser.infra', 'INFRA', `${detail} — remedy: ${remedy}`);
}

const budgetLeft = () => DEADLINE - Date.now();

// ---------------------------------------------------------------- page setup

function instrument(page, label) {
  page.on('console', (msg) => {
    consoleLog.push({
      page: label, level: msg.type(), text: msg.text(),
      location: msg.location()?.url || null, at: new Date().toISOString(),
    });
  });
  // A pageerror is an uncaught exception. There is no benign version of one.
  page.on('pageerror', (err) => {
    consoleLog.push({
      page: label, level: 'pageerror', text: String(err?.stack || err),
      location: null, at: new Date().toISOString(),
    });
  });
  page.on('requestfailed', (req) => {
    networkLog.push({
      page: label, url: req.url(), method: req.method(), status: null,
      failure: req.failure()?.errorText || 'failed', at: new Date().toISOString(),
    });
  });
  page.on('response', (res) => {
    if (res.status() < 400) return;
    networkLog.push({
      page: label, url: res.url(), method: res.request().method(),
      status: res.status(), failure: null, at: new Date().toISOString(),
    });
  });
}

// waitReady polls the status bar. Both pages drive aria-busy from their own
// say() helper, which is the only signal that boot() finished — there is no
// load event that means "the engine is up and the datasets are registered".
async function waitReady(page) {
  const started = Date.now();
  for (;;) {
    const st = await page.evaluate(() => {
      const el = document.getElementById('status');
      if (!el) return null;
      return {
        busy: el.getAttribute('aria-busy'),
        err: el.classList.contains('err'),
        text: (el.textContent || '').trim(),
      };
    });
    if (st?.err) return { ok: false, reason: 'error', text: st.text };
    if (st?.busy === 'false') return { ok: true, ms: Date.now() - started, text: st.text };
    if (Date.now() - started > BUSY_TIMEOUT_MS) {
      return { ok: false, reason: 'timeout', text: st?.text ?? '(no #status element)' };
    }
    await page.waitForTimeout(250);
  }
}

// The status text at the moment of the timeout is what separates "the CDN is
// slow" from "our code is broken": both pages report download progress while
// the WASM bundle is in flight and say something else once it lands.
const STILL_DOWNLOADING = /downloading DuckDB engine|Loading query engine/i;

function cdnBroken() {
  return networkLog.some(n => n.url.includes(CDN_HOST) && (n.failure || n.status >= 400));
}

async function openPage(context, path, label) {
  const page = await context.newPage();
  instrument(page, label);
  await page.goto(BASE + path, { waitUntil: 'domcontentloaded', timeout: 30_000 });
  return page;
}

async function settle(page, label) {
  const ready = await waitReady(page);
  if (ready.ok) {
    record(`${label}.ready`, 'PASS',
      `status bar left busy after ${(ready.ms / 1000).toFixed(1)}s: ${ready.text.slice(0, 110)}`);
    return true;
  }
  if (ready.reason === 'timeout' && (STILL_DOWNLOADING.test(ready.text) || cdnBroken())) {
    markInfra(
      `${label}: still downloading DuckDB-WASM after ${BUSY_TIMEOUT_MS / 1000}s ("${ready.text.slice(0, 90)}")`,
      `check ${CDN_HOST} is reachable: curl -I https://${CDN_HOST}/npm/@duckdb/duckdb-wasm@1.32.0/+esm`);
    return false;
  }
  record(`${label}.ready`, 'FAIL',
    ready.reason === 'error'
      ? `status bar reports an error: ${ready.text.slice(0, 140)}`
      : `still busy after ${BUSY_TIMEOUT_MS / 1000}s: ${ready.text.slice(0, 140)}`,
    `screenshots/${label}.png`);
  return false;
}

async function capture(page, label) {
  if (!RUN_DIR) return;
  await page.screenshot({
    path: join(RUN_DIR, 'screenshots', `${label}.png`), fullPage: true,
  }).catch(() => {});
}

// First contentful paint is CDN-bound on a cold cache, so it is reported and
// never fails the run.
async function recordPaint(page, label) {
  const fcp = await page.evaluate(() =>
    performance.getEntriesByName('first-contentful-paint')[0]?.startTime ?? null);
  if (fcp === null) return;
  const s = (fcp / 1000).toFixed(2);
  if (fcp > 5000) {
    record(`${label}.fcp`, 'WARN', `first contentful paint ${s}s (> 5s; CDN-bound, not page code)`);
  } else {
    record(`${label}.fcp`, 'PASS', `first contentful paint ${s}s`);
  }
}

// ----------------------------------------------------------------- / (index)

async function driveIndex(context) {
  const page = await openPage(context, '/', 'index');
  try {
    if (!await settle(page, 'index')) { await capture(page, 'index'); return; }
    await recordPaint(page, 'index');

    // Run a built-in example rather than typed SQL: the example list is itself
    // schema-aware (buttons hide when gold lacks their columns), so clicking
    // one exercises the catalog, the DESCRIBE pass and the query path at once.
    const label = 'Which trains were late?';
    const button = page.locator('#examples button').filter({ hasText: label });
    if (await button.count() === 0) {
      const shown = await page.locator('#examples button').allTextContents();
      record('index.query', 'FAIL',
        `example "${label}" is not offered; available: ${shown.join(' / ') || 'none'}`,
        'screenshots/index.png');
      await capture(page, 'index');
      return;
    }
    await button.first().click();
    await page.waitForFunction(
      () => /\d+ row\(s\)/.test(document.getElementById('timing')?.textContent || ''),
      null, { timeout: 60_000 }).catch(() => {});

    const table = await page.evaluate(() => {
      const out = document.getElementById('out');
      const cols = [...out.querySelectorAll('thead th')].map(th => th.textContent);
      const rows = [...out.querySelectorAll('tbody tr')].map(
        tr => [...tr.cells].map(td => ({ text: td.textContent, empty: td.classList.contains('empty') })));
      return { cols, rows };
    });
    const dataRows = table.rows.filter(r => !r.some(c => c.empty));
    if (dataRows.length < 1) {
      record('index.query', 'FAIL',
        `"${label}" rendered ${table.rows.length} row(s), none with data`,
        'screenshots/index.png');
      await capture(page, 'index');
      return;
    }
    record('index.query', 'PASS',
      `"${label}" rendered ${dataRows.length} data row(s) over ${table.cols.length} column(s)`);

    // The epoch/human toggle. Ask chandb.js which columns it reformats rather
    // than keeping a second copy of that list here — the point of the check is
    // that the toggle works, not that this file remembers the schema.
    const { instant, duration } = await page.evaluate(async () => {
      const m = await import('/chandb.js');
      return { instant: [...m.INSTANT_COLS], duration: [...m.DURATION_COLS] };
    });
    const formatted = new Set([...instant, ...duration]);
    const col = table.cols.findIndex(c => formatted.has(c));
    const row = col < 0 ? -1 : dataRows.findIndex(r => r[col] && r[col].text !== 'NULL');
    if (col < 0 || row < 0) {
      record('index.timetoggle', 'FAIL',
        `no non-NULL time or duration column in [${table.cols.join(', ')}] to toggle`,
        'screenshots/index.png');
    } else {
      const cellText = () => page.evaluate(([r, c]) =>
        document.querySelectorAll('#out tbody tr')[r]?.cells[c]?.textContent ?? null, [row, col]);
      const before = await cellText();
      await page.locator('#tmode').click();
      let after = before;
      for (const _ of Array(60)) {
        await page.waitForTimeout(250);
        after = await cellText();
        if (after !== null && after !== before) break;
      }
      if (after !== null && after !== before) {
        record('index.timetoggle', 'PASS',
          `${table.cols[col]} re-rendered "${before}" → "${after}"`);
      } else {
        record('index.timetoggle', 'FAIL',
          `${table.cols[col]} still reads "${before}" after toggling the time mode`,
          'screenshots/index.png');
      }
    }
    await capture(page, 'index');
  } finally {
    await page.close();
  }
}

// ------------------------------------------------------------------- /board

const stationCount = (page) => page.locator('.station').count();

// pickFilter returns the first button in box that is not the "show everything"
// option, which is always rendered first.
async function pickFilter(page, box) {
  const buttons = page.locator(`${box} button`);
  const n = await buttons.count();
  if (n < 2) return null;
  return buttons.nth(1);
}

async function driveBoard(context) {
  const page = await openPage(context, '/board', 'board');
  try {
    if (!await settle(page, 'board')) { await capture(page, 'board'); return; }
    await recordPaint(page, 'board');

    const baseline = await stationCount(page);
    if (baseline < 1) {
      record('board.stations', 'FAIL',
        'no station cards rendered; the board found no arrivals in the window',
        'screenshots/board.png');
      await capture(page, 'board');
      return;
    }
    record('board.stations', 'PASS',
      `${baseline} station card(s) — ${(await page.locator('#counts').textContent() || '').trim()}`);

    const agency = await pickFilter(page, '#agencies');
    const mode = await pickFilter(page, '#types');
    if (!agency || !mode) {
      record('board.filters', 'FAIL',
        'agency or mode filter buttons were not rendered (both need 2+ distinct values)',
        'screenshots/board.png');
      await capture(page, 'board');
      return;
    }

    // Each filter is exercised on its own, with the board reset in between.
    // Stacking them hides a dead filter: BART's stations are all Subway, so a
    // mode filter that did nothing at all would still "change" the count if
    // the agency filter were still applied underneath it.
    const click = async (locator) => {
      const name = (await locator.textContent() || '').trim();
      await locator.click();
      await page.waitForTimeout(300);
      return { name, count: await stationCount(page) };
    };

    const byAgency = await click(agency);
    const allAgencies = await click(page.locator('#agencies button').first());
    const byMode = await click(mode);
    const cleared = await click(page.locator('#clear'));

    const problems = [];
    if (byAgency.count === baseline) problems.push(`agency "${byAgency.name}" left the count at ${baseline}`);
    if (allAgencies.count !== baseline) problems.push(`"${allAgencies.name}" restored ${allAgencies.count}, want ${baseline}`);
    if (byMode.count === baseline) problems.push(`mode "${byMode.name}" left the count at ${baseline}`);
    if (cleared.count !== baseline) problems.push(`clear restored ${cleared.count}, want ${baseline}`);

    if (problems.length) {
      record('board.filters', 'FAIL', problems.join('; '), 'screenshots/board.png');
    } else {
      record('board.filters', 'PASS',
        `${baseline} → ${byAgency.name} ${byAgency.count} → ${allAgencies.name} ${allAgencies.count}`
        + ` → ${byMode.name} ${byMode.count} → clear ${cleared.count}`);
    }
    await capture(page, 'board');
  } finally {
    await page.close();
  }
}

// -------------------------------------------------------------------- /maps

// Canvas pixels are the only evidence that /maps worked. Every other signal —
// the status bar, the cell counts, the absence of console errors — is
// identical whether the heat was plotted or silently projected off-screen, and
// a blank map is the regression class this page keeps producing.
async function readCanvases(page) {
  return page.evaluate((ids) => {
    const BG = [16, 16, 20];              // the #101014 fill addActivity starts with
    const out = {};
    for (const id of ids) {
      const host = document.getElementById(`map-${id}`);
      const canvas = host?.querySelector('canvas');
      const count = (document.getElementById(`count-${id}`)?.textContent || '').trim();
      if (!canvas) { out[id] = { error: 'no canvas element', count }; continue; }
      const w = canvas.width, h = canvas.height;
      if (!w || !h) { out[id] = { error: `zero-sized canvas (${w}x${h})`, count }; continue; }

      // Inset past the 1px accent border addActivity strokes around the frame;
      // counting it would make every canvas look painted.
      const inset = Math.max(3, Math.ceil(6 * (w / (canvas.clientWidth || w))));
      const data = canvas.getContext('2d').getImageData(
        inset, inset, Math.max(1, w - 2 * inset), Math.max(1, h - 2 * inset)).data;

      let painted = 0, heat = 0, total = 0;
      for (let i = 0; i < data.length; i += 4) {
        const [r, g, b, a] = [data[i], data[i + 1], data[i + 2], data[i + 3]];
        total++;
        if (a === 0) continue;
        if (r === BG[0] && g === BG[1] && b === BG[2]) continue;
        painted++;
        // The heat palette is hsl(190..330 88% 64%) — vivid and light. The
        // graticule (#22222a) and its labels (#52677a) are neither, so this
        // separates "a dot was plotted" from "only the grid was drawn".
        const max = Math.max(r, g, b), min = Math.min(r, g, b);
        if (max > 150 && max - min > 60) heat++;
      }
      out[id] = { w, h, painted, heat, total, count };
    }
    return out;
  }, MAP_IDS);
}

async function driveMaps(context) {
  const page = await openPage(context, '/maps', 'maps');
  try {
    const ready = await settle(page, 'maps');
    // Unlike the other two pages, keep going when the page reported an error.
    // maps.html wraps its whole run() in one try/catch, so a throw partway
    // through drawing leaves some canvases painted and the rest blank — and
    // which ones is the most useful thing the run can tell you. Bailing here
    // would reduce that to a single error string.
    if (!ready && infra) { await capture(page, 'maps'); return; }
    if (ready) await recordPaint(page, 'maps');

    // addActivity runs synchronously right before the status bar clears, but
    // give the compositor a frame so getImageData sees the finished canvas.
    await page.waitForTimeout(500);
    const stats = await readCanvases(page);

    for (const id of MAP_IDS) {
      const s = stats[id];
      if (s.error) {
        record(`maps.${id}`, 'FAIL', `#map-${id}: ${s.error}`, 'screenshots/maps.png');
        continue;
      }
      if (s.painted === 0) {
        record(`maps.${id}`, 'FAIL',
          `#map-${id} canvas is fully blank (${s.w}x${s.h}, 0 painted pixels)`,
          'screenshots/maps.png');
        continue;
      }
      if (s.heat === 0) {
        // The fixture has no vehicle positions for some agencies — BART
        // publishes none at all, so gold has no bart_vehicle_positions table
        // and the Bay map is correctly empty. Only warn when the page itself
        // agrees there was nothing to plot.
        const expectedEmpty = /no positions published/i.test(s.count);
        record(`maps.${id}`, expectedEmpty ? 'WARN' : 'FAIL',
          expectedEmpty
            ? `#map-${id} drew the grid but no activity — page reports "${s.count}" (no rows for this agency)`
            : `#map-${id} has ${s.painted} painted pixels but no plotted activity, while reporting "${s.count}"`,
          'screenshots/maps.png');
        continue;
      }
      record(`maps.${id}`, 'PASS',
        `#map-${id}: ${s.heat} activity pixels of ${s.painted} painted (${s.count})`);
    }
    await capture(page, 'maps');
  } finally {
    await page.close();
  }
}

// ------------------------------------------------------------------- driver

async function main() {
  if (RUN_DIR) mkdirSync(join(RUN_DIR, 'screenshots'), { recursive: true });

  let chromium;
  try {
    ({ chromium } = await import('playwright'));
  } catch {
    markInfra('the playwright package is not installed',
      'npm install && npx playwright install chromium');
    return EXIT_INFRA;
  }

  let browser;
  try {
    browser = await chromium.launch({ headless: true });
  } catch (e) {
    const msg = String(e.message || e);
    // Playwright's own wording when the browser binary was never downloaded.
    if (/Executable doesn't exist|please run.*playwright install/i.test(msg)) {
      markInfra('the headless chromium build is missing',
        'npx playwright install chromium   # ~150MB');
    } else {
      markInfra(`chromium would not launch: ${msg.split('\n')[0]}`,
        'npx playwright install --with-deps chromium');
    }
    return EXIT_INFRA;
  }

  const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  await context.tracing.start({ screenshots: true, snapshots: true, sources: false });

  try {
    for (const [name, fn] of [['index', driveIndex], ['board', driveBoard], ['maps', driveMaps]]) {
      if (infra) break;
      if (budgetLeft() <= 0) {
        markInfra(`the browser tier used its ${TIER_BUDGET_MS / 60_000}-minute budget before reaching /${name === 'index' ? '' : name}`,
          `check ${CDN_HOST} latency, or re-run with a warm browser cache`);
        break;
      }
      await fn(context);
    }
  } catch (e) {
    record('browser.driver', 'FAIL', `the driver threw: ${String(e.message || e).split('\n')[0]}`);
  } finally {
    if (RUN_DIR) {
      await context.tracing.stop({ path: join(RUN_DIR, 'trace.zip') }).catch(() => {});
    } else {
      await context.tracing.stop().catch(() => {});
    }
    await browser.close().catch(() => {});
  }

  // Console and network verdicts are global: an uncaught exception on any page
  // is a failure of the run, not of whichever check happened to be mid-flight.
  const errors = consoleLog.filter(c => c.level === 'error' || c.level === 'pageerror');
  const warnings = consoleLog.filter(c => c.level === 'warning');
  if (errors.length) {
    record('browser.console', 'FAIL',
      `${errors.length} console error(s)/pageerror(s); first: ${errors[0].page} — ${errors[0].text.slice(0, 160)}`,
      'console.json');
  } else {
    record('browser.console', 'PASS',
      `${consoleLog.length} console message(s), none at error level`, 'console.json');
  }
  if (warnings.length) {
    record('browser.warnings', 'WARN',
      `${warnings.length} console warning(s); first: ${warnings[0].text.slice(0, 140)}`, 'console.json');
  }

  const cdn = networkLog.filter(n => n.url.includes(CDN_HOST));
  const own = networkLog.filter(n => !n.url.includes(CDN_HOST));
  if (cdn.length && !infra) {
    markInfra(`${cdn.length} request(s) to ${CDN_HOST} failed; DuckDB-WASM could not load`,
      `curl -I https://${CDN_HOST}/npm/@duckdb/duckdb-wasm@1.32.0/+esm`);
  }
  if (own.length) {
    record('browser.network', 'FAIL',
      `${own.length} failed or non-2xx request(s); first: ${own[0].status ?? own[0].failure} ${own[0].url}`,
      'network.json');
  } else {
    record('browser.network', 'PASS',
      `no failed or 4xx/5xx requests from this origin (${cdn.length} CDN issue(s))`, 'network.json');
  }

  if (RUN_DIR) {
    writeFileSync(join(RUN_DIR, 'console.json'), JSON.stringify(consoleLog, null, 2) + '\n');
    writeFileSync(join(RUN_DIR, 'network.json'), JSON.stringify(networkLog, null, 2) + '\n');
  }

  if (infra) return EXIT_INFRA;
  return results.some(r => r.status === 'FAIL') ? EXIT_FAIL : EXIT_PASS;
}

process.exit(await main());
