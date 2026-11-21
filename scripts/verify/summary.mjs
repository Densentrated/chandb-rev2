// Step 3: turn checks.tsv into the run's summary.md and decide the exit code.
//
// Every tier appends its results to one TSV instead of each writing its own
// report, so the summary is a single ordered table and the exit code is
// derived from the same rows a human reads. A check that passes in the log and
// fails in the exit code — or the reverse — is how a verification loop ends up
// chasing nothing.

import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

const RUN_DIR = process.env.RUN_DIR;
const CHECKS_TSV = process.env.CHECKS_TSV || join(RUN_DIR, 'checks.tsv');
const EXIT_PASS = 0, EXIT_FAIL = 1, EXIT_INFRA = 2;

const rows = existsSync(CHECKS_TSV)
  ? readFileSync(CHECKS_TSV, 'utf8').split('\n').filter(Boolean).map((line) => {
      const [id, status, detail = '', artifact = '-'] = line.split('\t');
      return { id, status, detail, artifact };
    })
  : [];

const tally = { PASS: 0, FAIL: 0, WARN: 0, SKIP: 0, INFRA: 0 };
for (const r of rows) tally[r.status] = (tally[r.status] ?? 0) + 1;

const verdict = tally.INFRA ? 'INFRA_FAIL' : tally.FAIL ? 'FAIL' : 'PASS';
const exitCode = tally.INFRA ? EXIT_INFRA : tally.FAIL ? EXIT_FAIL : EXIT_PASS;

// Where to look first, per failing check. The point of naming a file here is
// that whoever reads this summary has no other context about the app.
const WHERE_TO_LOOK = {
  'seed.bronze': 'scripts/verify/seedlake/main.go and testdata/lake/ — a fixture file or a @offset@ token is malformed',
  'seed.gold': 'cmd/pipeline/gold.go (goldTables, the JSON column lists) vs the bronze field names in testdata/lake/*.ndjson',
  'go.vet': '.verify/run-*/go-vet.txt',
  'go.build': '.verify/run-*/go-build.txt',
  'go.test': '.verify/run-*/go-test.txt',
  'go.lint': '.verify/run-*/lint.txt',
  'http.range': 'dataServer in cmd/portal/main.go — http.FileServer must handle Range natively; DuckDB-WASM breaks without 206',
  'http.traversal': 'dataServer in cmd/portal/main.go — the EvalSymlinks prefix guard',
  'http.cache': 'servePage / serveImmutable / buildAssets in cmd/portal/main.go',
  'http.datasets': 'scanCatalog in cmd/portal/main.go, and goldTables in cmd/pipeline/gold.go',
  'http.etag': 'catalogCache.serveHTTP in cmd/portal/main.go',
  'http.assets': 'portalRoutes in cmd/portal/main.go',
  'static.syntax': 'the named module — a parse error means no handler ever runs and the page hangs silently',
  'static.cssvars': 'the :root token set in cmd/portal/portal.css, then the page\'s own <style> block — every var(--x) must resolve to a definition there',
  'index.ready': 'boot() in cmd/portal/chandb.js, then console.json for the first error',
  'index.query': 'the gold schema (cmd/pipeline/gold.go) or the seeder — 0 rows usually means the fixture window moved',
  'index.timetoggle': 'the #tmode handler and cell() in cmd/portal/index.js; INSTANT_COLS/DURATION_COLS in chandb.js',
  'board.ready': 'boot() in cmd/portal/chandb.js, then console.json',
  'board.stations': 'agencySQL in cmd/portal/board.js — "now" is max(last_seen), window -10min..+60min; re-seed if the fixture aged out',
  'board.filters': 'renderAgencyFilter / renderTypeFilter / render() in cmd/portal/board.js',
  'maps.ready': 'the run() catch in cmd/portal/maps.js, then console.json',
  'maps.world': 'addActivity in cmd/portal/maps.js — projection bounds, heatColor and the heat loop',
  'maps.boston': 'addActivity bounds for boston, and whether mbta_vehicle_positions has rows',
  'maps.bay': 'gold has no bart_vehicle_positions table at all (gold.go) — expected empty',
  'maps.philadelphia': 'addActivity bounds for philadelphia, and septa/septarail vehicle_positions',
  'broadband.ready': 'the run() catch in cmd/portal/broadband.js, then console.json — a missing broadband_tract_coverage table reports itself here',
  'broadband.ledger': 'renderLedger in cmd/portal/broadband.js, and the unmeasured flag in broadbandTables (cmd/pipeline/broadband.go) — measured + unmeasured must equal every tract',
  'broadband.distribution': 'renderDistribution in cmd/portal/broadband.js — BINS, the per-tier split, and the detached #dist-unmeasured note that keeps unmeasured tracts off the speed axis',
  'broadband.evidence': 'drawEvidenceChart in cmd/portal/broadband.js — the log-x domain guard, niceMax, and the rule that only the speed ramp may be saturated',
  'broadband.tracts': 'drawTractMap in cmd/portal/broadband.js — the INK invariant (ramp = measured, --ink-dim = no test), the projection pad for zero-span centroids, and whether centroid_lat/lon are in gold',
  'broadband.unmeasured': 'renderTable and renderUnmeasured in cmd/portal/broadband.js — unmeasured tracts are listed on their own and must never enter the ranking',
  'broadband.filters': 'renderEvidenceFilter / renderTable in cmd/portal/broadband.js — the caption\'s "of N measured" total is what the check reads, not the capped row count',
  'browser.console': 'console.json — a pageerror is an uncaught exception, start at the top frame',
  'browser.network': 'network.json — a 404 on a page asset means portalRoutes lost a route',
  'portal.start': '.verify/run-*/portal.log',
  'portal.build': '.verify/run-*/portal-build.txt',
};

const ICON = { PASS: 'PASS', FAIL: 'FAIL', WARN: 'WARN', SKIP: 'SKIP', INFRA: 'INFRA_FAIL' };

const md = [];
md.push(`# verify — ${verdict}`);
md.push('');
md.push(`- run: \`${RUN_DIR}\``);
md.push(`- finished: ${new Date().toISOString()}`);
md.push(`- exit code: **${exitCode}** (0 = PASS, 1 = FAIL, 2 = INFRA_FAIL)`);
md.push(`- ${tally.PASS} passed · ${tally.FAIL} failed · ${tally.WARN} warned · ${tally.SKIP} skipped · ${tally.INFRA} blocked`);
md.push('');

if (tally.INFRA) {
  md.push('> **INFRA_FAIL: the environment blocked the run, not the code.**');
  md.push('> Do not treat this as a regression and do not retry in a loop — apply the');
  md.push('> remedy printed in the blocked row below, then re-run once.');
  md.push('');
}

md.push('| Check | Result | Detail | Artifact |');
md.push('|---|---|---|---|');
for (const r of rows) {
  const art = r.artifact && r.artifact !== '-' ? `\`${r.artifact}\`` : '—';
  md.push(`| \`${r.id}\` | ${ICON[r.status] ?? r.status} | ${r.detail.replace(/\|/g, '\\|')} | ${art} |`);
}
md.push('');

const bad = rows.filter(r => r.status === 'FAIL' || r.status === 'INFRA');
if (bad.length) {
  md.push('## Where to look first');
  md.push('');
  for (const r of bad) {
    md.push(`- **\`${r.id}\`** — ${WHERE_TO_LOOK[r.id] ?? 'see the artifact listed above'}`);
  }
  md.push('');
}

md.push('## Artifacts');
md.push('');
md.push('| File | What it holds |');
md.push('|---|---|');
for (const [file, what] of [
  ['screenshots/index.png', 'full-page capture of `/` after the example query ran'],
  ['screenshots/board.png', 'full-page capture of `/board` after the filters were exercised'],
  ['screenshots/maps.png', 'full-page capture of `/maps` with all four canvases drawn'],
  ['screenshots/broadband.png', 'full-page capture of `/broadband` after the evidence floor was exercised'],
  ['console.json', 'every console message and pageerror, tagged by page'],
  ['network.json', 'every failed or 4xx/5xx request, tagged by page'],
  ['trace.zip', 'Playwright trace — `npx playwright show-trace <path>`'],
  ['http.json', 'every HTTP exchange the CLI tier made against the portal'],
  ['go-test.txt', '`go test -race ./...` output'],
  ['go-vet.txt', '`go vet ./...` output'],
  ['lint.txt', 'golangci-lint output, or why it was skipped'],
  ['static-checks.txt', '`node --check` results and the CSS custom-property resolution'],
  ['seed.txt', 'seedlake output and the `pipeline -rebuild` log'],
  ['portal.log', 'the portal process stdout/stderr for this run'],
]) {
  md.push(`| \`${file}\` | ${what} |`);
}
md.push('');

const out = md.join('\n');
if (RUN_DIR) writeFileSync(join(RUN_DIR, 'summary.md'), out);
console.log(out);
process.exit(exitCode);
