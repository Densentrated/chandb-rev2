// Step 2, tier 3: checks that need no browser and no server.
//
// Both of these catch a class of bug that a headless screenshot cannot. A
// syntax error in a module fails at PARSE time, so no handler runs, nothing is
// logged, and the page simply sits on its initial text — the same symptom as a
// slow CDN. And an undefined CSS custom property is not an error at all:
// var(--nope) with no fallback resolves to nothing, the declaration is dropped,
// and the element inherits. Text ends up the wrong colour on a dark background
// and the page still "works".

import { execFileSync } from 'node:child_process';
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync, rmSync, appendFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';

const REPO_ROOT = fileURLToPath(new URL('../..', import.meta.url));
const RUN_DIR = process.env.RUN_DIR;
const CHECKS_TSV = process.env.CHECKS_TSV;
const PORTAL = join(REPO_ROOT, 'cmd', 'portal');
const PAGES = ['index.html', 'board.html', 'maps.html', 'broadband.html'];

const lines = [];
const log = (s) => { lines.push(s); console.log(s); };

function record(id, status, detail, artifact = 'static-checks.txt') {
  const row = [id, status, String(detail).replace(/[\t\n]/g, ' '), artifact];
  if (CHECKS_TSV) appendFileSync(CHECKS_TSV, row.join('\t') + '\n');
}

// ---------------------------------------------------------------- node --check
//
// Everything here is ESM — chandb.js imports from jsDelivr, the page scripts
// are <script type="module"> with top-level await. `node --check` on a .js path
// is ambiguous about module vs script, so each source is copied to a .mjs
// temp file first and the extension settles it.
// Structural problems found while locating the modules — a page with no script
// tag at all, or a tag that loads nothing. These are not syntax errors, so they
// have no temp file to parse; they are merged into the syntax check's failures
// because the consequence is identical: the page runs no code.
const structural = [];

function extractModules() {
  const out = [{ name: 'cmd/portal/chandb.js', code: readFileSync(join(PORTAL, 'chandb.js'), 'utf8') }];

  // Each page's behaviour lives in its own module, referenced by src=. Follow
  // the reference rather than assuming the filename: a page that lost its
  // <script> tag entirely would otherwise still "pass" this check by having
  // nothing to parse, which is exactly how an empty inline block used to slip
  // through after the modules were extracted.
  for (const page of PAGES) {
    const html = readFileSync(join(PORTAL, page), 'utf8');
    const re = /<script\b[^>]*type=["']module["'][^>]*>([\s\S]*?)<\/script>/gi;
    let m, found = 0;
    while ((m = re.exec(html)) !== null) {
      found++;
      const src = /\bsrc=["']\/([\w.-]+\.js)["']/.exec(m[0]);
      if (src) {
        out.push({ name: `cmd/portal/${src[1]} (loaded by ${page})`, code: readFileSync(join(PORTAL, src[1]), 'utf8') });
      } else if (m[1].trim()) {
        out.push({ name: `cmd/portal/${page} inline <script type="module">`, code: m[1] });
      } else {
        structural.push(`${page}: a <script type="module"> has neither src= nor a body`);
      }
    }
    if (found === 0) structural.push(`${page}: loads no module script at all`);
  }
  // The harness's own browser driver is checked too: a typo there fails the
  // run in a way that looks like a page regression.
  for (const f of ['drive.mjs', 'static-checks.mjs', 'summary.mjs']) {
    out.push({ name: `scripts/verify/${f}`, code: readFileSync(join(REPO_ROOT, 'scripts', 'verify', f), 'utf8') });
  }
  return out;
}

function checkSyntax() {
  const dir = mkdtempSync(join(tmpdir(), 'chandb-verify-'));
  const bad = [];
  try {
    for (const { name, code } of extractModules()) {
      const tmp = join(dir, `m${bad.length}-${Math.random().toString(36).slice(2)}.mjs`);
      writeFileSync(tmp, code);
      try {
        execFileSync(process.execPath, ['--check', tmp], { stdio: 'pipe' });
        log(`  ok   ${name}`);
      } catch (e) {
        const msg = String(e.stderr || e.message).trim().split('\n').slice(0, 4).join(' | ');
        log(`  FAIL ${name}: ${msg}`);
        bad.push(name);
      }
    }
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
  for (const s of structural) {
    log(`  FAIL ${s}`);
    bad.push(s);
  }
  return bad;
}

// -------------------------------------------------- CSS custom properties
//
// Collect every --name: definition and every var(--name) reference across the
// pages' inline <style> blocks and portal.css, then assert the reference set is
// a subset of the definition set. Both files style the same elements — the
// pages carry a base layout and portal.css overrides it — so a token defined
// in one and used in the other is correct and must not be flagged.
const DEFINE = /(--[a-zA-Z0-9_-]+)\s*:/g;
const REFER = /var\(\s*(--[a-zA-Z0-9_-]+)\s*(,|\))/g;

function cssSources() {
  const sources = [{ file: 'cmd/portal/portal.css', css: readFileSync(join(PORTAL, 'portal.css'), 'utf8') }];
  for (const page of PAGES) {
    const html = readFileSync(join(PORTAL, page), 'utf8');
    const styles = [...html.matchAll(/<style[^>]*>([\s\S]*?)<\/style>/gi)].map(m => m[1]);
    // style="..." attributes can carry var() too.
    const inline = [...html.matchAll(/style=["']([^"']*)["']/gi)].map(m => m[1]);
    sources.push({ file: `cmd/portal/${page}`, css: [...styles, ...inline].join('\n') });
  }
  return sources;
}

function checkCustomProperties() {
  const sources = cssSources();
  const defined = new Set();
  for (const { css } of sources) {
    for (const m of css.matchAll(DEFINE)) defined.add(m[1]);
  }

  // A var() with a fallback — var(--x, #fff) — degrades gracefully, so it is
  // not a defect. Only the bare form is reported.
  const undefinedRefs = new Map();
  for (const { file, css } of sources) {
    for (const m of css.matchAll(REFER)) {
      const [, name, next] = m;
      if (defined.has(name) || next === ',') continue;
      if (!undefinedRefs.has(name)) undefinedRefs.set(name, new Set());
      undefinedRefs.get(name).add(file);
    }
  }

  log(`  ${defined.size} custom properties defined across ${sources.length} source(s)`);
  for (const [name, files] of [...undefinedRefs].sort()) {
    log(`  FAIL ${name} is referenced but never defined — used in: ${[...files].sort().join(', ')}`);
  }
  return undefinedRefs;
}

// ------------------------------------------------------------------- driver

log('== node --check on every JS module');
const badSyntax = checkSyntax();

log('');
log('== CSS custom property resolution');
const undefinedRefs = checkCustomProperties();

// mkdir first: this tier is also runnable standalone (`make verify-static`),
// where RUN_DIR may point at a directory run.sh has not created.
if (RUN_DIR) {
  mkdirSync(RUN_DIR, { recursive: true });
  writeFileSync(join(RUN_DIR, 'static-checks.txt'), lines.join('\n') + '\n');
}

if (badSyntax.length) {
  record('static.syntax', 'FAIL', `${badSyntax.length} module(s) failed node --check: ${badSyntax.join(', ')}`);
} else {
  record('static.syntax', 'PASS', 'every JS module parses as ESM');
}

if (undefinedRefs.size) {
  const names = [...undefinedRefs.keys()].sort();
  const where = [...new Set([...undefinedRefs.values()].flatMap(s => [...s]))]
    .map(f => relative('cmd/portal', f) || f).sort();
  record('static.cssvars', 'FAIL',
    `${names.length} undefined custom propert${names.length === 1 ? 'y' : 'ies'}: ${names.join(', ')} (in ${where.join(', ')})`);
} else {
  record('static.cssvars', 'PASS', 'every var(--x) resolves to a definition');
}

process.exit(badSyntax.length || undefinedRefs.size ? 1 : 0);
