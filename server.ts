/**
 * Simple UI server: lists -nobg.png files from R2, serves before/after pairs.
 * Run: source .env && npx tsx server.ts
 */

import { createServer } from "http";
import { S3Client, ListObjectsV2Command } from "@aws-sdk/client-s3";
import * as path from "path";

for (const k of ["R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET_NAME", "R2_PUBLIC_URL"]) {
  if (!process.env[k]) {
    console.error(`Missing: ${k}`);
    process.exit(1);
  }
}
const R2_ACCOUNT_ID = process.env.R2_ACCOUNT_ID!;
const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID!;
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY!;
const R2_BUCKET_NAME = process.env.R2_BUCKET_NAME!;
const R2_PUBLIC_URL = process.env.R2_PUBLIC_URL!.replace(/\/$/, "");

const r2 = new S3Client({
  region: "auto",
  endpoint: `https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: R2_ACCESS_KEY_ID,
    secretAccessKey: R2_SECRET_ACCESS_KEY,
  },
});

async function listAllKeys(): Promise<string[]> {
  const keys: string[] = [];
  let continuationToken: string | undefined;
  do {
    const res = await r2.send(
      new ListObjectsV2Command({
        Bucket: R2_BUCKET_NAME,
        Prefix: "products/",
        ContinuationToken: continuationToken,
        MaxKeys: 1000,
      })
    );
    for (const obj of res.Contents ?? []) {
      if (obj.Key) keys.push(obj.Key);
    }
    continuationToken = res.NextContinuationToken;
  } while (continuationToken);
  return keys;
}

/** products/retailer/slug/file -> products/retailer/slug */
function productPrefixFromKey(key: string): string | null {
  const parts = key.split("/");
  if (parts.length < 4 || parts[0] !== "products") return null;
  return `${parts[0]}/${parts[1]}/${parts[2]}`;
}

function originalKeyFromNobg(nobgKey: string, allKeys: Set<string>): string | null {
  // products/retailer/slug/0-nobg.png -> products/retailer/slug/0
  const base = nobgKey.replace(/-nobg\.png$/, "");
  const exts = ["jpg", "jpeg", "png", "webp", "gif", "avif"];
  for (const ext of exts) {
    const candidate = `${base}.${ext}`;
    if (allKeys.has(candidate)) return candidate;
  }
  return null;
}

async function getPairs(): Promise<{ before: string; after: string; name: string }[]> {
  const keys = await listAllKeys();
  const keySet = new Set(keys);
  const nobgKeys = keys.filter((k) => k.endsWith("-nobg.png"));
  const pairs: { before: string; after: string; name: string }[] = [];

  for (const nobgKey of nobgKeys) {
    const original = originalKeyFromNobg(nobgKey, keySet);
    if (original) {
      pairs.push({
        before: `${R2_PUBLIC_URL}/${original}`,
        after: `${R2_PUBLIC_URL}/${nobgKey}`,
        name: path.basename(original),
      });
    }
  }

  return pairs;
}

async function getStats(): Promise<{
  total: number;
  totalProducts: number;
  withNobg: number;
  percent: number;
  remaining: number;
}> {
  const keys = await listAllKeys();
  const keySet = new Set(keys);
  const imageExts = new Set([".jpg", ".jpeg", ".png", ".webp", ".gif", ".avif"]);
  const originals = keys.filter((k) => {
    if (k.endsWith("-nobg.png")) return false;
    return imageExts.has(path.extname(k).toLowerCase());
  });
  const productPrefixes = new Set<string>();
  for (const o of originals) {
    const prefix = productPrefixFromKey(o);
    if (prefix) productPrefixes.add(prefix);
  }
  const withNobg = originals.filter((o) => {
    const p = path.parse(o);
    const nobg = `${p.dir}/${p.name}-nobg.png`;
    return keySet.has(nobg);
  }).length;
  const total = originals.length;
  return {
    total,
    totalProducts: productPrefixes.size,
    withNobg,
    percent: total > 0 ? Math.round((withNobg / total) * 1000) / 10 : 0,
    remaining: total - withNobg,
  };
}

type RateWindow = { count: number; ratePerHour: number };

/** Recent activity from R2 object LastModified (works when server ≠ batch machine). */
async function getRates(): Promise<{
  last24h: RateWindow;
  last1h: RateWindow;
  last10m: RateWindow;
  last60s: RateWindow;
  newProducts: {
    last24h: RateWindow;
    last1h: RateWindow;
    last10m: RateWindow;
    last60s: RateWindow;
  };
}> {
  const now = Date.now();
  const windows = [
    { name: "last24h" as const, seconds: 24 * 3600 },
    { name: "last1h" as const, seconds: 3600 },
    { name: "last10m" as const, seconds: 10 * 60 },
    { name: "last60s" as const, seconds: 60 },
  ];

  const result = {
    last24h: { count: 0, ratePerHour: 0 },
    last1h: { count: 0, ratePerHour: 0 },
    last10m: { count: 0, ratePerHour: 0 },
    last60s: { count: 0, ratePerHour: 0 },
  };

  const prefixMinTime = new Map<string, number>();

  let continuationToken: string | undefined;
  do {
    const res = await r2.send(
      new ListObjectsV2Command({
        Bucket: R2_BUCKET_NAME,
        Prefix: "products/",
        ContinuationToken: continuationToken,
        MaxKeys: 1000,
      })
    );
    for (const obj of res.Contents ?? []) {
      if (!obj.Key || !obj.LastModified) continue;
      const ts = obj.LastModified.getTime();
      const prefix = productPrefixFromKey(obj.Key);
      if (prefix) {
        const prev = prefixMinTime.get(prefix);
        if (prev === undefined || ts < prev) prefixMinTime.set(prefix, ts);
      }
      if (!obj.Key.endsWith("-nobg.png")) continue;
      for (const w of windows) {
        const cutoff = now - w.seconds * 1000;
        if (ts >= cutoff) {
          result[w.name].count++;
        }
      }
    }
    continuationToken = res.NextContinuationToken;
  } while (continuationToken);

  const newProducts = {
    last24h: { count: 0, ratePerHour: 0 },
    last1h: { count: 0, ratePerHour: 0 },
    last10m: { count: 0, ratePerHour: 0 },
    last60s: { count: 0, ratePerHour: 0 },
  };

  for (const w of windows) {
    const { count } = result[w.name];
    result[w.name].ratePerHour =
      w.seconds > 0 ? Math.round(((count * 3600) / w.seconds) * 10) / 10 : count * 60;
  }

  for (const w of windows) {
    const cutoff = now - w.seconds * 1000;
    let c = 0;
    for (const minTs of prefixMinTime.values()) {
      if (minTs >= cutoff) c++;
    }
    newProducts[w.name].count = c;
    newProducts[w.name].ratePerHour =
      w.seconds > 0 ? Math.round(((c * 3600) / w.seconds) * 10) / 10 : c * 60;
  }

  return { ...result, newProducts };
}

const SHARED_PAGE_CSS = `
    :root {
      --bg: #0c0c0d;
      --surface: #131316;
      --border: rgba(255, 255, 255, 0.07);
      --text: #ececed;
      --muted: #8e8e93;
      --faint: #5a5a5e;
      --link: #7a9fd4;
      --link-hover: #9bb6e8;
      --radius: 8px;
      --font: -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif;
      --before-pct: 50%;
      --shell-max: min(95vw, 1800px);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      font-family: var(--font);
      font-size: 15px;
      line-height: 1.5;
      background: var(--bg);
      color: var(--text);
      -webkit-font-smoothing: antialiased;
    }
    body.resizing { cursor: col-resize !important; user-select: none; }
    body.resizing img, body.resizing a { pointer-events: none; }
    .shell { max-width: var(--shell-max); margin: 0 auto; padding: 0 1.5rem 3rem; }
    .toolbar {
      display: flex;
      align-items: center;
      gap: 0.5rem;
      padding: 0.5rem 0 1rem;
      font-size: 0.75rem;
      color: var(--muted);
    }
    .toolbar .hint { color: var(--faint); }
    .toolbar button {
      background: var(--surface);
      border: 1px solid var(--border);
      color: var(--text);
      font: inherit;
      font-size: 0.75rem;
      padding: 0.35rem 0.7rem;
      border-radius: 6px;
      cursor: pointer;
    }
    .toolbar button:hover { border-color: var(--link); color: var(--link); }
    .toolbar .ratio { font-variant-numeric: tabular-nums; min-width: 7ch; text-align: right; }
    .page-head {
      padding: 1.75rem 0 1.5rem;
      border-bottom: 1px solid var(--border);
      margin-bottom: 2rem;
    }
    .page-head h1 {
      margin: 0 0 0.35rem 0;
      font-size: 1.125rem;
      font-weight: 600;
      letter-spacing: -0.02em;
    }
    .page-head .sub { margin: 0; font-size: 0.8125rem; color: var(--muted); font-weight: 400; }
    .page-head .row { display: flex; flex-wrap: wrap; align-items: baseline; justify-content: space-between; gap: 1rem; }
    .nav a {
      font-size: 0.8125rem;
      color: var(--link);
      text-decoration: none;
    }
    .nav a:hover { color: var(--link-hover); text-decoration: underline; text-underline-offset: 3px; }
    .loading { color: var(--muted); font-size: 0.875rem; }
    .grid { display: grid; gap: 1.75rem; }
    .pair {
      display: grid;
      grid-template-columns: calc(var(--before-pct) - 6px) 12px 1fr;
      gap: 1rem 0;
      align-items: start;
      padding-bottom: 1.75rem;
      border-bottom: 1px solid var(--border);
    }
    .pair:last-child { border-bottom: none; padding-bottom: 0; }
    .pair h2 {
      grid-column: 1 / -1;
      margin: 0 0 0.5rem 0;
      font-size: 0.75rem;
      font-weight: 500;
      color: var(--muted);
      letter-spacing: 0.02em;
    }
    .resizer {
      cursor: col-resize;
      position: relative;
      align-self: stretch;
      touch-action: none;
      -webkit-user-select: none;
      user-select: none;
    }
    .resizer::before {
      content: "";
      position: absolute;
      top: 1.5rem;
      bottom: 0;
      left: 50%;
      transform: translateX(-50%);
      width: 2px;
      background: var(--border);
      border-radius: 1px;
      transition: background 0.15s ease, width 0.15s ease;
    }
    .resizer:hover::before, .resizer.dragging::before {
      background: var(--link);
      width: 3px;
    }
    .col { text-align: center; }
    .col label {
      display: block;
      font-size: 0.6875rem;
      font-weight: 500;
      color: var(--faint);
      margin-bottom: 0.5rem;
      text-transform: uppercase;
      letter-spacing: 0.06em;
    }
    .col img {
      max-width: 100%;
      height: auto;
      border-radius: var(--radius);
      background: var(--surface);
      display: block;
      border: 1px solid var(--border);
    }
    .col.after img {
      background: repeating-conic-gradient(#1e1e22 0% 25%, #16161a 0% 50%) 50% / 14px 14px;
    }
    .col img[src=""] { min-height: 120px; }
    .error { color: #d96b6b; font-size: 0.875rem; }
    .empty { color: var(--muted); font-size: 0.875rem; margin: 0; }
`;

const HTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Before / after — Background removal</title>
  <style>${SHARED_PAGE_CSS}</style>
</head>
<body>
  <div class="shell">
    <header class="page-head">
      <div class="row">
        <div>
          <h1>Before / after</h1>
          <p class="sub">Original and background-removed images from R2 · Drag the divider between columns to resize · Double-click to reset</p>
        </div>
        <nav class="nav"><a href="/">Dashboard</a></nav>
      </div>
    </header>
    <div class="toolbar">
      <span class="hint">Split:</span>
      <span class="ratio" id="ratio">50% / 50%</span>
      <button type="button" id="reset-split">Reset</button>
    </div>
    <div id="root" class="loading">Loading from R2…</div>
  <script>
    (function() {
      var KEY = 'bgr.beforePct';
      var MIN = 15, MAX = 85;
      var ratioEl = document.getElementById('ratio');
      function updateLabel(pct) {
        var b = Math.round(pct);
        if (ratioEl) ratioEl.textContent = b + '% / ' + (100 - b) + '%';
      }
      function setPct(pct, persist) {
        if (pct < MIN) pct = MIN;
        if (pct > MAX) pct = MAX;
        document.documentElement.style.setProperty('--before-pct', pct + '%');
        updateLabel(pct);
        if (persist) localStorage.setItem(KEY, pct.toFixed(2));
      }
      var saved = parseFloat(localStorage.getItem(KEY));
      setPct(!isNaN(saved) ? saved : 50, false);

      var dragging = null;
      document.addEventListener('pointerdown', function(e) {
        var r = e.target && e.target.closest && e.target.closest('.resizer');
        if (!r) return;
        var pair = r.closest('.pair');
        if (!pair) return;
        dragging = { resizer: r, pair: pair };
        r.classList.add('dragging');
        document.body.classList.add('resizing');
        if (r.setPointerCapture) { try { r.setPointerCapture(e.pointerId); } catch (_) {} }
        e.preventDefault();
      });
      document.addEventListener('pointermove', function(e) {
        if (!dragging) return;
        var rect = dragging.pair.getBoundingClientRect();
        if (rect.width <= 0) return;
        var pct = ((e.clientX - rect.left) / rect.width) * 100;
        setPct(pct, true);
      });
      function endDrag() {
        if (!dragging) return;
        dragging.resizer.classList.remove('dragging');
        document.body.classList.remove('resizing');
        dragging = null;
      }
      document.addEventListener('pointerup', endDrag);
      document.addEventListener('pointercancel', endDrag);
      document.addEventListener('dblclick', function(e) {
        var r = e.target && e.target.closest && e.target.closest('.resizer');
        if (!r) return;
        setPct(50, true);
      });
      var resetBtn = document.getElementById('reset-split');
      if (resetBtn) resetBtn.addEventListener('click', function() { setPct(50, true); });
    })();

    fetch('/api/pairs')
      .then(r => r.json())
      .then(pairs => {
        if (pairs.error) {
          document.getElementById('root').innerHTML = '<p class="error">' + pairs.error + '</p>';
          return;
        }
        if (pairs.length === 0) {
          document.getElementById('root').innerHTML = '<p class="empty">No -nobg.png files found in R2.</p>';
          return;
        }
        document.getElementById('root').innerHTML = '<div class="grid">' + pairs.map(p =>
          '<div class="pair"><h2>' + p.name + '</h2>' +
          '<div class="col"><label>Before</label><img src="' + p.before + '" alt="Before" loading="lazy" onerror="this.alt=\\'Failed to load\\'"></div>' +
          '<div class="resizer" role="separator" aria-orientation="vertical" aria-label="Resize columns" title="Drag to resize · double-click to reset"></div>' +
          '<div class="col after"><label>After</label><img src="' + p.after + '" alt="After" loading="lazy" onerror="this.alt=\\'Failed to load\\'"></div></div>'
        ).join('') + '</div>';
      })
      .catch(e => {
        document.getElementById('root').innerHTML = '<p class="error">' + e.message + '</p>';
      });
  </script>
  </div>
</body>
</html>`;

const DASHBOARD_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Background removal — Dashboard</title>
  <style>
    :root {
      --bg: #0c0c0d;
      --surface: #131316;
      --border: rgba(255, 255, 255, 0.07);
      --text: #ececed;
      --muted: #8e8e93;
      --faint: #5a5a5e;
      --link: #7a9fd4;
      --link-hover: #9bb6e8;
      --accent: #2f8f62;
      --accent-dim: rgba(47, 143, 98, 0.35);
      --radius: 8px;
      --font: -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      font-family: var(--font);
      font-size: 15px;
      line-height: 1.5;
      background: var(--bg);
      color: var(--text);
      -webkit-font-smoothing: antialiased;
    }
    .shell { max-width: 1080px; margin: 0 auto; padding: 0 1.5rem 2.5rem; }
    .page-head {
      padding: 1.75rem 0 1.5rem;
      border-bottom: 1px solid var(--border);
      margin-bottom: 2rem;
    }
    .page-head .row {
      display: flex;
      flex-wrap: wrap;
      align-items: flex-end;
      justify-content: space-between;
      gap: 1rem 1.5rem;
    }
    .page-head h1 {
      margin: 0 0 0.35rem 0;
      font-size: 1.125rem;
      font-weight: 600;
      letter-spacing: -0.02em;
    }
    .page-head .sub { margin: 0; font-size: 0.8125rem; color: var(--muted); font-weight: 400; }
    .nav a {
      font-size: 0.8125rem;
      color: var(--link);
      text-decoration: none;
    }
    .nav a:hover { color: var(--link-hover); text-decoration: underline; text-underline-offset: 3px; }
    .stack { display: flex; flex-direction: column; gap: 1.75rem; }
    .section-label {
      margin: 0 0 0.75rem 0;
      font-size: 0.6875rem;
      font-weight: 600;
      color: var(--faint);
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }
    .kpi-row {
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 0.75rem;
    }
    @media (max-width: 720px) {
      .kpi-row { grid-template-columns: repeat(2, 1fr); }
    }
    .stat {
      background: var(--surface);
      padding: 1rem 0.875rem;
      border-radius: var(--radius);
      border: 1px solid var(--border);
      text-align: center;
    }
    .stat-value {
      font-size: 1.5rem;
      font-weight: 600;
      letter-spacing: -0.02em;
      font-variant-numeric: tabular-nums;
      line-height: 1.2;
    }
    .stat-label {
      font-size: 0.6875rem;
      color: var(--muted);
      margin-top: 0.4rem;
      letter-spacing: 0.04em;
    }
    .panel {
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      padding: 1.25rem 1.35rem;
    }
    .panel--progress .percent-value {
      font-size: 2rem;
      font-weight: 600;
      letter-spacing: -0.03em;
      margin-bottom: 0.65rem;
      font-variant-numeric: tabular-nums;
    }
    .progress-bar {
      height: 6px;
      background: rgba(255, 255, 255, 0.06);
      border-radius: 3px;
      overflow: hidden;
    }
    .progress-fill {
      height: 100%;
      background: var(--accent);
      border-radius: 3px;
      transition: width 0.45s ease;
      box-shadow: 0 0 12px var(--accent-dim);
    }
    .eta-label {
      font-size: 0.6875rem;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.06em;
      margin-bottom: 0.35rem;
    }
    .eta-label + .eta-value + .eta-label { margin-top: 0.85rem; }
    .eta-value {
      font-size: 1.2rem;
      font-weight: 600;
      letter-spacing: -0.02em;
      font-variant-numeric: tabular-nums;
      color: var(--text);
    }
    .eta-at { font-size: 1rem; font-weight: 500; color: var(--muted); }
    .eta-note { font-size: 0.75rem; color: var(--faint); margin-top: 0.65rem; line-height: 1.45; }
    .rates-block { margin-top: 0.25rem; }
    .rates-block + .rates-block { margin-top: 1.75rem; padding-top: 1.75rem; border-top: 1px solid var(--border); }
    .rates-grid {
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 0.75rem;
    }
    @media (max-width: 900px) {
      .rates-grid { grid-template-columns: repeat(2, 1fr); }
    }
    .rate-card {
      background: var(--bg);
      padding: 0.875rem 0.75rem;
      border-radius: 6px;
      border: 1px solid var(--border);
    }
    .rate-card .window {
      font-size: 0.6875rem;
      color: var(--faint);
      text-transform: uppercase;
      letter-spacing: 0.05em;
      margin-bottom: 0.2rem;
    }
    .rate-card .count {
      font-size: 1.25rem;
      font-weight: 600;
      font-variant-numeric: tabular-nums;
    }
    .rate-card .rate { font-size: 0.75rem; color: var(--muted); margin-top: 0.2rem; }
    .footnote { font-size: 0.75rem; color: var(--faint); margin-top: 0.65rem; line-height: 1.45; max-width: 52ch; }
    .page-foot {
      margin-top: 2.25rem;
      padding-top: 1.25rem;
      border-top: 1px solid var(--border);
      font-size: 0.75rem;
      color: var(--faint);
    }
    .loading { color: var(--muted); font-size: 0.875rem; }
    .error { color: #d96b6b; font-size: 0.875rem; }
  </style>
</head>
<body>
  <div class="shell">
    <header class="page-head">
      <div class="row">
        <div>
          <h1>Background removal</h1>
          <p class="sub">Progress across images in R2 · Refreshes every 5s</p>
        </div>
        <nav class="nav"><a href="/images">Before / after</a></nav>
      </div>
    </header>
    <div id="root" class="loading">Loading…</div>
    <footer id="updated" class="page-foot" aria-live="polite"></footer>
  </div>
  <script>
    function formatEtaHms(totalSeconds) {
      var sec = Math.max(0, Math.floor(totalSeconds));
      var h = Math.floor(sec / 3600);
      var m = Math.floor((sec % 3600) / 60);
      var s = sec % 60;
      return h + 'h ' + m + 'm ' + s + 's';
    }
    function formatCompletionAt(etaSec) {
      var d = new Date(Date.now() + etaSec * 1000);
      return d.toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'medium' });
    }
    function etaBlock(stats, rates) {
      var rem = stats.remaining;
      if (rem <= 0) {
        return '<div class="panel">' +
          '<div class="eta-label">Time remaining (60s rate)</div>' +
          '<div class="eta-value">' + formatEtaHms(0) + '</div>' +
          '<div class="eta-label">Completion time</div>' +
          '<div class="eta-value eta-at">—</div>' +
          '<div class="eta-note">Nothing left to process</div></div>';
      }
      if (!rates || rates.error || !rates.last60s) {
        return '<div class="panel">' +
          '<div class="eta-label">Time remaining (60s rate)</div>' +
          '<div class="eta-value">—</div>' +
          '<div class="eta-label">Completion time</div>' +
          '<div class="eta-value eta-at">—</div>' +
          '<div class="eta-note">Rates unavailable</div></div>';
      }
      var c60 = rates.last60s.count;
      if (c60 <= 0) {
        return '<div class="panel">' +
          '<div class="eta-label">Time remaining (60s rate)</div>' +
          '<div class="eta-value">—</div>' +
          '<div class="eta-label">Completion time</div>' +
          '<div class="eta-value eta-at">—</div>' +
          '<div class="eta-note">No -nobg uploads in the last 60s</div></div>';
      }
      var etaSec = Math.ceil(rem * 60 / c60);
      return '<div class="panel">' +
        '<div class="eta-label">Time remaining (60s rate)</div>' +
        '<div class="eta-value">' + formatEtaHms(etaSec) + '</div>' +
        '<div class="eta-label">Completion time</div>' +
        '<div class="eta-value eta-at">' + formatCompletionAt(etaSec) + '</div>' +
        '<div class="eta-note">' + rem.toLocaleString() + ' remaining at ' + c60 + ' / 60s</div></div>';
    }
    function render(stats, rates) {
      if (stats.error) {
        document.getElementById('root').innerHTML = '<p class="error">' + stats.error + '</p>';
        return;
      }
      var ratesHtml = '';
      var np = rates && !rates.error && rates.newProducts ? rates.newProducts : null;
      if (rates && !rates.error) {
        ratesHtml = '<div class="rates-block">' +
          '<p class="section-label">Backgrounds removed</p>' +
          '<div class="rates-grid">' +
          '<div class="rate-card"><div class="window">Last 24 hours</div><div class="count">' + rates.last24h.count + '</div><div class="rate">' + rates.last24h.ratePerHour + '/hr</div></div>' +
          '<div class="rate-card"><div class="window">Last hour</div><div class="count">' + rates.last1h.count + '</div><div class="rate">' + rates.last1h.ratePerHour + '/hr</div></div>' +
          '<div class="rate-card"><div class="window">Last 10 mins</div><div class="count">' + rates.last10m.count + '</div><div class="rate">' + rates.last10m.ratePerHour + '/hr</div></div>' +
          '<div class="rate-card"><div class="window">Last 60 secs</div><div class="count">' + rates.last60s.count + '</div><div class="rate">' + rates.last60s.ratePerHour + '/hr</div></div>' +
          '</div></div>';
        if (np) {
          ratesHtml += '<div class="rates-block">' +
            '<p class="section-label">New products</p>' +
            '<div class="rates-grid">' +
            '<div class="rate-card"><div class="window">Last 24 hours</div><div class="count">' + np.last24h.count + '</div><div class="rate">' + np.last24h.ratePerHour + '/hr</div></div>' +
            '<div class="rate-card"><div class="window">Last hour</div><div class="count">' + np.last1h.count + '</div><div class="rate">' + np.last1h.ratePerHour + '/hr</div></div>' +
            '<div class="rate-card"><div class="window">Last 10 mins</div><div class="count">' + np.last10m.count + '</div><div class="rate">' + np.last10m.ratePerHour + '/hr</div></div>' +
            '<div class="rate-card"><div class="window">Last 60 secs</div><div class="count">' + np.last60s.count + '</div><div class="rate">' + np.last60s.ratePerHour + '/hr</div></div>' +
            '</div>' +
            '<p class="footnote">Inferred from oldest object time per product folder (min. LastModified).</p></div>';
        }
      }
      var totalProductsVal = typeof stats.totalProducts === 'number' ? stats.totalProducts.toLocaleString() : '—';
      document.getElementById('root').innerHTML =
        '<div class="stack">' +
        '<div><p class="section-label">Overview</p><div class="kpi-row">' +
        '<div class="stat"><div class="stat-value">' + stats.total.toLocaleString() + '</div><div class="stat-label">Total images</div></div>' +
        '<div class="stat"><div class="stat-value">' + totalProductsVal + '</div><div class="stat-label">Products</div></div>' +
        '<div class="stat"><div class="stat-value">' + stats.withNobg.toLocaleString() + '</div><div class="stat-label">With no-bg</div></div>' +
        '<div class="stat"><div class="stat-value">' + stats.remaining.toLocaleString() + '</div><div class="stat-label">Remaining</div></div>' +
        '</div></div>' +
        '<div class="panel panel--progress">' +
        '<p class="section-label">Progress</p>' +
        '<div class="percent-value">' + stats.percent + '%</div>' +
        '<div class="progress-bar"><div class="progress-fill" style="width:' + stats.percent + '%"></div></div>' +
        '</div>' +
        '<div><p class="section-label">ETA</p>' + etaBlock(stats, rates) + '</div>' +
        ratesHtml +
        '</div>';
    }
    function poll() {
      Promise.all([fetch('/api/stats').then(r => r.json()), fetch('/api/rates').then(r => r.json())])
        .then(([stats, rates]) => {
          render(stats, rates);
          document.getElementById('updated').textContent = 'Last updated: ' + new Date().toLocaleTimeString();
        })
        .catch(e => {
          document.getElementById('root').innerHTML = '<p class="error">' + e.message + '</p>';
        });
    }
    poll();
    setInterval(poll, 5000);
  </script>
</body>
</html>`;

const server = createServer(async (req, res) => {
  if (req.url === "/api/pairs") {
    try {
      const pairs = await getPairs();
      res.setHeader("Content-Type", "application/json");
      res.setHeader("Access-Control-Allow-Origin", "*");
      res.end(JSON.stringify(pairs));
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      res.statusCode = 500;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ error: msg }));
    }
    return;
  }
  if (req.url === "/api/stats") {
    try {
      const stats = await getStats();
      res.setHeader("Content-Type", "application/json");
      res.setHeader("Access-Control-Allow-Origin", "*");
      res.end(JSON.stringify(stats));
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      res.statusCode = 500;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ error: msg }));
    }
    return;
  }
  if (req.url === "/api/rates") {
    try {
      const rates = await getRates();
      res.setHeader("Content-Type", "application/json");
      res.setHeader("Access-Control-Allow-Origin", "*");
      res.end(JSON.stringify(rates));
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      res.statusCode = 500;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ error: msg }));
    }
    return;
  }
  if (req.url === "/" || req.url === "/index.html") {
    res.setHeader("Content-Type", "text/html");
    res.end(DASHBOARD_HTML);
    return;
  }
  if (req.url === "/images") {
    res.setHeader("Content-Type", "text/html");
    res.end(HTML);
    return;
  }
  res.statusCode = 404;
  res.end("Not found");
});

const port = parseInt(process.env.PORT || "3457", 10);
server.listen(port, "0.0.0.0", () => {
  console.log(`Dashboard at http://0.0.0.0:${port}`);
});
