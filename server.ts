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
  const withNobg = originals.filter((o) => {
    const p = path.parse(o);
    const nobg = `${p.dir}/${p.name}-nobg.png`;
    return keySet.has(nobg);
  }).length;
  const total = originals.length;
  return {
    total,
    withNobg,
    percent: total > 0 ? Math.round((withNobg / total) * 1000) / 10 : 0,
    remaining: total - withNobg,
  };
}

/** Recent activity from R2 object LastModified (works when server ≠ batch machine). */
async function getRates(): Promise<{
  last24h: { count: number; ratePerHour: number };
  last1h: { count: number; ratePerHour: number };
  last10m: { count: number; ratePerHour: number };
  last60s: { count: number; ratePerHour: number };
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
      if (!obj.Key?.endsWith("-nobg.png") || !obj.LastModified) continue;
      const ts = obj.LastModified.getTime();
      for (const w of windows) {
        const cutoff = now - w.seconds * 1000;
        if (ts >= cutoff) {
          result[w.name].count++;
        }
      }
    }
    continuationToken = res.NextContinuationToken;
  } while (continuationToken);

  for (const w of windows) {
    const { count } = result[w.name];
    result[w.name].ratePerHour =
      w.seconds > 0 ? Math.round(((count * 3600) / w.seconds) * 10) / 10 : count * 60;
  }

  return result;
}

const HTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Before & After — Background Removal</title>
  <style>
    * { box-sizing: border-box; }
    body { font-family: system-ui, sans-serif; margin: 0; padding: 1.5rem; background: #111; color: #eee; }
    h1 { font-size: 1.25rem; margin-bottom: 1.5rem; }
    .nav { margin-bottom: 1rem; }
    .nav a { color: #6af; text-decoration: none; }
    .nav a:hover { text-decoration: underline; }
    .loading { color: #888; }
    .grid { display: grid; gap: 1.5rem; max-width: 1000px; margin: 0 auto; }
    .pair { display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; align-items: start; }
    .pair h2 { grid-column: 1 / -1; font-size: 0.8rem; color: #666; margin: 0 0 0.25rem 0; font-weight: 500; }
    .col { text-align: center; }
    .col label { display: block; font-size: 0.7rem; color: #888; margin-bottom: 0.5rem; text-transform: uppercase; letter-spacing: 0.05em; }
    .col img { max-width: 100%; height: auto; border-radius: 6px; background: #222; display: block; }
    .col.after img { background: repeating-conic-gradient(#333 0% 25%, #222 0% 50%) 50% / 12px 12px; }
    .col img[src=""] { min-height: 120px; }
    .error { color: #c44; }
  </style>
</head>
<body>
  <h1>Before & After — Background Removal</h1>
  <div class="nav"><a href="/">Dashboard</a></div>
  <div id="root" class="loading">Loading from R2...</div>
  <script>
    fetch('/api/pairs')
      .then(r => r.json())
      .then(pairs => {
        if (pairs.error) {
          document.getElementById('root').innerHTML = '<p class="error">' + pairs.error + '</p>';
          return;
        }
        if (pairs.length === 0) {
          document.getElementById('root').innerHTML = '<p>No -nobg.png files found in R2.</p>';
          return;
        }
        document.getElementById('root').innerHTML = '<div class="grid">' + pairs.map(p =>
          '<div class="pair"><h2>' + p.name + '</h2>' +
          '<div class="col"><label>Before</label><img src="' + p.before + '" alt="Before" loading="lazy" onerror="this.alt=\\'Failed to load\\'"></div>' +
          '<div class="col after"><label>After</label><img src="' + p.after + '" alt="After" loading="lazy" onerror="this.alt=\\'Failed to load\\'"></div></div>'
        ).join('') + '</div>';
      })
      .catch(e => {
        document.getElementById('root').innerHTML = '<p class="error">' + e.message + '</p>';
      });
  </script>
</body>
</html>`;

const DASHBOARD_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Background Removal — Dashboard</title>
  <style>
    * { box-sizing: border-box; }
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif; margin: 0; padding: 2rem; background: #0d0d0d; color: #f5f5f5; min-height: 100vh; }
    h1 { font-size: 1.5rem; font-weight: 600; margin-bottom: 2rem; letter-spacing: -0.02em; }
    .nav { margin-bottom: 2rem; }
    .nav a { color: #8b9dc3; text-decoration: none; font-size: 0.9rem; }
    .nav a:hover { color: #a8b8e0; }
    .stats { display: grid; grid-template-columns: repeat(3, 1fr); gap: 1.25rem; max-width: 560px; margin-bottom: 2.5rem; }
    .stat { background: #1a1a1a; padding: 1.25rem; border-radius: 10px; text-align: center; border: 1px solid #252525; }
    .stat-value { font-size: 1.875rem; font-weight: 600; letter-spacing: -0.02em; }
    .stat-label { font-size: 0.7rem; color: #6b6b6b; text-transform: uppercase; letter-spacing: 0.08em; margin-top: 0.35rem; }
    .percent-wrap { max-width: 420px; margin-bottom: 2rem; }
    .percent-value { font-size: 2.75rem; font-weight: 600; margin-bottom: 0.75rem; letter-spacing: -0.03em; color: #e8e8e8; }
    .progress-bar { height: 10px; background: #252525; border-radius: 5px; overflow: hidden; }
    .progress-fill { height: 100%; background: linear-gradient(90deg, #3d7a5c, #4a9d6e); border-radius: 5px; transition: width 0.4s ease; }
    .eta-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 1rem; max-width: 720px; margin-bottom: 2rem; }
    @media (max-width: 640px) { .eta-grid { grid-template-columns: 1fr; } }
    .eta-wrap { padding: 1rem 1.25rem; background: #1a1a1a; border-radius: 10px; border: 1px solid #252525; }
    .eta-label { font-size: 0.7rem; color: #6b6b6b; text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 0.5rem; }
    .eta-value { font-size: 1.35rem; font-weight: 600; letter-spacing: -0.02em; color: #c9d4e8; font-variant-numeric: tabular-nums; }
    .eta-note { font-size: 0.75rem; color: #5a5a5a; margin-top: 0.35rem; }
    .rates-section { margin-top: 2.5rem; }
    .rates-title { font-size: 0.85rem; font-weight: 600; color: #8b9dc3; margin-bottom: 1rem; text-transform: uppercase; letter-spacing: 0.06em; }
    .rates-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 1rem; max-width: 720px; }
    .rate-card { background: #1a1a1a; padding: 1rem; border-radius: 10px; border: 1px solid #252525; }
    .rate-card .window { font-size: 0.7rem; color: #6b6b6b; text-transform: uppercase; letter-spacing: 0.06em; margin-bottom: 0.25rem; }
    .rate-card .count { font-size: 1.5rem; font-weight: 600; }
    .rate-card .rate { font-size: 0.75rem; color: #6a9d7a; margin-top: 0.25rem; }
    .updated { font-size: 0.75rem; color: #4a4a4a; margin-top: 1rem; }
    .loading { color: #6b6b6b; }
    .error { color: #c45c5c; }
  </style>
</head>
<body>
  <h1>Background Removal</h1>
  <div class="nav"><a href="/images">View before/after pairs</a></div>
  <div id="root" class="loading">Loading...</div>
  <div id="updated" class="updated"></div>
  <script>
    function formatEtaHms(totalSeconds) {
      var sec = Math.max(0, Math.floor(totalSeconds));
      var h = Math.floor(sec / 3600);
      var m = Math.floor((sec % 3600) / 60);
      var s = sec % 60;
      return h + 'h ' + m + 'm ' + s + 's';
    }
    function etaOne(rem, rates, label, windowSec, countKey, noteWhenOk) {
      if (rem <= 0) {
        return '<div class="eta-wrap"><div class="eta-label">' + label + '</div>' +
          '<div class="eta-value">' + formatEtaHms(0) + '</div><div class="eta-note">Nothing left to process</div></div>';
      }
      if (!rates || rates.error || !rates[countKey]) {
        return '<div class="eta-wrap"><div class="eta-label">' + label + '</div>' +
          '<div class="eta-value">—</div><div class="eta-note">Rates unavailable</div></div>';
      }
      var c = rates[countKey].count;
      if (c <= 0) {
        return '<div class="eta-wrap"><div class="eta-label">' + label + '</div>' +
          '<div class="eta-value">—</div><div class="eta-note">No -nobg uploads in this window</div></div>';
      }
      var etaSec = Math.ceil(rem * windowSec / c);
      return '<div class="eta-wrap"><div class="eta-label">' + label + '</div>' +
        '<div class="eta-value">' + formatEtaHms(etaSec) + '</div>' +
        '<div class="eta-note">' + noteWhenOk + '</div></div>';
    }
    function etaBlock(stats, rates) {
      var rem = stats.remaining;
      var note60 = rem > 0 && rates && !rates.error && rates.last60s && rates.last60s.count > 0
        ? rem.toLocaleString() + ' remaining at ' + rates.last60s.count + ' / 60s' : '';
      var note1h = rem > 0 && rates && !rates.error && rates.last1h && rates.last1h.count > 0
        ? rem.toLocaleString() + ' remaining at ' + rates.last1h.count + ' / 1h' : '';
      var a = etaOne(rem, rates, 'Est. time to completion (last 60s rate)', 60, 'last60s', note60);
      var b = etaOne(rem, rates, 'Est. time to completion (last hour rate)', 3600, 'last1h', note1h);
      return '<div class="eta-grid">' + a + b + '</div>';
    }
    function render(stats, rates) {
      if (stats.error) {
        document.getElementById('root').innerHTML = '<p class="error">' + stats.error + '</p>';
        return;
      }
      var ratesHtml = '';
      if (rates && !rates.error) {
        ratesHtml = '<div class="rates-section">' +
          '<div class="rates-title">Backgrounds removed (recent activity)</div>' +
          '<div class="rates-grid">' +
          '<div class="rate-card"><div class="window">Last 24 hours</div><div class="count">' + rates.last24h.count + '</div><div class="rate">' + rates.last24h.ratePerHour + '/hr</div></div>' +
          '<div class="rate-card"><div class="window">Last hour</div><div class="count">' + rates.last1h.count + '</div><div class="rate">' + rates.last1h.ratePerHour + '/hr</div></div>' +
          '<div class="rate-card"><div class="window">Last 10 mins</div><div class="count">' + rates.last10m.count + '</div><div class="rate">' + rates.last10m.ratePerHour + '/hr</div></div>' +
          '<div class="rate-card"><div class="window">Last 60 secs</div><div class="count">' + rates.last60s.count + '</div><div class="rate">' + rates.last60s.ratePerHour + '/hr</div></div>' +
          '</div></div>';
      }
      document.getElementById('root').innerHTML =
        '<div class="stats">' +
        '<div class="stat"><div class="stat-value">' + stats.total.toLocaleString() + '</div><div class="stat-label">Total images</div></div>' +
        '<div class="stat"><div class="stat-value">' + stats.withNobg.toLocaleString() + '</div><div class="stat-label">With no-bg</div></div>' +
        '<div class="stat"><div class="stat-value">' + stats.remaining.toLocaleString() + '</div><div class="stat-label">Remaining</div></div>' +
        '</div>' +
        '<div class="percent-wrap">' +
        '<div class="percent-value">' + stats.percent + '%</div>' +
        '<div class="progress-bar"><div class="progress-fill" style="width:' + stats.percent + '%"></div></div>' +
        '</div>' +
        etaBlock(stats, rates) +
        ratesHtml;
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
