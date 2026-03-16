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
  <div class="nav"><a href="/dashboard">View progress dashboard</a></div>
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
  <title>Background Removal Progress</title>
  <style>
    * { box-sizing: border-box; }
    body { font-family: system-ui, sans-serif; margin: 0; padding: 1.5rem; background: #111; color: #eee; }
    h1 { font-size: 1.25rem; margin-bottom: 1.5rem; }
    .nav { margin-bottom: 1.5rem; }
    .nav a { color: #6af; text-decoration: none; }
    .nav a:hover { text-decoration: underline; }
    .stats { display: grid; grid-template-columns: repeat(3, 1fr); gap: 1rem; max-width: 600px; margin-bottom: 2rem; }
    .stat { background: #222; padding: 1rem; border-radius: 8px; text-align: center; }
    .stat-value { font-size: 1.75rem; font-weight: 600; }
    .stat-label { font-size: 0.75rem; color: #888; text-transform: uppercase; letter-spacing: 0.05em; margin-top: 0.25rem; }
    .percent-wrap { max-width: 400px; margin-bottom: 1rem; }
    .percent-value { font-size: 2.5rem; font-weight: 600; margin-bottom: 0.5rem; }
    .progress-bar { height: 12px; background: #333; border-radius: 6px; overflow: hidden; }
    .progress-fill { height: 100%; background: #4a8; transition: width 0.3s ease; }
    .updated { font-size: 0.8rem; color: #666; }
    .loading { color: #888; }
    .error { color: #c44; }
  </style>
</head>
<body>
  <h1>Background Removal Progress</h1>
  <div class="nav"><a href="/">View before/after pairs</a></div>
  <div id="root" class="loading">Loading...</div>
  <div id="updated" class="updated"></div>
  <script>
    function render(stats) {
      if (stats.error) {
        document.getElementById('root').innerHTML = '<p class="error">' + stats.error + '</p>';
        return;
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
        '</div>';
    }
    function poll() {
      fetch('/api/stats')
        .then(r => r.json())
        .then(stats => {
          render(stats);
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

createServer(async (req, res) => {
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
  if (req.url === "/dashboard") {
    res.setHeader("Content-Type", "text/html");
    res.end(DASHBOARD_HTML);
    return;
  }
  if (req.url === "/" || req.url === "/index.html") {
    res.setHeader("Content-Type", "text/html");
    res.end(HTML);
    return;
  }
  res.statusCode = 404;
  res.end("Not found");
}).listen(3457, "127.0.0.1", () => {
  console.log("UI at http://127.0.0.1:3457 (or http://localhost:3457)");
});
