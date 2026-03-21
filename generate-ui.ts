/**
 * Generates a standalone before-after.html with pairs from R2.
 * No server needed — open the file in your browser.
 * Run: source .env && npx tsx generate-ui.ts
 */

import { S3Client, ListObjectsV2Command } from "@aws-sdk/client-s3";
import * as fs from "fs";
import * as path from "path";

for (const k of ["R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET_NAME", "R2_PUBLIC_URL"]) {
  if (!process.env[k]) {
    console.error(`Missing: ${k}`);
    process.exit(1);
  }
}

const r2 = new S3Client({
  region: "auto",
  endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: process.env.R2_ACCESS_KEY_ID!,
    secretAccessKey: process.env.R2_SECRET_ACCESS_KEY!,
  },
});

const R2_PUBLIC_URL = process.env.R2_PUBLIC_URL!.replace(/\/$/, "");
const BUCKET = process.env.R2_BUCKET_NAME!;
const OUT = path.join(__dirname, "before-after.html");

async function listAllKeys(): Promise<string[]> {
  const keys: string[] = [];
  let token: string | undefined;
  do {
    const res = await r2.send(
      new ListObjectsV2Command({
        Bucket: BUCKET,
        Prefix: "products/",
        ContinuationToken: token,
        MaxKeys: 1000,
      })
    );
    for (const obj of res.Contents ?? []) {
      if (obj.Key) keys.push(obj.Key);
    }
    token = res.NextContinuationToken;
  } while (token);
  return keys;
}

function originalKeyFromNobg(nobgKey: string, allKeys: Set<string>): string | null {
  const base = nobgKey.replace(/-nobg\.png$/, "");
  for (const ext of ["jpg", "jpeg", "png", "webp", "gif", "avif"]) {
    const candidate = `${base}.${ext}`;
    if (allKeys.has(candidate)) return candidate;
  }
  return null;
}

async function main() {
  console.log("Listing R2...");
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

  console.log(`Found ${pairs.length} before/after pairs`);

  const html = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Before / after — Background removal</title>
  <style>
    :root {
      --bg: #0c0c0d;
      --surface: #131316;
      --border: rgba(255, 255, 255, 0.07);
      --text: #ececed;
      --muted: #8e8e93;
      --faint: #5a5a5e;
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
    .shell { max-width: 1080px; margin: 0 auto; padding: 0 1.5rem 3rem; }
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
    .grid { display: grid; gap: 1.75rem; }
    .pair {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 1rem 1.25rem;
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
    .empty { color: var(--muted); font-size: 0.875rem; margin: 0; }
  </style>
</head>
<body>
  <div class="shell">
    <header class="page-head">
      <h1>Before / after</h1>
      <p class="sub">Static export from R2 · Open this file in a browser</p>
    </header>
    <div id="root"></div>
  <script>
    const pairs = ${JSON.stringify(pairs)};
    if (pairs.length === 0) {
      document.getElementById('root').innerHTML = '<p class="empty">No matching pairs in R2.</p>';
    } else {
      document.getElementById('root').innerHTML = '<div class="grid">' + pairs.map(p =>
        '<div class="pair"><h2>' + p.name + '</h2>' +
        '<div class="col"><label>Before</label><img src="' + p.before + '" alt="Before" loading="lazy" onerror="this.alt=\\'Failed to load\\'"></div>' +
        '<div class="col after"><label>After</label><img src="' + p.after + '" alt="After" loading="lazy" onerror="this.alt=\\'Failed to load\\'"></div></div>'
      ).join('') + '</div>';
    }
  </script>
  </div>
</body>
</html>`;

  fs.writeFileSync(OUT, html);
  console.log(`Wrote ${OUT}`);
  console.log("Open it in your browser (no server needed)");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
