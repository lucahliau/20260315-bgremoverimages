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
  <title>Before & After — Background Removal</title>
  <style>
    * { box-sizing: border-box; }
    body { font-family: system-ui, sans-serif; margin: 0; padding: 1.5rem; background: #111; color: #eee; }
    h1 { font-size: 1.25rem; margin-bottom: 1.5rem; }
    .grid { display: grid; gap: 1.5rem; max-width: 1000px; margin: 0 auto; }
    .pair { display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; align-items: start; }
    .pair h2 { grid-column: 1 / -1; font-size: 0.8rem; color: #666; margin: 0 0 0.25rem 0; font-weight: 500; }
    .col { text-align: center; }
    .col label { display: block; font-size: 0.7rem; color: #888; margin-bottom: 0.5rem; text-transform: uppercase; letter-spacing: 0.05em; }
    .col img { max-width: 100%; height: auto; border-radius: 6px; background: #222; display: block; }
    .col.after img { background: repeating-conic-gradient(#333 0% 25%, #222 0% 50%) 50% / 12px 12px; }
    .empty { color: #666; }
  </style>
</head>
<body>
  <h1>Before & After — Background Removal</h1>
  <div id="root"></div>
  <script>
    const pairs = ${JSON.stringify(pairs)};
    if (pairs.length === 0) {
      document.getElementById('root').innerHTML = '<p class="empty">No -nobg.png files found in R2.</p>';
    } else {
      document.getElementById('root').innerHTML = '<div class="grid">' + pairs.map(p =>
        '<div class="pair"><h2>' + p.name + '</h2>' +
        '<div class="col"><label>Before</label><img src="' + p.before + '" alt="Before" loading="lazy" onerror="this.alt=\\'Failed to load\\'"></div>' +
        '<div class="col after"><label>After</label><img src="' + p.after + '" alt="After" loading="lazy" onerror="this.alt=\\'Failed to load\\'"></div></div>'
      ).join('') + '</div>';
    }
  </script>
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
