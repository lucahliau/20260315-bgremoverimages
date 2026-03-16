/**
 * Generates before-after.html from progress.json
 * Run after a successful batch: npx tsx generate-html.ts
 */

import * as fs from "fs";
import * as path from "path";

const PROGRESS_FILE = path.join(__dirname, "progress.json");
const HTML_FILE = path.join(__dirname, "before-after.html");

function nobgKey(originalKey: string): string {
  const parsed = path.parse(originalKey);
  return `${parsed.dir}/${parsed.name}-nobg.png`;
}

function getR2PublicUrl(): string {
  const envPath = path.join(__dirname, ".env");
  if (!fs.existsSync(envPath)) return "";
  const content = fs.readFileSync(envPath, "utf-8");
  const match = content.match(/R2_PUBLIC_URL="?([^"\s]+)"?/);
  return match ? match[1].trim() : "";
}

function main() {
  const baseUrl = getR2PublicUrl();
  if (!baseUrl) {
    console.error("Could not read R2_PUBLIC_URL from .env");
    process.exit(1);
  }

  if (!fs.existsSync(PROGRESS_FILE)) {
    console.error("No progress.json found. Run 'npm start' first.");
    process.exit(1);
  }

  const progress = JSON.parse(fs.readFileSync(PROGRESS_FILE, "utf-8"));
  const completed: string[] = progress.completed || [];
  const failed: string[] = progress.failed || [];
  const keys = completed.length >= 2
    ? completed.slice(-2)
    : [...completed, ...failed].slice(0, 2);

  if (keys.length === 0) {
    console.error("No images in progress.json. Run 'npm start' first.");
    process.exit(1);
  }

  const hasAfter = completed.length > 0;
  const pairs = keys.map((key: string) => ({
    before: `${baseUrl}/${key}`,
    after: `${baseUrl}/${nobgKey(key)}`,
    name: key.split("/").pop() || key,
    hasAfter: completed.includes(key),
  }));

  const html = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Before & After — Background Removal</title>
  <style>
    * { box-sizing: border-box; }
    body { font-family: system-ui, sans-serif; margin: 0; padding: 2rem; background: #1a1a1a; color: #eee; }
    h1 { font-size: 1.5rem; margin-bottom: 2rem; }
    .grid { display: grid; gap: 2rem; max-width: 900px; margin: 0 auto; }
    .pair { display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; align-items: start; }
    .pair h2 { grid-column: 1 / -1; font-size: 0.9rem; color: #888; margin: 0; }
    .col { text-align: center; }
    .col label { display: block; font-size: 0.75rem; color: #888; margin-bottom: 0.5rem; }
    .col img { max-width: 100%; height: auto; border-radius: 8px; background: #333; }
    .col.after img { background: repeating-conic-gradient(#444 0% 25%, #333 0% 50%) 50% / 16px 16px; }
    .placeholder { padding: 2rem; background: #333; border-radius: 8px; font-size: 0.8rem; color: #888; }
    .placeholder code { background: #444; padding: 0.2em 0.4em; border-radius: 4px; }
  </style>
</head>
<body>
  <h1>Before & After — Background Removal</h1>
  <div class="grid">
${pairs
  .map(
    (p) => `    <div class="pair">
      <h2>${p.name}</h2>
      <div class="col">
        <label>Before</label>
        <img src="${p.before}" alt="Before" loading="lazy" onerror="this.src='';this.alt='Failed to load'">
      </div>
      <div class="col after">
        <label>After</label>
        ${p.hasAfter ? `<img src="${p.after}" alt="After" loading="lazy" onerror="this.src='';this.alt='Failed to load'">` : `<div class="placeholder">Run <code>pip3 install rembg[cli]</code> then <code>npm start</code></div>`}
      </div>
    </div>`
  )
  .join("\n")}
  </div>
</body>
</html>`;

  fs.writeFileSync(HTML_FILE, html);
  console.log(`Wrote ${HTML_FILE} (${pairs.length} image pairs)`);
}

main();
