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
    .placeholder {
      padding: 1.5rem 1rem;
      background: var(--surface);
      border-radius: var(--radius);
      border: 1px solid var(--border);
      font-size: 0.8125rem;
      color: var(--muted);
      line-height: 1.5;
    }
    .placeholder code {
      font-size: 0.75em;
      background: rgba(255, 255, 255, 0.06);
      padding: 0.15em 0.4em;
      border-radius: 4px;
    }
  </style>
</head>
<body>
  <div class="shell">
    <header class="page-head">
      <h1>Before / after</h1>
      <p class="sub">From progress.json · Last batch samples</p>
    </header>
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
  </div>
</body>
</html>`;

  fs.writeFileSync(HTML_FILE, html);
  console.log(`Wrote ${HTML_FILE} (${pairs.length} image pairs)`);
}

main();
