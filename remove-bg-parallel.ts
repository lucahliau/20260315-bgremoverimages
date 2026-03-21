/**
 * remove-bg-parallel.ts — Same as remove-bg.ts but runs up to 5 images in parallel
 * for ~5x faster throughput.
 *
 * Usage:  source .env && npx tsx remove-bg-parallel.ts [count] [parallel]
 *         count defaults to all images; parallel defaults to 5
 *
 * Required env vars:
 *   R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY,
 *   R2_BUCKET_NAME, R2_PUBLIC_URL
 */

import {
  S3Client,
  ListObjectsV2Command,
  GetObjectCommand,
} from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import { NodeHttpHandler } from "@smithy/node-http-handler";
import { execFile, execSync } from "child_process";
import * as fs from "fs";
import * as path from "path";
import * as https from "https";

// ── Config ───────────────────────────────────────────────────────────────────

const REQUIRED_ENV = [
  "R2_ACCOUNT_ID",
  "R2_ACCESS_KEY_ID",
  "R2_SECRET_ACCESS_KEY",
  "R2_BUCKET_NAME",
  "R2_PUBLIC_URL",
] as const;

for (const k of REQUIRED_ENV) {
  if (!process.env[k]) {
    console.error(`Missing env var: ${k}`);
    process.exit(1);
  }
}

const R2_ACCOUNT_ID = process.env.R2_ACCOUNT_ID!;
const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID!;
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY!;
const R2_BUCKET_NAME = process.env.R2_BUCKET_NAME!;
const R2_PUBLIC_URL = process.env.R2_PUBLIC_URL!.replace(/\/$/, "");

const IMAGE_COUNT = process.argv[2] === undefined ? Infinity : Math.max(1, parseInt(process.argv[2], 10));
const PARALLEL_CHAINS = Math.max(1, parseInt(process.env.PARALLEL_CHAINS || process.argv[3] || "5", 10));
const MAX_RETRIES = 1;
const PROGRESS_FILE = path.join(__dirname, "progress.json");
const HISTORY_FILE = path.join(__dirname, "history.jsonl");
const TMP_DIR = path.join(__dirname, "tmp");
const HTML_FILE = path.join(__dirname, "before-after.html");
const HELPER_SCRIPT = path.join(__dirname, "rembg_helper.py");

// ── S3 Client — key fix: forcePathStyle + long timeout ──────────────────────

const r2 = new S3Client({
  region: "auto",
  endpoint: `https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  forcePathStyle: true,
  credentials: {
    accessKeyId: R2_ACCESS_KEY_ID,
    secretAccessKey: R2_SECRET_ACCESS_KEY,
  },
  requestHandler: new NodeHttpHandler({
    connectionTimeout: 30_000,
    requestTimeout: 180_000,
    socketTimeout: 180_000,
    httpsAgent: new https.Agent({ keepAlive: false }),
  }),
});

// ── Logging ──────────────────────────────────────────────────────────────────

const LOG_FILE = path.join(__dirname, "run.log");
function log(msg: string) {
  const line = `[${new Date().toISOString()}] ${msg}`;
  console.log(line);
  fs.appendFileSync(LOG_FILE, line + "\n");
}

// ── Helpers ──────────────────────────────────────────────────────────────────

function ensureDir(dir: string) {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}

function nobgKey(originalKey: string): string {
  const p = path.parse(originalKey);
  return `${p.dir}/${p.name}-nobg.png`;
}

/** Stems (path without extension) that have a *-nobg__REJECT-*.png sibling */
function buildRejectStemSet(allKeys: Iterable<string>): Set<string> {
  const stems = new Set<string>();
  const re = /^(.+)-nobg__REJECT-.+\.png$/;
  for (const k of allKeys) {
    const m = k.match(re);
    if (m) stems.add(m[1]!);
  }
  return stems;
}

/** True if a -nobg.png exists or a tag-nobg-quality reject sibling exists */
function hasNobgResult(
  originalKey: string,
  allKeys: Set<string>,
  rejectStems: Set<string>
): boolean {
  const p = path.parse(originalKey);
  const stem = p.dir ? `${p.dir}/${p.name}` : p.name;
  if (allKeys.has(`${stem}-nobg.png`)) return true;
  return rejectStems.has(stem);
}

/** Retry wrapper for transient network errors (SSL handshake, ECONNRESET) */
async function withRetry<T>(label: string, fn: () => Promise<T>, retries = 3): Promise<T> {
  for (let i = 1; i <= retries; i++) {
    try {
      return await fn();
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      const transient =
        msg.includes("EPROTO") ||
        msg.includes("ECONNRESET") ||
        msg.includes("EPIPE") ||
        msg.includes("socket hang up") ||
        msg.includes("handshake");
      if (transient && i < retries) {
        const delay = 2000 * i;
        log(`  ⚠️  ${label} attempt ${i}/${retries} failed (${msg}), retrying in ${delay}ms...`);
        await sleep(delay);
        continue;
      }
      throw err;
    }
  }
  throw new Error("unreachable");
}

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

// ── Detect rembg ─────────────────────────────────────────────────────────────

interface RembgRunner {
  run: (inputPath: string, outputPath: string) => Promise<void>;
  label: string;
}

function findPython(): string {
  const candidates = ["python3", "python", "/usr/local/bin/python3", "/opt/homebrew/bin/python3"];
  for (const py of candidates) {
    try {
      execSync(`${py} -c "import rembg"`, { stdio: "ignore", timeout: 10_000 });
      return py;
    } catch {
      // continue
    }
  }
  return "python3";
}

function detectRembg(): RembgRunner {
  const python = findPython();

  if (fs.existsSync(HELPER_SCRIPT)) {
    log(`Using rembg_helper.py with ${python}`);
    return {
      label: `${python} rembg_helper.py`,
      run: (inp, out) => execPromise(python, [HELPER_SCRIPT, inp, out]),
    };
  }

  try {
    execSync("rembg --version", { stdio: "ignore", timeout: 5_000 });
    log("Using rembg CLI");
    return {
      label: "rembg CLI",
      run: (inp, out) => execPromise("rembg", ["i", inp, out]),
    };
  } catch {
    // not in PATH
  }

  log(`Falling back to ${python} -m rembg`);
  return {
    label: `${python} -m rembg`,
    run: (inp, out) => execPromise(python, ["-m", "rembg", "i", inp, out]),
  };
}

function execPromise(cmd: string, args: string[]): Promise<void> {
  return new Promise((resolve, reject) => {
    execFile(cmd, args, { timeout: 120_000 }, (err, _stdout, stderr) => {
      if (err) {
        reject(new Error(`Command failed: ${cmd} ${args.join(" ")}\n${stderr || err.message}`));
      } else {
        resolve();
      }
    });
  });
}

// ── R2 Operations ────────────────────────────────────────────────────────────

async function listOriginalKeys(): Promise<string[]> {
  return withRetry("R2 listing", async () => {
    const keys: string[] = [];
    let token: string | undefined;
    do {
      const res = await r2.send(
        new ListObjectsV2Command({
          Bucket: R2_BUCKET_NAME,
          Prefix: "products/",
          ContinuationToken: token,
          MaxKeys: 1000,
        })
      );
      for (const obj of res.Contents ?? []) {
        if (obj.Key) keys.push(obj.Key);
      }
      token = res.NextContinuationToken;
      if (keys.length > 0 && keys.length % 1000 === 0) {
        log(`   Listed ${keys.length} keys so far...`);
      }
    } while (token);
    return keys;
  });
}

async function downloadToFile(key: string, destPath: string): Promise<void> {
  const res = await r2.send(
    new GetObjectCommand({ Bucket: R2_BUCKET_NAME, Key: key })
  );
  const body = res.Body;
  if (!body) throw new Error(`Empty body for ${key}`);
  const chunks: Buffer[] = [];
  for await (const chunk of body as AsyncIterable<Buffer>) {
    chunks.push(Buffer.from(chunk));
  }
  fs.writeFileSync(destPath, Buffer.concat(chunks));
}

async function uploadFromFile(key: string, filePath: string): Promise<void> {
  const body = fs.readFileSync(filePath);
  const sizeMB = (body.length / 1024 / 1024).toFixed(1);
  log(`   Uploading ${key} (${sizeMB} MB)...`);

  const upload = new Upload({
    client: r2,
    params: {
      Bucket: R2_BUCKET_NAME,
      Key: key,
      Body: body,
      ContentType: "image/png",
    },
    partSize: 5 * 1024 * 1024,
    leavePartsOnError: false,
  });

  await upload.done();
}

// ── Process one image ────────────────────────────────────────────────────────

async function processImage(
  key: string,
  rembg: RembgRunner
): Promise<boolean> {
  const uid = `${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  const ext = path.extname(key) || ".jpg";
  const inputPath = path.join(TMP_DIR, `input_${uid}${ext}`);
  const outputPath = path.join(TMP_DIR, `output_${uid}.png`);
  const uploadKey = nobgKey(key);

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      log(`⬇️  [${attempt}/${MAX_RETRIES}] Downloading ${key}...`);
      const dlStart = Date.now();
      await downloadToFile(key, inputPath);
      log(`   Downloaded in ${((Date.now() - dlStart) / 1000).toFixed(1)}s`);

      log(`🔄 Running rembg on ${key}...`);
      const rbStart = Date.now();
      await rembg.run(inputPath, outputPath);
      log(`   rembg done in ${((Date.now() - rbStart) / 1000).toFixed(1)}s`);

      if (!fs.existsSync(outputPath)) {
        throw new Error("rembg produced no output file");
      }

      await uploadFromFile(uploadKey, outputPath);
      log(`✅ Uploaded ${uploadKey}`);

      return true;
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      log(`⚠️  Attempt ${attempt}/${MAX_RETRIES} failed for ${key}: ${message}`);
      if (attempt < MAX_RETRIES) {
        await sleep(2000 * attempt);
      }
    } finally {
      for (const f of [inputPath, outputPath]) {
        try { fs.unlinkSync(f); } catch { /* ignore */ }
      }
    }
  }

  log(`❌ FAILED after ${MAX_RETRIES} attempts: ${key}`);
  return false;
}

// ── HTML generation ──────────────────────────────────────────────────────────

function generateHTML(pairs: { before: string; after: string; name: string }[]) {
  const html = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Before & After — Background Removal</title>
  <style>
    * { box-sizing: border-box; margin: 0; }
    body { font-family: system-ui, sans-serif; padding: 2rem; background: #111; color: #eee; }
    h1 { font-size: 1.25rem; margin-bottom: 1.5rem; }
    .grid { display: grid; gap: 2rem; max-width: 1000px; margin: 0 auto; }
    .pair { display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; }
    .pair h2 { grid-column: 1 / -1; font-size: 0.85rem; color: #888; font-weight: 500; }
    .col { text-align: center; }
    .col label { display: block; font-size: 0.7rem; color: #999; margin-bottom: 0.5rem;
                 text-transform: uppercase; letter-spacing: 0.05em; }
    .col img { max-width: 100%; height: auto; border-radius: 6px; background: #222; display: block; }
    .col.after img {
      background: repeating-conic-gradient(#333 0% 25%, #222 0% 50%) 50% / 14px 14px;
    }
  </style>
</head>
<body>
  <h1>Before &amp; After — Background Removal</h1>
  <div class="grid">
${pairs
  .map(
    (p) => `    <div class="pair">
      <h2>${p.name}</h2>
      <div class="col"><label>Before</label><img src="${p.before}" alt="Before" loading="lazy"></div>
      <div class="col after"><label>After</label><img src="${p.after}" alt="After" loading="lazy"></div>
    </div>`
  )
  .join("\n")}
  </div>
</body>
</html>`;
  fs.writeFileSync(HTML_FILE, html);
  log(`Wrote ${HTML_FILE} (${pairs.length} pairs)`);
}

// ── Progress tracking ────────────────────────────────────────────────────────

interface Progress {
  completed: string[];
  failed: string[];
}

function loadProgress(): Progress {
  if (fs.existsSync(PROGRESS_FILE)) {
    try {
      return JSON.parse(fs.readFileSync(PROGRESS_FILE, "utf-8"));
    } catch {
      /* corrupt file, start fresh */
    }
  }
  return { completed: [], failed: [] };
}

function saveProgress(p: Progress) {
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify(p, null, 2));
}

function appendHistory(key: string, status: "success" | "failed") {
  const line = JSON.stringify({ key, status, ts: new Date().toISOString() }) + "\n";
  fs.appendFileSync(HISTORY_FILE, line);
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  log("============================================================");
  log("Background Removal Script (PARALLEL) Starting");
  log(`Target: ${IMAGE_COUNT === Infinity ? "all" : IMAGE_COUNT} images, ${PARALLEL_CHAINS} parallel chains`);
  log("============================================================");

  ensureDir(TMP_DIR);

  const rembg = detectRembg();
  log(`rembg method: ${rembg.label}`);

  try {
    const python = findPython();
    execSync(`${python} -c "from rembg import remove; print('ok')"`, {
      stdio: "pipe",
      timeout: 30_000,
    });
    log("rembg import check: OK");
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : String(err);
    log(`⚠️  rembg import check failed: ${msg}`);
    log("Make sure rembg is installed: pip3 install rembg[cpu] Pillow");
    process.exit(1);
  }

  const progress = loadProgress();
  log(`Resuming: ${progress.completed.length} completed, ${progress.failed.length} failed`);

  log("Listing R2 objects...");
  const allKeys = await listOriginalKeys();
  const allKeySet = new Set(allKeys);
  const rejectStems = buildRejectStemSet(allKeys);

  const imageExts = new Set([".jpg", ".jpeg", ".png", ".webp", ".gif", ".avif"]);
  const originals = allKeys.filter((k) => {
    if (k.endsWith("-nobg.png")) return false;
    if (!imageExts.has(path.extname(k).toLowerCase())) return false;
    if (hasNobgResult(k, allKeySet, rejectStems)) return false;
    if (progress.completed.includes(k)) return false;
    return true;
  });

  log(`Found ${originals.length} unprocessed images out of ${allKeys.length} total keys`);

  const toProcess = [...originals].reverse().slice(0, IMAGE_COUNT);
  if (toProcess.length === 0) {
    log("Nothing to process! All images already have -nobg versions.");
    if (progress.completed.length > 0) {
      const pairs = progress.completed.slice(-IMAGE_COUNT).map((k) => ({
        before: `${R2_PUBLIC_URL}/${k}`,
        after: `${R2_PUBLIC_URL}/${nobgKey(k)}`,
        name: path.basename(k),
      }));
      generateHTML(pairs);
    }
    return;
  }

  log(`📋 Processing ${toProcess.length} images (${PARALLEL_CHAINS} parallel chains)`);

  const succeeded: string[] = [];
  const failed: string[] = [];

  for (let i = 0; i < toProcess.length; i += PARALLEL_CHAINS) {
    const chunk = toProcess.slice(i, i + PARALLEL_CHAINS);
    const results = await Promise.all(
      chunk.map((key) => processImage(key, rembg).then((ok) => ({ key, ok })))
    );
    for (const { key, ok } of results) {
      if (ok) {
        succeeded.push(key);
        progress.completed.push(key);
        appendHistory(key, "success");
      } else {
        failed.push(key);
        progress.failed.push(key);
        appendHistory(key, "failed");
      }
    }
    saveProgress(progress);
  }

  log("============================================================");
  log(`Done! ${succeeded.length} succeeded, ${failed.length} failed out of ${toProcess.length}`);
  log("============================================================");

  if (succeeded.length > 0) {
    const pairs = succeeded.map((k) => ({
      before: `${R2_PUBLIC_URL}/${k}`,
      after: `${R2_PUBLIC_URL}/${nobgKey(k)}`,
      name: path.basename(k),
    }));
    generateHTML(pairs);
    log(`\n🎉 Open before-after.html in your browser to see results!`);
  } else {
    log("\n⚠️  No images succeeded. Check errors above.");
  }
}

main().catch((err) => {
  log(`FATAL: ${err instanceof Error ? err.message : String(err)}`);
  process.exit(1);
});
