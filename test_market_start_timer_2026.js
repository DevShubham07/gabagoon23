/**
 * Test "place immediately at market start" timing against local JSON files.
 *
 * For Polymarket 15m BTC up/down markets, the slug often ends with the market start epoch seconds:
 *   btc-updown-15m-1767224700  -> 2025-12-31T23:45:00.000Z
 *
 * This script scans all json files under a directory and prints:
 * - marketSlug
 * - marketStart (from slug epoch)
 * - endDate (from first tick market.endDate)
 * - firstTick time (when your session starts recording)
 * - lag = firstTick - marketStart
 *
 * Usage:
 *   node test_market_start_timer_2026.js --dir "/Users/.../data/2026" --limit 10
 */

const fs = require("fs");
const path = require("path");

function getArgValue(flag) {
  const idx = process.argv.indexOf(flag);
  if (idx === -1) return undefined;
  const val = process.argv[idx + 1];
  if (val === undefined || val.startsWith("--")) return undefined;
  return val;
}

function toNum(v, d) {
  if (v === undefined) return d;
  const n = Number(v);
  return Number.isFinite(n) ? n : d;
}

function listJsonFilesRec(dir, out = []) {
  const entries = fs.readdirSync(dir);
  for (const e of entries) {
    const full = path.join(dir, e);
    const st = fs.statSync(full);
    if (st.isDirectory()) listJsonFilesRec(full, out);
    else if (e.endsWith(".json")) out.push(full);
  }
  return out;
}

function readHead(filePath, maxBytes = 256 * 1024) {
  const fd = fs.openSync(filePath, "r");
  try {
    const buf = Buffer.alloc(maxBytes);
    const bytesRead = fs.readSync(fd, buf, 0, maxBytes, 0);
    return buf.subarray(0, bytesRead).toString("utf8");
  } finally {
    fs.closeSync(fd);
  }
}

function parseMarketStartFromSlugMs(marketSlug) {
  const parts = String(marketSlug).split("-");
  const last = parts[parts.length - 1];
  const sec = Number(last);
  if (!Number.isFinite(sec) || sec <= 0) return null;
  return Math.floor(sec * 1000);
}

function extractFirstMatch(re, text) {
  const m = re.exec(text);
  return m ? m[1] : null;
}

function analyzeFileFast(filePath) {
  const head = readHead(filePath);
  const marketSlug =
    extractFirstMatch(/"marketSlug"\s*:\s*"([^"]+)"/, head) ||
    extractFirstMatch(/"slug"\s*:\s*"([^"]+)"/, head);
  const startTimeIso = extractFirstMatch(/"startTime"\s*:\s*"([^"]+)"/, head);
  const firstTickTsMsStr = extractFirstMatch(/"timestampMs"\s*:\s*(\d+)/, head);
  const endDateIso = extractFirstMatch(/"endDate"\s*:\s*"([^"]+)"/, head);

  if (!marketSlug || !startTimeIso || !firstTickTsMsStr) return null;

  const firstTickMs = Number(firstTickTsMsStr);
  const marketStartMs = parseMarketStartFromSlugMs(marketSlug);
  const endDateMs = endDateIso ? Date.parse(endDateIso) : NaN;
  const expectedStartFromEndDateMs = Number.isFinite(endDateMs) ? endDateMs - 15 * 60 * 1000 : NaN;

  return {
    file: filePath,
    marketSlug,
    marketStartIso: marketStartMs ? new Date(marketStartMs).toISOString() : null,
    marketStartMs,
    endDateIso: Number.isFinite(endDateMs) ? new Date(endDateMs).toISOString() : null,
    expectedStartFromEndDateIso: Number.isFinite(expectedStartFromEndDateMs) ? new Date(expectedStartFromEndDateMs).toISOString() : null,
    firstTickIso: new Date(firstTickMs).toISOString(),
    lagFirstTickFromMarketStartSec: marketStartMs ? (firstTickMs - marketStartMs) / 1000 : null,
    startTimeIso,
  };
}

function main() {
  const dir = getArgValue("--dir");
  const limit = toNum(getArgValue("--limit"), 20);
  if (!dir) {
    console.log('Missing --dir');
    process.exit(1);
  }

  const files = listJsonFilesRec(dir);
  const rows = [];
  for (const f of files) {
    const r = analyzeFileFast(f);
    if (r) rows.push(r);
  }

  // Sort by market start (if present), else by file
  rows.sort((a, b) => {
    const am = a.marketStartMs ?? Number.POSITIVE_INFINITY;
    const bm = b.marketStartMs ?? Number.POSITIVE_INFINITY;
    if (am !== bm) return am - bm;
    return a.file.localeCompare(b.file);
  });

  const sample = rows.slice(0, Math.min(limit, rows.length));
  for (const r of sample) {
    console.log("\n------------------------------");
    console.log(`market: ${r.marketSlug}`);
    console.log(`marketStart(from slug): ${r.marketStartIso ?? "n/a"}`);
    console.log(`expectedStart(from endDate-15m): ${r.expectedStartFromEndDateIso ?? "n/a"}`);
    console.log(`endDate: ${r.endDateIso ?? "n/a"}`);
    console.log(`firstTick: ${r.firstTickIso}`);
    console.log(`lag(firstTick - marketStart): ${r.lagFirstTickFromMarketStartSec === null ? "n/a" : r.lagFirstTickFromMarketStartSec.toFixed(3) + "s"}`);
    console.log(`file: ${r.file}`);
  }

  const lags = rows.map(r => r.lagFirstTickFromMarketStartSec).filter(x => typeof x === "number" && Number.isFinite(x));
  if (lags.length) {
    const min = Math.min(...lags);
    const max = Math.max(...lags);
    const avg = lags.reduce((a,b)=>a+b,0)/lags.length;
    console.log("\n=== Summary ===");
    console.log(`files analyzed: ${rows.length}`);
    console.log(`files with parsable marketStart: ${lags.length}`);
    console.log(`lag sec min/avg/max: ${min.toFixed(3)} / ${avg.toFixed(3)} / ${max.toFixed(3)}`);
  }
}

main();

