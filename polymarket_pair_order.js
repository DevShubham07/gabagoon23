/**
 * Place an "arb pair" on Polymarket CLOB:
 * - BUY UP @ price (default 0.49)
 * - BUY DOWN @ price (default 0.49)
 * then cancel any remaining open orders after N minutes (default 15).
 *
 * Uses Polymarket docs flow:
 * - init client with private key
 * - createOrDeriveApiKey()
 * - re-init client with user API creds + signature type + funder
 *
 * Docs: https://docs.polymarket.com/quickstart/first-order
 */

require("dotenv").config();

const { ClobClient, Side, OrderType } = require("@polymarket/clob-client");
const { Wallet } = require("ethers");

const HOST = process.env.POLY_CLOB_HOST || "https://clob.polymarket.com";
const CHAIN_ID = Number(process.env.POLY_CHAIN_ID || 137); // Polygon mainnet

function printUsage() {
  // Keep this short; refer user to Polymarket docs for auth details.
  console.log(`
Usage:
  node polymarket_pair_order.js --up <UP_TOKEN_ID> --down <DOWN_TOKEN_ID> [options]

Required (via env or args):
  PRIVATE_KEY or POLY_PRIVATE_KEY   Wallet private key (used to derive user API creds)
  --up <tokenId>                    UP token ID
  --down <tokenId>                  DOWN token ID

Options:
  --price <p>           Limit price per share (default: 0.49)
  --invest <usd>        USD to spend per side (default: 5). Size shares = invest/price
  --shares <n>          Override share size directly (instead of --invest)
  --duration-min <m>    Cancel open orders after m minutes (default: 15)
  --start-at <iso>      Wait until ISO timestamp before placing orders
  --signature-type <0|1|2>  Default 0 (EOA). See Polymarket docs.
  --funder <address>    Funder address (default: signer.address)
  --min-mb <mb>         (not used here)
  --help                Print this help

Example:
  PRIVATE_KEY=... node polymarket_pair_order.js --up 123 --down 456 --price 0.49 --invest 5 --duration-min 15
`);
}

function getArgValue(flag) {
  const idx = process.argv.indexOf(flag);
  if (idx === -1) return undefined;
  const val = process.argv[idx + 1];
  if (val === undefined || val.startsWith("--")) return undefined;
  return val;
}

function mustGet(name, value) {
  if (value === undefined || value === null || value === "") {
    throw new Error(`Missing required ${name}.`);
  }
  return value;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function parseIsoMs(s) {
  const ms = Date.parse(s);
  if (!Number.isFinite(ms)) throw new Error(`Invalid ISO timestamp: ${s}`);
  return ms;
}

function nowIso() {
  return new Date().toISOString();
}

function roundDownToTick(x, tickSize) {
  // Conservative: round DOWN to tick size to avoid invalid prices.
  if (!Number.isFinite(tickSize) || tickSize <= 0) return x;
  const inv = 1 / tickSize;
  // Avoid floating drift by snapping to nearest integer at tick precision.
  const snapped = Math.floor(x * inv + 1e-9) / inv;
  return Number(snapped.toFixed(8));
}

function computeSizeShares({ investUsd, price }) {
  if (!Number.isFinite(investUsd) || investUsd <= 0) throw new Error(`Invalid investUsd=${investUsd}`);
  if (!Number.isFinite(price) || price <= 0) throw new Error(`Invalid price=${price}`);
  // Size is in shares. We keep some decimals; CLOB will validate precision.
  const size = investUsd / price;
  return Number(size.toFixed(6));
}

async function main() {
  if (process.argv.includes("--help") || process.argv.includes("-h")) {
    printUsage();
    return;
  }

  const PRIVATE_KEY = process.env.PRIVATE_KEY || process.env.POLY_PRIVATE_KEY;
  try {
    mustGet("PRIVATE_KEY (or POLY_PRIVATE_KEY)", PRIVATE_KEY);
  } catch (e) {
    printUsage();
    throw e;
  }

  const UP_TOKEN_ID = process.env.UP_TOKEN_ID || getArgValue("--up");
  const DOWN_TOKEN_ID = process.env.DOWN_TOKEN_ID || getArgValue("--down");
  try {
    mustGet("UP_TOKEN_ID or --up <tokenId>", UP_TOKEN_ID);
    mustGet("DOWN_TOKEN_ID or --down <tokenId>", DOWN_TOKEN_ID);
  } catch (e) {
    printUsage();
    throw e;
  }

  const priceArg = getArgValue("--price");
  const investArg = getArgValue("--invest");
  const sharesArg = getArgValue("--shares");
  const durationMinArg = getArgValue("--duration-min");
  const startAtArg = getArgValue("--start-at");
  const signatureTypeArg = getArgValue("--signature-type");
  const funderArg = getArgValue("--funder");

  const PRICE = priceArg !== undefined ? Number(priceArg) : Number(process.env.PRICE || 0.49);
  const INVEST_PER_SIDE =
    investArg !== undefined ? Number(investArg) : Number(process.env.INVEST_PER_SIDE || 5);
  const DURATION_MIN =
    durationMinArg !== undefined ? Number(durationMinArg) : Number(process.env.DURATION_MIN || 15);

  const SIGNATURE_TYPE =
    signatureTypeArg !== undefined
      ? Number(signatureTypeArg)
      : Number(process.env.POLY_SIGNATURE_TYPE || 0);

  if (!Number.isFinite(PRICE) || PRICE <= 0 || PRICE >= 1) throw new Error(`Invalid --price ${PRICE}`);
  if (!Number.isFinite(DURATION_MIN) || DURATION_MIN <= 0) throw new Error(`Invalid --duration-min ${DURATION_MIN}`);
  if (![0, 1, 2].includes(SIGNATURE_TYPE)) throw new Error(`Invalid signature type ${SIGNATURE_TYPE} (expected 0,1,2)`);

  const signer = new Wallet(PRIVATE_KEY);
  const FUNDER_ADDRESS = funderArg || process.env.POLY_FUNDER_ADDRESS || signer.address;

  console.log(`[${nowIso()}] Initializing client (derive creds)…`);
  const bootstrapClient = new ClobClient(HOST, CHAIN_ID, signer);

  // Per docs: private key is used once to derive User API credentials.
  const userApiCreds = await bootstrapClient.createOrDeriveApiKey();

  console.log(`[${nowIso()}] Reinitializing client (authenticated)…`);
  const client = new ClobClient(HOST, CHAIN_ID, signer, userApiCreds, SIGNATURE_TYPE, FUNDER_ADDRESS);

  // Fetch per-token market metadata (tickSize/negRisk).
  console.log(`[${nowIso()}] Fetching market metadata…`);
  const upMarket = await client.getMarket(UP_TOKEN_ID);
  const downMarket = await client.getMarket(DOWN_TOKEN_ID);

  const upTickSize = upMarket.tickSize;
  const downTickSize = downMarket.tickSize;
  const upNegRisk = upMarket.negRisk;
  const downNegRisk = downMarket.negRisk;

  const upPrice = roundDownToTick(PRICE, upTickSize);
  const downPrice = roundDownToTick(PRICE, downTickSize);

  const size =
    sharesArg !== undefined ? Number(sharesArg) : computeSizeShares({ investUsd: INVEST_PER_SIDE, price: PRICE });
  if (!Number.isFinite(size) || size <= 0) throw new Error(`Invalid size/shares ${size}`);

  // Optional scheduling: wait until a specific timestamp.
  if (startAtArg) {
    const startMs = parseIsoMs(startAtArg);
    const delay = startMs - Date.now();
    if (delay > 0) {
      console.log(`[${nowIso()}] Waiting ${Math.ceil(delay / 1000)}s until --start-at ${startAtArg}…`);
      await sleep(delay);
    }
  }

  console.log(`[${nowIso()}] Placing orders:`);
  console.log(`- UP   token=${UP_TOKEN_ID}  price=${upPrice}  size=${size} shares`);
  console.log(`- DOWN token=${DOWN_TOKEN_ID}  price=${downPrice}  size=${size} shares`);

  const orderIds = [];

  const upResp = await client.createAndPostOrder(
    {
      tokenID: UP_TOKEN_ID,
      price: upPrice,
      size,
      side: Side.BUY,
    },
    { tickSize: upTickSize, negRisk: upNegRisk },
    OrderType.GTC
  );
  console.log(`[${nowIso()}] UP order: id=${upResp.orderID} status=${upResp.status}`);
  orderIds.push(upResp.orderID);

  const downResp = await client.createAndPostOrder(
    {
      tokenID: DOWN_TOKEN_ID,
      price: downPrice,
      size,
      side: Side.BUY,
    },
    { tickSize: downTickSize, negRisk: downNegRisk },
    OrderType.GTC
  );
  console.log(`[${nowIso()}] DOWN order: id=${downResp.orderID} status=${downResp.status}`);
  orderIds.push(downResp.orderID);

  const cancelAtMs = Date.now() + DURATION_MIN * 60 * 1000;
  console.log(`[${nowIso()}] Will cancel remaining open orders in ${DURATION_MIN} minutes @ ${new Date(cancelAtMs).toISOString()}`);

  await sleep(DURATION_MIN * 60 * 1000);

  console.log(`[${nowIso()}] Cancelling orders (best-effort)…`);
  for (const id of orderIds) {
    try {
      await client.cancelOrder(id);
      console.log(`[${nowIso()}] Cancelled order ${id}`);
    } catch (e) {
      console.log(`[${nowIso()}] Cancel failed for ${id}: ${e?.message || String(e)}`);
    }
  }

  console.log(`[${nowIso()}] Done.`);
}

main().catch((e) => {
  console.error(`Fatal: ${e?.message || String(e)}`);
  process.exit(1);
});

