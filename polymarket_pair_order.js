/**
 * Place an "arb pair" on Polymarket CLOB:
 * - BUY UP @ price (default 0.49)
 * - BUY DOWN @ price (default 0.49)
 * then manage risk during the window, and cancel any remaining open orders after N minutes (default 15).
 *
 * Uses Polymarket docs flow:
 * - init client with private key
 * - createOrDeriveApiKey()
 * - re-init client with user API creds + signature type + funder
 *
 * Docs:
 * - https://docs.polymarket.com/quickstart/first-order
 * - https://docs.polymarket.com/developers/market-makers/introduction
 *
 * IMPORTANT LIMITATION:
 * This script cannot be "guaranteed safe/profitable" because it cannot see orderbook depth/size beyond what the API returns,
 * and it cannot force both legs to fill. It tries to minimize naked exposure with simple rules.
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
  --post-only <true|false>  Post-only for the initial pair orders (default: true)
  --poll-sec <s>        Poll open order status every s seconds (default: 3)
  --pair-grace-sec <s>  If only one side fills, wait s seconds for the other fill before acting (default: 10)
  --on-single-fill <wait|cancel|hedge>  What to do if only one side fills after grace (default: hedge)
  --min-edge <p>        Minimum required pair edge: (1 - 2*price) >= min-edge (default: 0.005)
  --min-hedge-edge <p>  Minimum required edge after hedging the 2nd leg (default: 0.002)
  --allow-weird-quotes <true|false> Allow best ask/bid being 0 or ~1 without refusing (default: false)
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

function toBool(v, defaultVal) {
  if (v === undefined) return defaultVal;
  if (typeof v === "boolean") return v;
  const s = String(v).trim().toLowerCase();
  if (["1", "true", "yes", "y"].includes(s)) return true;
  if (["0", "false", "no", "n"].includes(s)) return false;
  return defaultVal;
}

function toNum(v, defaultVal) {
  if (v === undefined) return defaultVal;
  const n = Number(v);
  return Number.isFinite(n) ? n : defaultVal;
}

function numOrThrow(name, v) {
  if (!Number.isFinite(v)) throw new Error(`Invalid ${name}=${v}`);
  return v;
}

function parseOrderBookTop(ob) {
  // OrderBookSummary: bids/asks are [{price,size}, ...] as strings.
  const bestBid = ob?.bids?.length ? Number(ob.bids[0].price) : null;
  const bestAsk = ob?.asks?.length ? Number(ob.asks[0].price) : null;
  const bidSize = ob?.bids?.length ? Number(ob.bids[0].size) : null;
  const askSize = ob?.asks?.length ? Number(ob.asks[0].size) : null;
  return { bestBid, bestAsk, bidSize, askSize };
}

async function safeGetOrder(client, orderID) {
  try {
    return await client.getOrder(orderID);
  } catch {
    return null;
  }
}

function parseMatchedShares(order) {
  if (!order) return 0;
  const m = Number(order.size_matched);
  return Number.isFinite(m) ? m : 0;
}

function parseOriginalShares(order) {
  if (!order) return 0;
  const s = Number(order.original_size);
  return Number.isFinite(s) ? s : 0;
}

async function cancelBestEffort(client, orderID) {
  try {
    await client.cancelOrder({ orderID });
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e?.message || String(e) };
  }
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
  const postOnlyArg = getArgValue("--post-only");
  const pollSecArg = getArgValue("--poll-sec");
  const pairGraceSecArg = getArgValue("--pair-grace-sec");
  const onSingleFillArg = getArgValue("--on-single-fill");
  const minEdgeArg = getArgValue("--min-edge");
  const minHedgeEdgeArg = getArgValue("--min-hedge-edge");
  const allowWeirdQuotesArg = getArgValue("--allow-weird-quotes");

  const PRICE = priceArg !== undefined ? Number(priceArg) : Number(process.env.PRICE || 0.49);
  const INVEST_PER_SIDE =
    investArg !== undefined ? Number(investArg) : Number(process.env.INVEST_PER_SIDE || 5);
  const DURATION_MIN =
    durationMinArg !== undefined ? Number(durationMinArg) : Number(process.env.DURATION_MIN || 15);
  const POST_ONLY = toBool(postOnlyArg ?? process.env.POST_ONLY, true);
  const POLL_SEC = toNum(pollSecArg ?? process.env.POLL_SEC, 3);
  const PAIR_GRACE_SEC = toNum(pairGraceSecArg ?? process.env.PAIR_GRACE_SEC, 10);
  const ON_SINGLE_FILL = String(onSingleFillArg ?? process.env.ON_SINGLE_FILL ?? "hedge").toLowerCase();
  const MIN_EDGE = toNum(minEdgeArg ?? process.env.MIN_EDGE, 0.005);
  const MIN_HEDGE_EDGE = toNum(minHedgeEdgeArg ?? process.env.MIN_HEDGE_EDGE, 0.002);
  const ALLOW_WEIRD_QUOTES = toBool(allowWeirdQuotesArg ?? process.env.ALLOW_WEIRD_QUOTES, false);

  const SIGNATURE_TYPE =
    signatureTypeArg !== undefined
      ? Number(signatureTypeArg)
      : Number(process.env.POLY_SIGNATURE_TYPE || 0);

  if (!Number.isFinite(PRICE) || PRICE <= 0 || PRICE >= 1) throw new Error(`Invalid --price ${PRICE}`);
  if (!Number.isFinite(DURATION_MIN) || DURATION_MIN <= 0) throw new Error(`Invalid --duration-min ${DURATION_MIN}`);
  if (!Number.isFinite(POLL_SEC) || POLL_SEC <= 0) throw new Error(`Invalid --poll-sec ${POLL_SEC}`);
  if (!Number.isFinite(PAIR_GRACE_SEC) || PAIR_GRACE_SEC < 0) throw new Error(`Invalid --pair-grace-sec ${PAIR_GRACE_SEC}`);
  if (!["wait", "cancel", "hedge"].includes(ON_SINGLE_FILL)) {
    throw new Error(`Invalid --on-single-fill ${ON_SINGLE_FILL} (expected wait|cancel|hedge)`);
  }
  if (![0, 1, 2].includes(SIGNATURE_TYPE)) throw new Error(`Invalid signature type ${SIGNATURE_TYPE} (expected 0,1,2)`);

  // Basic arb sanity: if price*2 is too close to 1, the "pair edge" is tiny and gets eaten by costs.
  // User can override by setting MIN_EDGE lower.
  const pairEdge = 1 - 2 * PRICE;
  if (pairEdge < MIN_EDGE) {
    throw new Error(
      `Refusing: pair edge too small. (1 - 2*price) = ${pairEdge.toFixed(6)} < MIN_EDGE=${MIN_EDGE}. ` +
        `Either lower --price or reduce --min-edge (at your own risk).`
    );
  }

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

  // Quick market sanity checks (top-of-book), to avoid acting on obviously broken quotes.
  // Market-maker guidance emphasizes monitoring orderbook / feeds continuously before quoting.
  const upBook = await client.getOrderBook(UP_TOKEN_ID);
  const downBook = await client.getOrderBook(DOWN_TOKEN_ID);
  const upTop = parseOrderBookTop(upBook);
  const downTop = parseOrderBookTop(downBook);

  const isWeird = (x) => x !== null && (x <= 0 || x >= 1);
  if (!ALLOW_WEIRD_QUOTES) {
    if (isWeird(upTop.bestAsk) || isWeird(upTop.bestBid) || isWeird(downTop.bestAsk) || isWeird(downTop.bestBid)) {
      throw new Error(
        `Refusing: detected weird top-of-book quotes. ` +
          `UP bid/ask=${upTop.bestBid}/${upTop.bestAsk}, DOWN bid/ask=${downTop.bestBid}/${downTop.bestAsk}. ` +
          `Pass --allow-weird-quotes true if you want to ignore this (not recommended).`
      );
    }
  }

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
  console.log(`- postOnly=${POST_ONLY}  pollSec=${POLL_SEC}  pairGraceSec=${PAIR_GRACE_SEC}  onSingleFill=${ON_SINGLE_FILL}`);

  const orderIds = [];

  const upResp = await client.createAndPostOrder(
    {
      tokenID: UP_TOKEN_ID,
      price: upPrice,
      size,
      side: Side.BUY,
    },
    { tickSize: upTickSize, negRisk: upNegRisk },
    OrderType.GTC,
    undefined,
    POST_ONLY
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
    OrderType.GTC,
    undefined,
    POST_ONLY
  );
  console.log(`[${nowIso()}] DOWN order: id=${downResp.orderID} status=${downResp.status}`);
  orderIds.push(downResp.orderID);

  const cancelAtMs = Date.now() + DURATION_MIN * 60 * 1000;
  console.log(`[${nowIso()}] Will cancel remaining open orders in ${DURATION_MIN} minutes @ ${new Date(cancelAtMs).toISOString()}`);

  // Safety loop:
  // - Poll matched sizes (size_matched) for each order
  // - If one side fills and the other doesn't within PAIR_GRACE_SEC:
  //   - hedge: try to complete the pair within a minimum profit bound, else cancel unfilled
  //   - cancel: cancel unfilled (you are left with directional exposure from the filled side)
  //   - wait: do nothing (highest exposure risk)
  const eps = 1e-9;
  let firstSingleFillAtMs = null;
  let actedOnSingleFill = false;

  while (Date.now() < cancelAtMs) {
    const upOrder = await safeGetOrder(client, upResp.orderID);
    const downOrder = await safeGetOrder(client, downResp.orderID);

    const upMatched = parseMatchedShares(upOrder);
    const downMatched = parseMatchedShares(downOrder);
    const upOrig = parseOriginalShares(upOrder) || size;
    const downOrig = parseOriginalShares(downOrder) || size;

    const upFull = upMatched + eps >= upOrig;
    const downFull = downMatched + eps >= downOrig;
    const bothFull = upFull && downFull;

    const upSome = upMatched > eps;
    const downSome = downMatched > eps;
    const oneSome = (upSome && !downSome) || (!upSome && downSome);

    if (bothFull) {
      console.log(`[${nowIso()}] Both legs fully filled. (upMatched=${upMatched}, downMatched=${downMatched})`);
      // You can optionally cancel any remaining (should be none) and stop monitoring early.
      break;
    }

    if (oneSome && !actedOnSingleFill) {
      if (firstSingleFillAtMs === null) {
        firstSingleFillAtMs = Date.now();
        console.log(
          `[${nowIso()}] Single-leg fill detected. upMatched=${upMatched} downMatched=${downMatched}. Starting grace timer (${PAIR_GRACE_SEC}s)…`
        );
      }

      const elapsed = (Date.now() - firstSingleFillAtMs) / 1000;
      if (elapsed >= PAIR_GRACE_SEC) {
        actedOnSingleFill = true;

        const filledSide = upMatched > downMatched ? "UP" : "DOWN";
        const missingShares = Math.abs(upMatched - downMatched);
        const otherToken = filledSide === "UP" ? DOWN_TOKEN_ID : UP_TOKEN_ID;
        const otherTick = filledSide === "UP" ? downTickSize : upTickSize;
        const otherNegRisk = filledSide === "UP" ? downNegRisk : upNegRisk;

        console.log(
          `[${nowIso()}] Grace elapsed. filledSide=${filledSide} missingShares=${missingShares}. Action=${ON_SINGLE_FILL}`
        );

        if (ON_SINGLE_FILL === "wait") {
          // Do nothing; continue monitoring until cancelAtMs.
        } else if (ON_SINGLE_FILL === "cancel") {
          const unfilledOrderId = filledSide === "UP" ? downResp.orderID : upResp.orderID;
          const r = await cancelBestEffort(client, unfilledOrderId);
          console.log(
            `[${nowIso()}] Cancel unfilled leg (${unfilledOrderId}) => ${r.ok ? "ok" : `failed: ${r.error}`}`
          );
        } else if (ON_SINGLE_FILL === "hedge") {
          // Hedge rule:
          // We only attempt hedging if the other leg can be bought while preserving MIN_HEDGE_EDGE.
          // (Assumes filled leg price ~= PRICE; real average price depends on execution.)
          const maxOtherPrice = 1 - PRICE - MIN_HEDGE_EDGE;

          const otherBook = await client.getOrderBook(otherToken);
          const otherTop = parseOrderBookTop(otherBook);
          const bestAsk = otherTop.bestAsk;

          if (!Number.isFinite(bestAsk)) {
            console.log(`[${nowIso()}] Hedge skipped: could not read best ask for other leg.`);
          } else if (bestAsk > maxOtherPrice) {
            console.log(
              `[${nowIso()}] Hedge skipped: bestAsk=${bestAsk} exceeds maxOtherPrice=${maxOtherPrice.toFixed(6)} to keep edge >= ${MIN_HEDGE_EDGE}`
            );
          } else {
            // Use a marketable limit order at maxOtherPrice to avoid paying above that.
            const hedgePrice = roundDownToTick(maxOtherPrice, otherTick);
            console.log(
              `[${nowIso()}] Hedging by buying other leg token=${otherToken} shares=${missingShares} price<=${hedgePrice} (bestAsk=${bestAsk})`
            );

            try {
              const hedgeResp = await client.createAndPostOrder(
                { tokenID: otherToken, price: hedgePrice, size: missingShares, side: Side.BUY },
                { tickSize: otherTick, negRisk: otherNegRisk },
                OrderType.GTC,
                undefined,
                false // not post-only: allow taking to complete hedge
              );
              console.log(`[${nowIso()}] Hedge order posted: id=${hedgeResp.orderID} status=${hedgeResp.status}`);
            } catch (e) {
              console.log(`[${nowIso()}] Hedge failed: ${e?.message || String(e)}`);
            }
          }
        }
      }
    }

    await sleep(POLL_SEC * 1000);
  }

  console.log(`[${nowIso()}] Cancelling orders (best-effort)…`);
  for (const id of orderIds) {
    const r = await cancelBestEffort(client, id);
    console.log(`[${nowIso()}] Cancel ${id} => ${r.ok ? "ok" : `failed: ${r.error}`}`);
  }

  console.log(`[${nowIso()}] Done.`);
}

main().catch((e) => {
  console.error(`Fatal: ${e?.message || String(e)}`);
  process.exit(1);
});

