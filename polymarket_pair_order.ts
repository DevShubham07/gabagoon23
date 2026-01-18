/**
 * TypeScript version of polymarket_pair_order.js
 *
 * Place an "arb pair" on Polymarket CLOB:
 * - BUY UP @ price (default 0.49)
 * - BUY DOWN @ price (default 0.49)
 * then manage risk during the window, and cancel any remaining open orders after N minutes (default 15).
 *
 * Docs:
 * - https://docs.polymarket.com/quickstart/first-order
 * - https://docs.polymarket.com/developers/market-makers/introduction
 */

import dotenv from "dotenv";
dotenv.config();

import { ClobClient, OrderType, Side } from "@polymarket/clob-client";
import type { ApiKeyCreds, ApiKeyRaw, OpenOrder, OrderBookSummary, TickSize } from "@polymarket/clob-client";
import { RealTimeDataClient } from "@polymarket/real-time-data-client"; 
import type { Message } from "@polymarket/real-time-data-client";
import { Wallet } from "ethers";

const HOST: string = process.env.POLY_CLOB_HOST ?? "https://clob.polymarket.com";
const CHAIN_ID: number = Number(process.env.POLY_CHAIN_ID ?? 137); // Polygon mainnet
const RTDS_HOST: string | undefined = process.env.POLY_RTDS_HOST; // optional override; otherwise uses lib default

type SingleFillAction = "wait" | "cancel" | "hedge";

type CancelResult = { ok: true } | { ok: false; error: string };

function printUsage(): void {
  console.log(`
Usage:
  node dist/polymarket_pair_order.js --up <UP_TOKEN_ID> --down <DOWN_TOKEN_ID> [options]

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
  --align-15m <true|false>  If --start-at not provided, align start to next 15m boundary (default: false)
  --align-market-start <true|false> If --start-at not provided, align start to the market start derived from --market-slug epoch (default: false)
  --repeat <true|false>  Repeat forever in 15m windows (default: false)
  --post-only <true|false>  Post-only for the initial pair orders (default: true)
  --poll-sec <s>        Poll open order status every s seconds (default: 3)
  --pair-grace-sec <s>  If only one side fills, wait s seconds for the other fill before acting (default: 10)
  --on-single-fill <wait|cancel|hedge>  What to do if only one side fills after grace (default: hedge)
  --min-edge <p>        Minimum required pair edge: (1 - 2*price) >= min-edge (default: 0.005)
  --min-hedge-edge <p>  Minimum required edge after hedging the 2nd leg (default: 0.002)
  --allow-weird-quotes <true|false> Allow best ask/bid being 0 or ~1 without refusing (default: false)
  --use-rtds <true|false>  Use RealTimeDataClient for realtime events (default: true)
  --market-slug <slug>  (optional) Subscribe to activity events for this market slug
  --signature-type <0|1|2>  Default 0 (EOA). See Polymarket docs.
  --funder <address>    Funder address (default: signer.address)
  --help                Print this help
`);
}

function getArgValue(flag: string): string | undefined {
  const idx = process.argv.indexOf(flag);
  if (idx === -1) return undefined;
  const val = process.argv[idx + 1];
  if (val === undefined || val.startsWith("--")) return undefined;
  return val;
}

function mustGet(name: string, value: unknown): string {
  if (typeof value !== "string" || value.trim() === "") {
    throw new Error(`Missing required ${name}.`);
  }
  return value;
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

function parseIsoMs(s: string): number {
  const ms = Date.parse(s);
  if (!Number.isFinite(ms)) throw new Error(`Invalid ISO timestamp: ${s}`);
  return ms;
}

function nowIso(): string {
  return new Date().toISOString();
}

function ceilToWindowMs(nowMs: number, windowMinutes: number): number {
  const windowMs = windowMinutes * 60 * 1000;
  return Math.ceil(nowMs / windowMs) * windowMs;
}

function parseMarketStartFromSlugMs(marketSlug: string): number {
  // Many Polymarket slugs end with a unix timestamp in seconds.
  // Example: btc-updown-15m-1767224700 -> 1767224700s -> 2025-12-31T23:45:00Z (market start)
  const parts = marketSlug.split("-");
  const last = parts[parts.length - 1];
  const sec = Number(last);
  if (!Number.isFinite(sec) || sec <= 0) {
    throw new Error(`Cannot parse market start from market slug: ${marketSlug}`);
  }
  return Math.floor(sec * 1000);
}

function roundDownToTick(x: number, tickSize: TickSize | number): number {
  const t = typeof tickSize === "string" ? Number(tickSize) : tickSize;
  if (!Number.isFinite(t) || t <= 0) return x;
  const inv = 1 / t;
  const snapped = Math.floor(x * inv + 1e-9) / inv;
  return Number(snapped.toFixed(8));
}

function computeSizeShares(investUsd: number, price: number): number {
  if (!Number.isFinite(investUsd) || investUsd <= 0) throw new Error(`Invalid investUsd=${investUsd}`);
  if (!Number.isFinite(price) || price <= 0) throw new Error(`Invalid price=${price}`);
  return Number((investUsd / price).toFixed(6));
}

function toBool(v: unknown, defaultVal: boolean): boolean {
  if (v === undefined || v === null) return defaultVal;
  if (typeof v === "boolean") return v;
  const s = String(v).trim().toLowerCase();
  if (["1", "true", "yes", "y"].includes(s)) return true;
  if (["0", "false", "no", "n"].includes(s)) return false;
  return defaultVal;
}

function toNum(v: unknown, defaultVal: number): number {
  if (v === undefined || v === null) return defaultVal;
  const n = Number(v);
  return Number.isFinite(n) ? n : defaultVal;
}

function parseOrderBookTop(ob: OrderBookSummary): {
  bestBid: number | null;
  bestAsk: number | null;
  bidSize: number | null;
  askSize: number | null;
} {
  const bestBid = ob.bids?.length ? Number(ob.bids[0].price) : null;
  const bestAsk = ob.asks?.length ? Number(ob.asks[0].price) : null;
  const bidSize = ob.bids?.length ? Number(ob.bids[0].size) : null;
  const askSize = ob.asks?.length ? Number(ob.asks[0].size) : null;
  return { bestBid, bestAsk, bidSize, askSize };
}

async function safeGetOrder(client: ClobClient, orderID: string): Promise<OpenOrder | null> {
  try {
    return await client.getOrder(orderID);
  } catch {
    return null;
  }
}

function parseMatchedShares(order: OpenOrder | null): number {
  if (!order) return 0;
  const m = Number(order.size_matched);
  return Number.isFinite(m) ? m : 0;
}

function parseOriginalShares(order: OpenOrder | null): number {
  if (!order) return 0;
  const s = Number(order.original_size);
  return Number.isFinite(s) ? s : 0;
}

async function cancelBestEffort(client: ClobClient, orderID: string): Promise<CancelResult> {
  try {
    await client.cancelOrder({ orderID });
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : String(e) };
  }
}

async function main(): Promise<void> {
  if (process.argv.includes("--help") || process.argv.includes("-h")) {
    printUsage();
    return;
  }

  const privateKey = (() => {
    const v = process.env.PRIVATE_KEY ?? process.env.POLY_PRIVATE_KEY;
    try {
      return mustGet("PRIVATE_KEY (or POLY_PRIVATE_KEY)", v);
    } catch (e) {
      printUsage();
      throw e;
    }
  })();

  const UP_TOKEN_ID = (() => {
    const v = process.env.UP_TOKEN_ID ?? getArgValue("--up");
    try {
      return mustGet("UP_TOKEN_ID or --up <tokenId>", v);
    } catch (e) {
      printUsage();
      throw e;
    }
  })();

  const DOWN_TOKEN_ID = (() => {
    const v = process.env.DOWN_TOKEN_ID ?? getArgValue("--down");
    try {
      return mustGet("DOWN_TOKEN_ID or --down <tokenId>", v);
    } catch (e) {
      printUsage();
      throw e;
    }
  })();

  const priceArg = getArgValue("--price");
  const investArg = getArgValue("--invest");
  const sharesArg = getArgValue("--shares");
  const durationMinArg = getArgValue("--duration-min");
  const startAtArg = getArgValue("--start-at");
  const align15mArg = getArgValue("--align-15m");
  const alignMarketStartArg = getArgValue("--align-market-start");
  const repeatArg = getArgValue("--repeat");
  const signatureTypeArg = getArgValue("--signature-type");
  const funderArg = getArgValue("--funder");
  const postOnlyArg = getArgValue("--post-only");
  const pollSecArg = getArgValue("--poll-sec");
  const pairGraceSecArg = getArgValue("--pair-grace-sec");
  const onSingleFillArg = getArgValue("--on-single-fill");
  const minEdgeArg = getArgValue("--min-edge");
  const minHedgeEdgeArg = getArgValue("--min-hedge-edge");
  const allowWeirdQuotesArg = getArgValue("--allow-weird-quotes");
  const useRtdsArg = getArgValue("--use-rtds");
  const marketSlugArg = getArgValue("--market-slug");

  const PRICE = priceArg !== undefined ? Number(priceArg) : Number(process.env.PRICE ?? 0.49);
  const INVEST_PER_SIDE = investArg !== undefined ? Number(investArg) : Number(process.env.INVEST_PER_SIDE ?? 5);
  const DURATION_MIN = durationMinArg !== undefined ? Number(durationMinArg) : Number(process.env.DURATION_MIN ?? 15);
  const ALIGN_15M = toBool(align15mArg ?? process.env.ALIGN_15M, false);
  const ALIGN_MARKET_START = toBool(alignMarketStartArg ?? process.env.ALIGN_MARKET_START, false);
  const REPEAT = toBool(repeatArg ?? process.env.REPEAT, false);
  const POST_ONLY = toBool(postOnlyArg ?? process.env.POST_ONLY, true);
  const POLL_SEC = toNum(pollSecArg ?? process.env.POLL_SEC, 3);
  const PAIR_GRACE_SEC = toNum(pairGraceSecArg ?? process.env.PAIR_GRACE_SEC, 10);
  const ON_SINGLE_FILL = String(onSingleFillArg ?? process.env.ON_SINGLE_FILL ?? "hedge").toLowerCase() as SingleFillAction;
  const MIN_EDGE = toNum(minEdgeArg ?? process.env.MIN_EDGE, 0.005);
  const MIN_HEDGE_EDGE = toNum(minHedgeEdgeArg ?? process.env.MIN_HEDGE_EDGE, 0.002);
  const ALLOW_WEIRD_QUOTES = toBool(allowWeirdQuotesArg ?? process.env.ALLOW_WEIRD_QUOTES, false);
  const USE_RTDS = toBool(useRtdsArg ?? process.env.USE_RTDS, true);
  const MARKET_SLUG = marketSlugArg ?? process.env.MARKET_SLUG; // optional

  const SIGNATURE_TYPE =
    signatureTypeArg !== undefined ? Number(signatureTypeArg) : Number(process.env.POLY_SIGNATURE_TYPE ?? 0);

  if (!Number.isFinite(PRICE) || PRICE <= 0 || PRICE >= 1) throw new Error(`Invalid --price ${PRICE}`);
  if (!Number.isFinite(DURATION_MIN) || DURATION_MIN <= 0) throw new Error(`Invalid --duration-min ${DURATION_MIN}`);
  if (!Number.isFinite(POLL_SEC) || POLL_SEC <= 0) throw new Error(`Invalid --poll-sec ${POLL_SEC}`);
  if (!Number.isFinite(PAIR_GRACE_SEC) || PAIR_GRACE_SEC < 0) throw new Error(`Invalid --pair-grace-sec ${PAIR_GRACE_SEC}`);
  if (!["wait", "cancel", "hedge"].includes(ON_SINGLE_FILL)) {
    throw new Error(`Invalid --on-single-fill ${ON_SINGLE_FILL} (expected wait|cancel|hedge)`);
  }
  if (![0, 1, 2].includes(SIGNATURE_TYPE)) throw new Error(`Invalid signature type ${SIGNATURE_TYPE} (expected 0,1,2)`);

  const pairEdge = 1 - 2 * PRICE;
  if (pairEdge < MIN_EDGE) {
    throw new Error(
      `Refusing: pair edge too small. (1 - 2*price) = ${pairEdge.toFixed(6)} < MIN_EDGE=${MIN_EDGE}. ` +
        `Either lower --price or reduce --min-edge (at your own risk).`
    );
  }

  const signer = new Wallet(privateKey);
  const FUNDER_ADDRESS = funderArg ?? process.env.POLY_FUNDER_ADDRESS ?? signer.address;

  async function runOneWindow(args: { windowStartMs: number }): Promise<void> {
    console.log(`[${nowIso()}] Initializing client (derive creds)…`);
    const bootstrapClient = new ClobClient(HOST, CHAIN_ID, signer);
    const userApiCredsRaw = (await bootstrapClient.createOrDeriveApiKey()) as ApiKeyRaw | ApiKeyCreds;
    const userApiKey = "key" in userApiCredsRaw ? userApiCredsRaw.key : userApiCredsRaw.apiKey;
    const userApiCreds: ApiKeyCreds = {
      key: mustGet("derived user api key", userApiKey),
      secret: mustGet("derived user api secret", userApiCredsRaw.secret),
      passphrase: mustGet("derived user api passphrase", userApiCredsRaw.passphrase),
    };

    console.log(`[${nowIso()}] Reinitializing client (authenticated)…`);
    const client = new ClobClient(HOST, CHAIN_ID, signer, userApiCreds, SIGNATURE_TYPE, FUNDER_ADDRESS);

    console.log(`[${nowIso()}] Fetching market metadata…`);
    const upMarket = await client.getMarket(UP_TOKEN_ID);
    const downMarket = await client.getMarket(DOWN_TOKEN_ID);

    const upTickSize = upMarket.tickSize as TickSize;
    const downTickSize = downMarket.tickSize as TickSize;
    const upNegRisk = Boolean(upMarket.negRisk);
    const downNegRisk = Boolean(downMarket.negRisk);

    const upPrice = roundDownToTick(PRICE, upTickSize);
    const downPrice = roundDownToTick(PRICE, downTickSize);

    const size = sharesArg !== undefined ? Number(sharesArg) : computeSizeShares(INVEST_PER_SIDE, PRICE);
    if (!Number.isFinite(size) || size <= 0) throw new Error(`Invalid size/shares ${size}`);

    const upBook = await client.getOrderBook(UP_TOKEN_ID);
    const downBook = await client.getOrderBook(DOWN_TOKEN_ID);
    const upTop = parseOrderBookTop(upBook);
    const downTop = parseOrderBookTop(downBook);

    const isWeird = (x: number | null): boolean => x !== null && (x <= 0 || x >= 1);
    if (!ALLOW_WEIRD_QUOTES) {
      if (isWeird(upTop.bestAsk) || isWeird(upTop.bestBid) || isWeird(downTop.bestAsk) || isWeird(downTop.bestBid)) {
        throw new Error(
          `Refusing: detected weird top-of-book quotes. ` +
            `UP bid/ask=${upTop.bestBid}/${upTop.bestAsk}, DOWN bid/ask=${downTop.bestBid}/${downTop.bestAsk}. ` +
            `Pass --allow-weird-quotes true if you want to ignore this (not recommended).`
        );
      }
    }

    const delay = args.windowStartMs - Date.now();
    if (delay > 0) {
      console.log(`[${nowIso()}] Waiting ${Math.ceil(delay / 1000)}s until window start @ ${new Date(args.windowStartMs).toISOString()}…`);
      await sleep(delay);
    }

    console.log(`[${nowIso()}] Placing orders:`);
    console.log(`- UP   token=${UP_TOKEN_ID}  price=${upPrice}  size=${size} shares`);
    console.log(`- DOWN token=${DOWN_TOKEN_ID}  price=${downPrice}  size=${size} shares`);
    console.log(`- postOnly=${POST_ONLY}  pollSec=${POLL_SEC}  pairGraceSec=${PAIR_GRACE_SEC}  onSingleFill=${ON_SINGLE_FILL}`);

    const orderIds: string[] = [];

    const upResp = (await client.createAndPostOrder(
      { tokenID: UP_TOKEN_ID, price: upPrice, size, side: Side.BUY },
      { tickSize: upTickSize, negRisk: upNegRisk },
      OrderType.GTC,
      undefined,
      POST_ONLY
    )) as { orderID: string; status?: string };
    console.log(`[${nowIso()}] UP order: id=${upResp.orderID} status=${upResp.status ?? "n/a"}`);
    orderIds.push(upResp.orderID);

    const downResp = (await client.createAndPostOrder(
      { tokenID: DOWN_TOKEN_ID, price: downPrice, size, side: Side.BUY },
      { tickSize: downTickSize, negRisk: downNegRisk },
      OrderType.GTC,
      undefined,
      POST_ONLY
    )) as { orderID: string; status?: string };
    console.log(`[${nowIso()}] DOWN order: id=${downResp.orderID} status=${downResp.status ?? "n/a"}`);
    orderIds.push(downResp.orderID);

    const cancelAtMs = Date.now() + DURATION_MIN * 60 * 1000;
    console.log(`[${nowIso()}] Will cancel remaining open orders in ${DURATION_MIN} minutes @ ${new Date(cancelAtMs).toISOString()}`);

    let rtdClient: RealTimeDataClient | null = null;
    let rtdsTriggered = false;

    if (USE_RTDS) {
      try {
        rtdClient = new RealTimeDataClient({
          host: RTDS_HOST,
          autoReconnect: true,
          onConnect: (c) => {
            console.log(`[${nowIso()}] RTDS connected. Subscribing…`);

            const clob_auth = {
              key: userApiCreds.key,
              secret: userApiCreds.secret,
              passphrase: userApiCreds.passphrase,
            };

            c.subscribe({
              subscriptions: [
                { topic: "clob_user", type: "order", clob_auth },
                { topic: "clob_user", type: "trade", clob_auth },
              ],
            });

            if (MARKET_SLUG) {
              c.subscribe({
                subscriptions: [
                  { topic: "activity", type: "orders_matched", filters: JSON.stringify({ market_slug: MARKET_SLUG }) },
                  { topic: "activity", type: "trades", filters: JSON.stringify({ market_slug: MARKET_SLUG }) },
                ],
              });
            }
          },
          onMessage: (_c, msg: Message) => {
            if (msg.topic === "clob_user") rtdsTriggered = true;
          },
        }).connect();
      } catch (e) {
        console.log(`[${nowIso()}] RTDS init failed, falling back to polling: ${e instanceof Error ? e.message : String(e)}`);
        rtdClient = null;
      }
    }

    const eps = 1e-9;
    let firstSingleFillAtMs: number | null = null;
    let actedOnSingleFill = false;

    while (Date.now() < cancelAtMs) {
      if (USE_RTDS && rtdClient) {
        if (!rtdsTriggered) {
          await sleep(Math.max(POLL_SEC, 3) * 1000);
          continue;
        }
        rtdsTriggered = false;
      }

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
        break;
      }

      if (oneSome && !actedOnSingleFill) {
        if (firstSingleFillAtMs === null) {
          firstSingleFillAtMs = Date.now();
          console.log(`[${nowIso()}] Single-leg fill detected. upMatched=${upMatched} downMatched=${downMatched}. Starting grace timer (${PAIR_GRACE_SEC}s)…`);
        }

        const elapsed = (Date.now() - firstSingleFillAtMs) / 1000;
        if (elapsed >= PAIR_GRACE_SEC) {
          actedOnSingleFill = true;
          const filledSide = upMatched > downMatched ? "UP" : "DOWN";
          const missingShares = Math.abs(upMatched - downMatched);
          const otherToken = filledSide === "UP" ? DOWN_TOKEN_ID : UP_TOKEN_ID;
          const otherTick = filledSide === "UP" ? downTickSize : upTickSize;
          const otherNegRisk = filledSide === "UP" ? downNegRisk : upNegRisk;

          console.log(`[${nowIso()}] Grace elapsed. filledSide=${filledSide} missingShares=${missingShares}. Action=${ON_SINGLE_FILL}`);

          if (ON_SINGLE_FILL === "wait") {
            // no-op
          } else if (ON_SINGLE_FILL === "cancel") {
            const unfilledOrderId = filledSide === "UP" ? downResp.orderID : upResp.orderID;
            const r = await cancelBestEffort(client, unfilledOrderId);
            console.log(`[${nowIso()}] Cancel unfilled leg (${unfilledOrderId}) => ${r.ok ? "ok" : `failed: ${r.error}`}`);
          } else if (ON_SINGLE_FILL === "hedge") {
            const maxOtherPrice = 1 - PRICE - MIN_HEDGE_EDGE;
            const otherBook = await client.getOrderBook(otherToken);
            const otherTop = parseOrderBookTop(otherBook);
            const bestAsk = otherTop.bestAsk;

            if (!Number.isFinite(bestAsk ?? NaN)) {
              console.log(`[${nowIso()}] Hedge skipped: could not read best ask for other leg.`);
            } else if ((bestAsk as number) > maxOtherPrice) {
              console.log(`[${nowIso()}] Hedge skipped: bestAsk=${bestAsk} exceeds maxOtherPrice=${maxOtherPrice.toFixed(6)} to keep edge >= ${MIN_HEDGE_EDGE}`);
            } else {
              const hedgePrice = roundDownToTick(maxOtherPrice, otherTick);
              console.log(`[${nowIso()}] Hedging by buying other leg token=${otherToken} shares=${missingShares} price<=${hedgePrice} (bestAsk=${bestAsk})`);
              try {
                const hedgeResp = (await client.createAndPostOrder(
                  { tokenID: otherToken, price: hedgePrice, size: missingShares, side: Side.BUY },
                  { tickSize: otherTick, negRisk: otherNegRisk },
                  OrderType.GTC,
                  undefined,
                  false
                )) as { orderID: string; status?: string };
                console.log(`[${nowIso()}] Hedge order posted: id=${hedgeResp.orderID} status=${hedgeResp.status ?? "n/a"}`);
              } catch (e) {
                console.log(`[${nowIso()}] Hedge failed: ${e instanceof Error ? e.message : String(e)}`);
              }
            }
          }
        }
      }

      await sleep(POLL_SEC * 1000);
    }

    if (rtdClient) {
      try {
        rtdClient.disconnect();
      } catch {
        // ignore
      }
    }

    console.log(`[${nowIso()}] Cancelling orders (best-effort)…`);
    for (const id of orderIds) {
      const r = await cancelBestEffort(client, id);
      console.log(`[${nowIso()}] Cancel ${id} => ${r.ok ? "ok" : `failed: ${r.error}`}`);
    }
  }

  let firstWindowStartMs: number;
  if (startAtArg) firstWindowStartMs = parseIsoMs(startAtArg);
  else if (ALIGN_MARKET_START) {
    if (!MARKET_SLUG) {
      throw new Error(`--align-market-start requires --market-slug (or env MARKET_SLUG).`);
    }
    firstWindowStartMs = parseMarketStartFromSlugMs(MARKET_SLUG);
  } else if (ALIGN_15M) firstWindowStartMs = ceilToWindowMs(Date.now(), 15);
  else firstWindowStartMs = Date.now();

  if (!REPEAT) {
    await runOneWindow({ windowStartMs: firstWindowStartMs });
    console.log(`[${nowIso()}] Done.`);
    return;
  }

  while (true) {
    // Repeat every 15 minutes from the previous window start to avoid drift.
    // Note: this does NOT automatically switch to the "next market slug/token IDs" — you must update those externally.
    const nextStart = firstWindowStartMs + 15 * 60 * 1000;
    firstWindowStartMs = nextStart;
    await runOneWindow({ windowStartMs: nextStart });
  }
}

main().catch((e: unknown) => {
  console.error(`Fatal: ${e instanceof Error ? e.message : String(e)}`);
  process.exit(1);
});

