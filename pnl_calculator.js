const fs = require('fs');
const path = require('path');

const DATA_DIR = path.join(__dirname, 'data');
const DEFAULT_LIMIT_PRICE = 0.02;
const DEFAULT_INVESTMENT_PER_SIDE = 5;
const DEFAULT_MIN_FILE_SIZE_BYTES = 3 * 1024 * 1024;
const DEFAULT_SAMPLE_N = 5;

function getArgValue(flag) {
    const idx = process.argv.indexOf(flag);
    if (idx === -1) return undefined;
    const val = process.argv[idx + 1];
    if (val === undefined || val.startsWith('--')) return undefined;
    return val;
}

function parseConfig() {
    const limit = getArgValue('--limit');
    const invest = getArgValue('--invest');
    const minMb = getArgValue('--min-mb');
    const sampleN = getArgValue('--sample');
    const seed = getArgValue('--seed');
    const mode = getArgValue('--mode');

    const LIMIT_PRICE = limit !== undefined ? Number(limit) : DEFAULT_LIMIT_PRICE;
    const INVESTMENT_PER_SIDE = invest !== undefined ? Number(invest) : DEFAULT_INVESTMENT_PER_SIDE;
    const MIN_FILE_SIZE_BYTES =
        minMb !== undefined ? Math.floor(Number(minMb) * 1024 * 1024) : DEFAULT_MIN_FILE_SIZE_BYTES;
    const SAMPLE_N = sampleN !== undefined ? Math.floor(Number(sampleN)) : DEFAULT_SAMPLE_N;
    const SEED = seed !== undefined ? String(seed) : undefined;
    const MODE = mode !== undefined ? String(mode) : 'summary';

    return { LIMIT_PRICE, INVESTMENT_PER_SIDE, MIN_FILE_SIZE_BYTES, SAMPLE_N, SEED, MODE };
}

function assertValidConfig({ LIMIT_PRICE, INVESTMENT_PER_SIDE, MIN_FILE_SIZE_BYTES }) {
    if (!Number.isFinite(LIMIT_PRICE) || LIMIT_PRICE <= 0) {
        throw new Error(
            `Invalid LIMIT_PRICE=${LIMIT_PRICE}. It must be a number > 0. ` +
                `If you set LIMIT_PRICE=0 then shares = INVESTMENT_PER_SIDE / LIMIT_PRICE becomes Infinity.`
        );
    }
    if (!Number.isFinite(INVESTMENT_PER_SIDE) || INVESTMENT_PER_SIDE <= 0) {
        throw new Error(`Invalid INVESTMENT_PER_SIDE=${INVESTMENT_PER_SIDE}. It must be a number > 0.`);
    }
    if (!Number.isFinite(MIN_FILE_SIZE_BYTES) || MIN_FILE_SIZE_BYTES < 0) {
        throw new Error(`Invalid MIN_FILE_SIZE_BYTES=${MIN_FILE_SIZE_BYTES}. It must be a number >= 0.`);
    }
}

function hashSeedToUint32(seedStr) {
    // Simple deterministic 32-bit hash (FNV-1a)
    let h = 2166136261;
    for (let i = 0; i < seedStr.length; i++) {
        h ^= seedStr.charCodeAt(i);
        h = Math.imul(h, 16777619);
    }
    return h >>> 0;
}

function mulberry32(seed) {
    let a = seed >>> 0;
    return function rand() {
        a |= 0;
        a = (a + 0x6D2B79F5) | 0;
        let t = Math.imul(a ^ (a >>> 15), 1 | a);
        t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
        return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
}

function sampleWithoutReplacement(arr, n, rand) {
    const copy = arr.slice();
    for (let i = copy.length - 1; i > 0; i--) {
        const j = Math.floor(rand() * (i + 1));
        [copy[i], copy[j]] = [copy[j], copy[i]];
    }
    return copy.slice(0, Math.min(n, copy.length));
}

function getAllJsonFiles(dir, minBytes, files = []) {
    const list = fs.readdirSync(dir);
    for (const file of list) {
        const fullPath = path.join(dir, file);
        const stats = fs.statSync(fullPath);
        if (stats.isDirectory()) {
            getAllJsonFiles(fullPath, minBytes, files);
        } else if (file.endsWith('.json')) {
            // Only consider files greater than 3MB
            if (stats.size > minBytes) {
                files.push(fullPath);
            }
        }
    }
    return files;
}

function processFile(filePath, { LIMIT_PRICE, INVESTMENT_PER_SIDE }) {
    try {
        const content = fs.readFileSync(filePath, 'utf8');
        const data = JSON.parse(content);
        
        if (!data.ticks || data.ticks.length === 0) return null;

        const strikePrice = data.ticks[0].market.strikePrice;
        let upFilled = false;
        let downFilled = false;

        for (const tick of data.ticks) {
            if (!upFilled && tick.prices.upAsk <= LIMIT_PRICE) {
                upFilled = true;
            }
            if (!downFilled && tick.prices.downAsk <= LIMIT_PRICE) {
                downFilled = true;
            }
            if (upFilled && downFilled) break;
        }

        const lastTick = data.ticks[data.ticks.length - 1];
        const finalSpotPrice = lastTick.spotPrice;
        
        // Settlement logic: UP wins if spot > strike, DOWN wins otherwise
        const upWins = finalSpotPrice > strikePrice;
        const downWins = !upWins;

        // We model a full fill of $INVESTMENT_PER_SIDE at exactly LIMIT_PRICE once per side per window.
        // (We don't have orderbook size/slippage data in the ticks.)
        const unitsPerSide = INVESTMENT_PER_SIDE / LIMIT_PRICE;
        
        let payout = 0;
        let investment = 0;

        if (upFilled) {
            investment += INVESTMENT_PER_SIDE;
            if (upWins) {
                payout += unitsPerSide * 1.00;
            }
        }

        if (downFilled) {
            investment += INVESTMENT_PER_SIDE;
            if (downWins) {
                payout += unitsPerSide * 1.00;
            }
        }

        const pnl = payout - investment;
        const bothFilled = upFilled && downFilled;
        const onlyUpFilled = upFilled && !downFilled;
        const onlyDownFilled = !upFilled && downFilled;

        return {
            pnl,
            investment,
            payout,
            upFilled,
            downFilled,
            bothFilled,
            onlyUpFilled,
            onlyDownFilled,
            upWins,
            downWins,
            // NOTE: This "won" is NOT a meaningful metric for the overall strategy when both sides fill.
            // It is kept for debugging, but the headline winrate should be based on pnl > 0.
            won: (upFilled && upWins) || (downFilled && downWins),
            isTrade: upFilled || downFilled,
            // Uses the last recorded tick as a proxy for the final settlement.
            // If your session stops before market expiry, directional results can be wrong.
            settlementIsProxy: true
        };
    } catch (e) {
        console.error(`Error processing ${filePath}: ${e.message}`);
        return null;
    }
}

function analyzeFileDetailed(filePath, { LIMIT_PRICE, INVESTMENT_PER_SIDE }) {
    const content = fs.readFileSync(filePath, 'utf8');
    const data = JSON.parse(content);
    if (!data.ticks || data.ticks.length === 0) return null;

    const firstTick = data.ticks[0];
    const lastTick = data.ticks[data.ticks.length - 1];

    const marketSlug = firstTick.market?.slug ?? data.marketSlug ?? path.basename(filePath);
    const strikePrice = firstTick.market?.strikePrice;
    const endDate = firstTick.market?.endDate;

    const orderPlacedAt = firstTick.timestamp;
    const orderPlacedAtMs = firstTick.timestampMs;

    let upFill = null;
    let downFill = null;
    let minUpAsk = Number.POSITIVE_INFINITY;
    let minDownAsk = Number.POSITIVE_INFINITY;

    for (const tick of data.ticks) {
        const upAsk = tick?.prices?.upAsk;
        const downAsk = tick?.prices?.downAsk;
        if (Number.isFinite(upAsk)) minUpAsk = Math.min(minUpAsk, upAsk);
        if (Number.isFinite(downAsk)) minDownAsk = Math.min(minDownAsk, downAsk);

        if (!upFill && Number.isFinite(upAsk) && upAsk <= LIMIT_PRICE) {
            upFill = { timestamp: tick.timestamp, timestampMs: tick.timestampMs, ask: upAsk, bid: tick?.prices?.upBid };
        }
        if (!downFill && Number.isFinite(downAsk) && downAsk <= LIMIT_PRICE) {
            downFill = { timestamp: tick.timestamp, timestampMs: tick.timestampMs, ask: downAsk, bid: tick?.prices?.downBid };
        }
        if (upFill && downFill) break;
    }

    const strike = Number.isFinite(strikePrice) ? strikePrice : null;
    const finalSpotPrice = lastTick.spotPrice;
    const upWins = strike === null ? null : finalSpotPrice > strike;
    const downWins = strike === null ? null : !upWins;

    const unitsPerSide = INVESTMENT_PER_SIDE / LIMIT_PRICE;
    const upFilled = Boolean(upFill);
    const downFilled = Boolean(downFill);

    let payout = 0;
    let investment = 0;
    if (upFilled) {
        investment += INVESTMENT_PER_SIDE;
        if (upWins) payout += unitsPerSide;
    }
    if (downFilled) {
        investment += INVESTMENT_PER_SIDE;
        if (downWins) payout += unitsPerSide;
    }
    const pnl = payout - investment;

    const pairCompleteAtMs =
        upFill && downFill ? Math.max(upFill.timestampMs, downFill.timestampMs) : null;
    const pairCompleteAt =
        pairCompleteAtMs === null
            ? null
            : upFill.timestampMs >= downFill.timestampMs
                ? upFill.timestamp
                : downFill.timestamp;

    return {
        filePath,
        sessionId: data.sessionId,
        marketSlug,
        startTime: data.startTime,
        endTime: data.endTime,
        marketEndDate: endDate,
        strikePrice: strike,
        order: {
            placedAt: orderPlacedAt,
            placedAtMs: orderPlacedAtMs,
            limitPrice: LIMIT_PRICE,
            investmentPerSide: INVESTMENT_PER_SIDE
        },
        fills: {
            up: upFill,
            down: downFill,
            pairCompleteAt,
            pairCompleteAtMs
        },
        windowStats: {
            totalTicks: data.totalTicks,
            minUpAsk: Number.isFinite(minUpAsk) ? minUpAsk : null,
            minDownAsk: Number.isFinite(minDownAsk) ? minDownAsk : null
        },
        settlementProxy: {
            usesLastTick: true,
            lastTickTimestamp: lastTick.timestamp,
            lastTickTimestampMs: lastTick.timestampMs,
            finalSpotPrice,
            upWins,
            downWins
        },
        result: {
            upFilled,
            downFilled,
            bothFilled: upFilled && downFilled,
            investment,
            payout,
            pnl
        },
        caveats: [
            'Fill assumes you can buy $INVESTMENT_PER_SIDE at exactly LIMIT_PRICE the first time ask <= LIMIT_PRICE. Depth/slippage is unknown.',
            'Directional result uses last tick in file as settlement proxy; if file ends before market expiry, this can be wrong.'
        ]
    };
}

function main() {
    const config = parseConfig();
    const { LIMIT_PRICE, INVESTMENT_PER_SIDE, MIN_FILE_SIZE_BYTES, SAMPLE_N, SEED, MODE } = config;
    assertValidConfig(config);
    console.log('Finding JSON files...');
    const files = getAllJsonFiles(DATA_DIR, MIN_FILE_SIZE_BYTES);
    console.log(`Found ${files.length} files.`);

    if (MODE === 'sample') {
        const seedStr = SEED ?? `seed:${LIMIT_PRICE}:${INVESTMENT_PER_SIDE}:${MIN_FILE_SIZE_BYTES}:${files.length}`;
        const rand = mulberry32(hashSeedToUint32(seedStr));
        const picked = sampleWithoutReplacement(files, SAMPLE_N, rand);

        console.log('\n--- Sample Analysis ---');
        console.log(`Sample size: ${picked.length}`);
        console.log(`Seed: ${seedStr}`);
        console.log(
            `NOTE: This is a *fill-tape* view (when ask <= limit), not a true execution simulator (no depth/size/slippage).`
        );

        const analyses = [];
        for (const file of picked) {
            const a = analyzeFileDetailed(file, config);
            if (!a) continue;
            analyses.push(a);

            console.log('\n------------------------------');
            console.log(`Market: ${a.marketSlug}`);
            console.log(`Session: ${a.sessionId}`);
            console.log(`Order placed: ${a.order.placedAt} @ limit=${a.order.limitPrice} (up + down, $${a.order.investmentPerSide} each)`);
            console.log(`Strike: ${a.strikePrice ?? 'n/a'}  Market end: ${a.marketEndDate ?? 'n/a'}`);
            console.log(`Min asks in file: up=${a.windowStats.minUpAsk} down=${a.windowStats.minDownAsk}`);

            console.log(`UP fill: ${a.fills.up ? `${a.fills.up.timestamp} ask=${a.fills.up.ask} bid=${a.fills.up.bid}` : 'NOT FILLED'}`);
            console.log(`DOWN fill: ${a.fills.down ? `${a.fills.down.timestamp} ask=${a.fills.down.ask} bid=${a.fills.down.bid}` : 'NOT FILLED'}`);
            console.log(`Pair complete: ${a.fills.pairCompleteAt ?? 'n/a'}`);

            console.log(
                `Settlement (proxy last tick @ ${a.settlementProxy.lastTickTimestamp}): spot=${a.settlementProxy.finalSpotPrice} => upWins=${a.settlementProxy.upWins}`
            );
            console.log(
                `Result: bothFilled=${a.result.bothFilled} investment=$${a.result.investment.toFixed(2)} payout=$${a.result.payout.toFixed(2)} pnl=$${a.result.pnl.toFixed(2)}`
            );
        }

        const outPath = path.join(__dirname, 'sample_analysis.json');
        fs.writeFileSync(outPath, JSON.stringify({ config, seed: seedStr, analyses }, null, 2));
        console.log(`\nWrote detailed JSON report to: ${outPath}`);
        return;
    }

    let totalPnL = 0;
    let totalInvestment = 0;
    let tradesCount = 0;
    let bothFilledCount = 0;
    let onlyUpCount = 0;
    let onlyDownCount = 0;
    let noFillCount = 0;
    let upFilledCount = 0;
    let downFilledCount = 0;
    let pnlPositiveCount = 0;
    let pnlNegativeCount = 0;
    let pnlZeroCount = 0;

    for (const file of files) {
        const result = processFile(file, config);
        if (!result) continue;
        if (!result.isTrade) {
            noFillCount++;
            continue;
        }

        if (result && result.isTrade) {
            totalPnL += result.pnl;
            totalInvestment += result.investment;
            tradesCount++;
            if (result.bothFilled) bothFilledCount++;
            if (result.onlyUpFilled) onlyUpCount++;
            if (result.onlyDownFilled) onlyDownCount++;
            if (result.upFilled) upFilledCount++;
            if (result.downFilled) downFilledCount++;

            if (result.pnl > 0) pnlPositiveCount++;
            else if (result.pnl < 0) pnlNegativeCount++;
            else pnlZeroCount++;
        }
    }

    // Meaningful win rate = % of windows with pnl > 0
    const winRate = tradesCount > 0 ? (pnlPositiveCount / tradesCount) * 100 : 0;
    const roi = totalInvestment > 0 ? (totalPnL / totalInvestment) * 100 : 0;
    const arbRate = tradesCount > 0 ? (bothFilledCount / tradesCount) * 100 : 0;
    const pnlPerArbWindow = (INVESTMENT_PER_SIDE / LIMIT_PRICE) - (2 * INVESTMENT_PER_SIDE);

    console.log('\n--- Simulation Results ---');
    console.log(`Limit Price: ${LIMIT_PRICE}`);
    console.log(`Investment per side: $${INVESTMENT_PER_SIDE}`);
    console.log(`Total Windows Processed: ${files.length}`);
    console.log(`Windows with at least one fill: ${tradesCount}`);
    console.log(`Windows with no fills: ${noFillCount}`);
    console.log(`Windows with both filled (Arbitrage): ${bothFilledCount}`);
    console.log(`Windows with only UP filled: ${onlyUpCount}`);
    console.log(`Windows with only DOWN filled: ${onlyDownCount}`);
    console.log(`Arb-fill rate (both filled / traded): ${arbRate.toFixed(2)}%`);
    console.log(`UP orders filled: ${upFilledCount}`);
    console.log(`DOWN orders filled: ${downFilledCount}`);
    console.log(`Total Investment: $${totalInvestment.toFixed(2)}`);
    console.log(`Total PnL: $${totalPnL.toFixed(2)}`);
    console.log(`Win Rate (PnL > 0): ${winRate.toFixed(2)}%`);
    console.log(`PnL>0 / PnL<0 / PnL=0: ${pnlPositiveCount} / ${pnlNegativeCount} / ${pnlZeroCount}`);
    console.log(`ROI: ${roi.toFixed(2)}%`);
    console.log(
        `PnL per arb window (model, if both fill at LIMIT_PRICE): $${pnlPerArbWindow.toFixed(4)}`
    );
    console.log(
        `NOTE: Directional windows (only one side filled) use last-tick spot vs strike as a settlement proxy.`
    );
    console.log(
        `Run examples: node pnl_calculator.js --limit 0.49 --invest 5 --min-mb 3`
    );
    console.log(`Sample 5 markets: node pnl_calculator.js --mode sample --sample 5 --seed demo --limit ${LIMIT_PRICE} --invest ${INVESTMENT_PER_SIDE} --min-mb ${Math.round(MIN_FILE_SIZE_BYTES / 1024 / 1024)}`);
}

main();
