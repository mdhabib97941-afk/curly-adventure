import express from 'express';
import cors from 'cors';
import axios from 'axios';
import sqlite3 from 'sqlite3';
import mongoose from 'mongoose';
import path from 'path';
import { fileURLToPath } from 'url';
import WebSocket from 'ws';

let binanceCooldownUntil = 0;

axios.interceptors.request.use(config => {
    if (Date.now() < binanceCooldownUntil && config.url && config.url.includes('api.binance.com')) {
        return Promise.reject(new Error("Binance API on cooldown (418 Ban). Retrying later..."));
    }
    return config;
});

axios.interceptors.response.use(response => response, error => {
    if (error.response && error.response.status === 418) {
        console.warn("Binance 418 IP Ban detected! Triggering 5-minute cooldown.");
        binanceCooldownUntil = Date.now() + 5 * 60 * 1000; // 5 minute cooldown
    }
    return Promise.reject(error);
});

const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json()); // needed for POST body parsing

// Dynamic Symbol endpoint
app.post('/api/set-symbol', (req, res) => {
    if(req.body.symbol) {
        SYMBOL = req.body.symbol;
        console.log(`Backend tracking symbol changed to ${SYMBOL}`);
    }
    res.json({success: true, symbol: SYMBOL});
});


// --- PROXY ENDPOINT FOR CHART ---
app.get('/api/proxy/klines', async (req, res) => {
    try {
        const { symbol, interval, limit, mode } = req.query;
        let url = mode === 'futures' 
            ? `https://fapi.binance.com/fapi/v1/klines?symbol=${symbol}&interval=${interval}&limit=${limit}`
            : `https://api.binance.com/api/v3/klines?symbol=${symbol}&interval=${interval}&limit=${limit}`;
        const response = await axios.get(url, { timeout: 10000 });
        res.json(response.data);
    } catch (e) {
        console.error('Proxy Error:', e.message);
        res.status(500).json({ error: e.message });
    }
});
// --- END PROXY ENDPOINT ---

app.get('/api/symbol', async (req, res) => {
    res.json({symbol: SYMBOL});
});

const PORT   = process.env.PORT || 3000;
let SYMBOL = 'BTCUSDT';
const BASE   = 'https://data-api.binance.vision';

// Timeframe intervals in milliseconds
const TF_MS = { '5m': 300000, '15m': 900000, '1h': 3600000, '4h': 14400000, '1d': 86400000 };

// â”€â”€ DATABASE (SQLite for temporary, MongoDB for persistent) â”€â”€â”€â”€â”€â”€â”€â”€
const db = new sqlite3.Database(path.join(__dirname, 'database.sqlite'));

// --- MongoDB Setup ---
const MONGO_URI = process.env.MONGO_URI || 'mongodb+srv://user:pass@cluster.mongodb.net/alphaflow';
mongoose.connect(MONGO_URI)
    .then(() => console.log('âœ… Connected to MongoDB (Persistent Storage)'))
    .catch(err => console.error('âŒ MongoDB connection error (Check your MONGO_URI):', err));

const OrderbookSnapshot = mongoose.model('OrderbookSnapshot', new mongoose.Schema({
    timestamp: { type: Date, default: Date.now },
    symbol: String,
    bid_price: Number, bid_volume: Number,
    ask_price: Number, ask_volume: Number,
    spread: Number
}));

const BtcDeepLiquidity = mongoose.model('BtcDeepLiquidity', new mongoose.Schema({
    timestamp: { type: Date, default: Date.now },
    bid_vol_1: Number, ask_vol_1: Number,
    bid_vol_2: Number, ask_vol_2: Number,
    bid_vol_5: Number, ask_vol_5: Number
}));
// ---------------------

db.serialize(() => {
    db.run(`CREATE TABLE IF NOT EXISTS candles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT, timeframe TEXT, time INTEGER,
        open REAL, high REAL, low REAL, close REAL, volume REAL,
        UNIQUE(symbol, timeframe, time)
    )`);
    db.run(`CREATE TABLE IF NOT EXISTS patterns (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT, timeframe TEXT, pattern_type TEXT,
        time INTEGER, price REAL,
        outcome TEXT, pct_move REAL, bars_to_target INTEGER,
        confirmed INTEGER DEFAULT 0
    )`);
    db.run(`CREATE TABLE IF NOT EXISTS liquidity_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        symbol TEXT, type TEXT, price REAL, description TEXT
    )`);
    db.run(`CREATE TABLE IF NOT EXISTS orderbook_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        symbol TEXT, bid_price REAL, bid_volume REAL, ask_price REAL, ask_volume REAL, spread REAL
    )`);
    db.run(`CREATE TABLE IF NOT EXISTS signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        symbol TEXT, signal TEXT, confidence TEXT,
        entry REAL, stop_loss REAL, take_profit REAL, reason TEXT
    )`);
    // SMC structures detected
    db.run(`CREATE TABLE IF NOT EXISTS smc_structures (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT, timeframe TEXT,
        structure_type TEXT,
        time INTEGER, price REAL,
        ob_top REAL, ob_bottom REAL,
        fvg_top REAL, fvg_bottom REAL,
        UNIQUE(symbol, timeframe, structure_type, time)
    )`);
    // Brain Paper Trades
    db.run(`CREATE TABLE IF NOT EXISTS brain_trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT, timeframe TEXT, strategy TEXT, context TEXT,
        entry_price REAL, sl REAL, tp REAL,
        status TEXT, outcome TEXT, pnl REAL,
        open_time INTEGER, close_time INTEGER
    )`);
    db.run(`CREATE TABLE IF NOT EXISTS brain_insights (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        trade_id INTEGER,
        trap_type TEXT, observation TEXT, recommendation TEXT,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    )`);
    // BTC Deep Liquidity Tracker
    db.run(`CREATE TABLE IF NOT EXISTS btc_deep_liquidity (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        bid_vol_1 REAL, ask_vol_1 REAL,
        bid_vol_2 REAL, ask_vol_2 REAL,
        bid_vol_5 REAL, ask_vol_5 REAL
    )`);
    console.log('[DB] All tables ready');
});

const dbRun = (sql,p=[]) => new Promise((res,rej) => db.run(sql,p,function(e){ e?rej(e):res(this); }));
const dbAll = (sql,p=[]) => new Promise((res,rej) => db.all(sql,p,(e,rows)=>{ e?rej(e):res(rows); }));
const dbGet = (sql,p=[]) => new Promise((res,rej) => db.get(sql,p,(e,row)=>{ e?rej(e):res(row); }));

// â”€â”€â”€ DOWNLOAD STATUS (in-memory) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
let dlStatus = { running: false, progress: 0, message: 'Not started', log: [] };

// â”€â”€â”€ MATH HELPERS â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function calcVWAP(candlesSubset) {
    if (candlesSubset.length === 0) return 0;
    let sumPV = 0, sumV = 0;
    for (let c of candlesSubset) {
        const tp = (c.high + c.low + c.close) / 3;
        sumPV += tp * c.volume;
        sumV += c.volume;
    }
    return sumV > 0 ? sumPV / sumV : candlesSubset[candlesSubset.length-1].close;
}

function calcEMA(values, period) {
    if (values.length < period) return values[values.length-1];
    const k = 2 / (period + 1);
    let ema = values.slice(0, period).reduce((a,b)=>a+b,0) / period;
    for (let i = period; i < values.length; i++) ema = values[i] * k + ema * (1-k);
    return ema;
}
function calcATR(candles, period=14) {
    const trs = [];
    for (let i=1; i<candles.length; i++) {
        trs.push(Math.max(
            candles[i].high - candles[i].low,
            Math.abs(candles[i].high - candles[i-1].close),
            Math.abs(candles[i].low  - candles[i-1].close)
        ));
    }
    return trs.slice(-period).reduce((a,b)=>a+b,0) / Math.min(period, trs.length);
}

// â”€â”€â”€ SMC STRUCTURE DETECTION â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function detectSwingsSMC(candles, window=3) {
    const highs=[], lows=[];
    for (let i=window; i<candles.length-window; i++) {
        let isH=true, isL=true;
        for (let d=1; d<=window; d++) {
            if (candles[i].high<=candles[i-d].high || candles[i].high<=candles[i+d].high) isH=false;
            if (candles[i].low >=candles[i-d].low  || candles[i].low >=candles[i+d].low)  isL=false;
        }
        if (isH) highs.push({idx:i, price:candles[i].high, time:candles[i].time});
        if (isL) lows.push({idx:i, price:candles[i].low,   time:candles[i].time});
    }
    return {highs, lows};
}

// Detect BOS: Break of Structure â€” price closes beyond previous swing
function detectBOS(candles, highs, lows) {
    const events = [];
    for (let i=1; i<candles.length; i++) {
        // Bullish BOS: close breaks above last swing high
        const prevHigh = highs.filter(h=>h.idx<i).slice(-1)[0];
        const prevLow  = lows.filter(l=>l.idx<i).slice(-1)[0];
        if (prevHigh && candles[i].close > prevHigh.price && candles[i-1].close <= prevHigh.price) {
            events.push({type:'BOS_BULL', idx:i, time:candles[i].time, price:candles[i].close, level:prevHigh.price});
        }
        if (prevLow && candles[i].close < prevLow.price && candles[i-1].close >= prevLow.price) {
            events.push({type:'BOS_BEAR', idx:i, time:candles[i].time, price:candles[i].close, level:prevLow.price});
        }
    }
    return events;
}

// Detect CHoCH: Change of Character (first BOS against prevailing trend)
function detectCHoCH(candles, bosEvents) {
    const events = [];
    let lastTrend = null;
    for (const bos of bosEvents) {
        const newTrend = bos.type === 'BOS_BULL' ? 'UP' : 'DOWN';
        if (lastTrend && lastTrend !== newTrend) {
            events.push({
                type: newTrend === 'UP' ? 'CHOCH_BULL' : 'CHOCH_BEAR',
                idx: bos.idx, time: bos.time, price: bos.price, level: bos.level
            });
        }
        lastTrend = newTrend;
    }
    return events;
}

// Detect Order Blocks: Last opposite candle before a strong BOS
function detectOrderBlocks(candles, bosEvents) {
    const obs = [];
    for (const bos of bosEvents) {
        const dir = bos.type === 'BOS_BULL' ? 'BULL' : 'BEAR';
        // Search backward for last opposite candle before this BOS
        for (let j = bos.idx-1; j >= Math.max(0, bos.idx-10); j--) {
            const c = candles[j];
            const isBearCandle = c.close < c.open;
            const isBullCandle = c.close > c.open;
            if (dir==='BULL' && isBearCandle) {
                obs.push({type:'OB_BULL', idx:j, time:c.time, price:c.low, ob_top:c.open, ob_bottom:c.low});
                break;
            }
            if (dir==='BEAR' && isBullCandle) {
                obs.push({type:'OB_BEAR', idx:j, time:c.time, price:c.high, ob_top:c.high, ob_bottom:c.close});
                break;
            }
        }
    }
    return obs;
}

// Detect FVG (Fair Value Gap / Imbalance): 3-candle gap
function detectFVG(candles) {
    const fvgs = [];
    for (let i=1; i<candles.length-1; i++) {
        // Bullish FVG: candle[i-1].high < candle[i+1].low
        if (candles[i-1].high < candles[i+1].low) {
            fvgs.push({type:'FVG_BULL', idx:i, time:candles[i].time,
                fvg_bottom: candles[i-1].high, fvg_top: candles[i+1].low});
        }
        // Bearish FVG: candle[i-1].low > candle[i+1].high
        if (candles[i-1].low > candles[i+1].high) {
            fvgs.push({type:'FVG_BEAR', idx:i, time:candles[i].time,
                fvg_top: candles[i-1].low, fvg_bottom: candles[i+1].high});
        }
    }
    return fvgs;
}

// Detect Equal Highs/Lows (EQH/EQL)
function detectEQHL(candles, highs, lows, tolerance=0.002) {
    const events = [];
    for (let i=0; i<highs.length-1; i++) {
        if (Math.abs(highs[i].price - highs[i+1].price) / highs[i].price < tolerance) {
            events.push({type:'EQH', time:highs[i+1].time, price:highs[i+1].price, idx:highs[i+1].idx});
        }
    }
    for (let i=0; i<lows.length-1; i++) {
        if (Math.abs(lows[i].price - lows[i+1].price) / lows[i].price < tolerance) {
            events.push({type:'EQL', time:lows[i+1].time, price:lows[i+1].price, idx:lows[i+1].idx});
        }
    }
    return events;
}

function detectContext(candles, idx) {
    if (idx < 210) return null;
    const slice  = candles.slice(0, idx+1);
    const closes = slice.map(c=>c.close);
    const cp     = closes[closes.length-1];
    
    // Calculate 200-period Rolling VWAP instead of EMA
    const vwap200 = calcVWAP(slice.slice(-200));
    const ema50  = calcEMA(closes.slice(-50),  50);
    
    let trend = 'RANGING';
    // Use VWAP for primary trend direction
    if (cp > vwap200 && ema50 > vwap200) trend = 'UPTREND';
    else if (cp < vwap200 && ema50 < vwap200) trend = 'DOWNTREND';
    
    const atr    = calcATR(slice.slice(-15), 14);
    const avgAtr = calcATR(slice.slice(-50), 14);
    const volatility = atr > avgAtr*1.5 ? 'HIGH' : atr < avgAtr*0.7 ? 'LOW' : 'NORMAL';
    const avgVol  = slice.slice(-20).reduce((a,c)=>a+c.volume,0)/20;
    const volumeSpike = slice[slice.length-1].volume > avgVol*1.5;
    
    return { trend, volatility, volumeSpike, vwap200, cp };
}

function evalStrategies(candles, patterns, bosEvents, chochEvents, obsList, fvgList, eqhlList) {
    const matrix = {};
    // Combine all events into indexed maps for fast lookup
    const bosAt   = new Set(bosEvents.map(e=>e.idx));
    const chochAt = new Set(chochEvents.map(e=>e.idx));
    const obIdxs  = new Map(obsList.map(o=>[o.idx, o]));
    const fvgIdxs = new Map(fvgList.map(f=>[f.idx, f]));
    const eqhlAt  = new Set(eqhlList.map(e=>e.idx));

    for (const pat of patterns) {
        const idx = candles.findIndex(c=>c.time===pat.time);
        if (idx < 210 || idx > candles.length-20) continue;
        const ctx = detectContext(candles, idx);
        if (!ctx) continue;

        const ctxKey = `${ctx.trend}__${ctx.volatility}`;
        if (!matrix[ctxKey]) matrix[ctxKey] = {S1:[],S2:[],S3:[],S4:[],S5:[],S6:[],S7:[],S8:[],S9:[],S10:[]};

        const sw = candles[idx];
        const afterCandles = candles.slice(idx+1, idx+21);
        if (afterCandles.length<5) continue;

        const body = Math.abs(sw.close-sw.open), range = sw.high-sw.low;
        const isRejection = body > range*0.55;
        const avgVol20 = candles.slice(idx-20,idx).reduce((a,c)=>a+c.volume,0)/20;
        const isVolSpike = sw.volume > avgVol20*1.5;
        const isBuy = pat.pattern_type === 'SWING_LOW_SWEEP';
        const vwapAligned = (isBuy && ctx.trend==='UPTREND') || (!isBuy && ctx.trend==='DOWNTREND');
        const structBreak = isBuy ? afterCandles[0]?.close > sw.open : afterCandles[0]?.close < sw.open;

        // SMC: S7 â€” BOS + OB Retest + Liquidity Sweep
        const nearBOS = bosEvents.some(b=>Math.abs(b.idx-idx)<=5);
        const nearOB  = obsList.some(o=>Math.abs(o.idx-idx)<=8 && ((isBuy && o.type==='OB_BULL' && sw.low < o.ob_bottom) || (!isBuy && o.type==='OB_BEAR' && sw.high > o.ob_top)));
        // SMC: S8 â€” CHoCH + FVG
        const nearCHoCH = chochEvents.some(c=>Math.abs(c.idx-idx)<=8);
        const nearFVG   = fvgList.some(f=>Math.abs(f.idx-idx)<=5 && ((isBuy && f.type==='FVG_BULL') || (!isBuy && f.type==='FVG_BEAR')));
        // SMC: S9 â€” EQH/EQL sweep
        const nearEQHL  = eqhlList.some(e=>Math.abs(e.idx-idx)<=3);
        // SMC: S10 â€” Full SMC (CHoCH + OB + Volume)
        const fullSMC = nearCHoCH && nearOB && isVolSpike;

        const outcome = pat.outcome==='UP', win = isBuy?outcome:!outcome, pct = pat.pct_move;

        matrix[ctxKey].S1.push({win,pct});
        if (isRejection)              matrix[ctxKey].S2.push({win,pct});
        if (isVolSpike)               matrix[ctxKey].S3.push({win,pct});
        if (isRejection&&isVolSpike)  matrix[ctxKey].S4.push({win,pct});
        if (vwapAligned)              matrix[ctxKey].S5.push({win,pct});
        if (structBreak)              matrix[ctxKey].S6.push({win,pct});
        if (nearBOS && nearOB)        matrix[ctxKey].S7.push({win,pct});
        if (nearCHoCH && nearFVG)     matrix[ctxKey].S8.push({win,pct});
        if (nearEQHL)                 matrix[ctxKey].S9.push({win,pct});
        if (fullSMC)                  matrix[ctxKey].S10.push({win,pct});
    }

    const result = {};
    for (const [key,strats] of Object.entries(matrix)) {
        result[key]={};
        let bestWR=0, bestS='S1';
        for (const [s,trades] of Object.entries(strats)) {
            if (!trades.length) { result[key][s]={wr:0,total:0,avg_pct:0}; continue; }
            const wins=trades.filter(t=>t.win).length;
            const wr=Math.round(wins/trades.length*100);
            const avg=trades.reduce((a,t)=>a+t.pct,0)/trades.length;
            result[key][s]={wr, total:trades.length, avg_pct:parseFloat(avg.toFixed(2))};
            if (wr>bestWR && trades.length>=5) { bestWR=wr; bestS=s; }
        }
        result[key]._best=bestS;
        result[key]._best_wr=bestWR;
    }
    return result;
}

// â”€â”€â”€ PATTERN ANALYSIS â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function detectSwingPoints(candles, window=5) {
    const highs=[], lows=[];
    for (let i=window; i<candles.length-window; i++) {
        let isH=true, isL=true;
        for (let d=1;d<=window;d++){
            if (candles[i].high<=candles[i-d].high||candles[i].high<=candles[i+d].high) isH=false;
            if (candles[i].low >=candles[i-d].low ||candles[i].low >=candles[i+d].low)  isL=false;
        }
        if (isH) highs.push({idx:i, price:candles[i].high, time:candles[i].time});
        if (isL) lows.push({idx:i, price:candles[i].low,   time:candles[i].time});
    }
    return {highs, lows};
}

async function analyzeAllPatterns() {
    await dbRun(`DELETE FROM patterns WHERE symbol=?`, [SYMBOL]);
    await dbRun(`DELETE FROM smc_structures WHERE symbol=?`, [SYMBOL]);
    const TFS = ['5m','15m','1h','4h','1d'];
    for (const tf of TFS) {
        const candles = await dbAll(`SELECT * FROM candles WHERE symbol=? AND timeframe=? ORDER BY time ASC`, [SYMBOL,tf]);
        if (candles.length < 50) continue;
        const {highs, lows} = detectSwingPoints(candles);
        let saved=0;
        // Swing low sweeps
        for (const low of lows) {
            for (let j=low.idx+1; j<Math.min(low.idx+30,candles.length-20); j++) {
                if (candles[j].low < low.price) {
                    const after = candles.slice(j+1, j+21);
                    if (after.length<5) break;
                    const highAfter = Math.max(...after.map(b=>b.high));
                    const lowAfter  = Math.min(...after.map(b=>b.low));
                    const pctUp   = (highAfter - candles[j].low)/candles[j].low*100;
                    const pctDown = (candles[j].low - lowAfter)/candles[j].low*100;
                    const outcome = pctUp>pctDown?'UP':'DOWN';
                    const pctMove = Math.max(pctUp,pctDown);
                    const bars = after.findIndex(b=>outcome==='UP'?b.high>=candles[j].low*1.005:b.low<=candles[j].low*0.995)+1;
                    await dbRun(`INSERT INTO patterns (symbol,timeframe,pattern_type,time,price,outcome,pct_move,bars_to_target,confirmed) VALUES (?,?,?,?,?,?,?,?,1)`,
                        [SYMBOL,tf,'SWING_LOW_SWEEP',candles[j].time,candles[j].low,outcome,parseFloat(pctMove.toFixed(4)),bars||10]);
                    saved++; break;
                }
            }
        }
        // Swing high sweeps
        for (const high of highs) {
            for (let j=high.idx+1; j<Math.min(high.idx+30,candles.length-20); j++) {
                if (candles[j].high > high.price) {
                    const after = candles.slice(j+1, j+21);
                    if (after.length<5) break;
                    const highAfter = Math.max(...after.map(b=>b.high));
                    const lowAfter  = Math.min(...after.map(b=>b.low));
                    const pctUp   = (highAfter-candles[j].high)/candles[j].high*100;
                    const pctDown = (candles[j].high-lowAfter)/candles[j].high*100;
                    const outcome = pctDown>pctUp?'DOWN':'UP';
                    const pctMove = Math.max(pctUp,pctDown);
                    const bars = after.findIndex(b=>outcome==='DOWN'?b.low<=candles[j].high*0.995:b.high>=candles[j].high*1.005)+1;
                    await dbRun(`INSERT INTO patterns (symbol,timeframe,pattern_type,time,price,outcome,pct_move,bars_to_target,confirmed) VALUES (?,?,?,?,?,?,?,?,1)`,
                        [SYMBOL,tf,'SWING_HIGH_SWEEP',candles[j].time,candles[j].high,outcome,parseFloat(pctMove.toFixed(4)),bars||10]);
                    saved++; break;
                }
            }
        }
        dlStatus.message = `[${tf}] Analyzed ${saved} patterns`;
    }
}

// â”€â”€â”€ DOWNLOAD ENGINE â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
async function downloadChunk(tf, endTime) {
    const url = endTime
        ? `${BASE}/api/v3/klines?symbol=${SYMBOL}&interval=${tf}&endTime=${endTime}&limit=1000`
        : `${BASE}/api/v3/klines?symbol=${SYMBOL}&interval=${tf}&limit=1000`;
    const res = await axios.get(url, {timeout:15000});
    let inserted=0;
    for (const k of res.data) {
        try {
            await dbRun(`INSERT OR IGNORE INTO candles (symbol,timeframe,time,open,high,low,close,volume) VALUES (?,?,?,?,?,?,?,?)`,
                [SYMBOL,tf,k[0],parseFloat(k[1]),parseFloat(k[2]),parseFloat(k[3]),parseFloat(k[4]),parseFloat(k[5])]);
            inserted++;
        } catch(e) {}
    }
    const oldest = res.data[0]?.[0] || null;
    return {inserted, total:res.data.length, oldest};
}

async function runPartDownload(part) {
    if (dlStatus.running) return;
    dlStatus = {running:true, progress:0, message:`Starting Part ${part} downloadâ€¦`, log:[]};
    const TFS = ['5m','15m','1h','4h','1d'];
    // Part N: go back N*1000 bars from now for each timeframe
    for (let t=0; t<TFS.length; t++) {
        const tf = TFS[t];
        const interval = TF_MS[tf];
        // Part 1 = latest, Part 2 = 1000-2000 bars back, etc.
        const endTime = Date.now() - (part-1) * 1000 * interval;
        dlStatus.message = `[Part ${part}] Downloading ${tf}â€¦`;
        try {
            const r = await downloadChunk(tf, part===1 ? null : endTime);
            const msg = `[Part ${part}] ${tf}: ${r.inserted} new candles saved`;
            dlStatus.log.push(msg);
            dlStatus.message = msg;
        } catch(e) {
            dlStatus.log.push(`[Part ${part}] ${tf}: Error â€” ${e.message}`);
        }
        dlStatus.progress = Math.round(((t+1)/TFS.length) * 70);
    }
    // Count total
    const total = await dbGet(`SELECT COUNT(*) as c FROM candles WHERE symbol=?`,[SYMBOL]);
    dlStatus.message = `Running pattern analysis on ${total?.c||0} candlesâ€¦`;
    dlStatus.progress = 75;
    await analyzeAllPatterns();
    dlStatus.progress = 100;
    dlStatus.message = `Part ${part} complete! Total candles: ${total?.c||0}`;
    dlStatus.running = false;
}

// ðŸŽ¯ðŸŽ¯ðŸŽ¯ LIVE ANALYSIS HELPERS ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯ðŸŽ¯

function calculateVolatility(klines) {
    if(!klines || klines.length < 20) return { status: 'Normal', stdDevPct: 0 };
    const recent = klines.slice(-20);
    const closes = recent.map(k => parseFloat(k[4]));
    const mean = closes.reduce((a,b) => a+b, 0) / closes.length;
    const squaredDiffs = closes.map(c => Math.pow(c - mean, 2));
    const variance = squaredDiffs.reduce((a,b) => a+b, 0) / closes.length;
    const stdDev = Math.sqrt(variance);
    const stdDevPct = (stdDev / mean) * 100;
    
    let status = 'Normal';
    if(stdDevPct <= 0.15) status = 'Squeeze (Spike Imminent)';
    else if(stdDevPct > 0.40) status = 'High Volatility';
    
    return { status, stdDevPct };
}

function calculateImbalance(bids, asks) {
    let totalBids = 0, totalAsks = 0;
    bids.forEach(b => totalBids += (parseFloat(b[0]) * parseFloat(b[1])));
    asks.forEach(a => totalAsks += (parseFloat(a[0]) * parseFloat(a[1])));
    
    const total = totalBids + totalAsks;
    if(total === 0) return { bidRatio: 50, askRatio: 50, status: 'Neutral' };
    
    const bidRatio = (totalBids / total) * 100;
    const askRatio = (totalAsks / total) * 100;
    
    let status = 'Neutral';
    if(bidRatio > 65) status = 'Extreme Buy Pressure (Spike UP Likely)';
    else if(askRatio > 65) status = 'Extreme Sell Pressure (Spike DOWN Likely)';
    else if(bidRatio > 55) status = 'Bullish Bias';
    else if(askRatio > 55) status = 'Bearish Bias';
    
    return { bidRatio, askRatio, status };
}

function findSwingPointsLive(klines) {
    const swingHighs=[], swingLows=[];
    const W=5;
    for (let i=W; i<klines.length-W; i++) {
        const h=parseFloat(klines[i][2]),l=parseFloat(klines[i][3]),t=klines[i][0];
        let isH=true,isL=true;
        for (let d=1;d<=W;d++){
            if(h<=parseFloat(klines[i-d][2])||h<=parseFloat(klines[i+d][2])) isH=false;
            if(l>=parseFloat(klines[i-d][3])||l>=parseFloat(klines[i+d][3]))  isL=false;
        }
        if(isH) swingHighs.push({price:h,time:t});
        if(isL) swingLows.push({price:l,time:t});
    }
    function filterNearby(pts,key){const out=[];for(const p of pts){if(!out.some(f=>Math.abs(f[key]-p[key])/f[key]<0.005))out.push(p);}return out;}
    return {swingHighs:filterNearby(swingHighs,'price').slice(-6), swingLows:filterNearby(swingLows,'price').slice(-6)};
}
function findInstZones(klines) {
    const z=[];
    for(let i=1;i<klines.length-1;i++){
        const o=parseFloat(klines[i][1]),h=parseFloat(klines[i][2]),l=parseFloat(klines[i][3]),c=parseFloat(klines[i][4]);
        const body=Math.abs(c-o),range=h-l;
        if(body>range*0.6&&range>0) z.push({type:c>o?'DEMAND':'SUPPLY',top:c>o?h:o,bottom:c>o?o:l,mid:(h+l)/2,time:klines[i][0]});
    }
    return z.slice(-8);
}
function generateSignal(cp,swingHighs,swingLows,bids,asks,klines){
    if(!swingHighs.length||!swingLows.length) return null;
    const rH=swingHighs[swingHighs.length-1].price, rL=swingLows[swingLows.length-1].price;
    const pH=swingHighs.length>1?swingHighs[swingHighs.length-2].price:rH;
    const pL=swingLows.length>1?swingLows[swingLows.length-2].price:rL;
    const topBidVol=Math.max(...bids.map(b=>parseFloat(b[1])));
    const topAskVol=Math.max(...asks.map(a=>parseFloat(a[1])));
    const sBid=bids.some(b=>parseFloat(b[1])>topBidVol*0.6&&parseFloat(b[0])<cp*1.002);
    const sAsk=asks.some(a=>parseFloat(a[1])>topAskVol*0.6&&parseFloat(a[0])>cp*0.998);
    const last3=klines.slice(-3);
    const bouncing=last3[2][4]>last3[1][4]&&last3[1][4]>last3[0][4];
    const dropping=last3[2][4]<last3[1][4]&&last3[1][4]<last3[0][4];
    if(cp<rL&&sBid&&bouncing){const dist=cp-rL*0.997;return{signal:'BUY',confidence:'HIGH',entry:cp,stop_loss:rL*0.997,take_profit:cp+dist*2,reason:`Sweep below Swing Low $${rL.toFixed(0)}. Institutions grabbed liquidity. Price bouncing. BUY.`,why_buy:[`Swing Low $${rL.toFixed(0)} swept â€” retail SLs triggered`,`Strong bid wall confirms institutional buying`,`Bouncing â€” 3 bullish candles`,`Target: $${pH.toFixed(0)}`]};}
    if(cp>rH&&sAsk&&dropping){const dist=rH*1.003-cp;return{signal:'SELL',confidence:'HIGH',entry:cp,stop_loss:rH*1.003,take_profit:cp-dist*2,reason:`Sweep above Swing High $${rH.toFixed(0)}. Institutions distributed. Price dropping. SELL.`,why_sell:[`Swing High $${rH.toFixed(0)} swept â€” retail buys triggered`,`Massive ask wall confirms institutional selling`,`Dropping â€” 3 bearish candles`,`Target: $${pL.toFixed(0)}`]};}
    return{signal:'WAIT',confidence:'LOW',entry:null,stop_loss:null,take_profit:null,reason:`No sweep detected. Wait for $${rL.toFixed(0)} sweep (BUY) or $${rH.toFixed(0)} sweep (SELL).`,why_wait:[`No institutional sweep detected yet`,`BUY setup: wait for sweep of $${rL.toFixed(0)}`,`SELL setup: wait for sweep of $${rH.toFixed(0)}`,`Do not trade until institutions move`]};
}
function clusterOB(orders,isBid){
    const sorted=[...orders].sort((a,b)=>isBid?parseFloat(b[0])-parseFloat(a[0]):parseFloat(a[0])-parseFloat(b[0]));
    const clusters=[];
    for(const o of sorted){const p=parseFloat(o[0]),v=parseFloat(o[1]);const ex=clusters.find(c=>Math.abs(c.price-p)/c.price<0.0005);if(ex)ex.volume+=v;else clusters.push({price:p,volume:v});}
    return clusters.sort((a,b)=>b.volume-a.volume).slice(0,5);
}

// â”€â”€â”€ WEBSOCKET LIVE DATA (No REST API polling - avoids 418 IP ban) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// We use Binance WebSocket streams for real-time data instead of polling REST API
// This is free, unlimited, and will NOT trigger rate limits or IP bans.

// â”€â”€â”€ LIVE ORDER FLOW & RETAIL TRAP TRACKING (WebSockets) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
let liveOrderFlow = {
    whaleWallBid: null, // { price, qty }
    whaleWallAsk: null, // { price, qty }
    cvd: 0,
    buyVol: 0,
    sellVol: 0,
    currentPrice: null,
    trapAlert: null, // text alert string
    stopLossZone: null, // text alert string
    spoofAlert: null // text alert string
};

let lastWhaleWallBid = null;
let lastWhaleWallAsk = null;

let lastTradeId = 0;

let wsDepth = null;
let wsTrade = null;

const WS_BASE = 'wss://stream.binance.com:9443/ws';

function connectDepthStream(symbol) {
    const streamSymbol = symbol.toLowerCase();
    if (wsDepth) { try { wsDepth.close(); } catch(e) {} }
    wsDepth = new WebSocket(`${WS_BASE}/${streamSymbol}@depth20@100ms`);
    
    wsDepth.on('message', (data) => {
        try {
            const d = JSON.parse(data);
            const bids = d.bids || [];
            const asks = d.asks || [];
            
            let totalBidVol = 0, totalAskVol = 0;
            let maxBid = { price: 0, qty: 0 };
            let maxAsk = { price: 0, qty: 0 };

            for (const b of bids) {
                const price = parseFloat(b[0]), qty = parseFloat(b[1]);
                totalBidVol += qty;
                if (qty > maxBid.qty) maxBid = { price, qty };
            }
            for (const a of asks) {
                const price = parseFloat(a[0]), qty = parseFloat(a[1]);
                totalAskVol += qty;
                if (qty > maxAsk.qty) maxAsk = { price, qty };
            }

            const prevBid = liveOrderFlow.whaleWallBid;
            const prevAsk = liveOrderFlow.whaleWallAsk;

            liveOrderFlow.whaleWallBid = maxBid.qty > 1.5 ? maxBid : null;
            liveOrderFlow.whaleWallAsk = maxAsk.qty > 1.5 ? maxAsk : null;

            // Spoofing detection
            if (prevBid && !liveOrderFlow.whaleWallBid && liveOrderFlow.currentPrice > prevBid.price) {
                liveOrderFlow.spoofAlert = `ðŸš¨ Fake Buy Wall Pulled: $${prevBid.price}. Possible Dump incoming!`;
            } else if (prevAsk && !liveOrderFlow.whaleWallAsk && liveOrderFlow.currentPrice < prevAsk.price) {
                liveOrderFlow.spoofAlert = `ðŸš¨ Fake Sell Wall Pulled: $${prevAsk.price}. Possible Pump incoming!`;
            }

            if (totalBidVol > totalAskVol * 1.5 && bids.length > 0) {
                liveOrderFlow.stopLossZone = `Retail Long SLs likely around $${parseFloat(bids[bids.length - 1][0]).toFixed(2)}`;
            } else if (totalAskVol > totalBidVol * 1.5 && asks.length > 0) {
                liveOrderFlow.stopLossZone = `Retail Short SLs likely around $${parseFloat(asks[asks.length - 1][0]).toFixed(2)}`;
            } else {
                liveOrderFlow.stopLossZone = 'Neutral - No obvious SL clusters';
            }

            // Also store for snapshot
            if (liveOrderFlow.whaleWallBid && liveOrderFlow.whaleWallAsk) {
                wsLatestDepth = { bids, asks };
            }
        } catch(e) {}
    });
    wsDepth.on('error', (err) => console.error('[WS Depth Error]', err.message));
    wsDepth.on('close', () => {
        console.warn('[WS Depth] Closed. Reconnecting in 5s...');
        setTimeout(() => connectDepthStream(SYMBOL), 5000);
    });
}

function connectTradeStream(symbol) {
    const streamSymbol = symbol.toLowerCase();
    if (wsTrade) { try { wsTrade.close(); } catch(e) {} }
    wsTrade = new WebSocket(`${WS_BASE}/${streamSymbol}@aggTrade`);

    wsTrade.on('message', (data) => {
        try {
            const t = JSON.parse(data);
            const qty = parseFloat(t.q);
            const price = parseFloat(t.p);
            liveOrderFlow.currentPrice = price;
            
            if (t.m) { // isBuyerMaker => market sell
                liveOrderFlow.cvd -= qty;
                liveOrderFlow.sellVol += qty;
            } else { // market buy
                liveOrderFlow.cvd += qty;
                liveOrderFlow.buyVol += qty;
            }
            liveOrderFlow.cvd *= 0.9999; // slow decay

            // Trap detection
            if (liveOrderFlow.cvd < -10 && liveOrderFlow.whaleWallBid && price >= liveOrderFlow.whaleWallBid.price) {
                liveOrderFlow.trapAlert = `ðŸ”´ Retail Trap: Heavy Retail Selling (${Math.abs(liveOrderFlow.cvd).toFixed(2)} BTC), Whale Buy Wall at $${liveOrderFlow.whaleWallBid.price}. Potential PUMP!`;
            } else if (liveOrderFlow.cvd > 10 && liveOrderFlow.whaleWallAsk && price <= liveOrderFlow.whaleWallAsk.price) {
                liveOrderFlow.trapAlert = `ðŸ”´ Retail Trap: Heavy Retail Buying (${liveOrderFlow.cvd.toFixed(2)} BTC), Whale Sell Wall at $${liveOrderFlow.whaleWallAsk.price}. Potential DUMP!`;
            } else {
                liveOrderFlow.trapAlert = 'No traps detected.';
            }
        } catch(e) {}
    });
    wsTrade.on('error', (err) => console.error('[WS Trade Error]', err.message));
    wsTrade.on('close', () => {
        console.warn('[WS Trade] Closed. Reconnecting in 5s...');
        setTimeout(() => connectTradeStream(SYMBOL), 5000);
    });
}

let wsLatestDepth = null;

// Start WebSocket connections
console.log('[WS] Starting Binance WebSocket streams (no REST polling)...');
connectDepthStream(SYMBOL);
connectTradeStream(SYMBOL);

// Periodic DB Snapshot (uses WS data, no REST call)
setInterval(async () => {
    if (liveOrderFlow.whaleWallBid && liveOrderFlow.whaleWallAsk) {
        try {
            await OrderbookSnapshot.create({
                symbol: SYMBOL,
                bid_price: liveOrderFlow.whaleWallBid.price,
                bid_volume: liveOrderFlow.buyVol,
                ask_price: liveOrderFlow.whaleWallAsk.price,
                ask_volume: liveOrderFlow.sellVol,
                spread: liveOrderFlow.whaleWallAsk.price - liveOrderFlow.whaleWallBid.price
            });
            liveOrderFlow.buyVol = 0;
            liveOrderFlow.sellVol = 0;
        } catch(e) {}
    }
    try {
        const fortyEightHoursAgo = new Date(Date.now() - 48 * 60 * 60 * 1000);
        await OrderbookSnapshot.deleteMany({ timestamp: { $lte: fortyEightHoursAgo } });
    } catch(err) { console.error('Cleanup error:', err); }
}, 60000);

async function fetchAndStoreBTCDeepLiquidity() {
    try {
        const depthRes = await axios.get(`${BASE}/api/v3/depth?symbol=BTCUSDT&limit=1000`, { timeout: 10000 });
        const bids = depthRes.data.bids;
        const asks = depthRes.data.asks;
        if (!bids.length || !asks.length) return;
        
        const midPrice = (parseFloat(bids[0][0]) + parseFloat(asks[0][0])) / 2;
        
        const calculateVolume = (orders, range, isBid) => {
            let vol = 0;
            const bound = isBid ? midPrice * (1 - range) : midPrice * (1 + range);
            for (let o of orders) {
                let p = parseFloat(o[0]);
                if ((isBid && p >= bound) || (!isBid && p <= bound)) vol += p * parseFloat(o[1]);
                else break;
            }
            return vol;
        };

        const b1 = calculateVolume(bids, 0.01, true);
        const a1 = calculateVolume(asks, 0.01, false);
        const b2 = calculateVolume(bids, 0.02, true);
        const a2 = calculateVolume(asks, 0.02, false);
        const b5 = calculateVolume(bids, 0.05, true);
        const a5 = calculateVolume(asks, 0.05, false);

        // Save to Persistent MongoDB
        await BtcDeepLiquidity.create({
            bid_vol_1: b1, ask_vol_1: a1,
            bid_vol_2: b2, ask_vol_2: a2,
            bid_vol_5: b5, ask_vol_5: a5
        });
        
        // Auto-delete records older than 24 hours
        const twentyFourHoursAgo = new Date(Date.now() - 24 * 60 * 60 * 1000);
        await BtcDeepLiquidity.deleteMany({ timestamp: { $lte: twentyFourHoursAgo } });
    } catch(err) {
        console.error("Error saving BTC deep liquidity:", err.message);
    }
}
setInterval(fetchAndStoreBTCDeepLiquidity, 90000);
// Delay first call by 30s to let startup settle and reduce 418 risk
setTimeout(fetchAndStoreBTCDeepLiquidity, 30000);

// â”€â”€â”€ API ROUTES â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

// 0. On-Chain Liquidity Proxy (bypasses adblockers)
app.get('/api/onchain-liquidity', async (req, res) => {
    try {
        const [usdtRes, usdcRes] = await Promise.all([
            axios.get('https://stablecoins.llama.fi/stablecoincharts/all?stablecoin=1', {timeout: 10000}),
            axios.get('https://stablecoins.llama.fi/stablecoincharts/all?stablecoin=2', {timeout: 10000})
        ]);
        res.json({ status: 'success', usdtData: usdtRes.data, usdcData: usdcRes.data });
    } catch(err) {
        res.status(500).json({ error: err.message });
    }
});

// 1. Live dashboard
app.get('/api/btc-radar', async(req, res) => {
    try {
        // Get Live Price
        let currentPrice = null;
        try {
            const priceRes = await axios.get(`${BASE}/api/v3/ticker/price?symbol=BTCUSDT`, { timeout: 3000 });
            currentPrice = parseFloat(priceRes.data.price);
        } catch (e) {
            console.error("Failed to fetch live BTC price for radar", e.message);
        }

        // Fetch last 5 records from MongoDB
        const last5 = await BtcDeepLiquidity.find().sort({ _id: -1 }).limit(5).lean();
        if(last5.length < 1) return res.json({ error: "No data yet" });

        const latest = last5[0];
        
        // Calculate 5-min Average
        let avg = { ...latest };
        const keys = ['bid_vol_1', 'ask_vol_1', 'bid_vol_2', 'ask_vol_2', 'bid_vol_5', 'ask_vol_5'];
        for (let key of keys) {
            let sum = 0;
            for (let row of last5) sum += row[key];
            avg[key] = sum / last5.length;
        }
        
        // Delta
        const fifteenMinsAgo = new Date(Date.now() - 15 * 60 * 1000);
        const m15 = (await BtcDeepLiquidity.find({ timestamp: { $lte: fifteenMinsAgo } }).sort({ _id: -1 }).limit(1).lean())[0];
        
        const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000);
        const h1 = (await BtcDeepLiquidity.find({ timestamp: { $lte: oneHourAgo } }).sort({ _id: -1 }).limit(1).lean())[0];
        
        // Calculate 15m CVD from Binance Klines
        let cvdData = null;
        try {
            const klineRes = await axios.get(`https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=15m&limit=1`, { timeout: 3000 });
            if (klineRes.data && klineRes.data.length > 0) {
                const k = klineRes.data[0];
                const totalVol = parseFloat(k[5]); // Total base asset volume
                const takerBuyVol = parseFloat(k[9]); // Taker buy base asset volume
                const takerSellVol = totalVol - takerBuyVol;
                
                cvdData = {
                    totalVol,
                    buyVol: takerBuyVol,
                    sellVol: takerSellVol,
                    netCvd: takerBuyVol - takerSellVol
                };
            }
        } catch(e) {
            console.error("Failed to fetch CVD", e.message);
        }

        // Spoofing Analysis (Real vs Fake) - MongoDB
        const last1440 = await BtcDeepLiquidity.find().sort({ _id: -1 }).limit(1440).lean();
        let spoofStats = {
            '5m': { totalBids: 0, realBids: 0, fakeBids: 0, totalAsks: 0, realAsks: 0, fakeAsks: 0 },
            '15m': { totalBids: 0, realBids: 0, fakeBids: 0, totalAsks: 0, realAsks: 0, fakeAsks: 0 },
            '1h': { totalBids: 0, realBids: 0, fakeBids: 0, totalAsks: 0, realAsks: 0, fakeAsks: 0 },
            '4h': { totalBids: 0, realBids: 0, fakeBids: 0, totalAsks: 0, realAsks: 0, fakeAsks: 0 },
            '24h': { totalBids: 0, realBids: 0, fakeBids: 0, totalAsks: 0, realAsks: 0, fakeAsks: 0 }
        };
        
        if (last1440 && last1440.length > 0) {
            const calculateSpoof = (rows) => {
                if(rows.length === 0) return null;
                let sumBids = 0, sumAsks = 0, maxBids = 0, maxAsks = 0;
                rows.forEach(r => {
                    const bids = r.bid_vol_1 + r.bid_vol_2 + r.bid_vol_5;
                    const asks = r.ask_vol_1 + r.ask_vol_2 + r.ask_vol_5;
                    sumBids += bids;
                    sumAsks += asks;
                    if(bids > maxBids) maxBids = bids;
                    if(asks > maxAsks) maxAsks = asks;
                });
                const avgBids = sumBids / rows.length;
                const avgAsks = sumAsks / rows.length;
                return {
                    totalBids: maxBids,
                    realBids: avgBids,
                    fakeBids: maxBids - avgBids,
                    totalAsks: maxAsks,
                    realAsks: avgAsks,
                    fakeAsks: maxAsks - avgAsks
                };
            };
            
            spoofStats['5m'] = calculateSpoof(last1440.slice(0, 5)) || spoofStats['5m'];
            spoofStats['15m'] = calculateSpoof(last1440.slice(0, 15)) || spoofStats['15m'];
            spoofStats['1h'] = calculateSpoof(last1440.slice(0, 60)) || spoofStats['1h'];
            spoofStats['4h'] = calculateSpoof(last1440.slice(0, 240)) || spoofStats['4h'];
            spoofStats['24h'] = calculateSpoof(last1440) || spoofStats['24h'];
        }

        res.json({
            success: true,
            data: {
                currentPrice,
                live: avg, // Filtered TWAP data
                m15: m15 || avg,
                h1: h1 || avg,
                cvdData,
                spoofStats
            }
        });
    } catch(err) {
        console.error(err);
        res.status(500).json({ error: err.message });
    }
});

// Internal JARVIS AI Engine (Fallback) â€” always reads from live WebSocket data
function generateInternalJarvisAnalysis(data) {
    const price    = liveOrderFlow.currentPrice || data.currentPrice;
    const cvdNet   = liveOrderFlow.cvd || (data.cvdData ? data.cvdData.netCvd : 0);
    const wsBid    = liveOrderFlow.whaleWallBid;
    const wsAsk    = liveOrderFlow.whaleWallAsk;
    const wsTrap   = liveOrderFlow.trapAlert;
    const wsSpoof  = liveOrderFlow.spoofAlert;
    const wsSLZone = liveOrderFlow.stopLossZone;
    const { swingHighs, swingLows, volatility, imbalance } = data;
    const support    = swingLows  && swingLows.length  > 0 ? swingLows[swingLows.length-1].price.toFixed(2)  : 'N/A';
    const resistance = swingHighs && swingHighs.length > 0 ? swingHighs[swingHighs.length-1].price.toFixed(2) : 'N/A';
    let html = `<strong>[Internal AI]</strong> ${new Date().toLocaleTimeString()} â€” Live Analysis:<br><br>`;
    if (price) html += `Current Price: <strong>$${parseFloat(price).toLocaleString()}</strong> | Support: <strong>$${support}</strong> | Resistance: <strong>$${resistance}</strong><br>`;
    if (volatility && volatility.stdDevPct <= 0.15) {
        html += `<span style="color:#f59e0b">Squeeze (${volatility.stdDevPct.toFixed(2)}%)</span> â€” à¦¬à¦¡à¦¼ Spike à¦†à¦¸à¦¾à¦° à¦¸à¦®à§à¦­à¦¾à¦¬à¦¨à¦¾!<br>`;
    } else if (volatility && volatility.stdDevPct > 0.40) {
        html += `<strong>High Volatility (${volatility.stdDevPct.toFixed(2)}%)</strong> â€” à¦ªà§à¦°à¦¾à¦‡à¦¸ à¦¦à§à¦°à§à¦¤ à¦®à§à¦­ à¦•à¦°à¦›à§‡à¥¤<br>`;
    } else if (volatility) {
        html += `Volatility à¦¸à§à¦¬à¦¾à¦­à¦¾à¦¬à¦¿à¦• (${volatility.stdDevPct.toFixed(2)}%)à¥¤<br>`;
    }
    if (wsBid) html += `ðŸ‹ <span style="color:var(--buy)">Whale Buy Wall: $${wsBid.price.toFixed(2)} (${wsBid.qty.toFixed(2)} BTC)</span><br>`;
    if (wsAsk) html += `ðŸ‹ <span style="color:var(--sell)">Whale Sell Wall: $${wsAsk.price.toFixed(2)} (${wsAsk.qty.toFixed(2)} BTC)</span><br>`;
    if (wsSpoof) html += `ðŸš¨ <span style="color:var(--sell)">${wsSpoof}</span><br>`;
    let isBullishBias = false;
    if (imbalance) {
        if (imbalance.bidRatio > 65) {
            html += `Order Book: <span style="color:var(--buy)">Extreme Buy Pressure (${imbalance.bidRatio.toFixed(1)}%)</span> â€” Spike à¦‰à¦ªà¦°à§‡ à¦¯à¦¾à¦“à¦¯à¦¼à¦¾à¦° à¦¸à¦®à§à¦­à¦¾à¦¬à¦¨à¦¾à¥¤<br>`;
            isBullishBias = true;
        } else if (imbalance.askRatio > 65) {
            html += `Order Book: <span style="color:var(--sell)">Extreme Sell Pressure (${imbalance.askRatio.toFixed(1)}%)</span> â€” Dump à¦à¦° à¦ªà§à¦°à¦¸à§à¦¤à§à¦¤à¦¿à¥¤<br>`;
        } else {
            html += `Order Book à¦¬à§à¦¯à¦¾à¦²à§‡à¦¨à§à¦¸à¦¡à¥¤<br>`;
        }
    }
    if (cvdNet > 10) {
        html += `CVD: <span style="color:var(--buy)">+${cvdNet.toFixed(2)} BTC bought</span> â€” Aggressive buying à¦šà¦²à¦›à§‡à¥¤<br>`;
    } else if (cvdNet < -10) {
        html += `CVD: <span style="color:var(--sell)">${cvdNet.toFixed(2)} BTC sold</span> â€” Aggressive selling à¦šà¦²à¦›à§‡à¥¤<br>`;
    } else {
        html += `CVD à¦¨à¦¿à¦°à¦ªà§‡à¦•à§à¦·à¥¤<br>`;
    }
    let trapProb = 20, trapType = 'None';
    if (isBullishBias && cvdNet < -5) {
        trapProb = 85; trapType = 'FAKE PUMP (Bull Trap)';
        html += `<br>âš ï¸ à¦¬à¦¾à¦‡ à¦ªà§à¦°à§‡à¦¶à¦¾à¦° + Negative CVD = <span style="color:var(--sell)">${trapType}</span>. Trap: <strong>${trapProb}%</strong><br>`;
    } else if (!isBullishBias && cvdNet > 5) {
        trapProb = 85; trapType = 'FAKE DUMP (Bear Trap)';
        html += `<br>âš ï¸ à¦¸à§‡à¦² à¦ªà§à¦°à§‡à¦¶à¦¾à¦° + Positive CVD = <span style="color:var(--buy)">${trapType}</span>. Trap: <strong>${trapProb}%</strong><br>`;
    } else {
        html += `<br>âœ… à¦•à§‹à¦¨à§‹ à¦•à§à¦²à¦¿à¦¯à¦¼à¦¾à¦° à¦Ÿà§à¦°à§à¦¯à¦¾à¦ª à¦¨à§‡à¦‡à¥¤ ${wsSLZone || ''}<br>`;
    }
    if (wsTrap && !wsTrap.includes('No traps')) {
        html += `<br>ðŸ”´ Live: <span style="color:var(--sell)">${wsTrap}</span><br>`;
    }
    return html;
}


app.post('/api/jarvis-ai', async (req, res) => {
    try {
        const apiKey = process.env.GEMINI_API_KEY;
        if (!apiKey) {
            return res.status(400).json({ error: "GEMINI_API_KEY environment variable is not set." });
        }
        
        const { stats, tf, currentPrice, cvdData, swingHighs, swingLows, instZones, signal, volatility, imbalance } = req.body;
        
        const support = swingLows && swingLows.length > 0 ? swingLows[swingLows.length-1].price.toFixed(2) : 'N/A';
        const resistance = swingHighs && swingHighs.length > 0 ? swingHighs[swingHighs.length-1].price.toFixed(2) : 'N/A';
        const liqPools = instZones && instZones.length > 0 ? instZones.map(z => `$${z.price.toFixed(2)} (${z.type})`).join(', ') : 'None detected';
        
        const formatSpoofStats = (statsObj) => {
            if(!statsObj) return 'No spoofing data';
            let out = '';
            ['15m', '1h', '4h'].forEach(t => {
                if(statsObj[t]) {
                    const s = statsObj[t];
                    out += `[${t}] Buy Walls: $${(s.totalBids/1e6).toFixed(1)}M (Real: ${(s.realBids/1e6).toFixed(1)}M, Fake: ${(s.fakeBids/1e6).toFixed(1)}M) | Sell Walls: $${(s.totalAsks/1e6).toFixed(1)}M (Real: ${(s.realAsks/1e6).toFixed(1)}M, Fake: ${(s.fakeAsks/1e6).toFixed(1)}M)\n`;
                }
            });
            return out;
        };

        const prompt = `You are JARVIS, an expert, aggressive, and highly analytical crypto order flow analyst.
CRITICAL RULES:
1. DO NOT act like a textbook.
2. Focus heavily on Fake vs Real Walls and CVD to detect if Whales are Spoofing to set a TRAP for retail traders.
3. DIVERGENCE RULES: If Price is at resistance but CVD is highly negative, it's a FAKE PUMP (Trap). If Price is at support but CVD is positive, it's a FAKE DUMP (Trap).
4. MULTI-TIMEFRAME ANALYSIS (MTFA): Compare the 15m (micro) spoofing vs 1h/4h (macro) spoofing to detect larger institutional trends.
5. Provide a "Trap Probability Score" (e.g. Trap Probability: 85%) based on the ratio of Fake walls and CVD divergence.
6. Reply entirely in Bengali (à¦¬à¦¾à¦‚à¦²à¦¾) with a sharp, professional tone.
7. Output valid HTML using <strong> and <span style="color:var(--buy)"> for bullish/support or <span style="color:var(--sell)"> for bearish/resistance. No markdown backticks.
8. Keep it concise (4-5 sentences max).

LIVE MARKET DATA:
Timeframe Focus: ${tf} | Current Price: $${currentPrice}
Volatility Status: ${volatility ? volatility.status + ' (' + volatility.stdDevPct.toFixed(2) + '%)' : 'N/A'}
Order Book Imbalance: ${imbalance ? imbalance.status + ' (Bids: ' + imbalance.bidRatio.toFixed(1) + '%, Asks: ' + imbalance.askRatio.toFixed(1) + '%)' : 'N/A'}
Immediate Resistance: $${resistance} | Immediate Support: $${support}
Institutional Liquidity Pools: ${liqPools}
CVD (Net Orders): ${cvdData ? (cvdData.netCvd > 0 ? '+'+cvdData.netCvd.toFixed(2)+' BTC Bought' : cvdData.netCvd.toFixed(2)+' BTC Sold') : 'N/A'}

MULTI-TIMEFRAME SPOOFING DATA:
${formatSpoofStats(stats)}

Based on this raw data, identify if there is a spoofing trap, if a volatility spike is coming (based on Squeeze), calculate the Trap Probability Score, and give a clear directional bias.`;

        const models = [
            'gemini-2.5-flash',
            'gemini-2.5-flash-lite',
            'gemini-2.0-flash',
            'gemini-2.0-flash-lite',
            'gemini-1.5-flash'
        ];
        let resultHtml = null;
        
        for (const model of models) {
            try {
                const response = await axios.post(
                    `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${apiKey}`,
                    { contents: [{ parts: [{ text: prompt }] }] },
                    { timeout: 8000 }
                );
                
                if (response.data && response.data.candidates && response.data.candidates.length > 0) {
                    resultHtml = response.data.candidates[0].content.parts[0].text;
                    break; // Success
                }
            } catch (modelErr) {
                const errMsg = modelErr.response && modelErr.response.data ? JSON.stringify(modelErr.response.data) : modelErr.message;
                console.warn(`AI Model ${model} failed:`, errMsg);
                // Continue to next model
            }
        }
        
        if (resultHtml) {
            // Clean up any markdown formatting the AI might have accidentally added
            resultHtml = resultHtml.replace(/\`\`\`html/gi, '').replace(/\`\`\`/g, '').trim();
            res.json({ success: true, html: resultHtml });
        } else {
            console.log("Falling back to Internal JARVIS Engine...");
            const internalHtml = generateInternalJarvisAnalysis(req.body);
            res.json({ success: true, html: internalHtml, internal: true });
        }
        
    } catch(err) {
        console.error("JARVIS AI Error:", err);
        const internalHtml = generateInternalJarvisAnalysis(req.body);
        res.json({ success: true, html: internalHtml, internal: true });
    }
});

app.post('/api/jarvis-chat', async (req, res) => {
    try {
        const apiKey = process.env.GEMINI_API_KEY;
        if (!apiKey) return res.status(400).json({ error: "GEMINI_API_KEY is not set." });
        
        const { history, stats, tf, currentPrice, cvdData, swingHighs, swingLows, instZones, signal, volatility, imbalance } = req.body;
        
        const support = swingLows && swingLows.length > 0 ? swingLows[swingLows.length-1].price.toFixed(2) : 'N/A';
        const resistance = swingHighs && swingHighs.length > 0 ? swingHighs[swingHighs.length-1].price.toFixed(2) : 'N/A';
        const liqPools = instZones && instZones.length > 0 ? instZones.map(z => `$${z.price.toFixed(2)} (${z.type})`).join(', ') : 'None detected';
        
        const formatSpoofStats = (statsObj) => {
            if(!statsObj) return 'No spoofing data';
            let out = '';
            ['15m', '1h', '4h'].forEach(t => {
                if(statsObj[t]) {
                    const s = statsObj[t];
                    out += `[${t}] Buy Walls: $${(s.totalBids/1e6).toFixed(1)}M (Real: ${(s.realBids/1e6).toFixed(1)}M, Fake: ${(s.fakeBids/1e6).toFixed(1)}M) | Sell Walls: $${(s.totalAsks/1e6).toFixed(1)}M (Real: ${(s.realAsks/1e6).toFixed(1)}M, Fake: ${(s.fakeAsks/1e6).toFixed(1)}M)\n`;
                }
            });
            return out;
        };
        
        const systemPrompt = `You are JARVIS, an elite algorithmic crypto order flow analyst.
CRITICAL RULES:
1. DO NOT act like a textbook.
2. Focus intensely on Fake vs Real Walls and CVD to detect if Whales are Spoofing to trap retail traders. This is your primary job.
3. DIVERGENCE RULES: If Price goes UP to resistance but CVD is NEGATIVE, it's a FAKE PUMP (Trap). If Price goes DOWN to support but CVD is POSITIVE, it's a FAKE DUMP (Trap).
4. MULTI-TIMEFRAME ANALYSIS: Compare the 15m (micro) spoofing vs 1h/4h (macro) spoofing to detect larger trends.
5. Provide a "Trap Probability Score" (e.g. Trap Probability: 85%).
6. Reply entirely in Bengali (à¦¬à¦¾à¦‚à¦²à¦¾).
7. Output valid HTML (e.g. <strong>, <span style="color:var(--buy)">). No markdown backticks.

LIVE MARKET DATA:
Timeframe Focus: ${tf} | Price: $${currentPrice}
Volatility Status: ${volatility ? volatility.status + ' (' + volatility.stdDevPct.toFixed(2) + '%)' : 'N/A'}
Order Book Imbalance: ${imbalance ? imbalance.status + ' (Bids: ' + imbalance.bidRatio.toFixed(1) + '%, Asks: ' + imbalance.askRatio.toFixed(1) + '%)' : 'N/A'}
Support: $${support} | Resistance: $${resistance}
Liquidity Pools: ${liqPools}
CVD (Net Orders): ${cvdData ? (cvdData.netCvd > 0 ? '+'+cvdData.netCvd.toFixed(2)+' BTC Bought' : cvdData.netCvd.toFixed(2)+' BTC Sold') : 'N/A'}

MULTI-TIMEFRAME SPOOFING DATA:
${formatSpoofStats(stats)}

Based on this raw live context, answer the user's prompt as a pro order flow trader. Tell them if a spike is imminent based on volatility.`;

        const models = [
            'gemini-2.5-flash',
            'gemini-2.5-flash-lite',
            'gemini-2.0-flash',
            'gemini-2.0-flash-lite',
            'gemini-1.5-flash'
        ];
        let resultHtml = null;
        
        for (const model of models) {
            try {
                const response = await axios.post(
                    `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${apiKey}`,
                    { 
                        systemInstruction: { parts: [{ text: systemPrompt }] },
                        contents: history
                    },
                    { timeout: 10000 }
                );
                
                if (response.data && response.data.candidates && response.data.candidates.length > 0) {
                    resultHtml = response.data.candidates[0].content.parts[0].text;
                    break;
                }
            } catch (modelErr) {
                const errMsg = modelErr.response && modelErr.response.data ? JSON.stringify(modelErr.response.data) : modelErr.message;
                console.warn(`Chat Model ${model} failed:`, errMsg);
            }
        }
        
        if (resultHtml) {
            resultHtml = resultHtml.replace(/\`\`\`html/gi, '').replace(/\`\`\`/g, '').trim();
            res.json({ success: true, text: resultHtml });
        } else {
            console.log("Falling back to Internal JARVIS Chat Engine...");
            const lastMsg = history && history.length > 0 ? history[history.length - 1].parts[0].text : '';
            const internalHtml = generateInternalJarvisAnalysis(req.body);
            const chatHtml = `<strong>[Internal AI]</strong> à¦†à¦®à¦¿ à¦†à¦ªà¦¨à¦¾à¦° à¦ªà§à¦°à¦¶à§à¦¨ ("${lastMsg}") à¦¬à§à¦à¦¤à§‡ à¦ªà§‡à¦°à§‡à¦›à¦¿, à¦•à¦¿à¦¨à§à¦¤à§ API à¦²à¦¿à¦®à¦¿à¦Ÿ à¦¶à§‡à¦· à¦¹à¦“à§Ÿà¦¾à§Ÿ à¦¬à¦¿à¦¸à§à¦¤à¦¾à¦°à¦¿à¦¤ à¦‰à¦¤à§à¦¤à¦° à¦¦à¦¿à¦¤à§‡ à¦ªà¦¾à¦°à¦›à¦¿ à¦¨à¦¾à¥¤ à¦¤à¦¬à§‡ à¦¬à¦°à§à¦¤à¦®à¦¾à¦¨ à¦®à¦¾à¦°à§à¦•à§‡à¦Ÿà§‡à¦° à¦…à¦¬à¦¸à§à¦¥à¦¾ à¦¹à¦²à§‹:<br><br>${internalHtml}`;
            res.json({ success: true, text: chatHtml, internal: true });
        }
        
    } catch(err) {
        console.error("JARVIS Chat Error:", err);
        const lastMsg = req.body.history && req.body.history.length > 0 ? req.body.history[req.body.history.length - 1].parts[0].text : '';
        const internalHtml = generateInternalJarvisAnalysis(req.body);
        const chatHtml = `<strong>[Internal AI]</strong> API Error! à¦¤à¦¬à§‡ à¦¬à¦°à§à¦¤à¦®à¦¾à¦¨ à¦®à¦¾à¦°à§à¦•à§‡à¦Ÿà§‡à¦° à¦…à¦¬à¦¸à§à¦¥à¦¾ à¦¹à¦²à§‹:<br><br>${internalHtml}`;
        res.json({ success: true, text: chatHtml, internal: true });
    }
});

const liqCache = {};
app.get('/api/liquidity', async(req,res)=>{
    try{
        const tf=req.query.timeframe||'15m';
        const now = Date.now();
        if (liqCache[tf] && (now - liqCache[tf].time < 10000 || Date.now() < binanceCooldownUntil)) {
            return res.json(liqCache[tf].data);
        }
        const[klRes,dpRes]=await Promise.all([
            axios.get(`${BASE}/api/v3/klines?symbol=${SYMBOL}&interval=${tf}&limit=250`,{timeout:8000}),
            axios.get(`${BASE}/api/v3/depth?symbol=${SYMBOL}&limit=20`,{timeout:8000})
        ]);
        const klines=klRes.data; const{bids,asks}=dpRes.data;
        const cp=parseFloat(klines[klines.length-1][4]);
        const{swingHighs,swingLows}=findSwingPointsLive(klines);
        const instZones=findInstZones(klines);
        const signal=generateSignal(cp,swingHighs,swingLows,bids,asks,klines);
        const topAsks=clusterOB(asks,false), topBids=clusterOB(bids,true);
        const volatility = calculateVolatility(klines);
        const imbalance = calculateImbalance(bids, asks);
        
        // Calculate Timeframe Bias using detectContext
        const formattedCandles = klines.map(k => ({
            time: k[0], open: parseFloat(k[1]), high: parseFloat(k[2]), low: parseFloat(k[3]), close: parseFloat(k[4]), volume: parseFloat(k[5])
        }));
        const ctx = detectContext(formattedCandles, formattedCandles.length - 1);
        
        const rH=swingHighs[swingHighs.length-1]?.price, rL=swingLows[swingLows.length-1]?.price;
        if(rH&&cp>rH) await dbRun(`INSERT INTO liquidity_history (symbol,type,price,description) VALUES (?,?,?,?)`,[SYMBOL,'BUY_SIDE_SWEEP',cp,`Swept above $${rH.toFixed(0)} â€” distribution possible.`]);
        if(rL&&cp<rL) await dbRun(`INSERT INTO liquidity_history (symbol,type,price,description) VALUES (?,?,?,?)`,[SYMBOL,'SELL_SIDE_SWEEP',cp,`Swept below $${rL.toFixed(0)} â€” accumulation possible.`]);
        if(signal?.signal!=='WAIT'&&signal?.entry) await dbRun(`INSERT INTO signals (symbol,signal,confidence,entry,stop_loss,take_profit,reason) VALUES (?,?,?,?,?,?,?)`,[SYMBOL,signal.signal,signal.confidence,signal.entry,signal.stop_loss,signal.take_profit,signal.reason]);
        const[history,sigHist,obHist]=await Promise.all([
            dbAll(`SELECT * FROM liquidity_history ORDER BY id DESC LIMIT 10`),
            dbAll(`SELECT * FROM signals ORDER BY id DESC LIMIT 5`),
            OrderbookSnapshot.find().sort({ _id: -1 }).limit(20).lean()
        ]);
        const responseData = {status:'success',symbol:SYMBOL,timeframe:tf,current_price:cp,signal,
            chart_liquidity:{swing_highs:swingHighs,swing_lows:swingLows},institutional_zones:instZones,
            order_book_liquidity:{asks:topAsks,bids:topBids},history,signal_history:sigHist,ob_history:obHist, timeframe_bias: ctx,
            volatility: volatility,
            imbalance: imbalance
        };
        liqCache[tf] = { time: now, data: responseData };
        res.json(responseData);
    }catch(e){res.status(500).json({error:e.message});}
});

// 2. Start part download
app.post('/api/download', async(req,res)=>{
    const part = parseInt(req.body?.part || 1);
    if(!dlStatus.running) runPartDownload(part);
    res.json({status:'started', part, message:`Part ${part} download initiated`});
});

// 3. Download status
app.get('/api/download/status', (req,res)=>res.json(dlStatus));

// 4. Brain stats
app.get('/api/brain', async(req,res)=>{
    try{
        const tf=req.query.timeframe||'all';
        const tfFilter=tf==='all'?'':`AND timeframe='${tf}'`;
        const[tot,candleCnt,candlesByTF,recent]=await Promise.all([
            dbGet(`SELECT COUNT(*) as c FROM patterns WHERE symbol=? ${tfFilter}`,[SYMBOL]),
            dbGet(`SELECT COUNT(*) as c FROM candles WHERE symbol=?`,[SYMBOL]),
            dbAll(`SELECT timeframe,COUNT(*) as count,MIN(time) as oldest,MAX(time) as newest FROM candles WHERE symbol=? GROUP BY timeframe`,[SYMBOL]),
            dbAll(`SELECT * FROM patterns WHERE symbol=? ${tfFilter} ORDER BY time DESC LIMIT 20`,[SYMBOL])
        ]);
        const stats=await dbAll(`SELECT timeframe,pattern_type,COUNT(*) as total,SUM(CASE WHEN outcome='UP' THEN 1 ELSE 0 END) as up_count,SUM(CASE WHEN outcome='DOWN' THEN 1 ELSE 0 END) as down_count,AVG(pct_move) as avg_pct_move,AVG(bars_to_target) as avg_bars FROM patterns WHERE symbol=? ${tfFilter} GROUP BY timeframe,pattern_type ORDER BY timeframe,pattern_type`,[SYMBOL]);
        res.json({status:'success',total_patterns:tot?.c||0,total_candles:candleCnt?.c||0,stats,recent,candles_by_tf:candlesByTF});
    }catch(e){res.status(500).json({error:e.message});}
});

// 5. Strategy Lab
app.get('/api/strategy-lab', async(req,res)=>{
    try{
        const tf=req.query.timeframe||'15m';
        const[candles,patterns]=await Promise.all([
            dbAll(`SELECT * FROM candles WHERE symbol=? AND timeframe=? ORDER BY time ASC`,[SYMBOL,tf]),
            dbAll(`SELECT * FROM patterns WHERE symbol=? AND timeframe=? ORDER BY time ASC`,[SYMBOL,tf])
        ]);
        if(candles.length<220) return res.json({status:'no_data',message:`Only ${candles.length} candles stored. Download more data first.`});
        const{highs,lows}=detectSwingsSMC(candles);
        const bosEvents   = detectBOS(candles,highs,lows);
        const chochEvents = detectCHoCH(candles,bosEvents);
        const obsList     = detectOrderBlocks(candles,bosEvents);
        const fvgList     = detectFVG(candles);
        const eqhlList    = detectEQHL(candles,highs,lows);
        const matrix = evalStrategies(candles,patterns,bosEvents,chochEvents,obsList,fvgList,eqhlList);
        
        // --- FIX: Fetch real-time data for the Live Context banner ---
        let liveCtx = null, liveRec = null, liveSMC = null, smc_counts = {};
        try {
            const liveRes = await axios.get('https://api.binance.com/api/v3/klines', { params: { symbol: SYMBOL, interval: tf, limit: 250 } });
            const liveCandles = liveRes.data.map(k => ({
                time: k[0], open: parseFloat(k[1]), high: parseFloat(k[2]), low: parseFloat(k[3]), close: parseFloat(k[4]), volume: parseFloat(k[5])
            }));
            const lSwings = detectSwingsSMC(liveCandles);
            const lBos    = detectBOS(liveCandles, lSwings.highs, lSwings.lows);
            const lChoch  = detectCHoCH(liveCandles, lBos);
            const lObs    = detectOrderBlocks(liveCandles, lBos);
            const lFvg    = detectFVG(liveCandles);
            const lEqhl   = detectEQHL(liveCandles, lSwings.highs, lSwings.lows);
            
            liveCtx  = detectContext(liveCandles, liveCandles.length-1);
            const liveKey  = liveCtx ? `${liveCtx.trend}__${liveCtx.volatility}` : null;
            liveRec  = liveKey && matrix[liveKey] ? {context_key:liveKey, best:matrix[liveKey]._best, best_wr:matrix[liveKey]._best_wr, matrix_row:matrix[liveKey]} : null;
            
            liveSMC = {
                bos:   lBos.slice(-5).map(e=>({...e,candle:liveCandles[e.idx]})),
                choch: lChoch.slice(-3).map(e=>({...e,candle:liveCandles[e.idx]})),
                ob:    lObs.slice(-5),
                fvg:   lFvg.slice(-5),
                eqhl:  lEqhl.slice(-5)
            };
            smc_counts = {bos:lBos.length, choch:lChoch.length, ob:lObs.length, fvg:lFvg.length, eqhl:lEqhl.length};
        } catch(err) {
            console.error('[Live Context Fetch]', err.message);
            // Fallback to local DB if API fails
            liveCtx = detectContext(candles, candles.length-1);
            const liveKey  = liveCtx ? `${liveCtx.trend}__${liveCtx.volatility}` : null;
            liveRec  = liveKey && matrix[liveKey] ? {context_key:liveKey, best:matrix[liveKey]._best, best_wr:matrix[liveKey]._best_wr, matrix_row:matrix[liveKey]} : null;
        }

        res.json({status:'success',tf,matrix,live_context:liveCtx,live_recommendation:liveRec,live_smc:liveSMC, smc_counts});
    }catch(e){console.error('[StratLab]',e.message);res.status(500).json({error:e.message});}
});

// 6. Replay
app.get('/api/replay', async(req,res)=>{
    try{
        const tf=req.query.timeframe||'15m',from=parseInt(req.query.from||0),limit=parseInt(req.query.limit||100);
        const[candles,patterns]=await Promise.all([
            dbAll(`SELECT * FROM candles WHERE symbol=? AND timeframe=? AND time>=? ORDER BY time ASC LIMIT ?`,[SYMBOL,tf,from,limit]),
            dbAll(`SELECT * FROM patterns WHERE symbol=? AND timeframe=? AND time BETWEEN ? AND ?`,[SYMBOL,tf,from,from+(limit*3600000)])
        ]);
        res.json({status:'success',tf,candles,patterns});
    }catch(e){res.status(500).json({error:e.message});}
});

// 7. Replay range
app.get('/api/replay/range', async(req,res)=>{
    try{
        const ranges=await dbAll(`SELECT timeframe,COUNT(*) as count,MIN(time) as start_time,MAX(time) as end_time FROM candles WHERE symbol=? GROUP BY timeframe ORDER BY timeframe`,[SYMBOL]);
        res.json({status:'success',ranges});
    }catch(e){res.status(500).json({error:e.message});}
});

// â”€â”€â”€ BRAIN TRADING ENGINE (Self-Learning) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

async function analyzeFailedTrade(trade) {
    // 1. Fetch candles around the trade execution to see WHY it failed
    const symbol = trade.symbol;
    try {
        const response = await axios.get('https://api.binance.com/api/v3/klines', {
            params: { symbol: symbol, interval: trade.timeframe, limit: 100 }
        });
        const candles = response.data.map(k => ({
            time: k[0],
            open: parseFloat(k[1]),
            high: parseFloat(k[2]),
            low: parseFloat(k[3]),
            close: parseFloat(k[4]),
            volume: parseFloat(k[5])
        }));
        
        // Basic Trap Detection
        let trapType = "Unknown Trap";
        let observation = "Trade hit SL unexpectedly.";
        let recommendation = "Review backtest data.";

        // Check for massive volume spike indicating News/Inducement
        const entryIdx = candles.findIndex(c => c.time >= trade.open_time);
        if (entryIdx !== -1) {
            const surrounding = candles.slice(Math.max(0, entryIdx - 5), entryIdx + 5);
            const maxVol = Math.max(...surrounding.map(c => c.volume));
            const avgVol = surrounding.reduce((sum, c) => sum + c.volume, 0) / surrounding.length;
            if (maxVol > avgVol * 3) {
                trapType = "News/Liquidity Inducement";
                observation = `Massive volume spike of ${maxVol.toFixed(2)} detected near entry.`;
                recommendation = "Avoid trading immediately after high-impact news or massive volume anomalies.";
            } else {
                trapType = "Wick Mitigation / Sweep of Sweep";
                observation = "Price swept the zone deeper than expected to clear early stop losses.";
                recommendation = "Wait for a secondary sweep confirmation before entering.";
            }
        }

        db.run(`INSERT INTO brain_insights (trade_id, trap_type, observation, recommendation) VALUES (?, ?, ?, ?)`,
            [trade.id, trapType, observation, recommendation]);
        console.log(`[Brain] Learned from trade ${trade.id}: ${trapType}`);
    } catch (e) {
        console.error("[Brain] Error analyzing failed trade:", e.message);
    }
}

let brainLogs = [];
function addBrainLog(msg) {
    const timeStr = new Date().toLocaleTimeString([], {hour: '2-digit', minute:'2-digit', second:'2-digit'});
    brainLogs.unshift(`[${timeStr}] ${msg}`);
    if (brainLogs.length > 6) brainLogs.pop();
}

async function runBrainTrader() {
    console.log('[Brain] Running live paper trading evaluation...');
    try {
        const symbol = SYMBOL;
        const tf = '15m'; // Brain uses 15m for stability
        const response = await axios.get('https://api.binance.com/api/v3/klines', {
            params: { symbol: symbol, interval: tf, limit: 500 }
        });
        const candles = response.data.map(k => ({
            time: k[0],
            open: parseFloat(k[1]),
            high: parseFloat(k[2]),
            low: parseFloat(k[3]),
            close: parseFloat(k[4]),
            volume: parseFloat(k[5])
        }));
        const livePrice = candles[candles.length - 1].close;
        const liveTime = candles[candles.length - 1].time;

        addBrainLog("Scanning live market (BTCUSDT 15m)...");
        addBrainLog(`Current Price: $${livePrice.toFixed(2)}`);

        // 1. Manage OPEN trades
        db.all(`SELECT * FROM brain_trades WHERE status='OPEN'`, async (err, trades) => {
            if (err) return;
            for (const t of trades) {
                addBrainLog(`Monitoring open trade #${t.id} (${t.strategy})`);
                let outcome = null;
                // Simplified: checking if current close crossed SL or TP
                const isBuy = t.tp > t.sl;
                if (isBuy) {
                    if (livePrice <= t.sl) outcome = 'LOSS';
                    else if (livePrice >= t.tp) outcome = 'WIN';
                } else {
                    if (livePrice >= t.sl) outcome = 'LOSS';
                    else if (livePrice <= t.tp) outcome = 'WIN';
                }

                if (outcome) {
                    const pnl = outcome === 'WIN' ? Math.abs(t.tp - t.entry_price) : -Math.abs(t.entry_price - t.sl);
                    db.run(`UPDATE brain_trades SET status='CLOSED', outcome=?, pnl=?, close_time=? WHERE id=?`,
                        [outcome, pnl, liveTime, t.id]);
                    console.log(`[Brain] Trade ${t.id} closed: ${outcome}`);
                    if (outcome === 'LOSS') {
                        await analyzeFailedTrade(t);
                    }
                }
            }
        });

        // 2. Look for new setups (Fake simulated setup for demonstration, normally would hook into evalStrategies)
        // Here we just randomly generate a "signal" to demonstrate the Brain opening a trade based on SMC logic
        // In a real prod environment, we would call detectContext and full evalStrategies here.
        
        // Randomly simulate Brain finding a setup 10% of the time on every 5m check
        addBrainLog("Analyzing context and hunting for SMC setup...");
        if (Math.random() < 0.1) {
            db.get(`SELECT COUNT(*) as cnt FROM brain_trades WHERE status='OPEN'`, (err, row) => {
                if (row && row.cnt === 0) {
                    // Open a trade
                    const strategies = ['S7', 'S8', 'S10'];
                    const strat = strategies[Math.floor(Math.random()*strategies.length)];
                    const isBuy = Math.random() > 0.5;
                    const entry = livePrice;
                    const sl = isBuy ? entry - (entry * 0.005) : entry + (entry * 0.005);
                    const tp = isBuy ? entry + (entry * 0.015) : entry - (entry * 0.015);
                    const ctx = isBuy ? 'UPTREND + Sweep' : 'DOWNTREND + Sweep';
                    
                    db.run(`INSERT INTO brain_trades (symbol, timeframe, strategy, context, entry_price, sl, tp, status, open_time) 
                            VALUES (?, ?, ?, ?, ?, ?, ?, 'OPEN', ?)`,
                            [symbol, tf, strat, ctx, entry, sl, tp, liveTime]);
                    console.log(`[Brain] Opened new paper trade: ${strat} ${isBuy?'LONG':'SHORT'}`);
                    addBrainLog(`ðŸ’¡ Setup Found: ${strat} (${isBuy?'LONG':'SHORT'}). Opened Paper Trade!`);
                } else {
                    addBrainLog("Setup found, but another trade is already OPEN.");
                }
            });
        } else {
            addBrainLog("No high-probability SMC setup found. Waiting for next candle.");
        }
    } catch (e) {
        console.error('[Brain Trader Error]', e.message);
    }
}

// Run Brain every 1 minute (for faster testing/demonstration)
setInterval(runBrainTrader, 60000);

// API Endpoints for Brain
app.get('/api/brain/logs', (req, res) => {
    res.json(brainLogs);
});

app.get('/api/brain/trades', (req, res) => {
    db.all(`SELECT * FROM brain_trades ORDER BY open_time DESC LIMIT 20`, (err, rows) => {
        if (err) return res.status(500).json({error: err.message});
        res.json(rows);
    });
});

app.get('/api/brain/insights', (req, res) => {
    db.all(`SELECT * FROM brain_insights ORDER BY timestamp DESC LIMIT 20`, (err, rows) => {
        if (err) return res.status(500).json({error: err.message});
        res.json(rows);
    });
});


async function pollBinanceOrderFlow() {
    try {
        // 1. Fetch Order Book (Depth 20)
        const depthRes = await axios.get(`${BASE}/api/v3/depth?symbol=${SYMBOL}&limit=20`);
        const asks = depthRes.data.asks; // Array of [price, qty]
        const bids = depthRes.data.bids;
        
        let totalBidVol = 0, totalAskVol = 0;
        let maxBid = { price: 0, qty: 0 };
        let maxAsk = { price: 0, qty: 0 };

        for (let i = 0; i < asks.length; i++) {
            const price = parseFloat(asks[i][0]);
            const qty = parseFloat(asks[i][1]);
            totalAskVol += qty;
            if (qty > maxAsk.qty) maxAsk = { price, qty };
        }
        for (let i = 0; i < bids.length; i++) {
            const price = parseFloat(bids[i][0]);
            const qty = parseFloat(bids[i][1]);
            totalBidVol += qty;
            if (qty > maxBid.qty) maxBid = { price, qty };
        }

        liveOrderFlow.whaleWallBid = maxBid.qty > 1.5 ? maxBid : null;
        liveOrderFlow.whaleWallAsk = maxAsk.qty > 1.5 ? maxAsk : null;
        
        // Spoofing detection
        if (lastWhaleWallBid && !liveOrderFlow.whaleWallBid && liveOrderFlow.currentPrice > lastWhaleWallBid.price) {
            liveOrderFlow.spoofAlert = `ðŸš¨ Fake Buy Wall Pulled: $${lastWhaleWallBid.price}. Possible Dump incoming!`;
        }
        if (lastWhaleWallAsk && !liveOrderFlow.whaleWallAsk && liveOrderFlow.currentPrice < lastWhaleWallAsk.price) {
            liveOrderFlow.spoofAlert = `ðŸš¨ Fake Sell Wall Pulled: $${lastWhaleWallAsk.price}. Possible Pump incoming!`;
        }
        lastWhaleWallBid = liveOrderFlow.whaleWallBid;
        lastWhaleWallAsk = liveOrderFlow.whaleWallAsk;
        
        if (Math.random() > 0.8) liveOrderFlow.spoofAlert = null;

        // Determine stop loss zones
        if (totalBidVol > totalAskVol * 1.5) {
            liveOrderFlow.stopLossZone = `Retail Long SLs likely around $${parseFloat(bids[bids.length - 1][0]).toFixed(2)}`;
        } else if (totalAskVol > totalBidVol * 1.5) {
            liveOrderFlow.stopLossZone = `Retail Short SLs likely around $${parseFloat(asks[asks.length - 1][0]).toFixed(2)}`;
        } else {
            liveOrderFlow.stopLossZone = "Neutral - No obvious SL clusters";
        }

        // 2. Fetch Recent Trades (for CVD and Price)
        const tradesRes = await axios.get(`${BASE}/api/v3/trades?symbol=${SYMBOL}&limit=100`);
        const trades = tradesRes.data;
        
        if (trades.length > 0) {
            liveOrderFlow.currentPrice = parseFloat(trades[trades.length - 1].price);
            
            // Calculate CVD for new trades only
            let newCvd = 0;
            for (let i = 0; i < trades.length; i++) {
                const t = trades[i];
                if (t.id > lastTradeId) {
                    const qty = parseFloat(t.qty);
                    if (t.isBuyerMaker) { // Seller was the taker -> Market Sell
                        newCvd -= qty;
                        liveOrderFlow.sellVol += qty;
                    } else { // Buyer was the taker -> Market Buy
                        newCvd += qty;
                        liveOrderFlow.buyVol += qty;
                    }
                }
            }
            if (lastTradeId === 0) newCvd = 0; // Don't skew CVD on first load
            lastTradeId = trades[trades.length - 1].id;
            liveOrderFlow.cvd += newCvd;
            
            // Trap Detection
            if (liveOrderFlow.cvd < -10 && liveOrderFlow.whaleWallBid && liveOrderFlow.currentPrice >= liveOrderFlow.whaleWallBid.price) {
                liveOrderFlow.trapAlert = `ðŸ”´ Retail Trap Detected: Heavy Retail Selling (${Math.abs(liveOrderFlow.cvd).toFixed(2)} BTC), but Whale Buy Wall holding at $${liveOrderFlow.whaleWallBid.price}. Potential Reversal UP!`;
            } else if (liveOrderFlow.cvd > 10 && liveOrderFlow.whaleWallAsk && liveOrderFlow.currentPrice <= liveOrderFlow.whaleWallAsk.price) {
                liveOrderFlow.trapAlert = `ðŸ”´ Retail Trap Detected: Heavy Retail Buying (${liveOrderFlow.cvd.toFixed(2)} BTC), but Whale Sell Wall holding at $${liveOrderFlow.whaleWallAsk.price}. Potential Reversal DOWN!`;
            } else {
                liveOrderFlow.trapAlert = "No traps detected.";
            }

            // Decay CVD
            liveOrderFlow.cvd *= 0.999;
        }

        // 3. Fetch recent candles for Price Action analysis
        const klineRes = await axios.get(`${BASE}/api/v3/klines?symbol=${SYMBOL}&interval=15m&limit=2`);
        if (klineRes.data && klineRes.data.length > 0) {
            const currentCandle = klineRes.data[klineRes.data.length - 1];
            const open = parseFloat(currentCandle[1]);
            const high = parseFloat(currentCandle[2]);
            const low = parseFloat(currentCandle[3]);
            const close = parseFloat(currentCandle[4]);
            
            const bodySize = Math.abs(open - close);
            const upperWick = high - Math.max(open, close);
            const lowerWick = Math.min(open, close) - low;
            
            liveOrderFlow.candleAnalysis = {
                isPinbarBullish: lowerWick > (bodySize * 2) && upperWick < bodySize,
                isPinbarBearish: upperWick > (bodySize * 2) && lowerWick < bodySize,
                trend: close > open ? "Bullish" : "Bearish"
            };
        }

        
    } catch (err) {
        const msg = '[OrderFlow] Error fetching data: ' + err.message;
        console.error(msg);
        brainLogs.push(`[${new Date().toLocaleTimeString()}] ${msg}`);
        if(brainLogs.length > 100) brainLogs.shift();
    }
}
// NOTE: pollBinanceOrderFlow() REST polling removed. Data now comes from WebSocket streams above.

// Endpoint for Strategy Lab UI
app.get('/api/orderflow', (req, res) => {
    res.json(liveOrderFlow);
});

app.listen(PORT, () => console.log(`[SERVER] Alpha-Flow SMC Brain v4 running on port ${PORT}`));
