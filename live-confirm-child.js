// Child process: node live-confirm-child.js <SYMBOL> <sign:1|-1> [maxMs]
// Monitor realtime (websocket candle 1m & 5m + liquidation) sampai semua konfirmasi terpenuhi atau timeout.
// Komunikasi ke parent lewat process.send: { type: 'confirmed' | 'timeout' | 'error', ... }
require('dotenv').config();
const Binance = require('binance-api-node').default;
const { RSI, SMA, MACD } = require('technicalindicators');

const [symbol, signArg, maxMsArg] = process.argv.slice(2);
const sign = Number(signArg) >= 0 ? 1 : -1;
const MAX_MS = Number(maxMsArg) || 10 * 60 * 1000;
const EVAL_EVERY_MS = 1000;
const RSI_PERIOD = 14, MA_PERIOD = 9, CROSS_LOOKBACK = 3, CVD_CANDLES = 3, LIMIT = 150;

if (!symbol) { console.error('symbol wajib'); process.exit(1); }

const client = Binance();
const state = {
  '1m': { closes: [], delta: [], times: [] },
  '5m': { closes: [], delta: [], times: [] },
};
const liq = { inflow: 0, outflow: 0 };
const stops = [];
let finished = false;
let tick;

// taker buy - taker sell (USDT). REST: quoteAssetVolume = taker buy quote; WS: quoteBuyVolume
const deltaRest = r => 2 * +r.quoteAssetVolume - +r.quoteVolume;
const deltaWs = c => 2 * +c.quoteBuyVolume - +c.quoteVolume;

function pushCandle(tf, t, close, delta) {
  const s = state[tf];
  const last = s.times.length - 1;
  if (last >= 0 && s.times[last] === t) { s.closes[last] = close; s.delta[last] = delta; }
  else if (last < 0 || t > s.times[last]) {
    s.times.push(t); s.closes.push(close); s.delta.push(delta);
    if (s.times.length > LIMIT) { s.times.shift(); s.closes.shift(); s.delta.shift(); }
  }
}

// `line` baru cross terhadap `ref` searah sign dalam CROSS_LOOKBACK candle dan masih di sisi itu
function crossed(line, ref) {
  const n = line.length - 1;
  if (line[n] == null || ref[n] == null || sign * (line[n] - ref[n]) <= 0) return false;
  for (let i = n; i > n - CROSS_LOOKBACK; i--) {
    if (line[i - 1] == null || ref[i - 1] == null) return false;
    if (sign * (line[i - 1] - ref[i - 1]) <= 0 && sign * (line[i] - ref[i]) > 0) return true;
  }
  return false;
}

function analyze(tf) {
  const { closes, delta } = state[tf];
  const n = closes.length - 1;
  if (n < 40) return null;

  const cvd = delta.slice(-CVD_CANDLES).reduce((a, b) => a + b, 0);
  const cvdOk = sign * cvd > 0 && sign * delta[n] >= 0;

  const rsiRaw = RSI.calculate({ values: closes, period: RSI_PERIOD });
  const rsi = new Array(closes.length - rsiRaw.length).fill(50).concat(rsiRaw);
  const rsiMa = new Array(MA_PERIOD - 1).fill(null).concat(SMA.calculate({ values: rsi, period: MA_PERIOD }));
  const rsiOk = crossed(rsi, rsiMa);

  const m = MACD.calculate({
    values: closes, fastPeriod: 12, slowPeriod: 26, signalPeriod: 9,
    SimpleMAOscillator: false, SimpleMASignal: false,
  });
  const hist = m[m.length - 1]?.histogram, prevHist = m[m.length - 2]?.histogram;
  const macdOk = hist != null && prevHist != null && sign * (hist - prevHist) > 0;

  return { interval: tf, cvdOk, rsiOk, macdOk, cvd, rsi: rsi[n], hist, prevHist };
}

function finish(msg, code = 0) {
  if (finished) return;
  finished = true;
  clearInterval(tick);
  clearTimeout(deadline);
  stops.forEach(s => { try { s(); } catch (_) {} });
  const done = () => process.exit(code);
  if (process.send && process.connected) process.send(msg, done); else done();
  setTimeout(done, 1500).unref();
}

function evaluate() {
  if (finished) return;
  const tf1 = analyze('1m'), tf5 = analyze('5m');
  if (!tf1 || !tf5) return;
  const allOk = [tf1, tf5].flatMap(t => [t.cvdOk, t.rsiOk, t.macdOk]).every(Boolean);
  if (allOk) {
    const s = state['1m'];
    finish({ type: 'confirmed', symbol, sign, tf1, tf5, price: s.closes[s.closes.length - 1], liq });
  }
}

const deadline = setTimeout(() => finish({ type: 'timeout', symbol, sign }), MAX_MS);

(async () => {
  try {
    for (const tf of ['1m', '5m']) {
      const rows = await client.futuresCandles({ symbol, interval: tf, limit: LIMIT });
      rows.forEach(r => pushCandle(tf, r.openTime, +r.close, deltaRest(r)));
    }
    for (const tf of ['1m', '5m']) {
      stops.push(client.ws.futuresCandles(symbol, tf, c =>
        pushCandle(tf, c.startTime, +c.close, deltaWs(c))));
    }
    stops.push(client.ws.futuresLiquidations(symbol, l => {
      if (l.status !== 'FILLED') return;
      const v = +l.price * +l.accumulatedQty;
      if (l.side === 'BUY') liq.inflow += v; else liq.outflow += v;
    }));
    tick = setInterval(evaluate, EVAL_EVERY_MS);
  } catch (e) {
    console.error(`Live-confirm ${symbol} gagal start:`, e.message);
    finish({ type: 'error', symbol, error: e.message }, 1);
  }
})();

process.on('disconnect', () => finish({ type: 'parent-gone' }));
process.on('SIGTERM', () => finish({ type: 'killed' }));
