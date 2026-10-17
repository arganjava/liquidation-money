const axios = require('axios');
const { emaSeries } = require('./macd');

const FAPI = process.env.FAPI_BASE || 'https://fapi.binance.com';
const RSI_PERIOD = 14;
const MA_PERIOD = 9;
const CROSS_LOOKBACK = 3;     // cross dianggap valid jika terjadi di N candle terakhir dan masih di sisi itu
const CVD_CANDLES = 3;        // jumlah candle terakhir untuk akumulasi tekanan beli/jual

const sma = (arr, p) => arr.map((_, i) => i < p - 1 ? null : arr.slice(i - p + 1, i + 1).reduce((a, b) => a + b, 0) / p);

function rsiSeries(closes, period = RSI_PERIOD) {
  const out = new Array(closes.length).fill(null);
  if (closes.length <= period) return out;
  let gain = 0, loss = 0;
  for (let i = 1; i <= period; i++) {
    const d = closes[i] - closes[i - 1];
    if (d >= 0) gain += d; else loss -= d;
  }
  gain /= period; loss /= period;
  out[period] = loss === 0 ? 100 : 100 - 100 / (1 + gain / loss);
  for (let i = period + 1; i < closes.length; i++) {
    const d = closes[i] - closes[i - 1];
    gain = (gain * (period - 1) + Math.max(d, 0)) / period;
    loss = (loss * (period - 1) + Math.max(-d, 0)) / period;
    out[i] = loss === 0 ? 100 : 100 - 100 / (1 + gain / loss);
  }
  return out;
}

// Apakah `line` baru cross terhadap `ref` searah `sign` (+1 up, -1 down) dalam lookback candle dan masih di sisi itu.
function crossed(line, ref, sign, lookback = CROSS_LOOKBACK) {
  const n = line.length - 1;
  if (line[n] == null || ref[n] == null || sign * (line[n] - ref[n]) <= 0) return false;
  for (let i = n; i > n - lookback; i--) {
    if (line[i - 1] == null || ref[i - 1] == null) return false;
    if (sign * (line[i - 1] - ref[i - 1]) <= 0 && sign * (line[i] - ref[i]) > 0) return true;
  }
  return false;
}

// Semua dihitung pada candle yang sedang terbentuk (forming) agar follow-up 15 detik tetap responsif.
async function analyzeTimeframe(symbol, interval, sign) {
  const { data } = await axios.get(`${FAPI}/fapi/v1/klines`, { params: { symbol, interval, limit: 120 } });
  const closes = data.map(x => +x[4]);
  const n = closes.length - 1;

  // CVD dari tekanan beli/jual taker: delta = buy agresif - sell agresif (USDT)
  const delta = data.map(x => 2 * (+x[10]) - (+x[7]));
  const recent = delta.slice(-CVD_CANDLES).reduce((a, b) => a + b, 0);
  const cvdOk = sign * recent > 0 && sign * delta[n] >= 0;

  const rsi = rsiSeries(closes);
  const rsiVals = rsi.map(v => v ?? 50);
  const rsiOk = crossed(rsiVals, sma(rsiVals, MA_PERIOD), sign);

  const emaF = emaSeries(closes, 12), emaS = emaSeries(closes, 26);
  const dif = emaF.map((v, i) => v - emaS[i]);
  const dea = emaSeries(dif, 9);
  const hist = dif.map((v, i) => v - dea[i]);
  const macdOk = sign * (hist[n] - hist[n - 1]) > 0;

  return {
    interval, cvdOk, rsiOk, macdOk,
    cvd: recent, rsi: rsi[n], hist: hist[n], prevHist: hist[n - 1],
  };
}

// sign > 0 = CONTINUATION UP, sign < 0 = CONTINUATION DOWN
async function checkContinuation(symbol, sign) {
  const [tf1, tf5] = await Promise.all([
    analyzeTimeframe(symbol, '1m', sign),
    analyzeTimeframe(symbol, '5m', sign),
  ]);
  const all = [tf1, tf5].flatMap(t => [t.cvdOk, t.rsiOk, t.macdOk]);
  return { tf1, tf5, allOk: all.every(Boolean) };
}

module.exports = { checkContinuation };
