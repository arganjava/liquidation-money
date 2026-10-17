const axios = require('axios');

const FAPI = process.env.FAPI_BASE || 'https://fapi.binance.com';

const MACD_FAST = 12;
const MACD_SLOW = 26;
const MACD_SIGNAL = 9;
const MACD_LOOKBACK = 20;      // pembanding: rata-rata |histogram| N candle sebelumnya
const MACD_EXPANSION_MIN = 1.5; // histogram harus >= x kali rata-rata itu agar dianggap momentum sungguhan

// EMA berulang. Dikembalikan sebagai array sepanjang input.
function emaSeries(values, period) {
  const k = 2 / (period + 1);
  const out = [values[0]];
  for (let i = 1; i < values.length; i++) out.push(values[i] * k + out[i - 1] * (1 - k));
  return out;
}

// DIF = EMA(fast) - EMA(slow); DEA = EMA(DIF, signal); hist (MACD bar) = DIF - DEA.
// Catatan: sebagian charting (gaya China) menampilkan bar = 2 x (DIF - DEA); TANDA-nya sama.
function macdFromCloses(closes, fast = MACD_FAST, slow = MACD_SLOW, signal = MACD_SIGNAL) {
  if (closes.length < slow + signal + 5) return null;

  const emaF = emaSeries(closes, fast);
  const emaS = emaSeries(closes, slow);
  const dif = emaF.map((v, i) => v - emaS[i]);
  const dea = emaSeries(dif, signal);
  const hist = dif.map((v, i) => v - dea[i]);

  const n = closes.length - 1;
  const last = { dif: dif[n], dea: dea[n], hist: hist[n] };

  // Aturan Anda:
  //   LONG_SETUP : DIF & DEA di bawah nol, histogram positif  (golden cross di bawah garis nol)
  //   SHORT_SETUP: DIF & DEA di atas nol,  histogram negatif  (dead cross di atas garis nol)
  let state = 'NONE';
  if (last.dif < 0 && last.dea < 0 && last.hist > 0) state = 'LONG_SETUP';
  else if (last.dif > 0 && last.dea > 0 && last.hist < 0) state = 'SHORT_SETUP';

  // PERINGATAN: pada tren stabil, histogram bisa merayap melewati nol hanya karena DEA "mengejar" DIF,
  // tanpa ada perubahan momentum nyata. Jadi state saja belum cukup. Ukur EKSPANSI histogram:
  // |histogram sekarang| dibanding rata-rata |histogram| beberapa candle sebelumnya.
  const prior = hist.slice(Math.max(0, n - MACD_LOOKBACK), n).map(Math.abs);
  const meanPrior = prior.reduce((a, b) => a + b, 0) / prior.length;
  const expansion = meanPrior > 0 ? Math.abs(hist[n]) / meanPrior : 0;

  const histRising = hist[n] > hist[n - 1];
  const movingWithSetup = state === 'LONG_SETUP' ? histRising
    : state === 'SHORT_SETUP' ? !histRising
    : false;
  // strong: setup terpenuhi, histogram bergerak searah setup, dan melebar jelas (bukan sekadar merayap)
  const strong = movingWithSetup && expansion >= MACD_EXPANSION_MIN;

  return { ...last, prevHist: hist[n - 1], histRising, expansion, strong, state };
}

// Ambil klines, buang candle yang belum closed, hitung MACD.
async function getMacd(symbol, interval = '5m', limit = 150) {
  const { data } = await axios.get(`${FAPI}/fapi/v1/klines`, { params: { symbol, interval, limit } });
  const nowMs = Date.now();
  const closes = data.filter(x => +x[6] < nowMs).map(x => +x[4]);
  return macdFromCloses(closes);
}

// Apakah state MACD searah signal? sign > 0 = signal UP (butuh LONG_SETUP), sign < 0 = signal DOWN (butuh SHORT_SETUP)
function macdAligned(sign, m) {
  if (!m) return null;
  return sign > 0 ? m.state === 'LONG_SETUP' : m.state === 'SHORT_SETUP';
}

// Setup berlawanan dengan signal = indikasi bounce / reversal (mis. signal DOWN tapi MACD LONG_SETUP)
function macdConflicts(sign, m) {
  if (!m) return null;
  return sign > 0 ? m.state === 'SHORT_SETUP' : m.state === 'LONG_SETUP';
}

// Cross DIF terhadap DEA pada candle CLOSED. lookback = berapa candle terakhir yang masih dianggap "baru cross"
// agar sinyal tidak basi. Hasil: 'UP' | 'DOWN' | 'NONE'
function macdCrossFromCloses(closes, lookback = 2) {
  if (closes.length < MACD_SLOW + MACD_SIGNAL + 5) return null;
  const emaF = emaSeries(closes, MACD_FAST);
  const emaS = emaSeries(closes, MACD_SLOW);
  const dif = emaF.map((v, i) => v - emaS[i]);
  const dea = emaSeries(dif, MACD_SIGNAL);
  const hist = dif.map((v, i) => v - dea[i]);
  const n = hist.length - 1;
  for (let i = n; i > n - lookback; i--) {
    if (hist[i - 1] <= 0 && hist[i] > 0) return { cross: 'UP', hist: hist[n] };
    if (hist[i - 1] >= 0 && hist[i] < 0) return { cross: 'DOWN', hist: hist[n] };
  }
  return { cross: 'NONE', hist: hist[n] };
}

async function getMacdCross(symbol, interval, lookback = 2, limit = 150) {
  const { data } = await axios.get(`${FAPI}/fapi/v1/klines`, { params: { symbol, interval, limit } });
  const nowMs = Date.now();
  const closes = data.filter(x => +x[6] < nowMs).map(x => +x[4]);
  return macdCrossFromCloses(closes, lookback);
}

async function getMacdSignal(symbol) {
  const [m1, m5] = await Promise.all([getMacdCross(symbol, '1m'), getMacdCross(symbol, '5m')]);
  if (!m1 || !m5) return null;
  let side = null;
  if (m1.cross === 'UP' && m5.cross === 'UP') side = 'LONG';
  else if (m1.cross === 'DOWN' && m5.cross === 'DOWN') side = 'SHORT';
  return { side, m1: m1.cross, m5: m5.cross };
}

module.exports = { emaSeries, macdFromCloses, getMacd, macdAligned, macdConflicts, macdCrossFromCloses, getMacdCross, getMacdSignal };