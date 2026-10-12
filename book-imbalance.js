const axios = require('axios');

const FAPI = process.env.FAPI_BASE || 'https://fapi.binance.com';

const BOOK_LEVELS = 50;        // jumlah level per sisi (limit 50 = weight ringan di Binance)
const BOOK_BAND_PCT = 0.3;     // jarak maksimum dari harga tengah yang dihitung (%)
const BOOK_SAMPLES = 3;        // ambil beberapa kali supaya dinding palsu (spoof) terlihat
const BOOK_GAP_MS = 1000;      // jeda antar sampel
const MAX_SPREAD_PCT = 0.15;   // spread lebih lebar dari ini = orderbook terlalu tipis, tidak dipercaya

const sleep = ms => new Promise(r => setTimeout(r, ms));
const avg = arr => arr.reduce((a, b) => a + b, 0) / arr.length;

// Satu snapshot. Nilai dalam USDT, hanya level yang masuk dalam "band" yang SAMA di kedua sisi
// (jadi sisi yang kedalamannya lebih dangkal tidak otomatis kalah).
async function getBookOnce(symbol) {
  const { data } = await axios.get(`${FAPI}/fapi/v1/depth`, { params: { symbol, limit: BOOK_LEVELS } });
  const bids = data.bids.map(([p, q]) => [+p, +q]);
  const asks = data.asks.map(([p, q]) => [+p, +q]);
  if (!bids.length || !asks.length) return null;

  const mid = (bids[0][0] + asks[0][0]) / 2;
  const spreadPct = ((asks[0][0] - bids[0][0]) / mid) * 100;

  const bidReach = ((mid - bids[bids.length - 1][0]) / mid) * 100;   // sejauh mana 50 level bid menjangkau
  const askReach = ((asks[asks.length - 1][0] - mid) / mid) * 100;
  const band = Math.min(BOOK_BAND_PCT, bidReach, askReach);
  if (!(band > 0)) return null;

  const bidUsd = bids.filter(([p]) => p >= mid * (1 - band / 100)).reduce((a, [p, q]) => a + p * q, 0);
  const askUsd = asks.filter(([p]) => p <= mid * (1 + band / 100)).reduce((a, [p, q]) => a + p * q, 0);
  if (bidUsd + askUsd === 0) return null;

  return { bidUsd, askUsd, band, spreadPct };
}

// Hasil:
//   ratio      = bid / ask           (>1 = bid lebih tebal)
//   imb        = (bid-ask)/(bid+ask) (-1..+1, + = bid dominan)
//   consistent = false kalau arah imbalance berubah-ubah antar sampel (indikasi spoof / book tidak stabil)
//   reliable   = consistent && spread wajar -> hanya pakai kalau true
async function getBookImbalance(symbol, { samples = BOOK_SAMPLES, gapMs = BOOK_GAP_MS } = {}) {
  const snaps = [];
  for (let i = 0; i < samples; i++) {
    try {
      const s = await getBookOnce(symbol);
      if (s) snaps.push(s);
    } catch (_) { /* sampel gagal dilewati */ }
    if (i < samples - 1) await sleep(gapMs);
  }
  if (!snaps.length) return null;

  const imbs = snaps.map(s => (s.bidUsd - s.askUsd) / (s.bidUsd + s.askUsd));
  const imb = avg(imbs);
  const ratio = avg(snaps.map(s => s.bidUsd)) / avg(snaps.map(s => s.askUsd));
  const consistent = snaps.length >= 2 && imbs.every(x => Math.sign(x) === Math.sign(imb));
  const spreadPct = avg(snaps.map(s => s.spreadPct));

  return {
    ratio, imb, consistent, spreadPct,
    band: snaps[0].band,
    samples: snaps.length,
    reliable: consistent && spreadPct <= MAX_SPREAD_PCT,
  };
}

module.exports = { getBookImbalance, getBookOnce };