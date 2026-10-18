const axios = require('axios');
const WebSocket = require('ws');
const { analyzeKlines, allConfirmed } = require('./follow-up-check');

const FAPI = process.env.FAPI_BASE || 'https://fapi.binance.com';
const WS_BASE = process.env.FWS_BASE || 'wss://fstream.binance.com';
const MAX_MS = 10 * 60 * 1000;
const EVAL_THROTTLE_MS = 1000;

const active = new Map(); // `${symbol}:${dir}` -> stop()

async function seed(symbol, interval) {
  const { data } = await axios.get(`${FAPI}/fapi/v1/klines`, { params: { symbol, interval, limit: 120 } });
  return data;
}

// Update/append candle dari kline event ke format REST
function applyKline(rows, k) {
  const row = [k.t, k.o, k.h, k.l, k.c, k.v, k.T, k.q, k.n, k.V, k.Q, '0'];
  const last = rows[rows.length - 1];
  if (last && last[0] === k.t) rows[rows.length - 1] = row;
  else if (!last || k.t > last[0]) { rows.push(row); if (rows.length > 150) rows.shift(); }
}

/**
 * Monitor realtime via websocket (kline 1m & 5m) maksimal 10 menit.
 * Jika semua konfirmasi tf1m & tf5m terpenuhi -> onConfirmed(result) lalu worker mati.
 * Jika timeout -> worker mati (onTimeout opsional).
 */
async function startLiveConfirm(symbol, sign, { onConfirmed, onTimeout, maxMs = MAX_MS } = {}) {
  const key = `${symbol}:${sign}`;
  if (active.has(key)) return false;

  let ws, timer, done = false, lastEval = 0, evaluating = false;
  const stop = () => {
    if (done) return;
    done = true;
    clearTimeout(timer);
    active.delete(key);
    try { ws && ws.terminate(); } catch (_) {}
  };
  active.set(key, stop);
  timer = setTimeout(() => {
    if (done) return;
    stop();
    console.log(`⌛ Live-confirm ${symbol} timeout ${maxMs / 60000}m`);
    if (onTimeout) Promise.resolve(onTimeout()).catch(() => {});
  }, maxMs);

  try {
    const [r1, r5] = await Promise.all([seed(symbol, '1m'), seed(symbol, '5m')]);
    if (done) return true;
    const rows = { '1m': r1, '5m': r5 };

    const evaluate = async () => {
      if (done || evaluating || Date.now() - lastEval < EVAL_THROTTLE_MS) return;
      lastEval = Date.now();
      evaluating = true;
      try {
        const tf1 = analyzeKlines(rows['1m'], '1m', sign);
        const tf5 = analyzeKlines(rows['5m'], '5m', sign);
        if (allConfirmed(tf1, tf5)) {
          stop();
          if (onConfirmed) await onConfirmed({ tf1, tf5, allOk: true, price: +rows['1m'][rows['1m'].length - 1][4] });
        }
      } catch (e) {
        console.error(`Live-confirm ${symbol} eval gagal:`, e.message);
      } finally { evaluating = false; }
    };

    const connect = () => {
      if (done) return;
      const s = symbol.toLowerCase();
      ws = new WebSocket(`${WS_BASE}/stream?streams=${s}@kline_1m/${s}@kline_5m`);
      ws.on('message', raw => {
        try {
          const k = JSON.parse(raw).data?.k;
          if (!k || !rows[k.i]) return;
          applyKline(rows[k.i], k);
          evaluate();
        } catch (_) {}
      });
      ws.on('error', e => console.error(`Live-confirm WS ${symbol}:`, e.message));
      ws.on('close', () => { if (!done) setTimeout(connect, 2000); });
    };
    connect();
  } catch (e) {
    console.error(`Live-confirm ${symbol} gagal start:`, e.message);
    stop();
  }
  return true;
}

module.exports = { startLiveConfirm };
