'use strict';

require('dotenv').config();
const axios = require('axios');
const telegram = require('./telegram');

const CFG = {
  restBase: (process.env.BINANCE_FAPI || 'https://fapi.binance.com').replace(/\/$/, ''),
  candleLimit: 60,
  maxCrossAge: 3,
  minQuoteVol24h: 25_000_000,
  cvdCandles: 3,
  concurrency: 10,
  requestTimeoutMs: 10000,
};

const MA_FAST_PERIOD = 5;
const MA_SLOW_PERIOD = 50;
const VOLUME_FAST_PERIOD = 10;
const VOLUME_SLOW_PERIOD = 30;
const alertedCandles = new Map();

function filterTradableSymbols(symbols, tickers, minQuoteVol24h = CFG.minQuoteVol24h) {
  const tickerBySymbol = new Map(tickers.map(ticker => [ticker.symbol, ticker]));
  return symbols
    .filter(symbol =>
      symbol.quoteAsset === 'USDT' &&
      symbol.contractType === 'PERPETUAL' &&
      symbol.status === 'TRADING'
    )
    .flatMap(symbol => {
      const ticker = tickerBySymbol.get(symbol.symbol);
      const quoteVolume = Number(ticker?.quoteVolume);
      const currentPrice = Number(ticker?.lastPrice);
      if (!Number.isFinite(quoteVolume) || quoteVolume < minQuoteVol24h ||
          !Number.isFinite(currentPrice) || currentPrice <= 0) return [];
      return [{ symbol: symbol.symbol, quoteVolume, currentPrice }];
    });
}

function average(values, end, period) {
  if (end + 1 < period) return NaN;
  let total = 0;
  for (let i = end - period + 1; i <= end; i++) total += values[i];
  return total / period;
}

function analyzeCandles(candles, maxCrossAge = CFG.maxCrossAge) {
  if (candles.length < MA_SLOW_PERIOD + maxCrossAge + 1) return null;
  if (candles.some(candle =>
    !Number.isFinite(candle.open) || candle.open <= 0 ||
    !Number.isFinite(candle.close) || candle.close <= 0 ||
    !Number.isFinite(candle.volume) || candle.volume < 0
  )) return null;

  const closes = candles.map(candle => candle.close);
  const volumes = candles.map(candle => candle.volume);
  const lastIndex = candles.length - 1;
  const ma5 = average(closes, lastIndex, MA_FAST_PERIOD);
  const previousMa5 = average(closes, lastIndex - 1, MA_FAST_PERIOD);
  const ma50 = average(closes, lastIndex, MA_SLOW_PERIOD);
  const volumeMa10 = average(volumes, lastIndex, VOLUME_FAST_PERIOD);
  const volumeMa30 = average(volumes, lastIndex, VOLUME_SLOW_PERIOD);

  if (![ma5, previousMa5, ma50, volumeMa10, volumeMa30].every(Number.isFinite)) return null;

  const longSetup = ma5 > previousMa5 && ma5 > ma50 &&
    candles[lastIndex].open > ma5 && volumeMa10 > volumeMa30;
  const shortSetup = ma5 < previousMa5 && ma5 < ma50 &&
    candles[lastIndex].open < ma5 && volumeMa10 > volumeMa30;
  if (!longSetup && !shortSetup) return null;

  const recentCandles = candles.slice(-CFG.cvdCandles);
  if (recentCandles.some(candle =>
    !Number.isFinite(candle.quoteVolume) || candle.quoteVolume < 0 ||
    !Number.isFinite(candle.takerBuyQuoteVolume) || candle.takerBuyQuoteVolume < 0
  )) return null;
  const deltas = recentCandles.map(candle => 2 * candle.takerBuyQuoteVolume - candle.quoteVolume);
  const cvd = deltas.reduce((sum, delta) => sum + delta, 0);
  const lastDelta = deltas[deltas.length - 1];
  const cvdUp = cvd > 0 && lastDelta >= 0;
  const cvdDown = cvd < 0 && lastDelta <= 0;

  for (let age = 0; age <= maxCrossAge; age++) {
    const index = lastIndex - age;
    const previousIndex = index - 1;
    const crossMa5 = average(closes, index, MA_FAST_PERIOD);
    const previousCrossMa5 = average(closes, previousIndex, MA_FAST_PERIOD);
    const crossMa50 = average(closes, index, MA_SLOW_PERIOD);
    const previousCrossMa50 = average(closes, previousIndex, MA_SLOW_PERIOD);

    const crossedUp = previousCrossMa5 <= previousCrossMa50 && crossMa5 > crossMa50;
    const crossedDown = previousCrossMa5 >= previousCrossMa50 && crossMa5 < crossMa50;

    if (crossedUp && longSetup && cvdUp) {
      return {
        side: 'LONG',
        age,
        ma5,
        previousMa5,
        ma50,
        volumeMa10,
        volumeMa30,
        cvd,
        candleOpenTime: candles[index].openTime,
      };
    }

    if (crossedDown && shortSetup && cvdDown) {
      return {
        side: 'SHORT',
        age,
        ma5,
        previousMa5,
        ma50,
        volumeMa10,
        volumeMa30,
        cvd,
        candleOpenTime: candles[index].openTime,
      };
    }
  }

  return null;
}

async function getSymbols() {
  const [exchangeInfo, ticker24h] = await Promise.all([
    axios.get(`${CFG.restBase}/fapi/v1/exchangeInfo`, { timeout: CFG.requestTimeoutMs }),
    axios.get(`${CFG.restBase}/fapi/v1/ticker/24hr`, { timeout: CFG.requestTimeoutMs }),
  ]);
  return filterTradableSymbols(exchangeInfo.data.symbols, ticker24h.data);
}

async function getClosedCandles(symbol) {
  const response = await axios.get(`${CFG.restBase}/fapi/v1/klines`, {
    params: { symbol, interval: '1m', limit: CFG.candleLimit },
    timeout: CFG.requestTimeoutMs,
  });
  const now = Date.now();
  return response.data
    .filter(row => Number(row[6]) < now)
    .map(row => ({
      openTime: Number(row[0]),
      open: Number(row[1]),
      close: Number(row[4]),
      volume: Number(row[5]),
      quoteVolume: Number(row[7]),
      takerBuyQuoteVolume: Number(row[10]),
    }));
}

async function mapLimit(items, limit, callback) {
  const results = new Array(items.length);
  let nextIndex = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (nextIndex < items.length) {
      const index = nextIndex++;
      results[index] = await callback(items[index]);
    }
  });
  await Promise.all(workers);
  return results;
}

function formatSignal(symbol, signal) {
  const label = signal.side === 'LONG' ? 'LONG' : 'SHORT';
  return `*${label} MOMENTUM* ${symbol}\n` +
    `Harga saat ini: ${signal.currentPrice}\n` +
    `Volume 24 jam: ${signal.quoteVolume.toLocaleString('en-US', { maximumFractionDigits: 0 })} USDT\n` +
    `MA5: ${signal.ma5} (sebelumnya ${signal.previousMa5})\n` +
    `MA50: ${signal.ma50}\n` +
    `Vol MA10: ${signal.volumeMa10} | Vol MA30: ${signal.volumeMa30}\n` +
    `CVD 3 candle 1m: ${signal.cvd}\n` +
    `Crossover: ${signal.age} candle lalu`;
}

async function scan() {
  const symbols = await getSymbols();
  console.log(`[${new Date().toISOString()}] Scan ${symbols.length} simbol futures USDT 1m (vol 24h >= 25 juta USDT)`);

  const signals = await mapLimit(symbols, CFG.concurrency, async market => {
    try {
      const candles = await getClosedCandles(market.symbol);
      const signal = analyzeCandles(candles);
      return signal ? { ...market, ...signal } : null;
    } catch (error) {
      console.error(`[${market.symbol}] Gagal memeriksa candle: ${error.message}`);
      return null;
    }
  });

  const freshSignals = signals
    .filter(Boolean)
    .filter(signal => alertedCandles.get(`${signal.symbol}:${signal.side}`) !== signal.candleOpenTime);

  if (freshSignals.length === 0) {
    console.log('Tidak ada momentum baru.');
    return;
  }

  for (const signal of freshSignals) {
    console.log(`${signal.side} MOMENTUM ${signal.symbol} @ ${signal.currentPrice} (CVD ${signal.cvd}, crossover ${signal.age} candle lalu)`);
  }

  let batch = [];
  let batchText = '';
  for (const signal of freshSignals) {
    const formatted = formatSignal(signal.symbol, signal);
    if (batch.length > 0 && batchText.length + formatted.length + 2 > 3500) {
      await sendSignalBatch(batch, batchText);
      batch = [];
      batchText = '';
    }
    batch.push(signal);
    batchText += `${batchText ? '\n\n' : ''}${formatted}`;
  }
  if (batch.length > 0) await sendSignalBatch(batch, batchText);
}

async function sendSignalBatch(signals, text) {
  const sent = await telegram.broadcast(text);
  if (sent.length === 0) return;
  for (const signal of signals) {
    alertedCandles.set(`${signal.symbol}:${signal.side}`, signal.candleOpenTime);
  }
}

function scheduleNextScan() {
  const delay = 60000 - (Date.now() % 60000) + 1500;
  setTimeout(async () => {
    try {
      await scan();
    } catch (error) {
      console.error(`[${new Date().toISOString()}] Gagal menjalankan scan: ${error.message}`);
    }
    scheduleNextScan();
  }, delay);
}

async function startMonitoring() {
  try {
    await scan();
  } catch (error) {
    console.error(`[${new Date().toISOString()}] Gagal menjalankan scan: ${error.message}`);
  }
  scheduleNextScan();
}

if (require.main === module) {
  startMonitoring();
}

module.exports = { analyzeCandles, filterTradableSymbols };