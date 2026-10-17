const Binance = require('binance-api-node').default;
const mongoose = require('mongoose');
const axios = require('axios');
const dotenv = require('dotenv');
dotenv.config();

const { FlowMoney, Setting } = require('./models');
const EventEmitter = require('events');
const bus = new EventEmitter();
const { getBookImbalance } = require('./book-imbalance');   // taruh di bagian atas file
const { checkContinuation } = require('./follow-up-check');


// ----------------------------------------------------
// 1. CONFIGURATION & NOTIFICATION SETTINGS
// ----------------------------------------------------

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_IDS;
const SPIKE_THRESHOLD_USDT = 10000;          // Alert jika net_flow > \$10,000 USDT dalam 1 menit akibat likuidasi


const FAPI = 'https://fapi.binance.com';
const lastSpike = {};   // symbol -> { absNet, time }
const cooldown = {};    // symbol -> timestamp
const COOLDOWN_MS = 5 * 60 * 1000;

async function getContext(symbol) {
  const [k, oi] = await Promise.all([
    axios.get(`${FAPI}/fapi/v1/klines`, { params: { symbol, interval: '1m', limit: 6 } }),
    axios.get(`${FAPI}/futures/data/openInterestHist`, { params: { symbol, period: '5m', limit: 2 } }),
  ]);
  const c = k.data.map(x => ({ o: +x[1], h: +x[2], l: +x[3], c: +x[4] }));
  const last = c[c.length - 1];
  const priceChg5m = ((last.c - c[0].o) / c[0].o) * 100;
  const closePos = last.h === last.l ? 0.5 : (last.c - last.l) / (last.h - last.l); // 0=low, 1=high
  let oiChg5m = 0;
  if (oi.data.length === 2) {
    const a = +oi.data[0].sumOpenInterestValue, b = +oi.data[1].sumOpenInterestValue;
    oiChg5m = ((b - a) / a) * 100;
  }

  const delta = k.data.map(x => 2 * (+x[10]) - (+x[7]));   // buy agresif - sell agresif (USDT)
  const cvd5m = delta.reduce((a, b) => a + b, 0);
  const lastDelta = delta[delta.length - 1];
  return { priceChg5m, closePos, oiChg5m, price: last.c, cvd5m, lastDelta };
}

function classify(netFlow, ctx, prevAbsNet) {
  const sign = netFlow > 0 ? 1 : -1;  
  const move = sign * ctx.priceChg5m;          // >0 = harga searah likuidasi
  const cvd = sign * ctx.cvd5m;             // >0 = taker searah
  const last = sign * ctx.lastDelta;      // >0 = candle terakhir masih searah
          // +1 = squeeze naik, -1 = long dump
  const dirClose = sign > 0 ? ctx.closePos : 1 - ctx.closePos; // 1 = close di ujung searah
  let score = 0;
  const why = [];
  let isOiUp = "NO";
  // 1. Harga 5m searah likuidasi?
  if (move > 0.3) { score++; why.push(`harga 5m searah (${ctx.priceChg5m.toFixed(2)}%)`); }
  else if (move < -0.1) { score--; why.push('harga 5m sudah berbalik'); }

  // 2. Close candle: dekat ujung = kuat, wick panjang = rejection
  if (dirClose > 0.7) { score++; why.push('close dekat ujung candle'); }
  else if (dirClose < 0.4) { score--; why.push('ada wick rejection'); }

  // 3. Open Interest
  if (ctx.oiChg5m <= -1.5) { score--; why.push(`OI anjlok ${ctx.oiChg5m.toFixed(1)}% (flush selesai?)`); }
  else if (ctx.oiChg5m >= 0.5) { score++; isOiUp = "YES"; why.push(`OI naik ${ctx.oiChg5m.toFixed(1)}% (posisi baru masuk)`); }


  if (cvd > 0.05 && last > 0.05) { score += 2; why.push('CVD & delta terakhir searah (taker masih agresif)'); }
  else if (cvd > 0.05) { score += 1; why.push('CVD searah, tapi delta terakhir melemah'); }

  if (move > 0.2 && cvd < 0)  { score -= 2; why.push('harga searah tapi CVD berlawanan (absorpsi)'); }
  if (last < -0.05)           { score -= 1; why.push('delta candle terakhir berbalik'); }


  // 4. Cascade membesar atau mereda?
  if (prevAbsNet) {
    const ratio = Math.abs(netFlow) / prevAbsNet;
    if (ratio > 1.5) { score++; why.push(`cascade membesar ${ratio.toFixed(1)}x`); }
    else if (ratio < 0.5) { score--; why.push('cascade mereda'); }
  }

  let label = '⚪ NEUTRAL';
  if (score >= 2) label = sign > 0 ? '🚀 CONTINUATION UP' : '📉 CONTINUATION DOWN';
  else if (score <= -2) label = sign > 0 ? '🔄 REVERSAL WATCH (pump kehabisan tenaga, potensi turun)' : '🔄 REVERSAL WATCH (dump kehabisan tenaga, potensi naik)';

  return { score, label, why, isOiUp };
}


async function sendTelegramAlert(message) {
  const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;
  try {
    await axios.post(url, { chat_id: TELEGRAM_CHAT_ID, text: message, parse_mode: 'Markdown' });
  } catch (err) {
    console.error('❌ Telegram Alert Error:', err.message);
  }
}

// ----------------------------------------------------
// 2. MONGOOSE SCHEMA & MODELS (WITH TTL INDEX)
// ----------------------------------------------------
const MONGO_URI = process.env.MONGO_URI;
mongoose.connect(MONGO_URI)
  .then(() => console.log('✅ Connected to MongoDB'))
  .catch(err => console.error('❌ MongoDB Connection Error:', err));

// Dokumen mentah per 1 menit (Auto-delete setelah 24 jam dengan expires: 86400)
// const FlowMoney = mongoose.model('flow_money', new mongoose.Schema({
//   symbol: { type: String, required: true, index: true },
//   inflow: { type: Number, required: true },
//   outflow: { type: Number, required: true },
//   net_flow: { type: Number, required: true },
//   timestamp: { type: Date, default: Date.now, index: { expires: 7 * 86400 } }
// }), 'flow_money');

// ----------------------------------------------------
// 3. STATE MEMORY FOR REAL-TIME ACCUMULATION
// ----------------------------------------------------
let localCache = {};

function initSymbolCache(symbol) {
  if (!localCache[symbol]) {
    localCache[symbol] = { inflow: 0, outflow: 0, price: 0, inflow_count: 0, outflow_count: 0 };
  }
}

// ----------------------------------------------------
// 4. BINANCE API NODE INITIALIZATION & WS STREAM
// ----------------------------------------------------
const client = Binance();

client.time().then(time => {
  console.log('📡 Connected to Binance API. Server Time:', new Date(time).toLocaleString());
});

// Menangkap data likuidasi pasar berjangka global
client.ws.futuresAllLiquidations(liquidation => {
  if (liquidation.status === 'FILLED' && liquidation.symbol.endsWith('USDT')) {
    const symbol = liquidation.symbol;
    const price = parseFloat(liquidation.price);
    const quantity = parseFloat(liquidation.accumulatedQty);
    const volumeUSDT = price * quantity;
    const side = liquidation.side; // "BUY" atau "SELL"

    initSymbolCache(symbol);
    localCache[symbol].price = price; // Simpan harga terakhir untuk referensi
    if (side === 'BUY') {
      // Short Liquidation -> Bursa melakukan market BUY paksa -> INFLOW
      localCache[symbol].inflow += volumeUSDT;
      localCache[symbol].inflow_count += 1;
    } else if (side === 'SELL') {
      // Long Liquidation -> Bursa melakukan market SELL paksa -> OUTFLOW
      localCache[symbol].outflow += volumeUSDT;
      localCache[symbol].outflow_count += 1;
    }

    console.log(`💥 ${new Date().toLocaleString()} | ${symbol} | ${side} | Volume: \$${volumeUSDT.toFixed(2)} | Price: \$${price.toFixed(2)} | Inflow: ${localCache[symbol].inflow_count} | Outflow: ${localCache[symbol].outflow_count}`);
  }
});

// ----------------------------------------------------
// 5. SCHEDULERS & INTERVAL JOBS
// ----------------------------------------------------

// JOB A: Setiap 1 Menit - Simpan State Memori ke `flow_money` & Deteksi Spike Telegram
async function runMinuteJob() {
  const now = new Date();
  const docsToInsert = [];

  // Duplikasi cache lokal saat ini untuk diproses dan langsung dikosongkan (reset per menit)
  const currentSnapshot = { ...localCache };
  localCache = {}; // Reset data lokal untuk menampung transaksi menit berikutnya

  for (const symbol in currentSnapshot) {
    const minuteInflow = currentSnapshot[symbol].inflow;
    const minuteOutflow = currentSnapshot[symbol].outflow;
    const netFlow = minuteInflow - minuteOutflow;

    if (minuteInflow > 0 || minuteOutflow > 0) {
      docsToInsert.push({
        symbol,
        inflow: minuteInflow,
        outflow: minuteOutflow,
        net_flow: netFlow,
        timestamp: now
      });

      if (Math.abs(netFlow) >= SPIKE_THRESHOLD_USDT) {
        const nowMs = Date.now();
        const prev = lastSpike[symbol];
        const prevAbsNet = prev && nowMs - prev.time < 3 * 60 * 1000 ? prev.absNet : null;
        lastSpike[symbol] = { absNet: Math.abs(netFlow), time: nowMs };

        if (cooldown[symbol] && nowMs - cooldown[symbol] < COOLDOWN_MS) continue;

        try {
          
          const ctx = await getContext(symbol);
          const { score, label, why, isOiUp } = classify(netFlow, ctx, prevAbsNet);
          if (label === '⚪ NEUTRAL') continue;   // skip sinyal ambigu
          const formattedNet = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(netFlow);
          cooldown[symbol] = nowMs;

          const book = await getBookImbalance(symbol).catch(() => null);
          const sign = netFlow > 0 ? 1 : -1;
          let bookLine = '• *Orderbook:* tidak tersedia';

          if (book && book.reliable) {
            const dirImb = sign * book.imb;      // + = mendukung arah signal
            bookLine = `• *Orderbook:* bid/ask ${book.ratio.toFixed(2)}x ` +
              `${dirImb > 0.2 ? '✅ mendukung' : dirImb < -0.2 ? '⚠️ melawan' : '⚪ netral'} (±${book.band.toFixed(2)}%)`;
          } else if (book) {
            bookLine = '• *Orderbook:* ⚠️ tidak stabil / spread lebar (diabaikan)';
          }

          const inflow_count = currentSnapshot[symbol].inflow_count;
          const outflow_count = currentSnapshot[symbol].outflow_count;
          console.log(`📢 [ALERT] ${label} | ${symbol} | Net Flow: ${formattedNet} | Score: ${score} | OI Up: ${isOiUp}`);
          const msg =
            `${label}\n\n` +
            `• *Coin:* #${symbol} ${ctx.price}\n` +
            `• *Net Liq Flow:* ${formattedNet}\n` +
            `• *Inflow:* ${inflow_count} \n` +
            `• *Outflow:* ${outflow_count}\n` +
            `• *Score:* ${score}\n` +
            `${bookLine}\n` +
            `• *Alasan:*\n   ↳ ${why.join('\n   ↳ ')}`;
          const isTelegramEnabled = (await Setting.findOne({ key: 'telegram' }).lean())?.enabled ?? true;
          if (isTelegramEnabled) {
           // sendTelegramAlert(msg);
            scheduleFollowUp(symbol, netFlow, ctx.price, label);
          }
        } catch (e) {
          console.error(`Gagal ambil konteks ${symbol}:`, e.message);
        }
      }

      // 🔥 LOGIKA DETEKSI SPIKE FLOW LIKUIDASI
      // if (Math.abs(netFlow) >= SPIKE_THRESHOLD_USDT ) {
      //   const type = netFlow > 0 ? '🟢 *LIQUIDATION SPIKE INFLOW (SHORT SQUEEZE)*' : '🔴 *LIQUIDATION SPIKE OUTFLOW (LONG DUMP)*';
      //   const formattedNet = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(Math.abs(netFlow));

      //   const alertMessage = `${type}\n\n` +
      //     `• *Coin:* #${symbol} ${currentSnapshot[symbol].price} \n` +
      //     `• *Net Liquidation Flow (1m):* ${formattedNet}\n` +
      //     `• *Inflow (Short Liq):* $${minuteInflow.toLocaleString(undefined, {maximumFractionDigits:0})}\n` +
      //     `• *Outflow (Long Liq):* $${minuteOutflow.toLocaleString(undefined, {maximumFractionDigits:0})}\n` +
      //     `• *Waktu:* ${now.toLocaleTimeString()}`;
      //   const isTelegramEnabled = (await Setting.findOne({ key: 'telegram' }).lean())?.enabled ?? true;
      //   if (isTelegramEnabled){
      //     sendTelegramAlert(alertMessage);
      //   }
      // }
    }
  }

  if (docsToInsert.length > 0) {
    try {
      const inserted = await FlowMoney.insertMany(docsToInsert);
      inserted.forEach(d => bus.emit('flow', d));
      console.log(`⏱️ [1 Menit] Berhasil menyimpan ${docsToInsert.length} data koin ke 'flow_money'`);
    } catch (err) {
      console.error('⚠️ Gagal menyimpan ke flow_money:', err.message);
    }
  }
}

// Follow-up 15 detik setelah alert: cek apakah arah flow berbalik atau berlanjut
function scheduleFollowUp(symbol, origNet, origPrice, label) {
  setTimeout(async () => {
    try {
      const c = localCache[symbol];
      const inflow = c ? c.inflow : 0;
      const outflow = c ? c.outflow : 0;
      const net = inflow - outflow;
      const fmt = v => new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 }).format(v);
      const origDir = origNet > 0 ? 'INFLOW (CVD naik)' : 'OUTFLOW (CVD turun)';
      let status;
      if (net === 0) status = '⚪ *SEPI* - tidak ada likuidasi baru dalam 15 detik';
      else if (Math.sign(net) === Math.sign(origNet)) status = '✅ *CONTINUE* - arah masih sama';
      else status = '🔄 *REVERSAL* - arah berbalik';
      const curPrice = c && c.price ? c.price : origPrice;

      let checkLines = '';
      // Arah yang dikonfirmasi: CONTINUATION searah sinyal, REVERSAL berlawanan dengan flow awal
      const isReversal = label.includes('REVERSAL');
      const dir = label.includes('CONTINUATION UP') ? 1
        : label.includes('CONTINUATION DOWN') ? -1
        : isReversal && origNet !== 0 ? -Math.sign(origNet) : 0;
      const isUp = dir > 0;
      if (dir !== 0) {
        try {
          const r = await checkContinuation(symbol, dir);
          const t = ok => ok ? '✅' : '❌';
          const n = v => v == null || !isFinite(v) ? '-' : v.toPrecision(3);
          const tfLine = x =>
            `   ↳ *${x.interval}:* CVD ${t(x.cvdOk)} (${fmt(x.cvd)}) | ` +
            `RSI cross ${isUp ? 'up' : 'down'} ${t(x.rsiOk)} (${x.rsi == null ? '-' : x.rsi.toFixed(1)}) | ` +
            `MACD ${isUp ? 'lebih besar' : 'lebih kecil'} ${t(x.macdOk)} (hist ${n(x.hist)} vs ${n(x.prevHist)})`;
          checkLines =
            `\n• *Konfirmasi ${isReversal ? 'REVERSAL ' : ''}${isUp ? 'UP' : 'DOWN'}:* ${r.allOk ? '✅ SEMUA TERPENUHI' : '⚠️ BELUM LENGKAP'}\n` +
            `${tfLine(r.tf1)}\n${tfLine(r.tf5)}`;
        } catch (e) {
          checkLines = '\n• *Konfirmasi:* gagal diambil';
          console.error(`Gagal cek konfirmasi ${symbol}:`, e.message);
        }
      }

      const msg =
        `⏱️ ${label} \n\n*FOLLOW-UP 15s* #${symbol}\n\n` +
        `• *Sinyal awal:* ${origDir} ${fmt(origNet)} @ ${origPrice}\n` +
        `• *Inflow:* ${fmt(inflow)} (${c ? c.inflow_count : 0}x)\n` +
        `• *Outflow:* ${fmt(outflow)} (${c ? c.outflow_count : 0}x)\n` +
        `• *Net/CVD:* ${fmt(net)}\n` +
        `• *Harga terakhir:* ${curPrice}\n` +
        `• *Status:* ${status}` +
        checkLines;
      const enabled = (await Setting.findOne({ key: 'telegram' }).lean())?.enabled ?? true;
      if (enabled) sendTelegramAlert(msg);
    } catch (e) {
      console.error(`Gagal follow-up ${symbol}:`, e.message);
    }
  }, 15 * 1000);
}

// Jalankan tepat di menit berikutnya (detik 0-1), lalu ulangi setiap menit
function scheduleNextMinuteTick() {
  const delay = 60000 - (Date.now() % 60000) + 500;
  setTimeout(async () => {
    try { await runMinuteJob(); } catch (e) { console.error('Job menit gagal:', e.message); }
    scheduleNextMinuteTick();
  }, delay);
}
scheduleNextMinuteTick();


// JOB B: Setiap 5 Menit - Agregasi data 5 menit terakhir ke `money_calculation`
// JOB A: Setiap 1 Menit - Simpan ke 'flow_money', Bandingkan dengan Histori, & Deteksi Anomali
// setInterval(async () => {
//   const now = new Date();
//   const docsToInsert = [];

//   // Pindahkan cache ke snaphot lokal dan reset cache utama
//   const currentSnapshot = { ...localCache };
//   localCache = {}; 

//   // Tentukan rentang waktu historis untuk pembanding (15 menit lalu)
//   const fifteenMinutesAgo = new Date(now.getTime() - 15 * 60 * 1000);

//   for (const symbol in currentSnapshot) {
//     const minuteInflow = currentSnapshot[symbol].inflow;
//     const minuteOutflow = currentSnapshot[symbol].outflow;
//     const netFlow = minuteInflow - minuteOutflow;
//     const absoluteNetFlow = Math.abs(netFlow);

//     if (minuteInflow > 0 || minuteOutflow > 0) {
//       // Siapkan dokumen untuk MongoDB
//       docsToInsert.push({
//         symbol,
//         inflow: minuteInflow,
//         outflow: minuteOutflow,
//         net_flow: netFlow,
//         timestamp: now
//       });

//       // 🔥 MESIN PEMBANDING OTOMATIS HISTORIS 🔥
//       try {
//         // Ambil rata-rata absolute net_flow koin ini selama 15 menit terakhir dari MongoDB
//         const stats = await FlowMoney.aggregate([
//           { 
//             $match: { 
//               symbol: symbol, 
//               timestamp: { $gte: fifteenMinutesAgo, $lt: now } 
//             } 
//           },
//           { 
//             $group: { 
//               _id: "$symbol", 
//               avgNetFlow: { $avg: { $abs: "$net_flow" } } 
//             } 
//           }
//         ]);

//         // Jika ada data historis, gunakan. Jika data baru (kosong), bandingkan dengan nilai minimum default
//         const avgHistoricalNet = stats.length > 0 ? stats[0].avgNetFlow : 10000; 

//         // Deteksi jika volume menit ini melebihi ambang batas dasar DAN terjadi lonjakan di atas rata-rata histori
//         if (absoluteNetFlow >= SPIKE_THRESHOLD_USDT && absoluteNetFlow > (avgHistoricalNet * 2.5)) {

//           // Hitung berapa kali lipat lonjakannya
//           const multiplier = (absoluteNetFlow / avgHistoricalNet).toFixed(1);

//           const flowDirection = netFlow > 0 ? '🟢 *ANOMALI LIQUIDATION INFLOW (PUMP)*' : '🔴 *ANOMALI LIQUIDATION OUTFLOW (DUMP)*';
//           const formattedNet = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(absoluteNetFlow);
//           const formattedAvg = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(avgHistoricalNet);

//           const alertMessage = `${flowDirection}\n\n` +
//             `• *Coin:* #${symbol}\n` +
//             `• *Net Flow Menit Ini:* ${formattedNet} (🔥 *${multiplier}x lipat* lebih tinggi!)\n` +
//             `• *Rata-rata 15m Lalu:* ${formattedAvg}\n` +
//             `• *Detail Transaksi Menit Ini:*\n` +
//             `   ↳ Inflow (Short Liq): $${minuteInflow.toLocaleString(undefined, {maximumFractionDigits:0})}\n` +
//             `   ↳ Outflow (Long Liq): $${minuteOutflow.toLocaleString(undefined, {maximumFractionDigits:0})}\n` +
//             `• *Waktu Analisis:* ${now.toLocaleTimeString()}`;

//           sendTelegramAlert(alertMessage);
//         }
//       } catch (err) {
//         console.error(`⚠️ Gagal membandingkan histori untuk ${symbol}:`, err.message);
//       }
//     }
//   }

//   // Masukkan data mentah menit ini secara massal ke MongoDB
//   if (docsToInsert.length > 0) {
//     try {
//       await FlowMoney.insertMany(docsToInsert);
//       console.log(`⏱️ [1 Menit] Berhasil menganalisis & menyimpan ${docsToInsert.length} data koin.`);
//     } catch (err) {
//       console.error('⚠️ Gagal menyimpan ke flow_money:', err.message);
//     }
//   }
// }, 60 * 1000);


// Penanganan global error agar skrip tidak mati mendadak
process.on('unhandledRejection', (reason) => {
  console.error('⚠️ Unhandled Rejection Terdeteksi:', reason);
});

module.exports = { bus, sendTelegramAlert };
