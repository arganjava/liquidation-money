const Binance = require('binance-api-node').default;
const mongoose = require('mongoose');
const axios = require('axios');
const dotenv = require('dotenv');
dotenv.config();

const { FlowMoney, Setting } = require('./models');
const EventEmitter = require('events');
const bus = new EventEmitter();

// ----------------------------------------------------
// 1. CONFIGURATION & NOTIFICATION SETTINGS
// ----------------------------------------------------

// TELEGRAM_BOT_TOKEN=8329903702:AAHUcZEjz_RgE6kM0GR9P0IF2Dxd1RlbU5c
// TELEGRAM_CHAT_IDS=1081770728
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_IDS;
const SPIKE_THRESHOLD_USDT = 5000;          // Alert jika net_flow > \$50,000 USDT dalam 1 menit akibat likuidasi

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
    localCache[symbol] = { inflow: 0, outflow: 0, price:0 };
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
    } else if (side === 'SELL') {
      // Long Liquidation -> Bursa melakukan market SELL paksa -> OUTFLOW
      localCache[symbol].outflow += volumeUSDT;
    }

    console.log(`💥 ${new Date().toLocaleString()} | ${symbol} | ${side} | Value: \$${volumeUSDT.toFixed(2)}`);
  }
});

// ----------------------------------------------------
// 5. SCHEDULERS & INTERVAL JOBS
// ----------------------------------------------------

// JOB A: Setiap 1 Menit - Simpan State Memori ke `flow_money` & Deteksi Spike Telegram
setInterval(async () => {
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

      // 🔥 LOGIKA DETEKSI SPIKE FLOW LIKUIDASI
      if (Math.abs(netFlow) >= SPIKE_THRESHOLD_USDT ) {
        const type = netFlow > 0 ? '🟢 *LIQUIDATION SPIKE INFLOW (SHORT SQUEEZE)*' : '🔴 *LIQUIDATION SPIKE OUTFLOW (LONG DUMP)*';
        const formattedNet = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(Math.abs(netFlow));
        
        const alertMessage = `${type}\n\n` +
          `• *Coin:* #${symbol} ${currentSnapshot[symbol].price} \n` +
          `• *Net Liquidation Flow (1m):* ${formattedNet}\n` +
          `• *Inflow (Short Liq):* $${minuteInflow.toLocaleString(undefined, {maximumFractionDigits:0})}\n` +
          `• *Outflow (Long Liq):* $${minuteOutflow.toLocaleString(undefined, {maximumFractionDigits:0})}\n` +
          `• *Waktu:* ${now.toLocaleTimeString()}`;
        const isTelegramEnabled = (await Setting.findOne({ key: 'telegram' }).lean())?.enabled ?? true;
        if (isTelegramEnabled){
          sendTelegramAlert(alertMessage);
        }
      }
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
}, 10 * 1000);


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
