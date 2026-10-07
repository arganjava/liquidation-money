require('dotenv').config();
const path = require('path');
const http = require('http');
const express = require('express');
const { WebSocketServer } = require('ws');

// Engine dijalankan dalam proses yang sama agar live feed bisa lewat event bus
const { bus, sendTelegramAlert } = require('./liquidation-engine');
const { FlowMoney, Setting } = require('./models');

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const h = fn => (req, res) => fn(req, res).catch(e => res.status(500).json({ error: e.message }));
const esc = s => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

// ---------- Money Flow ----------
app.get('/api/flow', h(async (req, res) => {
  const { symbol, from, to, sort = 'timestamp', order = 'desc', page = 1, limit = 50 } = req.query;
  const q = {};
  if (symbol) q.symbol = { $regex: esc(symbol.toUpperCase()) };
  if (from || to) {
    q.timestamp = {};
    if (from) q.timestamp.$gte = new Date(from);
    if (to) q.timestamp.$lte = new Date(to);
  }
  const s = ['timestamp', 'symbol', 'inflow', 'outflow', 'net_flow'].includes(sort) ? sort : 'timestamp';
  const lim = Math.min(+limit || 50, 500);
  const skip = (Math.max(+page, 1) - 1) * lim;
  const [rows, total] = await Promise.all([
    FlowMoney.find(q).sort({ [s]: order === 'asc' ? 1 : -1 }).skip(skip).limit(lim).lean(),
    FlowMoney.countDocuments(q)
  ]);
  res.json({ rows, total, page: +page, limit: lim });
}));

// ---------- Money Analisa ----------
// inflow = short liquidation (harga naik), outflow = long liquidation (harga turun).
// Rentang dibagi 2 (paruh awal vs akhir) untuk membedakan continuation vs reversal.
function getBias(n1, n2, total) {
  const sign = n => (Math.abs(n) < total * 0.1 ? 0 : n > 0 ? 1 : -1);
  const a = sign(n1), b = sign(n2);
  if (!a && !b) return 'consolidation';
  if (a && b && a !== b) return b > 0 ? 'long reversal' : 'short reversal';
  return (a || b) > 0 ? 'long continuation' : 'short continuation';
}

app.get('/api/analysis', h(async (req, res) => {
  const minutes = Math.min(+req.query.minutes || 60, 7 * 1440);
  const minTotal = +req.query.minTotal || 0;
  const to = new Date();
  const from = new Date(to - minutes * 60000);
  const mid = new Date(+from + (to - from) / 2);
  const cnt = f => ({ $sum: { $cond: [{ $gt: [f, 0] }, 1, 0] } });

  const rows = await FlowMoney.aggregate([
    { $match: { timestamp: { $gte: from, $lte: to } } },
    { $group: {
      _id: '$symbol',
      count_inflow: cnt('$inflow'),
      count_outflow: cnt('$outflow'),
      sum_inflow: { $sum: '$inflow' },
      sum_outflow: { $sum: '$outflow' },
      first_detect_time: { $min: '$timestamp' },
      last_detect_time: { $max: '$timestamp' },
      n1: { $sum: { $cond: [{ $lt: ['$timestamp', mid] }, '$net_flow', 0] } },
      n2: { $sum: { $cond: [{ $lt: ['$timestamp', mid] }, 0, '$net_flow'] } }
    } }
  ]);

  const out = rows
    .map(r => ({
      symbol: r._id,
      count_inflow: r.count_inflow, count_outflow: r.count_outflow,
      sum_inflow: r.sum_inflow, sum_outflow: r.sum_outflow,
      first_detect_time: r.first_detect_time, last_detect_time: r.last_detect_time,
      bias: getBias(r.n1, r.n2, r.sum_inflow + r.sum_outflow)
    }))
    .filter(r => r.sum_inflow + r.sum_outflow >= minTotal);
  res.json({ from, to, rows: out });
}));

// ---------- Telegram Setting ----------
app.get('/api/settings', h(async (req, res) => {
  const s = await Setting.findOne({ key: 'telegram' }).lean();
  res.json({ enabled: s ? s.enabled : true, threshold: s ? s.threshold : 5000 });
}));

app.put('/api/settings', h(async (req, res) => {
  const threshold = Number(req.body.threshold);
  if (!(threshold > 0)) return res.status(400).json({ error: 'Threshold harus > 0' });
  const s = await Setting.findOneAndUpdate(
    { key: 'telegram' },
    { enabled: !!req.body.enabled, threshold },
    { upsert: true, new: true }
  ).lean();
  res.json({ enabled: s.enabled, threshold: s.threshold });
}));

app.post('/api/settings/test', h(async (req, res) => {
  await sendTelegramAlert('✅ *Test notifikasi* dari Liquidation UI');
  res.json({ ok: true });
}));

// ---------- WebSocket live feed ----------
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });
bus.on('flow', doc => {
  const msg = JSON.stringify(doc);
  wss.clients.forEach(c => c.readyState === 1 && c.send(msg));
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`🖥️  UI: http://localhost:${PORT}`));