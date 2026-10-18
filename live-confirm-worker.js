// Spawner: menjalankan live-confirm-child.js sebagai process terpisah per (symbol, arah).
const { fork } = require('child_process');
const path = require('path');

const MAX_MS = 10 * 60 * 1000;
const active = new Map(); // `${symbol}:${sign}` -> child

/**
 * onConfirmed(result) dipanggil saat semua konfirmasi 1m & 5m terpenuhi (worker lalu mati sendiri).
 * onTimeout() dipanggil jika >= maxMs tanpa konfirmasi lengkap (worker mati).
 */
async function startLiveConfirm(symbol, sign, { onConfirmed, onTimeout, maxMs = MAX_MS } = {}) {
  const key = `${symbol}:${sign}`;
  if (active.has(key)) return false;

  const child = fork(path.join(__dirname, 'live-confirm-child.js'), [symbol, String(sign), String(maxMs)]);
  active.set(key, child);

  // Pengaman: parent ikut mematikan child jika lewat batas waktu
  const guard = setTimeout(() => child.kill('SIGKILL'), maxMs + 5000);

  child.on('message', msg => {
    if (msg.type === 'confirmed' && onConfirmed) Promise.resolve(onConfirmed(msg)).catch(e => console.error(e.message));
    else if (msg.type === 'timeout') {
      console.log(`⌛ Live-confirm ${symbol} timeout ${maxMs / 60000}m`);
      if (onTimeout) Promise.resolve(onTimeout()).catch(() => {});
    }
  });
  child.on('exit', () => { clearTimeout(guard); active.delete(key); });
  child.on('error', e => console.error(`Live-confirm ${symbol}:`, e.message));
  return true;
}

// Matikan semua worker saat engine berhenti
process.on('exit', () => active.forEach(c => c.kill('SIGTERM')));

module.exports = { startLiveConfirm };
