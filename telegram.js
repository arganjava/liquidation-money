const axios = require('axios');
require('dotenv').config();

const TOKEN = process.env.TELEGRAM_BOT_TOKEN;
// Bisa lebih dari satu chat id, pisahkan dengan koma: 123,456
const CHAT_IDS = (process.env.TELEGRAM_CHAT_IDS || '')
  .split(',').map(s => s.trim()).filter(Boolean);

const api = method => `https://api.telegram.org/bot${TOKEN}/${method}`;
const errMsg = err => err.response?.data?.description || err.message;

async function sendMessage(chat_id, text, extra = {}) {
  const res = await axios.post(api('sendMessage'), {
    chat_id, text, parse_mode: 'Markdown', ...extra,
  });
  return res.data.result; // { message_id, chat: { id }, ... }
}

async function editMessage(chat_id, message_id, text, extra = {}) {
  const res = await axios.post(api('editMessageText'), {
    chat_id, message_id, text, parse_mode: 'Markdown', ...extra,
  });
  return res.data.result;
}

// Kirim ke semua chat id. Return: [{ chat_id, message_id }] untuk yang berhasil.
async function broadcast(text, extra = {}) {
  if (!TOKEN || CHAT_IDS.length === 0) {
    console.error('❌ Telegram belum dikonfigurasi (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_IDS)');
    return [];
  }
  const sent = [];
  await Promise.all(CHAT_IDS.map(async chat_id => {
    try {
      const r = await sendMessage(chat_id, text, extra);
      sent.push({ chat_id: String(chat_id), message_id: r.message_id });
    } catch (err) {
      console.error('❌ Telegram Alert Error:', errMsg(err));
    }
  }));
  return sent;
}

async function answerCallback(callback_query_id, text = '', show_alert = false) {
  try {
    await axios.post(api('answerCallbackQuery'), { callback_query_id, text, show_alert });
  } catch (err) {
    console.error('⚠️ answerCallbackQuery gagal:', errMsg(err));
  }
}

// Long polling: hanya ambil callback_query (klik tombol inline)
async function getUpdates(offset, timeoutSec = 30) {
  const res = await axios.get(api('getUpdates'), {
    params: { offset, timeout: timeoutSec, allowed_updates: JSON.stringify(['callback_query']) },
    timeout: (timeoutSec + 10) * 1000,
  });
  return res.data.result;
}

module.exports = {
  CHAT_IDS, sendMessage, editMessage, broadcast, answerCallback, getUpdates, errMsg,
};