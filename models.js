const mongoose = require('mongoose');

const FlowMoney = mongoose.models.flow_money || mongoose.model('flow_money', new mongoose.Schema({
  symbol: { type: String, required: true, index: true },
  inflow: { type: Number, required: true },
  outflow: { type: Number, required: true },
  net_flow: { type: Number, required: true },
  timestamp: { type: Date, default: Date.now, index: { expires: 7 * 86400 } }
}), 'flow_money');

const Setting = mongoose.models.setting || mongoose.model('setting', new mongoose.Schema({
  key: { type: String, unique: true },
  enabled: { type: Boolean, default: true },
  threshold: { type: Number, default: 5000 }
}), 'settings');

module.exports = { FlowMoney, Setting };