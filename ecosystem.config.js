'use strict';

module.exports = {
  apps: [
    {
      name        : 'liquidation-engine',
      script      : 'server.js',
      instances   : 1,
      exec_mode   : 'fork',
      watch       : false,
      autorestart : true,
      max_restarts: 10,
      restart_delay: 5000,

      // Logging
      out_file    : './logs/out.log',
      error_file  : './logs/err.log',
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      merge_logs  : true,

      // Env
      env: {
        NODE_ENV: 'production',
      },
    },
  ],
};
