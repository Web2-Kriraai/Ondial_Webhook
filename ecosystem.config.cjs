/**
 * Production PM2 ecosystem — /var/www/html/Ondial-api
 * Fork × 1 only. Do not cluster: Express starts BullMQ workers in-process.
 *
 *   cd /var/www/html/Ondial-api
 *   pm2 start ecosystem.config.cjs
 *   pm2 save
 */
module.exports = {
  apps: [
    {
      name: 'ondial-webhook',
      script: 'index.js',
      cwd: '/var/www/html/Ondial-api',
      instances: 1,
      exec_mode: 'fork',
      listen_timeout: 15000,
      kill_timeout: 8000,
      exp_backoff_restart_delay: 500,
      max_memory_restart: '600M',
      time: true,
      env_file: '.env',
      env: {
        NODE_ENV: 'production',
        PORT: 9000,
      },
    },
  ],
};
