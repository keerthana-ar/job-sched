import { ClusterManager } from './core/ClusterManager.js';
import { createApp } from './api/server.js';
import { ClusterBus } from './core/ClusterBus.js';

const PORT = process.env.PORT || 4000;

async function main() {
  console.log('---------------------------------------------------------');
  console.log('⚡ AetherSched: Distributed Job Scheduler Starting...');
  console.log('---------------------------------------------------------');

  const cluster = ClusterManager.getInstance();
  await cluster.bootstrap({ schedulerCount: 2, workerCount: 3 });

  // Seed sample demonstration jobs
  await cluster.createJob({
    name: 'Recurring-Metrics-Aggregator',
    taskType: 'DATA_PROCESSING',
    priority: 'HIGH',
    schedule: {
      type: 'CRON',
      cronExpr: '*/15 * * * * *', // every 15 seconds
    },
    payload: { batchSize: 2500, durationMs: 1200 },
    retryPolicy: { maxRetries: 3, baseDelayMs: 2000, backoffMultiplier: 2, jitter: true },
  });

  await cluster.createJob({
    name: 'User-Billing-Sweep',
    taskType: 'HTTP_WEBHOOK',
    priority: 'CRITICAL',
    schedule: {
      type: 'DELAYED',
      runAt: Date.now() + 4000, // runs in 4 seconds
    },
    payload: { url: 'https://billing.internal/charge', latencyMs: 800 },
    retryPolicy: { maxRetries: 3, baseDelayMs: 1500, backoffMultiplier: 2, jitter: true },
  });

  await cluster.createJob({
    name: 'Nightly-Database-Compaction',
    taskType: 'DB_CLEANUP',
    priority: 'NORMAL',
    schedule: {
      type: 'DELAYED',
      runAt: Date.now() + 8000, // runs in 8 seconds
    },
    payload: {},
  });

  const app = createApp();
  app.listen(PORT, () => {
    console.log(`\n🚀 AetherSched Server & UI ready at: http://localhost:${PORT}`);
    console.log(`📊 Open http://localhost:${PORT} in your browser to view the live dashboard!`);
    console.log('---------------------------------------------------------\n');

    ClusterBus.getInstance().emit(
      'INFO',
      'System',
      `Cluster initialized. Observability dashboard live at http://localhost:${PORT}`
    );
  });
}

main().catch((err) => {
  console.error('Fatal bootstrapping error:', err);
  process.exit(1);
});
