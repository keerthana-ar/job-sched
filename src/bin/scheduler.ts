import { RedisStorageAdapter } from '../core/storage/RedisStorageAdapter.js';
import { SchedulerNode } from '../core/scheduler/SchedulerNode.js';
import { ClusterBus } from '../core/ClusterBus.js';

const redisUrl = process.env.REDIS_URL || 'redis://127.0.0.1:6379';
const idArg = process.argv.find((a) => a.startsWith('--id='))?.split('=')[1] || process.env.NODE_ID || `Scheduler-${process.pid}`;
const pollIntervalMs = parseInt(process.env.POLL_INTERVAL_MS || '300', 10);
const batchSize = parseInt(process.env.BATCH_SIZE || '50', 10);

console.log(`[Process ${process.pid}] Starting Scheduler Node: ${idArg}`);
console.log(`Connecting to Redis at: ${redisUrl}`);

const storage = new RedisStorageAdapter(redisUrl);
const scheduler = new SchedulerNode(idArg, storage, { pollIntervalMs, batchSize });

scheduler.start().then(() => {
  console.log(`[Scheduler ${idArg}] Running (PID: ${process.pid}). Polling every ${pollIntervalMs}ms.`);
});

// Broadcast log over stdout for process supervisors
ClusterBus.getInstance().subscribe((event) => {
  console.log(`[${new Date(event.timestamp).toISOString()}] [${event.level}] [${event.source}] ${event.message}`);
});

process.on('SIGTERM', async () => {
  console.log(`[Scheduler ${idArg}] SIGTERM received. Shutting down...`);
  scheduler.stop();
  await storage.disconnect();
  process.exit(0);
});

process.on('SIGINT', async () => {
  console.log(`[Scheduler ${idArg}] SIGINT received. Shutting down...`);
  scheduler.stop();
  await storage.disconnect();
  process.exit(0);
});
