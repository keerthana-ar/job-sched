import { RedisStorageAdapter } from '../core/storage/RedisStorageAdapter.js';
import { WorkerNode } from '../core/worker/WorkerNode.js';
import { ClusterBus } from '../core/ClusterBus.js';

const redisUrl = process.env.REDIS_URL || 'redis://127.0.0.1:6379';
const idArg = process.argv.find((a) => a.startsWith('--id='))?.split('=')[1] || process.env.NODE_ID || `Worker-${process.pid}`;
const concurrency = parseInt(process.env.CONCURRENCY || '4', 10);
const leaseTtlMs = parseInt(process.env.LEASE_TTL_MS || '4000', 10);

console.log(`[Process ${process.pid}] Starting Worker Node: ${idArg}`);
console.log(`Connecting to Redis at: ${redisUrl} (Concurrency: ${concurrency}, Lease TTL: ${leaseTtlMs}ms)`);

const storage = new RedisStorageAdapter(redisUrl);
const worker = new WorkerNode(idArg, storage, { concurrency, leaseTtlMs });

worker.start().then(() => {
  console.log(`[Worker ${idArg}] Running (PID: ${process.pid}). Ready to consume jobs.`);
});

ClusterBus.getInstance().subscribe((event) => {
  console.log(`[${new Date(event.timestamp).toISOString()}] [${event.level}] [${event.source}] ${event.message}`);
});

process.on('SIGTERM', async () => {
  console.log(`[Worker ${idArg}] SIGTERM received. Gracefully stopping...`);
  worker.stop();
  await storage.disconnect();
  process.exit(0);
});

process.on('SIGINT', async () => {
  console.log(`[Worker ${idArg}] SIGINT received. Shutting down...`);
  worker.stop();
  await storage.disconnect();
  process.exit(0);
});
