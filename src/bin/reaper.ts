import { RedisStorageAdapter } from '../core/storage/RedisStorageAdapter.js';
import { ZombieReaper } from '../core/recovery/ZombieReaper.js';
import { ClusterBus } from '../core/ClusterBus.js';

const redisUrl = process.env.REDIS_URL || 'redis://127.0.0.1:6379';
const scanIntervalMs = parseInt(process.env.REAPER_SCAN_INTERVAL_MS || '1500', 10);

console.log(`[Process ${process.pid}] Starting Zombie Job Reaper Scavenger`);
console.log(`Connecting to Redis at: ${redisUrl} (Scan Interval: ${scanIntervalMs}ms)`);

const storage = new RedisStorageAdapter(redisUrl);
const reaper = new ZombieReaper(storage, scanIntervalMs);

reaper.start();
console.log(`[ZombieReaper] Scavenger running (PID: ${process.pid}). Monitoring expired leases.`);

ClusterBus.getInstance().subscribe((event) => {
  console.log(`[${new Date(event.timestamp).toISOString()}] [${event.level}] [${event.source}] ${event.message}`);
});

process.on('SIGTERM', async () => {
  console.log('[ZombieReaper] SIGTERM received. Shutting down...');
  reaper.stop();
  await storage.disconnect();
  process.exit(0);
});

process.on('SIGINT', async () => {
  console.log('[ZombieReaper] SIGINT received. Shutting down...');
  reaper.stop();
  await storage.disconnect();
  process.exit(0);
});
