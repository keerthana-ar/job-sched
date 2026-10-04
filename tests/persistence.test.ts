import test from 'node:test';
import assert from 'node:assert/strict';
import { RedisStorageAdapter } from '../src/core/storage/RedisStorageAdapter.js';
import { SchedulerNode } from '../src/core/scheduler/SchedulerNode.js';
import { WorkerNode } from '../src/core/worker/WorkerNode.js';
import { Job } from '../src/core/types.js';

test('Persistence & Recovery: Cluster shutdown & restart preserves scheduled jobs', async () => {
  const redisUrl = 'redis://127.0.0.1:6379';
  const storage1 = new RedisStorageAdapter(redisUrl);
  await storage1.clear();

  const delayedKey = 'queue:delayed_jobs';
  const readyKey = 'queue:ready_jobs';
  const now = Date.now();
  const jobIds = ['persist_job_1', 'persist_job_2', 'persist_job_3'];

  // 1. Enqueue 3 delayed jobs due in 800ms
  for (const id of jobIds) {
    const job: Job = {
      id,
      name: `Persisted-Task-${id}`,
      taskType: 'DATA_PROCESSING',
      payload: { durationMs: 100 },
      priority: 'HIGH',
      status: 'SCHEDULED',
      schedule: { type: 'DELAYED', runAt: now + 800 },
      retryPolicy: { maxRetries: 2, baseDelayMs: 500, backoffMultiplier: 2, jitter: false },
      currentAttempt: 0,
      maxRetries: 2,
      timeoutMs: 5000,
      nextRunTime: now + 800,
      createdAt: now,
      updatedAt: now,
      executionHistory: [],
    };
    await storage1.set(`job:${id}`, JSON.stringify(job));
    await storage1.zadd(delayedKey, now + 800, id);
  }

  // Force Redis persistence save
  try {
    await storage1.getRawClient().save();
  } catch {
    // Background save might already be in progress
  }

  // 2. Simulate total catastrophic crash: disconnect and destroy all in-memory processes
  await storage1.disconnect();

  // Wait 1000ms until jobs are due in time
  await new Promise((r) => setTimeout(r, 1000));

  // 3. Cluster restart: brand new processes spin up from scratch
  const storage2 = new RedisStorageAdapter(redisUrl);

  // Assert jobs survived in Redis storage intact
  for (const id of jobIds) {
    const raw = await storage2.get(`job:${id}`);
    assert.ok(raw, `Job ${id} must survive cluster shutdown in Redis persistence layer`);
    const parsed = JSON.parse(raw);
    assert.equal(parsed.status, 'SCHEDULED');
  }

  const scheduler = new SchedulerNode('Scheduler-Recovered', storage2, { pollIntervalMs: 100, batchSize: 10 });
  const worker = new WorkerNode('Worker-Recovered', storage2, { concurrency: 3, leaseTtlMs: 5000 });

  await scheduler.start();
  await worker.start();

  // Wait for all 3 jobs to be executed to completion
  let allCompleted = false;
  for (let i = 0; i < 25; i++) {
    await new Promise((r) => setTimeout(r, 200));
    let completedCount = 0;
    for (const id of jobIds) {
      const raw = await storage2.get(`job:${id}`);
      if (raw && JSON.parse(raw).status === 'SUCCESS') {
        completedCount++;
      }
    }
    if (completedCount === jobIds.length) {
      allCompleted = true;
      break;
    }
  }

  assert.ok(allCompleted, 'All persisted jobs must be dispatched and executed after cluster recovery');

  scheduler.stop();
  worker.stop();
  await storage2.disconnect();
});
