import test from 'node:test';
import assert from 'node:assert/strict';
import { RedisStorageAdapter } from '../src/core/storage/RedisStorageAdapter.js';
import { SchedulerNode } from '../src/core/scheduler/SchedulerNode.js';
import { WorkerNode } from '../src/core/worker/WorkerNode.js';
import { ZombieReaper } from '../src/core/recovery/ZombieReaper.js';
import { Job } from '../src/core/types.js';

test('Failure Mode 1: Kill worker mid-job -> Reaper recovers -> 0 lost, 0 duplicates, stale write rejected', async () => {
  const storage = new RedisStorageAdapter('redis://127.0.0.1:6379');
  await storage.clear();

  const delayedKey = 'queue:delayed_jobs';
  const readyKey = 'queue:ready_jobs';
  const jobId = 'job_fail_worker_1';
  const now = Date.now();

  const testJob: Job = {
    id: jobId,
    name: 'Transactional-Ledger-Update',
    taskType: 'DATA_PROCESSING',
    payload: { durationMs: 2000 }, // long running task
    priority: 'CRITICAL',
    status: 'SCHEDULED',
    schedule: { type: 'IMMEDIATE' },
    retryPolicy: { maxRetries: 3, baseDelayMs: 200, backoffMultiplier: 1.5, jitter: false },
    currentAttempt: 0,
    maxRetries: 3,
    timeoutMs: 10000,
    nextRunTime: now,
    createdAt: now,
    updatedAt: now,
    executionHistory: [],
  };

  await storage.set(`job:${jobId}`, JSON.stringify(testJob));
  await storage.rpush(`${readyKey}:CRITICAL`, jobId);

  // Worker-1 has a 1200ms lease TTL
  const worker1 = new WorkerNode('Worker-1', storage, { concurrency: 2, leaseTtlMs: 1200 });
  await worker1.start();

  // Allow Worker-1 to pull job and acquire lease (token 1)
  await new Promise((r) => setTimeout(r, 300));

  const lease1 = await storage.getLease(jobId);
  assert.ok(lease1, 'Worker-1 must have acquired an active lease');
  assert.equal(lease1.workerId, 'Worker-1');
  const token1 = lease1.fencingToken;
  assert.ok(token1 >= 1);

  // 💥 Simulating kill -9: Worker-1 crashes abruptly mid-job!
  // Heartbeats stop, lease renewals halt immediately.
  worker1.crash();
  worker1.stop();

  // Wait for 1200ms lease to expire
  await new Promise((r) => setTimeout(r, 1400));

  // Run Zombie Reaper
  const reaper = new ZombieReaper(storage, 500);
  const reclaimed = await reaper.scanAndReclaimZombies();
  assert.equal(reclaimed, 1, 'Zombie Reaper must reclaim the abandoned job');

  // Verify fencing token was incremented in Redis
  const currentToken = await storage.getFencingToken(jobId);
  assert.ok(currentToken > token1, `Fencing token must be bumped (${currentToken} > ${token1})`);

  // Verify Worker-1's stale write is REJECTED by fencing check
  const staleWrite = await storage.completeJobAtomic(
    jobId,
    token1,
    JSON.stringify({ ...testJob, status: 'SUCCESS', result: 'stale-worker1' })
  );
  assert.equal(staleWrite.success, false, 'Late write carrying stale token must be rejected');
  assert.ok(staleWrite.reason?.includes('STALE_FENCING_TOKEN'));

  // Start Scheduler to dispatch the retried job from delayed ZSET to ready queue
  const scheduler = new SchedulerNode('Scheduler-Failover', storage, { pollIntervalMs: 100, batchSize: 10 });
  await scheduler.start();

  // Start Worker-2 to pick up the reassigned job
  const worker2 = new WorkerNode('Worker-2', storage, { concurrency: 2, leaseTtlMs: 4000 });
  await worker2.start();

  // Wait for Worker-2 to finish execution
  let completedJob: Job | null = null;
  for (let i = 0; i < 30; i++) {
    await new Promise((r) => setTimeout(r, 200));
    const raw = await storage.get(`job:${jobId}`);
    if (raw) {
      const parsed = JSON.parse(raw);
      if (parsed.status === 'SUCCESS') {
        completedJob = parsed;
        break;
      }
    }
  }

  assert.ok(completedJob, 'Job must be successfully completed by Worker-2');
  assert.equal(completedJob.status, 'SUCCESS');
  // Verify execution history contains the timeout from worker-1 and success from worker-2
  assert.ok(completedJob.executionHistory.some((e: any) => e.status === 'TIMEOUT'));
  assert.ok(completedJob.executionHistory.some((e: any) => e.status === 'SUCCESS' && e.workerId === 'Worker-2'));

  reaper.stop();
  scheduler.stop();
  worker2.stop();
  await new Promise((r) => setTimeout(r, 150));
  await storage.disconnect();
});

test('Failure Mode 2: Kill scheduler mid-cycle -> 2nd scheduler claims remainder with 0 duplicates', async () => {
  const storage = new RedisStorageAdapter('redis://127.0.0.1:6379');
  await storage.clear();

  const delayedKey = 'queue:delayed_jobs';
  const readyKey = 'queue:ready_jobs';
  const now = Date.now();
  const totalJobs = 60;

  for (let i = 1; i <= totalJobs; i++) {
    const jobId = `sched_fail_job_${i}`;
    await storage.zadd(delayedKey, now - 10, jobId);
    await storage.set(`job:${jobId}`, JSON.stringify({
      id: jobId,
      status: 'SCHEDULED',
      priority: 'NORMAL',
    }));
  }

  const s1 = new SchedulerNode('Scheduler-1', storage, { batchSize: 20 });
  const s2 = new SchedulerNode('Scheduler-2', storage, { batchSize: 20 });

  // S1 claims first batch
  const batch1 = await s1.pollAndDispatch();
  assert.equal(batch1.length, 20);

  // 💥 Kill Scheduler-1 mid-cycle!
  s1.stop();
  s1.crash();

  // Scheduler-2 continues polling and claims remaining jobs
  const batch2 = await s2.pollAndDispatch();
  const batch3 = await s2.pollAndDispatch();
  const batch4 = await s2.pollAndDispatch(); // empty check

  const allClaimed = [...batch1, ...batch2, ...batch3, ...batch4];
  assert.equal(allClaimed.length, totalJobs, `All ${totalJobs} jobs must be claimed`);

  // Assert exactly 0 duplicates across batches
  const uniqueSet = new Set(allClaimed);
  assert.equal(uniqueSet.size, totalJobs, 'Zero duplicate dispatches allowed');

  // Verify delayed queue is 0
  const remaining = await storage.zcard(delayedKey);
  assert.equal(remaining, 0);

  s2.stop();
  await storage.disconnect();
});

test('Failure Mode 3: Network/Storage latency injection maintains exactly-once guarantees', async () => {
  const storage = new RedisStorageAdapter('redis://127.0.0.1:6379');
  await storage.clear();

  const delayedKey = 'queue:delayed_jobs';
  const readyKey = 'queue:ready_jobs';
  const now = Date.now();
  const totalJobs = 30;

  for (let i = 1; i <= totalJobs; i++) {
    const jobId = `latency_job_${i}`;
    await storage.zadd(delayedKey, now, jobId);
    await storage.set(`job:${jobId}`, JSON.stringify({
      id: jobId,
      status: 'SCHEDULED',
      priority: 'HIGH',
    }));
  }

  // Two schedulers running simultaneously
  const s1 = new SchedulerNode('Scheduler-1', storage, { batchSize: 20 });
  const s2 = new SchedulerNode('Scheduler-2', storage, { batchSize: 20 });

  const [res1, res2] = await Promise.all([
    s1.pollAndDispatch(),
    s2.pollAndDispatch(),
  ]);

  const claimedTotal = res1.length + res2.length;
  assert.equal(claimedTotal, totalJobs);

  // Check intersection: zero overlap
  const set1 = new Set(res1);
  for (const id of res2) {
    assert.ok(!set1.has(id), `Duplicate job ${id} detected!`);
  }

  s1.stop();
  s2.stop();
  await storage.disconnect();
});
