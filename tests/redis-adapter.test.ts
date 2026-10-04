import test from 'node:test';
import assert from 'node:assert/strict';
import { RedisStorageAdapter } from '../src/core/storage/RedisStorageAdapter.js';
import { Job } from '../src/core/types.js';

test('Redis Adapter: Atomic Lookahead Claim with Lua Script prevents duplicate dispatches', async (t) => {
  const adapter = new RedisStorageAdapter('redis://127.0.0.1:6379');
  await adapter.clear();

  const delayedKey = 'test:queue:delayed_jobs';
  const readyKey = 'test:queue:ready_jobs';
  const now = Date.now();
  const totalJobs = 50;

  // Enqueue 50 jobs with score <= now
  for (let i = 1; i <= totalJobs; i++) {
    const jobId = `redis_job_${i}`;
    await adapter.zadd(delayedKey, now - 100, jobId);
    await adapter.set(`job:${jobId}`, JSON.stringify({
      id: jobId,
      name: `Test-Job-${i}`,
      status: 'SCHEDULED',
      priority: i % 2 === 0 ? 'CRITICAL' : 'NORMAL',
    }));
  }

  // 3 concurrent schedulers executing Lua script at the exact same moment
  const results = await Promise.all([
    adapter.atomicClaimDueJobs(delayedKey, readyKey, now, 25, 'Scheduler-1'),
    adapter.atomicClaimDueJobs(delayedKey, readyKey, now, 25, 'Scheduler-2'),
    adapter.atomicClaimDueJobs(delayedKey, readyKey, now, 25, 'Scheduler-3'),
  ]);

  const claimed1 = results[0];
  const claimed2 = results[1];
  const claimed3 = results[2];

  const totalClaimed = claimed1.length + claimed2.length + claimed3.length;
  assert.equal(totalClaimed, totalJobs, `Expected exactly ${totalJobs} claimed across all schedulers`);

  // Assert ZERO duplicates between any pairs
  const set1 = new Set(claimed1);
  const set2 = new Set(claimed2);
  const set3 = new Set(claimed3);

  for (const id of set1) {
    assert.ok(!set2.has(id), `Duplicate job ${id} found between Scheduler 1 & 2!`);
    assert.ok(!set3.has(id), `Duplicate job ${id} found between Scheduler 1 & 3!`);
  }
  for (const id of set2) {
    assert.ok(!set3.has(id), `Duplicate job ${id} found between Scheduler 2 & 3!`);
  }

  // Delayed queue must now be completely empty
  const remainingDelayed = await adapter.zcard(delayedKey);
  assert.equal(remainingDelayed, 0, 'Delayed ZSET must be empty after atomic claim');

  await adapter.disconnect();
});

test('Redis Adapter: Fencing Tokens reject stale writes from slow/paused workers', async () => {
  const adapter = new RedisStorageAdapter('redis://127.0.0.1:6379');
  await adapter.clear();

  const jobId = 'job_fencing_test_1';
  const initialJob: Job = {
    id: jobId,
    name: 'Fencing-Transfer',
    taskType: 'DATA_PROCESSING',
    payload: {},
    priority: 'HIGH',
    status: 'RUNNING',
    schedule: { type: 'IMMEDIATE' },
    retryPolicy: { maxRetries: 3, baseDelayMs: 1000, backoffMultiplier: 2, jitter: false },
    currentAttempt: 1,
    maxRetries: 3,
    timeoutMs: 5000,
    nextRunTime: Date.now(),
    createdAt: Date.now(),
    updatedAt: Date.now(),
    executionHistory: [],
  };

  await adapter.set(`job:${jobId}`, JSON.stringify(initialJob));

  // Worker-1 gets lease with token 1
  const token1 = await adapter.incrementFencingToken(jobId);
  assert.equal(token1, 1);

  // Worker-1 hangs (e.g. GC pause). Zombie Reaper times out lease and advances fencing token to 2
  const token2 = await adapter.incrementFencingToken(jobId);
  assert.equal(token2, 2);

  // Worker-1 wakes up and tries to complete job using stale token 1
  const staleJobData = JSON.stringify({ ...initialJob, status: 'SUCCESS', result: 'from-slow-worker-1' });
  const staleWriteResult = await adapter.completeJobAtomic(jobId, token1, staleJobData);

  assert.equal(staleWriteResult.success, false, 'Stale worker write carrying token 1 MUST be rejected');
  assert.ok(staleWriteResult.reason?.includes('STALE_FENCING_TOKEN'));

  // Worker-2 completes job using valid token 2
  const validJobData = JSON.stringify({ ...initialJob, status: 'SUCCESS', result: 'from-fast-worker-2' });
  const validWriteResult = await adapter.completeJobAtomic(jobId, token2, validJobData);

  assert.equal(validWriteResult.success, true, 'Valid worker write carrying token 2 MUST succeed');

  const finalJob = JSON.parse((await adapter.get(`job:${jobId}`))!);
  assert.equal(finalJob.result, 'from-fast-worker-2', 'State must reflect the valid write, not the stale write');

  await adapter.disconnect();
});

test('Redis Adapter: Priority queue pops CRITICAL before NORMAL', async () => {
  const adapter = new RedisStorageAdapter('redis://127.0.0.1:6379');
  await adapter.clear();

  const normalQueue = 'test:queue:ready:NORMAL';
  const criticalQueue = 'test:queue:ready:CRITICAL';

  // Push normal job first
  await adapter.rpush(normalQueue, 'job_normal_1');
  // Push critical job second
  await adapter.rpush(criticalQueue, 'job_critical_1');

  // Pop with priority ordering: CRITICAL -> NORMAL
  const pop1 = await adapter.lpopPriority([criticalQueue, normalQueue]);
  assert.ok(pop1);
  assert.equal(pop1.item, 'job_critical_1', 'Critical job must be popped first even though enqueued second');

  const pop2 = await adapter.lpopPriority([criticalQueue, normalQueue]);
  assert.ok(pop2);
  assert.equal(pop2.item, 'job_normal_1', 'Normal job must be popped second');

  await adapter.disconnect();
});

test('Redis Adapter: Idempotency cache skips duplicate execution', async () => {
  const adapter = new RedisStorageAdapter('redis://127.0.0.1:6379');
  await adapter.clear();

  const key = 'idem_payment_txn_999';
  const expectedResult = JSON.stringify({ paymentId: 'pay_123', status: 'PAID' });

  // Initially not found
  const before = await adapter.getIdempotency(key);
  assert.equal(before, null);

  // Set idempotency
  await adapter.setIdempotency(key, expectedResult, 3600);

  // Subsequent fetch finds cached result
  const after = await adapter.getIdempotency(key);
  assert.equal(after, expectedResult);

  await adapter.disconnect();
});

test('Redis Adapter: Sliding window rate limiter throttles burst traffic', async () => {
  const adapter = new RedisStorageAdapter('redis://127.0.0.1:6379');
  await adapter.clear();

  const limitKey = 'test_webhook_queue';
  const maxPerSec = 5;

  // First 5 requests must succeed
  for (let i = 0; i < maxPerSec; i++) {
    const allowed = await adapter.checkRateLimit(limitKey, maxPerSec);
    assert.equal(allowed, true, `Request ${i + 1} should be allowed`);
  }

  // 6th request within same second must be denied
  const denied = await adapter.checkRateLimit(limitKey, maxPerSec);
  assert.equal(denied, false, '6th request exceeding 5/sec limit must be denied');

  await adapter.disconnect();
});
