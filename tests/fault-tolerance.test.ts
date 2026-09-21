import test from 'node:test';
import assert from 'node:assert/strict';
import { MemoryClusterStore } from '../src/core/storage/MemoryClusterStore.js';
import { WorkerNode } from '../src/core/worker/WorkerNode.js';
import { ZombieReaper } from '../src/core/recovery/ZombieReaper.js';
import { Job } from '../src/core/types.js';

test('Fault Tolerance: Zombie reaper reclaims job when worker crashes mid-flight', async () => {
  const storage = new MemoryClusterStore();
  const readyKey = 'queue:ready_jobs';
  const jobId = 'job_crash_test_1';
  const now = Date.now();

  const testJob: Job = {
    id: jobId,
    name: 'Critical-Financial-Transfer',
    taskType: 'DATA_PROCESSING',
    payload: { durationMs: 4000 }, // Takes 4 seconds
    priority: 'CRITICAL',
    status: 'SCHEDULED',
    schedule: { type: 'IMMEDIATE' },
    retryPolicy: { maxRetries: 3, baseDelayMs: 500, backoffMultiplier: 2, jitter: false },
    currentAttempt: 0,
    maxRetries: 3,
    timeoutMs: 10000,
    nextRunTime: now,
    createdAt: now,
    updatedAt: now,
    executionHistory: [],
  };

  await storage.set(`job:${jobId}`, JSON.stringify(testJob));
  await storage.rpush(readyKey, jobId);

  // Worker-1 has a 1000ms lease TTL
  const worker1 = new WorkerNode('Worker-1', storage, { concurrency: 2, leaseTtlMs: 1000 });
  await worker1.start();

  // Wait for worker to pull job and acquire lease
  await new Promise((r) => setTimeout(r, 400));

  const lease = await storage.getLease(jobId);
  assert.ok(lease, 'Worker-1 should have acquired an execution lease');
  assert.equal(lease.workerId, 'Worker-1');

  // Crash Worker-1 abruptly!
  worker1.crash();

  // Wait for the 1000ms lease to expire
  await new Promise((r) => setTimeout(r, 1200));

  // Run the Zombie Reaper
  const reaper = new ZombieReaper(storage);
  const reclaimedCount = await reaper.scanAndReclaimZombies();

  assert.equal(reclaimedCount, 1, 'Reaper should reclaim exactly 1 zombie job');

  // Verify expired lease is deleted
  const leaseAfter = await storage.getLease(jobId);
  assert.equal(leaseAfter, null, 'Expired lease should have been removed');

  // Verify job is rescheduled with incremented attempt
  const updatedJobJson = await storage.get(`job:${jobId}`);
  assert.ok(updatedJobJson);
  const updatedJob: Job = JSON.parse(updatedJobJson);

  assert.equal(updatedJob.status, 'RETRYING', 'Job should be in RETRYING state');
  assert.equal(updatedJob.currentAttempt, 1, 'Attempt counter should be incremented');
  assert.equal(updatedJob.executionHistory.length, 1);
  assert.equal(updatedJob.executionHistory[0].status, 'TIMEOUT');

  worker1.stop();
});
