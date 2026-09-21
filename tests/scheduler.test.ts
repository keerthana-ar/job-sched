import test from 'node:test';
import assert from 'node:assert/strict';
import { MemoryClusterStore } from '../src/core/storage/MemoryClusterStore.js';
import { SchedulerNode } from '../src/core/scheduler/SchedulerNode.js';

test('Concurrency & Deduplication: Multiple concurrent schedulers never duplicate a job', async () => {
  const storage = new MemoryClusterStore();
  const delayedKey = 'queue:delayed_jobs';
  const readyKey = 'queue:ready_jobs';
  const now = Date.now();

  // Schedule 30 jobs all due at this exact millisecond
  const totalJobs = 30;
  for (let i = 1; i <= totalJobs; i++) {
    const jobId = `job_concurrent_${i}`;
    await storage.zadd(delayedKey, now, jobId);
    await storage.set(`job:${jobId}`, JSON.stringify({
      id: jobId,
      status: 'SCHEDULED',
      updatedAt: now,
    }));
  }

  // Spin up 3 independent Scheduler nodes
  const s1 = new SchedulerNode('Scheduler-1', storage, { batchSize: 20 });
  const s2 = new SchedulerNode('Scheduler-2', storage, { batchSize: 20 });
  const s3 = new SchedulerNode('Scheduler-3', storage, { batchSize: 20 });

  // Run all 3 schedulers at the exact same moment
  const results = await Promise.all([
    s1.pollAndDispatch(),
    s2.pollAndDispatch(),
    s3.pollAndDispatch(),
  ]);

  const claimedS1 = results[0];
  const claimedS2 = results[1];
  const claimedS3 = results[2];

  const totalClaimed = claimedS1.length + claimedS2.length + claimedS3.length;
  assert.equal(totalClaimed, totalJobs, `Expected exactly ${totalJobs} jobs claimed in total`);

  // Verify SET intersection: NO duplicates claimed across any schedulers
  const set1 = new Set(claimedS1);
  const set2 = new Set(claimedS2);
  const set3 = new Set(claimedS3);

  for (const id of set1) {
    assert.ok(!set2.has(id), `Duplicate job ${id} found between S1 and S2!`);
    assert.ok(!set3.has(id), `Duplicate job ${id} found between S1 and S3!`);
  }
  for (const id of set2) {
    assert.ok(!set3.has(id), `Duplicate job ${id} found between S2 and S3!`);
  }

  // Verify ready queue has all 30 jobs and delayed set is now empty
  const readyCount = await storage.llen(readyKey);
  const remainingDelayed = await storage.zcard(delayedKey);

  assert.equal(readyCount, totalJobs, 'Ready queue should contain all 30 dispatched jobs');
  assert.equal(remainingDelayed, 0, 'Delayed queue should be completely empty');
});
