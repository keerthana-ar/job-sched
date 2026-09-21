import test from 'node:test';
import assert from 'node:assert/strict';
import { MemoryClusterStore } from '../src/core/storage/MemoryClusterStore.js';
import { RetryManager } from '../src/core/recovery/RetryManager.js';
import { Job, JobExecutionRecord } from '../src/core/types.js';

test('Retry & DLQ: Exponential backoff progresses and exhausted retries land in DLQ', async () => {
  const storage = new MemoryClusterStore();
  const dlqKey = 'queue:dead_letter';
  const jobId = 'job_retry_test_1';
  const now = Date.now();

  const testJob: Job = {
    id: jobId,
    name: 'Poison-Pill-Task',
    taskType: 'HTTP_WEBHOOK',
    payload: { forceFail: true },
    priority: 'HIGH',
    status: 'RUNNING',
    schedule: { type: 'IMMEDIATE' },
    retryPolicy: { maxRetries: 2, baseDelayMs: 200, backoffMultiplier: 2, jitter: false },
    currentAttempt: 0,
    maxRetries: 2,
    timeoutMs: 5000,
    nextRunTime: now,
    createdAt: now,
    updatedAt: now,
    executionHistory: [],
  };

  await storage.set(`job:${jobId}`, JSON.stringify(testJob));

  // Failure Attempt 1
  const exec1: JobExecutionRecord = {
    id: 'exec_1',
    jobId,
    workerId: 'Worker-1',
    attempt: 1,
    startTime: now,
    endTime: now + 50,
    durationMs: 50,
    status: 'FAILED',
    error: '502 Bad Gateway',
  };

  await RetryManager.handleFailure(storage, testJob, exec1, '502 Bad Gateway');

  let jobState = JSON.parse((await storage.get(`job:${jobId}`))!);
  assert.equal(jobState.status, 'RETRYING');
  assert.equal(jobState.currentAttempt, 1);
  assert.ok(jobState.nextRunTime > now);

  // Failure Attempt 2 (Retry 1 fails)
  const exec2: JobExecutionRecord = {
    id: 'exec_2',
    jobId,
    workerId: 'Worker-2',
    attempt: 2,
    startTime: jobState.nextRunTime,
    endTime: jobState.nextRunTime + 50,
    durationMs: 50,
    status: 'FAILED',
    error: '502 Bad Gateway',
  };

  await RetryManager.handleFailure(storage, jobState, exec2, '502 Bad Gateway');

  jobState = JSON.parse((await storage.get(`job:${jobId}`))!);
  assert.equal(jobState.status, 'RETRYING', 'Job should still retry on attempt 2 when maxRetries is 2');
  assert.equal(jobState.currentAttempt, 2);

  // Failure Attempt 3 (Retry 2 fails -> maxRetries exhausted!)
  const exec3: JobExecutionRecord = {
    id: 'exec_3',
    jobId,
    workerId: 'Worker-1',
    attempt: 3,
    startTime: jobState.nextRunTime,
    endTime: jobState.nextRunTime + 50,
    durationMs: 50,
    status: 'FAILED',
    error: '502 Bad Gateway',
  };

  await RetryManager.handleFailure(storage, jobState, exec3, '502 Bad Gateway');

  jobState = JSON.parse((await storage.get(`job:${jobId}`))!);
  assert.equal(jobState.status, 'FAILED', 'Job should be FAILED after exhausting retries');
  assert.equal(jobState.currentAttempt, 3);

  // Assert presence in Dead Letter Queue
  const dlqItems = await storage.lrange(dlqKey, 0, -1);
  assert.ok(dlqItems.includes(jobId), 'Job must be quarantined in the Dead Letter Queue');
});
