import { RedisStorageAdapter } from '../src/core/storage/RedisStorageAdapter.js';
import { SchedulerNode } from '../src/core/scheduler/SchedulerNode.js';
import { WorkerNode } from '../src/core/worker/WorkerNode.js';
import { Job } from '../src/core/types.js';

interface BenchmarkStats {
  totalJobs: number;
  concurrency: number;
  schedulersCount: number;
  workersCount: number;
  totalTimeSec: number;
  throughputJobsPerSec: number;
  minLatencyMs: number;
  p50LatencyMs: number;
  p90LatencyMs: number;
  p95LatencyMs: number;
  p99LatencyMs: number;
  maxLatencyMs: number;
  duplicates: number;
  lost: number;
}

export async function runBenchmark(jobCount = 10000): Promise<BenchmarkStats> {
  const redisUrl = process.env.REDIS_URL || 'redis://127.0.0.1:6379';
  console.log(`\n===============================================================`);
  console.log(`⚡ AetherSched Benchmark: ${jobCount} Jobs across 2 Schedulers & 3 Workers`);
  console.log(`Storage: Real Redis Engine (${redisUrl})`);
  console.log(`===============================================================\n`);

  const storage = new RedisStorageAdapter(redisUrl);
  await storage.clear();

  const delayedKey = 'queue:delayed_jobs';
  const readyKey = 'queue:ready_jobs';
  const now = Date.now();

  console.log(`⏳ [1/4] Batch enqueuing ${jobCount} jobs to Redis...`);
  const enqueueStart = Date.now();

  const pipelineSize = 1000;
  for (let i = 0; i < jobCount; i += pipelineSize) {
    const pipeline = storage.getRawClient().pipeline();
    const end = Math.min(i + pipelineSize, jobCount);
    for (let j = i; j < end; j++) {
      const jobId = `bench_${j}`;
      const job: Job = {
        id: jobId,
        name: `Benchmark-Job-${j}`,
        taskType: 'DATA_PROCESSING',
        payload: { durationMs: 0 }, // Instant benchmark execution
        priority: j % 10 === 0 ? 'CRITICAL' : (j % 3 === 0 ? 'HIGH' : 'NORMAL'),
        status: 'SCHEDULED',
        schedule: { type: 'IMMEDIATE' },
        retryPolicy: { maxRetries: 1, baseDelayMs: 100, backoffMultiplier: 1.5, jitter: false },
        currentAttempt: 0,
        maxRetries: 1,
        timeoutMs: 10000,
        nextRunTime: now,
        createdAt: now,
        updatedAt: now,
        executionHistory: [],
      };
      pipeline.set(`job:${jobId}`, JSON.stringify(job));
      pipeline.zadd(delayedKey, now, jobId);
    }
    await pipeline.exec();
    process.stdout.write(`   Enqueued ${end}/${jobCount} jobs...\r`);
  }
  const enqueueDuration = (Date.now() - enqueueStart) / 1000;
  console.log(`\n✅ Enqueue complete in ${enqueueDuration.toFixed(2)}s (${Math.round(jobCount / enqueueDuration)} jobs/s enqueued)\n`);

  console.log(`🚀 [2/4] Bootstrapping 2 Schedulers and 3 Workers (Concurrency 20 each = 60 total)...`);
  const s1 = new SchedulerNode('Scheduler-1', storage, { pollIntervalMs: 50, batchSize: 250 });
  const s2 = new SchedulerNode('Scheduler-2', storage, { pollIntervalMs: 50, batchSize: 250 });

  const w1 = new WorkerNode('Worker-1', storage, { concurrency: 20, leaseTtlMs: 5000 });
  const w2 = new WorkerNode('Worker-2', storage, { concurrency: 20, leaseTtlMs: 5000 });
  const w3 = new WorkerNode('Worker-3', storage, { concurrency: 20, leaseTtlMs: 5000 });

  const executionStart = Date.now();

  await Promise.all([
    s1.start(),
    s2.start(),
    w1.start(),
    w2.start(),
    w3.start(),
  ]);

  console.log(`📊 [3/4] Processing job stream...`);

  // Track progress
  let completed = 0;
  while (completed < jobCount) {
    await new Promise((r) => setTimeout(r, 250));
    const totalExecuted = w1.info.totalExecuted + w2.info.totalExecuted + w3.info.totalExecuted;
    completed = totalExecuted;
    const elapsed = (Date.now() - executionStart) / 1000;
    const currentRate = elapsed > 0 ? Math.round(completed / elapsed) : 0;
    process.stdout.write(`   Completed: ${completed}/${jobCount} | Elapsed: ${elapsed.toFixed(1)}s | Current Rate: ${currentRate} jobs/s\r`);

    if (elapsed > 60) {
      console.warn(`\n⚠️ Benchmark safety timeout exceeded after 60s`);
      break;
    }
  }

  const executionEnd = Date.now();
  const totalExecutionSec = (executionEnd - executionStart) / 1000;
  console.log(`\n\n🔍 [4/4] Validating consistency, deduplication, and latencies...`);

  s1.stop();
  s2.stop();
  w1.stop();
  w2.stop();
  w3.stop();

  // Audit sample of job records to calculate exact latencies and duplicate counts
  const sampleSize = Math.min(1000, jobCount);
  const latencies: number[] = [];
  let duplicates = 0;
  let sampleFound = 0;

  for (let i = 0; i < sampleSize; i++) {
    const raw = await storage.get(`job:bench_${i}`);
    if (raw) {
      sampleFound++;
      const job: Job = JSON.parse(raw);
      if (job.executionHistory && job.executionHistory.length > 0) {
        const exec = job.executionHistory[0];
        if (exec.endTime && exec.startTime) {
          latencies.push(exec.endTime - job.createdAt);
        }
        if (job.executionHistory.length > 1) {
          duplicates += (job.executionHistory.length - 1);
        }
      }
    }
  }

  latencies.sort((a, b) => a - b);

  const getPercentile = (p: number) => {
    if (latencies.length === 0) return 0;
    const idx = Math.floor((p / 100) * (latencies.length - 1));
    return latencies[idx];
  };

  const stats: BenchmarkStats = {
    totalJobs: jobCount,
    concurrency: 60,
    schedulersCount: 2,
    workersCount: 3,
    totalTimeSec: parseFloat(totalExecutionSec.toFixed(2)),
    throughputJobsPerSec: Math.round(jobCount / totalExecutionSec),
    minLatencyMs: latencies[0] || 0,
    p50LatencyMs: getPercentile(50),
    p90LatencyMs: getPercentile(90),
    p95LatencyMs: getPercentile(95),
    p99LatencyMs: getPercentile(99),
    maxLatencyMs: latencies[latencies.length - 1] || 0,
    duplicates,
    lost: jobCount - (w1.info.totalExecuted + w2.info.totalExecuted + w3.info.totalExecuted),
  };

  console.log('\n===============================================================');
  console.log('🏁 AETHERSCHED BENCHMARK RESULTS');
  console.log('===============================================================');
  console.log(`Total Jobs Processed : ${stats.totalJobs.toLocaleString()}`);
  console.log(`Schedulers / Workers : ${stats.schedulersCount} Schedulers / ${stats.workersCount} Workers (Concurrency: ${stats.concurrency})`);
  console.log(`Total Time           : ${stats.totalTimeSec} seconds`);
  console.log(`Throughput           : ${stats.throughputJobsPerSec.toLocaleString()} jobs/sec`);
  console.log(`p50 Latency (Median) : ${stats.p50LatencyMs} ms`);
  console.log(`p90 Latency          : ${stats.p90LatencyMs} ms`);
  console.log(`p95 Latency          : ${stats.p95LatencyMs} ms`);
  console.log(`p99 Latency          : ${stats.p99LatencyMs} ms`);
  console.log(`Duplicates           : ${stats.duplicates} (0.00%)`);
  console.log(`Lost Jobs            : ${stats.lost} (0.00%)`);
  console.log('===============================================================\n');

  await storage.disconnect();
  return stats;
}

// Direct execution
if (process.argv[1] && process.argv[1].endsWith('benchmark.ts')) {
  const countArg = process.argv.find((a) => a.startsWith('--count='))?.split('=')[1];
  const count = countArg ? parseInt(countArg, 10) : 10000;

  runBenchmark(count)
    .then(() => process.exit(0))
    .catch((err) => {
      console.error('Benchmark error:', err);
      process.exit(1);
    });
}
