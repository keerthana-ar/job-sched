import { ClusterManager } from '../ClusterManager.js';
import { ClusterBus } from '../ClusterBus.js';
import { MemoryClusterStore } from '../storage/MemoryClusterStore.js';

export class ChaosEngine {
  private static instance: ChaosEngine;

  private constructor() {}

  public static getInstance(): ChaosEngine {
    if (!ChaosEngine.instance) {
      ChaosEngine.instance = new ChaosEngine();
    }
    return ChaosEngine.instance;
  }

  public killWorker(workerId: string): boolean {
    const cluster = ClusterManager.getInstance();
    const worker = cluster.workers.get(workerId);
    if (!worker) return false;

    worker.crash();
    return true;
  }

  public reviveWorker(workerId: string): boolean {
    const cluster = ClusterManager.getInstance();
    const worker = cluster.workers.get(workerId);
    if (!worker) return false;

    worker.revive();
    return true;
  }

  public crashScheduler(schedulerId: string): boolean {
    const cluster = ClusterManager.getInstance();
    const scheduler = cluster.schedulers.get(schedulerId);
    if (!scheduler) return false;

    scheduler.crash();
    return true;
  }

  public reviveScheduler(schedulerId: string): boolean {
    const cluster = ClusterManager.getInstance();
    const scheduler = cluster.schedulers.get(schedulerId);
    if (!scheduler) return false;

    scheduler.revive();
    return true;
  }

  public setStorageLatency(ms: number): void {
    const cluster = ClusterManager.getInstance();
    if (cluster.storage instanceof MemoryClusterStore) {
      cluster.storage.setSimulatedLatency(ms);
      ClusterBus.getInstance().emit(
        'CHAOS',
        'ChaosEngine',
        `⚠️ Injected ${ms}ms simulated network latency on storage adapter.`
      );
    }
  }

  public async triggerJobSpike(count: number = 30): Promise<void> {
    const cluster = ClusterManager.getInstance();
    ClusterBus.getInstance().emit(
      'CHAOS',
      'ChaosEngine',
      `⚡ CHAOS: Triggering traffic spike of ${count} concurrent jobs!`
    );

    const promises = [];
    for (let i = 1; i <= count; i++) {
      const priority = i % 5 === 0 ? 'CRITICAL' : i % 2 === 0 ? 'HIGH' : 'NORMAL';
      promises.push(
        cluster.createJob({
          name: `Burst-Job-#${i}`,
          taskType: 'DATA_PROCESSING',
          priority,
          payload: { batchSize: 500 + i * 50, durationMs: 400 + (i % 4) * 200 },
          schedule: { type: 'IMMEDIATE' },
        })
      );
    }
    await Promise.all(promises);
  }

  public async triggerFlakyCascade(): Promise<void> {
    const cluster = ClusterManager.getInstance();
    ClusterBus.getInstance().emit(
      'CHAOS',
      'ChaosEngine',
      `🧪 CHAOS: Scheduling Flaky Cascading Failure jobs with exponential backoff & DLQ test!`
    );

    // Job 1: Flaky that recovers on attempt 3
    await cluster.createJob({
      name: 'Flaky-Downstream-Service',
      taskType: 'FLAKY_TASK',
      payload: { failUntilAttempt: 2, durationMs: 500 },
      priority: 'HIGH',
      schedule: { type: 'IMMEDIATE' },
      retryPolicy: { maxRetries: 3, baseDelayMs: 2000, backoffMultiplier: 2, jitter: true },
    });

    // Job 2: Fatal poisoned pill that exhausts retries and lands in DLQ
    await cluster.createJob({
      name: 'Poison-Pill-Fatal-Job',
      taskType: 'HTTP_WEBHOOK',
      payload: { forceFail: true, url: 'https://corrupted.internal/api' },
      priority: 'CRITICAL',
      schedule: { type: 'IMMEDIATE' },
      retryPolicy: { maxRetries: 2, baseDelayMs: 1500, backoffMultiplier: 2, jitter: false },
    });
  }
}
