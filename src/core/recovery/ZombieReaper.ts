import { IStorageAdapter } from '../storage/IStorageAdapter.js';
import { Job, JobExecutionRecord } from '../types.js';
import { ClusterBus } from '../ClusterBus.js';
import { RetryManager } from './RetryManager.js';

export class ZombieReaper {
  private scanTimer: NodeJS.Timeout | null = null;
  private isRunning = false;

  constructor(
    private storage: IStorageAdapter,
    private scanIntervalMs: number = 2000
  ) {}

  public start(): void {
    if (this.isRunning) return;
    this.isRunning = true;

    this.scanTimer = setInterval(async () => {
      await this.scanAndReclaimZombies();
    }, this.scanIntervalMs);

    ClusterBus.getInstance().emit('INFO', 'ZombieReaper', 'Scavenger / Zombie Job Reaper online.');
  }

  public async scanAndReclaimZombies(): Promise<number> {
    try {
      const now = Date.now();
      const allLeases = await this.storage.getAllLeases();
      let reclaimedCount = 0;

      for (const lease of allLeases) {
        if (now > lease.expiresAt) {
          // Lease has expired! The assigned worker is dead or partitioned.
          reclaimedCount += 1;
          await this.reclaimZombieJob(lease.jobId, lease.workerId, lease.grantedAt, now);
        }
      }

      return reclaimedCount;
    } catch (err: any) {
      ClusterBus.getInstance().emit('ERROR', 'ZombieReaper', `Error scanning leases: ${err.message}`);
      return 0;
    }
  }

  private async reclaimZombieJob(jobId: string, deadWorkerId: string, startedAt: number, now: number): Promise<void> {
    // 1. Remove expired lease
    await this.storage.removeLease(jobId);

    // 2. Fetch job
    const jobJson = await this.storage.get(`job:${jobId}`);
    if (!jobJson) return;

    const job: Job = JSON.parse(jobJson);

    // Only recover if currently in RUNNING or DISPATCHED state
    if (job.status !== 'RUNNING' && job.status !== 'DISPATCHED') {
      return;
    }

    ClusterBus.getInstance().emit(
      'CHAOS',
      'ZombieReaper',
      `🧟 ZOMBIE JOB RECLAIMED: Worker ${deadWorkerId} crashed mid-execution on job ${job.name} (${job.id}). Lease expired! Reclaiming and rescheduling...`,
      { jobId: job.id, deadWorkerId }
    );

    const execRecord: JobExecutionRecord = {
      id: 'exec_zombie_' + Math.random().toString(36).substring(2, 8),
      jobId: job.id,
      workerId: deadWorkerId,
      attempt: job.currentAttempt + 1,
      startTime: startedAt,
      endTime: now,
      durationMs: now - startedAt,
      status: 'TIMEOUT',
      error: `Worker ${deadWorkerId} unresponsive; execution lease expired without ACK`,
    };

    // 3. Delegate to RetryManager for backoff rescheduling or DLQ
    await RetryManager.handleFailure(
      this.storage,
      job,
      execRecord,
      `Lease expired: assigned worker ${deadWorkerId} crashed or lost heartbeat`
    );
  }

  public stop(): void {
    this.isRunning = false;
    if (this.scanTimer) clearInterval(this.scanTimer);
  }
}
