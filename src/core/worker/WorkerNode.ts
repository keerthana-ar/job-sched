import { IStorageAdapter } from '../storage/IStorageAdapter.js';
import { ExecutionLease, Job, JobExecutionRecord, WorkerInfo } from '../types.js';
import { ClusterBus } from '../ClusterBus.js';
import { TaskRegistry } from './TaskRegistry.js';
import { RetryManager } from '../recovery/RetryManager.js';

export class WorkerNode {
  public info: WorkerInfo;
  private isRunning = false;
  private pollTimer: NodeJS.Timeout | null = null;
  private heartbeatTimer: NodeJS.Timeout | null = null;
  private leaseRenewTimer: NodeJS.Timeout | null = null;
  private activeJobsMap = new Map<string, { job: Job; startTime: number; leaseId: string; fencingToken: number }>();

  private readyQueueKey = 'queue:ready_jobs';
  private priorityQueueKeys = [
    'queue:ready_jobs:CRITICAL',
    'queue:ready_jobs:HIGH',
    'queue:ready_jobs:NORMAL',
    'queue:ready_jobs:LOW',
    'queue:ready_jobs',
  ];
  private leaseTtlMs: number;

  constructor(
    public readonly id: string,
    private storage: IStorageAdapter,
    options: { concurrency?: number; leaseTtlMs?: number } = {}
  ) {
    const concurrency = options.concurrency ?? 5;
    this.leaseTtlMs = options.leaseTtlMs ?? 5000; // 5s lease TTL

    this.info = {
      id,
      hostname: `worker-host-${id.toLowerCase()}`,
      status: 'HEALTHY',
      concurrency,
      activeJobs: [],
      totalExecuted: 0,
      totalFailed: 0,
      lastHeartbeat: Date.now(),
      startedAt: Date.now(),
    };
  }

  // Lease extension loop: renew active leases every 1.5s
  private setupLeaseRenewal(): void {
    this.leaseRenewTimer = setInterval(async () => {
      if (this.info.status === 'CRASHED' || this.info.status === 'DEAD') {
        return; // If crashed, STOP renewing lease so it expires!
      }
      const now = Date.now();
      for (const [jobId, item] of this.activeJobsMap.entries()) {
        const lease: ExecutionLease = {
          jobId,
          workerId: this.id,
          leaseId: item.leaseId,
          fencingToken: item.fencingToken,
          grantedAt: item.startTime,
          expiresAt: now + this.leaseTtlMs,
          renewCount: 1,
        };
        await this.storage.setLease(lease);
      }
    }, 1500);
  }

  public async start(): Promise<void> {
    if (this.isRunning) return;
    this.isRunning = true;
    this.info.status = 'HEALTHY';

    ClusterBus.getInstance().emit('INFO', this.id, `Worker daemon ${this.id} online (Concurrency: ${this.info.concurrency}).`);

    // Worker node heartbeat
    this.heartbeatTimer = setInterval(() => {
      if (this.info.status !== 'CRASHED' && this.info.status !== 'DEAD') {
        this.info.lastHeartbeat = Date.now();
      }
    }, 1000);

    this.setupLeaseRenewal();

    // Polling loop for ready queues with priority ordering
    this.pollTimer = setInterval(async () => {
      if (!this.isRunning || this.info.status === 'CRASHED' || this.info.status === 'DEAD') {
        return;
      }
      while (
        this.isRunning &&
        (this.info.status as string) !== 'CRASHED' &&
        (this.info.status as string) !== 'DEAD' &&
        this.activeJobsMap.size < this.info.concurrency
      ) {
        const popped = await this.pullAndProcessJob();
        if (!popped) break;
      }
    }, 50);
  }

  private async pullAndProcessJob(): Promise<boolean> {
    if (this.activeJobsMap.size >= this.info.concurrency) {
      this.info.status = 'BUSY';
      return false;
    } else {
      this.info.status = 'HEALTHY';
    }

    // 1. Pop from highest priority queue available
    const popResult = await this.storage.lpopPriority(this.priorityQueueKeys);
    if (!popResult) return false;
    const jobId = popResult.item;

    const jobJson = await this.storage.get(`job:${jobId}`);
    if (!jobJson) return false;

    const job: Job = JSON.parse(jobJson);
    if (job.status === 'CANCELLED' || job.status === 'PAUSED') {
      ClusterBus.getInstance().emit('INFO', this.id, `Skipping job ${job.id} as it is ${job.status}`);
      return true;
    }

    const leaseId = 'lease_' + Math.random().toString(36).substring(2, 9);
    const now = Date.now();

    // 2. Grant execution lease with monotonically increasing fencing token
    const fencingToken = await this.storage.incrementFencingToken(job.id);
    const lease: ExecutionLease = {
      jobId: job.id,
      workerId: this.id,
      leaseId,
      fencingToken,
      grantedAt: now,
      expiresAt: now + this.leaseTtlMs,
      renewCount: 0,
    };
    await this.storage.setLease(lease);

    // 3. Mark job state RUNNING
    job.status = 'RUNNING';
    job.updatedAt = now;
    await this.storage.set(`job:${job.id}`, JSON.stringify(job));

    this.activeJobsMap.set(job.id, { job, startTime: now, leaseId, fencingToken });
    this.info.activeJobs = Array.from(this.activeJobsMap.keys());

    ClusterBus.getInstance().emit(
      'INFO',
      this.id,
      `Acquired lease (token=${fencingToken}) on job ${job.name} (${job.id}). Commencing execution...`
    );

    // Execute asynchronously (non-blocking)
    this.executeJob(job, leaseId, fencingToken, now);
    return true;
  }

  private async executeJob(job: Job, leaseId: string, fencingToken: number, startTime: number): Promise<void> {
    const execId = 'exec_' + Math.random().toString(36).substring(2, 9);
    const executionRecord: JobExecutionRecord = {
      id: execId,
      jobId: job.id,
      workerId: this.id,
      attempt: job.currentAttempt + 1,
      fencingToken,
      startTime,
      status: 'SUCCESS',
    };

    try {
      let taskResult: any;

      // 4. Idempotency Check: if job specifies idempotencyKey and was already executed, return cached result
      const effectiveIdempKey = job.idempotencyKey || (job.payload && job.payload.idempotencyKey);
      if (effectiveIdempKey) {
        const cached = await this.storage.getIdempotency(effectiveIdempKey);
        if (cached) {
          ClusterBus.getInstance().emit(
            'INFO',
            this.id,
            `🔁 Idempotency hit: Job ${job.id} with key '${effectiveIdempKey}' already completed. Using cached result.`
          );
          taskResult = JSON.parse(cached);
        }
      }

      if (taskResult === undefined) {
        // Execute the registered task handler
        const res = await TaskRegistry.execute(job);
        taskResult = res.result;
      }

      const endTime = Date.now();
      executionRecord.endTime = endTime;
      executionRecord.durationMs = endTime - startTime;
      executionRecord.result = taskResult;

      // Ensure worker didn't crash while processing
      if (this.info.status === 'CRASHED' || this.info.status === 'DEAD') {
        ClusterBus.getInstance().emit('WARN', this.id, `Worker ${this.id} crashed before acknowledging job ${job.id}.`);
        return; // Do not commit success, let scavenger handle it
      }

      // 5. Atomic Completion with Fencing Token Verification
      // If a zombie reaper reassigned the job while we were slow/paused, our fencingToken will be stale!
      job.updatedAt = Date.now();
      job.executionHistory.push(executionRecord);
      if (job.schedule.type !== 'CRON' || !job.schedule.cronExpr) {
        job.status = 'SUCCESS';
      }

      const atomicResult = await this.storage.completeJobAtomic(
        job.id,
        fencingToken,
        JSON.stringify(job),
        effectiveIdempKey,
        JSON.stringify(taskResult)
      );

      if (!atomicResult.success) {
        // FENCING TOKEN STALE REJECTION!
        ClusterBus.getInstance().emit(
          'WARN',
          this.id,
          `⛔ Fencing rejection on job ${job.id}: ${atomicResult.reason}. Write rejected to prevent split-brain state overwrite!`
        );
        return;
      }

      this.info.totalExecuted += 1;
      await RetryManager.handleSuccess(this.storage, job, executionRecord);

      ClusterBus.getInstance().emit(
        'INFO',
        this.id,
        `✅ Job ${job.name} (${job.id}) completed successfully in ${executionRecord.durationMs}ms (fencing token=${fencingToken}).`
      );
    } catch (err: any) {
      const endTime = Date.now();
      executionRecord.endTime = endTime;
      executionRecord.durationMs = endTime - startTime;
      executionRecord.status = 'FAILED';
      executionRecord.error = err.message;

      if (this.info.status === 'CRASHED' || this.info.status === 'DEAD') return;

      this.info.totalFailed += 1;
      await this.storage.removeLease(job.id);
      await RetryManager.handleFailure(this.storage, job, executionRecord, err.message);
    } finally {
      this.activeJobsMap.delete(job.id);
      this.info.activeJobs = Array.from(this.activeJobsMap.keys());
      if (this.isRunning && this.info.status !== 'CRASHED' && this.info.status !== 'DEAD' && this.activeJobsMap.size < this.info.concurrency) {
        setImmediate(() => this.pullAndProcessJob());
      }
    }
  }

  public stop(): void {
    this.isRunning = false;
    if (this.pollTimer) clearInterval(this.pollTimer);
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    if (this.leaseRenewTimer) clearInterval(this.leaseRenewTimer);
    this.info.status = 'DEAD';
  }

  // --- Chaos Injection ---
  public crash(): void {
    this.info.status = 'CRASHED';
    // We intentionally DO NOT clear activeJobsMap or remove leases!
    // The worker's heartbeats and lease renewals stop abruptly.
    ClusterBus.getInstance().emit(
      'CHAOS',
      this.id,
      `🔥 CHAOS: Worker ${this.id} forcefully terminated mid-flight with ${this.activeJobsMap.size} active job(s)! Leases will expire.`
    );
  }

  public revive(): void {
    this.info.status = 'HEALTHY';
    this.info.lastHeartbeat = Date.now();
    this.activeJobsMap.clear();
    this.info.activeJobs = [];
    ClusterBus.getInstance().emit('INFO', this.id, `✅ Worker ${this.id} revived and ready for work.`);
  }
}
