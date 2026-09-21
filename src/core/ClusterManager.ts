import { IStorageAdapter } from './storage/IStorageAdapter.js';
import { MemoryClusterStore } from './storage/MemoryClusterStore.js';
import { SchedulerNode } from './scheduler/SchedulerNode.js';
import { WorkerNode } from './worker/WorkerNode.js';
import { ZombieReaper } from './recovery/ZombieReaper.js';
import { ClusterBus } from './ClusterBus.js';
import { CronEngine } from './scheduler/CronParser.js';
import { ClusterTelemetry, Job, JobPriority, JobSchedule, RetryPolicy, TaskType } from './types.js';

export interface CreateJobInput {
  name: string;
  taskType?: TaskType;
  payload?: Record<string, any>;
  priority?: JobPriority;
  schedule: JobSchedule;
  retryPolicy?: Partial<RetryPolicy>;
  timeoutMs?: number;
}

export class ClusterManager {
  private static instance: ClusterManager;
  public storage: IStorageAdapter;
  public schedulers: Map<string, SchedulerNode> = new Map();
  public workers: Map<string, WorkerNode> = new Map();
  public reaper: ZombieReaper;

  private delayedZSetKey = 'queue:delayed_jobs';
  private dlqKey = 'queue:dead_letter';

  private constructor() {
    this.storage = new MemoryClusterStore();
    this.reaper = new ZombieReaper(this.storage, 1500);
  }

  public static getInstance(): ClusterManager {
    if (!ClusterManager.instance) {
      ClusterManager.instance = new ClusterManager();
    }
    return ClusterManager.instance;
  }

  public async bootstrap(options: { schedulerCount?: number; workerCount?: number } = {}): Promise<void> {
    const sCount = options.schedulerCount ?? 2;
    const wCount = options.workerCount ?? 3;

    // 1. Initialize Schedulers
    for (let i = 1; i <= sCount; i++) {
      const id = `Scheduler-${i}`;
      const scheduler = new SchedulerNode(id, this.storage, { pollIntervalMs: 500, batchSize: 20 });
      this.schedulers.set(id, scheduler);
      await scheduler.start();
    }

    // 2. Initialize Workers
    for (let i = 1; i <= wCount; i++) {
      const id = `Worker-${i}`;
      const worker = new WorkerNode(id, this.storage, { concurrency: 4, leaseTtlMs: 5000 });
      this.workers.set(id, worker);
      await worker.start();
    }

    // 3. Start Zombie Scavenger
    this.reaper.start();

    ClusterBus.getInstance().emit(
      'INFO',
      'ClusterManager',
      `Cluster bootstrapped with ${sCount} Scheduler(s) and ${wCount} Worker(s).`
    );
  }

  public async createJob(input: CreateJobInput): Promise<Job> {
    const id = 'job_' + Math.random().toString(36).substring(2, 9);
    const now = Date.now();

    // Determine initial nextRunTime
    let nextRunTime = now;
    if (input.schedule.type === 'DELAYED' && input.schedule.runAt) {
      nextRunTime = input.schedule.runAt;
    } else if (input.schedule.type === 'CRON' && input.schedule.cronExpr) {
      nextRunTime = CronEngine.getNextRunTime(input.schedule.cronExpr, new Date(), input.schedule.timezone);
    }

    const defaultRetry: RetryPolicy = {
      maxRetries: input.retryPolicy?.maxRetries ?? 3,
      baseDelayMs: input.retryPolicy?.baseDelayMs ?? 1500,
      backoffMultiplier: input.retryPolicy?.backoffMultiplier ?? 2,
      jitter: input.retryPolicy?.jitter ?? true,
    };

    const job: Job = {
      id,
      name: input.name,
      taskType: input.taskType || 'DATA_PROCESSING',
      payload: input.payload || {},
      priority: input.priority || 'NORMAL',
      status: 'SCHEDULED',
      schedule: input.schedule,
      retryPolicy: defaultRetry,
      currentAttempt: 0,
      maxRetries: defaultRetry.maxRetries,
      timeoutMs: input.timeoutMs || 30000,
      nextRunTime,
      createdAt: now,
      updatedAt: now,
      executionHistory: [],
    };

    // Store definition
    await this.storage.set(`job:${id}`, JSON.stringify(job));
    // Index in delayed ZSET
    await this.storage.zadd(this.delayedZSetKey, nextRunTime, id);

    ClusterBus.getInstance().emit(
      'INFO',
      'API',
      `Job '${job.name}' (${job.id}) registered. Schedule: ${job.schedule.type}, Next run: ${new Date(nextRunTime).toLocaleTimeString()}`,
      { jobId: job.id, nextRunTime }
    );

    return job;
  }

  public async getJob(id: string): Promise<Job | null> {
    const raw = await this.storage.get(`job:${id}`);
    return raw ? JSON.parse(raw) : null;
  }

  public async getAllJobs(): Promise<Job[]> {
    const keys = await this.storage.keys('job:*');
    const jobs: Job[] = [];
    for (const k of keys) {
      const raw = await this.storage.get(k);
      if (raw) {
        jobs.push(JSON.parse(raw));
      }
    }
    // Sort descending by createdAt
    return jobs.sort((a, b) => b.createdAt - a.createdAt);
  }

  public async runNow(id: string): Promise<Job | null> {
    const job = await this.getJob(id);
    if (!job) return null;

    const now = Date.now();
    job.status = 'SCHEDULED';
    job.nextRunTime = now;
    job.updatedAt = now;

    await this.storage.set(`job:${id}`, JSON.stringify(job));
    await this.storage.zadd(this.delayedZSetKey, now, id);

    ClusterBus.getInstance().emit('INFO', 'API', `Immediate execution requested for job ${job.name} (${job.id})`);
    return job;
  }

  public async pauseJob(id: string): Promise<boolean> {
    const job = await this.getJob(id);
    if (!job) return false;

    job.status = 'PAUSED';
    job.updatedAt = Date.now();
    await this.storage.set(`job:${id}`, JSON.stringify(job));
    await this.storage.zrem(this.delayedZSetKey, id);

    ClusterBus.getInstance().emit('INFO', 'API', `Job ${job.name} (${job.id}) PAUSED.`);
    return true;
  }

  public async resumeJob(id: string): Promise<boolean> {
    const job = await this.getJob(id);
    if (!job || job.status !== 'PAUSED') return false;

    const now = Date.now();
    let nextRunTime = now;
    if (job.schedule.type === 'CRON' && job.schedule.cronExpr) {
      nextRunTime = CronEngine.getNextRunTime(job.schedule.cronExpr);
    }
    job.status = 'SCHEDULED';
    job.nextRunTime = nextRunTime;
    job.updatedAt = now;

    await this.storage.set(`job:${id}`, JSON.stringify(job));
    await this.storage.zadd(this.delayedZSetKey, nextRunTime, id);

    ClusterBus.getInstance().emit('INFO', 'API', `Job ${job.name} (${job.id}) RESUMED.`);
    return true;
  }

  public async cancelJob(id: string): Promise<boolean> {
    const job = await this.getJob(id);
    if (!job) return false;

    job.status = 'CANCELLED';
    job.updatedAt = Date.now();
    await this.storage.set(`job:${id}`, JSON.stringify(job));
    await this.storage.zrem(this.delayedZSetKey, id);

    ClusterBus.getInstance().emit('INFO', 'API', `Job ${job.name} (${job.id}) CANCELLED.`);
    return true;
  }

  public async getDlqJobs(): Promise<Job[]> {
    const dlqIds = await this.storage.lrange(this.dlqKey, 0, -1);
    const jobs: Job[] = [];
    for (const id of dlqIds) {
      const job = await this.getJob(id);
      if (job) jobs.push(job);
    }
    return jobs;
  }

  public async replayDlqJob(id: string): Promise<Job | null> {
    const job = await this.getJob(id);
    if (!job) return null;

    job.currentAttempt = 0;
    job.status = 'SCHEDULED';
    job.nextRunTime = Date.now();
    job.updatedAt = Date.now();

    await this.storage.set(`job:${id}`, JSON.stringify(job));
    await this.storage.zadd(this.delayedZSetKey, job.nextRunTime, id);

    ClusterBus.getInstance().emit('INFO', 'DLQ', `Replaying failed job ${job.name} (${job.id}) from DLQ.`);
    return job;
  }

  public async getTelemetry(): Promise<ClusterTelemetry> {
    const allJobs = await this.getAllJobs();
    const dlqJobs = await this.getDlqJobs();

    let scheduledCount = 0;
    let dispatchedCount = 0;
    let runningCount = 0;
    let successCount = 0;
    let failedCount = 0;
    let totalDurations = 0;
    let durationsSampleCount = 0;

    const recentExecutions: any[] = [];

    for (const job of allJobs) {
      if (job.status === 'SCHEDULED') scheduledCount++;
      else if (job.status === 'DISPATCHED') dispatchedCount++;
      else if (job.status === 'RUNNING') runningCount++;
      else if (job.status === 'SUCCESS') successCount++;
      else if (job.status === 'FAILED') failedCount++;

      for (const exec of job.executionHistory) {
        recentExecutions.push(exec);
        if (exec.durationMs) {
          totalDurations += exec.durationMs;
          durationsSampleCount++;
        }
      }
    }

    recentExecutions.sort((a, b) => (b.startTime || 0) - (a.startTime || 0));

    const schedulers = Array.from(this.schedulers.values()).map((s) => s.info);
    const workers = Array.from(this.workers.values()).map((w) => w.info);

    return {
      timestamp: Date.now(),
      metrics: {
        totalJobs: allJobs.length,
        scheduledCount,
        dispatchedCount,
        runningCount,
        successCount,
        failedCount,
        dlqCount: dlqJobs.length,
        executionsPerSecond: Math.min(20, Math.round(durationsSampleCount / 2)),
        avgExecutionDurationMs: durationsSampleCount ? Math.round(totalDurations / durationsSampleCount) : 0,
      },
      schedulers,
      workers,
      recentExecutions: recentExecutions.slice(0, 30),
      dlqJobs,
    };
  }
}
