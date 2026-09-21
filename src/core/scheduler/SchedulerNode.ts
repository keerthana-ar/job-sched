import { IStorageAdapter } from '../storage/IStorageAdapter.js';
import { Job, SchedulerNodeInfo } from '../types.js';
import { ClusterBus } from '../ClusterBus.js';

export class SchedulerNode {
  public info: SchedulerNodeInfo;
  private pollTimer: NodeJS.Timeout | null = null;
  private heartbeatTimer: NodeJS.Timeout | null = null;
  private isRunning = false;
  private delayedZSetKey = 'queue:delayed_jobs';
  private readyQueueKey = 'queue:ready_jobs';
  private pollIntervalMs: number;
  private batchSize: number;

  constructor(
    public readonly id: string,
    private storage: IStorageAdapter,
    options: { pollIntervalMs?: number; batchSize?: number } = {}
  ) {
    this.pollIntervalMs = options.pollIntervalMs ?? 500;
    this.batchSize = options.batchSize ?? 50;

    this.info = {
      id,
      hostname: `host-${id.toLowerCase()}`,
      isLeader: false,
      status: 'ACTIVE',
      jobsDispatched: 0,
      lastHeartbeat: Date.now(),
      startedAt: Date.now(),
    };
  }

  public async start(): Promise<void> {
    if (this.isRunning) return;
    this.isRunning = true;
    this.info.status = 'ACTIVE';

    ClusterBus.getInstance().emit('INFO', this.id, `Scheduler node ${this.id} online and polling.`);

    // Heartbeat
    this.heartbeatTimer = setInterval(() => {
      this.info.lastHeartbeat = Date.now();
    }, 1000);

    // Lookahead polling loop
    this.pollTimer = setInterval(async () => {
      if (!this.isRunning || this.info.status !== 'ACTIVE') return;
      await this.pollAndDispatch();
    }, this.pollIntervalMs);
  }

  public async pollAndDispatch(): Promise<string[]> {
    try {
      const now = Date.now();
      // Atomic dispatch using Lua/CAS primitive on the storage engine
      const claimedJobIds = await this.storage.atomicClaimDueJobs(
        this.delayedZSetKey,
        this.readyQueueKey,
        now,
        this.batchSize,
        this.id
      );

      if (claimedJobIds.length > 0) {
        for (const jobId of claimedJobIds) {
          // Update persistent job state
          const jobJson = await this.storage.get(`job:${jobId}`);
          if (jobJson) {
            const job: Job = JSON.parse(jobJson);
            if (job.status === 'SCHEDULED' || job.status === 'RETRYING') {
              job.status = 'DISPATCHED';
              job.updatedAt = Date.now();
              await this.storage.set(`job:${jobId}`, JSON.stringify(job));
            }
          }
        }

        this.info.jobsDispatched += claimedJobIds.length;
        ClusterBus.getInstance().emit(
          'INFO',
          this.id,
          `Atomically claimed & dispatched ${claimedJobIds.length} due job(s) [${claimedJobIds.slice(0, 3).join(', ')}${claimedJobIds.length > 3 ? '...' : ''}]`
        );
      }

      return claimedJobIds;
    } catch (err: any) {
      ClusterBus.getInstance().emit('ERROR', this.id, `Poll cycle error: ${err.message}`);
      return [];
    }
  }

  public stop(): void {
    this.isRunning = false;
    if (this.pollTimer) clearInterval(this.pollTimer);
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    this.info.status = 'STANDBY';
    ClusterBus.getInstance().emit('INFO', this.id, `Scheduler node ${this.id} stopped.`);
  }

  // Chaos simulation hooks
  public crash(): void {
    this.info.status = 'CRASHED';
    ClusterBus.getInstance().emit('CHAOS', this.id, `🔥 Chaos triggered: Scheduler ${this.id} CRASHED!`);
  }

  public revive(): void {
    this.info.status = 'ACTIVE';
    this.info.lastHeartbeat = Date.now();
    ClusterBus.getInstance().emit('INFO', this.id, `✅ Scheduler ${this.id} revived and resumed operations.`);
  }
}
