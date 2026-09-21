import { IStorageAdapter } from '../storage/IStorageAdapter.js';
import { Job, JobExecutionRecord } from '../types.js';
import { ClusterBus } from '../ClusterBus.js';
import { CronEngine } from '../scheduler/CronParser.js';

export class RetryManager {
  private static delayedZSetKey = 'queue:delayed_jobs';
  private static dlqKey = 'queue:dead_letter';

  /**
   * Calculates exponential backoff with full jitter in milliseconds
   */
  public static calculateBackoff(job: Job): number {
    const policy = job.retryPolicy;
    const attempt = job.currentAttempt;
    const base = policy.baseDelayMs || 1000;
    const multiplier = policy.backoffMultiplier || 2;
    const maxDelay = 60000; // 1 minute max cap

    let delay = Math.min(maxDelay, base * Math.pow(multiplier, attempt));

    if (policy.jitter) {
      // Full jitter: between 50% and 100% of calculated backoff
      const jitterFactor = 0.5 + Math.random() * 0.5;
      delay = Math.floor(delay * jitterFactor);
    }

    return delay;
  }

  /**
   * Handles a failed execution attempt: either schedules a retry with backoff
   * or archives into the Dead Letter Queue (DLQ).
   */
  public static async handleFailure(
    storage: IStorageAdapter,
    job: Job,
    executionRecord: JobExecutionRecord,
    failureReason: string
  ): Promise<void> {
    job.currentAttempt += 1;
    job.lastError = failureReason;
    job.updatedAt = Date.now();
    job.executionHistory.push(executionRecord);

    if (job.currentAttempt <= job.maxRetries) {
      // Reschedule with backoff
      const backoffMs = this.calculateBackoff(job);
      const nextRunTime = Date.now() + backoffMs;
      job.status = 'RETRYING';
      job.nextRunTime = nextRunTime;

      // Save job state & add to delayed ZSET
      await storage.set(`job:${job.id}`, JSON.stringify(job));
      await storage.zadd(this.delayedZSetKey, nextRunTime, job.id);

      ClusterBus.getInstance().emit(
        'WARN',
        'RetryManager',
        `Job ${job.name} (${job.id}) failed (attempt ${job.currentAttempt}/${job.maxRetries}). Retrying in ${backoffMs}ms with backoff.`,
        { jobId: job.id, nextRunTime, backoffMs }
      );
    } else {
      // Retries exhausted -> Move to Dead Letter Queue
      job.status = 'FAILED';
      await storage.set(`job:${job.id}`, JSON.stringify(job));
      await storage.rpush(this.dlqKey, job.id);

      ClusterBus.getInstance().emit(
        'ERROR',
        'DeadLetterQueue',
        `🚨 Job ${job.name} (${job.id}) EXHAUSTED all ${job.maxRetries} retries! Moved to Dead Letter Queue (DLQ). Reason: ${failureReason}`,
        { jobId: job.id, error: failureReason }
      );
    }
  }

  /**
   * Handles a successful execution: if the job is recurring (CRON), schedules next iteration;
   * otherwise marks as SUCCESS.
   */
  public static async handleSuccess(
    storage: IStorageAdapter,
    job: Job,
    executionRecord: JobExecutionRecord
  ): Promise<void> {
    job.updatedAt = Date.now();
    job.executionHistory.push(executionRecord);

    if (job.schedule.type === 'CRON' && job.schedule.cronExpr) {
      try {
        const nextRunTime = CronEngine.getNextRunTime(job.schedule.cronExpr, new Date(), job.schedule.timezone);
        job.status = 'SCHEDULED';
        job.nextRunTime = nextRunTime;
        job.currentAttempt = 0; // reset retry counter for next recurring window

        await storage.set(`job:${job.id}`, JSON.stringify(job));
        await storage.zadd(this.delayedZSetKey, nextRunTime, job.id);

        ClusterBus.getInstance().emit(
          'INFO',
          'Scheduler',
          `Recurring job ${job.name} scheduled for next run at ${new Date(nextRunTime).toISOString()}`,
          { jobId: job.id, nextRunTime }
        );
        return;
      } catch (err: any) {
        ClusterBus.getInstance().emit('ERROR', 'CronEngine', `Failed to calculate next run for ${job.id}: ${err.message}`);
      }
    }

    // One-time job finished
    job.status = 'SUCCESS';
    await storage.set(`job:${job.id}`, JSON.stringify(job));
  }
}
