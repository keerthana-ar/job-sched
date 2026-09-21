import { Job } from '../types.js';

export type TaskHandler = (job: Job) => Promise<{ result?: any; error?: string }>;

export class TaskRegistry {
  private static handlers = new Map<string, TaskHandler>();

  public static register(taskType: string, handler: TaskHandler): void {
    this.handlers.set(taskType, handler);
  }

  public static async execute(job: Job): Promise<{ result?: any; error?: string }> {
    const handler = this.handlers.get(job.taskType);
    if (!handler) {
      // Default execution fallback
      await new Promise((resolve) => setTimeout(resolve, 300));
      return { result: { message: `Completed ${job.taskType} successfully`, executedAt: new Date().toISOString() } };
    }
    return handler(job);
  }
}

// Register built-in realistic handlers
TaskRegistry.register('DATA_PROCESSING', async (job: Job) => {
  const duration = job.payload.durationMs || 1200;
  await new Promise((resolve) => setTimeout(resolve, duration));
  return {
    result: {
      recordsProcessed: job.payload.batchSize || 1500,
      checksum: 'sha256_' + Math.random().toString(36).substring(2, 10),
    },
  };
});

TaskRegistry.register('HTTP_WEBHOOK', async (job: Job) => {
  const duration = job.payload.latencyMs || 600;
  await new Promise((resolve) => setTimeout(resolve, duration));
  if (job.payload.forceFail) {
    throw new Error(`HTTP 502 Bad Gateway: Downstream service at ${job.payload.url || 'https://api.gateway.internal'} unavailable`);
  }
  return {
    result: {
      statusCode: 200,
      responseBody: { status: 'OK', target: job.payload.url || 'https://api.service.internal/webhook' },
    },
  };
});

TaskRegistry.register('EMAIL_DISPATCH', async (job: Job) => {
  await new Promise((resolve) => setTimeout(resolve, 400));
  return {
    result: {
      messageId: 'msg_' + Math.random().toString(36).substring(2, 10),
      recipient: job.payload.recipient || 'user@example.com',
      deliveredAt: new Date().toISOString(),
    },
  };
});

TaskRegistry.register('DB_CLEANUP', async () => {
  await new Promise((resolve) => setTimeout(resolve, 800));
  return {
    result: {
      purgedRows: Math.floor(Math.random() * 500) + 50,
      reclaimedSpaceKb: 1024,
    },
  };
});

// FLAKY_TASK is critical for demonstrating resilience:
// If configured with failUntilAttempt = 3, attempts 1 & 2 fail with error, attempt 3 succeeds!
TaskRegistry.register('FLAKY_TASK', async (job: Job) => {
  const failUntil = job.payload.failUntilAttempt ?? 2;
  const duration = job.payload.durationMs || 500;
  await new Promise((resolve) => setTimeout(resolve, duration));

  if (job.currentAttempt < failUntil) {
    throw new Error(`Downstream RateLimitExceeded: attempt ${job.currentAttempt} of ${failUntil} rejected with 429`);
  }

  return {
    result: {
      status: 'RECOVERED_AFTER_BACKOFF',
      successfulAttempt: job.currentAttempt,
    },
  };
});
