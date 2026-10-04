import { Job, JobPriority, JobSchedule, RetryPolicy, TaskType } from '../core/types.js';

export interface AetherClientConfig {
  baseUrl?: string;
  apiKey?: string;
  timeoutMs?: number;
}

export interface EnqueueJobOptions {
  name: string;
  taskType?: TaskType;
  payload?: Record<string, any>;
  priority?: JobPriority;
  schedule?: JobSchedule;
  retryPolicy?: Partial<RetryPolicy>;
  timeoutMs?: number;
  idempotencyKey?: string;
}

export class AetherClient {
  private baseUrl: string;
  private apiKey?: string;
  private timeoutMs: number;

  constructor(config: AetherClientConfig = {}) {
    this.baseUrl = (config.baseUrl || 'http://localhost:4000').replace(/\/$/, '');
    this.apiKey = config.apiKey || process.env.AETHERSCHED_API_KEY;
    this.timeoutMs = config.timeoutMs || 10000;
  }

  private async request<T>(path: string, options: RequestInit = {}): Promise<T> {
    const url = `${this.baseUrl}${path}`;
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
      ...((options.headers as Record<string, string>) || {}),
    };

    if (this.apiKey) {
      headers['X-API-Key'] = this.apiKey;
    }

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeoutMs);

    try {
      const response = await fetch(url, {
        ...options,
        headers,
        signal: controller.signal,
      });

      clearTimeout(timer);

      if (!response.ok) {
        let errMessage = `HTTP error ${response.status}`;
        try {
          const body: any = await response.json();
          if (body && body.error) errMessage = body.error;
        } catch {}
        throw new Error(`AetherClient Request Failed: ${errMessage}`);
      }

      return (await response.json()) as T;
    } catch (err: any) {
      clearTimeout(timer);
      throw err;
    }
  }

  /**
   * Enqueue a new job (Immediate, Delayed, or Cron)
   */
  public async enqueue(options: EnqueueJobOptions): Promise<Job> {
    const payload = {
      name: options.name,
      taskType: options.taskType || 'DATA_PROCESSING',
      payload: options.payload || {},
      priority: options.priority || 'NORMAL',
      schedule: options.schedule || { type: 'IMMEDIATE' },
      retryPolicy: options.retryPolicy,
      timeoutMs: options.timeoutMs,
      idempotencyKey: options.idempotencyKey,
    };

    const res = await this.request<{ success: boolean; job: Job }>('/api/v1/jobs', {
      method: 'POST',
      body: JSON.stringify(payload),
    });

    return res.job;
  }

  /**
   * Get job status and execution history
   */
  public async getJob(id: string): Promise<Job> {
    const res = await this.request<{ success: boolean; job: Job }>(`/api/v1/jobs/${id}`);
    return res.job;
  }

  /**
   * Trigger immediate execution of a job
   */
  public async runNow(id: string): Promise<Job> {
    const res = await this.request<{ success: boolean; job: Job }>(`/api/v1/jobs/${id}/run`, {
      method: 'POST',
    });
    return res.job;
  }

  /**
   * Pause a scheduled job
   */
  public async pause(id: string): Promise<boolean> {
    const res = await this.request<{ success: boolean }>(`/api/v1/jobs/${id}/pause`, {
      method: 'POST',
    });
    return res.success;
  }

  /**
   * Resume a paused job
   */
  public async resume(id: string): Promise<boolean> {
    const res = await this.request<{ success: boolean }>(`/api/v1/jobs/${id}/resume`, {
      method: 'POST',
    });
    return res.success;
  }

  /**
   * Cancel and remove a job
   */
  public async cancel(id: string): Promise<boolean> {
    const res = await this.request<{ success: boolean }>(`/api/v1/jobs/${id}`, {
      method: 'DELETE',
    });
    return res.success;
  }

  /**
   * Replay a job from the Dead Letter Queue
   */
  public async replayDlq(id: string): Promise<Job> {
    const res = await this.request<{ success: boolean; job: Job }>(`/api/v1/dlq/${id}/replay`, {
      method: 'POST',
    });
    return res.job;
  }

  /**
   * Polls until the job enters a terminal state (SUCCESS, FAILED, CANCELLED)
   */
  public async waitForJob(
    id: string,
    options: { timeoutMs?: number; pollIntervalMs?: number } = {}
  ): Promise<Job> {
    const timeout = options.timeoutMs || 30000;
    const interval = options.pollIntervalMs || 500;
    const startTime = Date.now();

    while (Date.now() - startTime < timeout) {
      const job = await this.getJob(id);
      if (job.status === 'SUCCESS' || job.status === 'FAILED' || job.status === 'CANCELLED') {
        return job;
      }
      await new Promise((r) => setTimeout(r, interval));
    }

    throw new Error(`Timeout waiting for job ${id} after ${timeout}ms`);
  }
}
