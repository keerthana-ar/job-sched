export type JobStatus = 
  | 'SCHEDULED' 
  | 'DISPATCHED' 
  | 'RUNNING' 
  | 'SUCCESS' 
  | 'FAILED' 
  | 'RETRYING' 
  | 'CANCELLED' 
  | 'PAUSED';

export type JobPriority = 'CRITICAL' | 'HIGH' | 'NORMAL' | 'LOW';

export type TaskType = 
  | 'HTTP_WEBHOOK' 
  | 'DATA_PROCESSING' 
  | 'EMAIL_DISPATCH' 
  | 'DB_CLEANUP' 
  | 'FLAKY_TASK' 
  | 'CUSTOM';

export interface RetryPolicy {
  maxRetries: number;
  baseDelayMs: number;
  backoffMultiplier: number;
  jitter: boolean;
}

export interface JobSchedule {
  type: 'IMMEDIATE' | 'DELAYED' | 'CRON';
  runAt?: number;          // Timestamp in ms for immediate/delayed
  cronExpr?: string;       // E.g. '*/10 * * * * *' (every 10s) or '0 * * * *'
  timezone?: string;
}

export interface Job {
  id: string;
  name: string;
  taskType: TaskType;
  payload: Record<string, any>;
  priority: JobPriority;
  status: JobStatus;
  schedule: JobSchedule;
  retryPolicy: RetryPolicy;
  currentAttempt: number;
  maxRetries: number;
  timeoutMs: number;
  nextRunTime: number;      // Epoch ms when job should execute
  createdAt: number;
  updatedAt: number;
  lastExecutionId?: string;
  lastError?: string;
  executionHistory: JobExecutionRecord[];
}

export interface JobExecutionRecord {
  id: string;
  jobId: string;
  workerId: string;
  attempt: number;
  startTime: number;
  endTime?: number;
  durationMs?: number;
  status: 'SUCCESS' | 'FAILED' | 'TIMEOUT';
  error?: string;
  result?: any;
}

export interface ExecutionLease {
  jobId: string;
  workerId: string;
  leaseId: string;
  grantedAt: number;
  expiresAt: number;
  renewCount: number;
}

export interface WorkerInfo {
  id: string;
  hostname: string;
  status: 'HEALTHY' | 'BUSY' | 'DEGRADED' | 'DEAD' | 'CRASHED';
  concurrency: number;
  activeJobs: string[];
  totalExecuted: number;
  totalFailed: number;
  lastHeartbeat: number;
  startedAt: number;
}

export interface SchedulerNodeInfo {
  id: string;
  hostname: string;
  isLeader: boolean;
  status: 'ACTIVE' | 'CRASHED' | 'STANDBY';
  jobsDispatched: number;
  lastHeartbeat: number;
  startedAt: number;
}

export interface ClusterTelemetry {
  timestamp: number;
  metrics: {
    totalJobs: number;
    scheduledCount: number;
    dispatchedCount: number;
    runningCount: number;
    successCount: number;
    failedCount: number;
    dlqCount: number;
    executionsPerSecond: number;
    avgExecutionDurationMs: number;
  };
  schedulers: SchedulerNodeInfo[];
  workers: WorkerInfo[];
  recentExecutions: JobExecutionRecord[];
  dlqJobs: Job[];
}

export interface ClusterLogEvent {
  id: string;
  timestamp: number;
  level: 'INFO' | 'WARN' | 'ERROR' | 'CHAOS';
  source: string; // 'Scheduler-1', 'Worker-2', 'ZombieReaper', etc.
  message: string;
  metadata?: Record<string, any>;
}
