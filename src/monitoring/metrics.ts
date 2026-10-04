import client from 'prom-client';

// Create a Registry which registers the metrics
export const register = new client.Registry();

// Add a default label which is added to all metrics
register.setDefaultLabels({
  app: 'aethersched',
});

// Enable the collection of default metrics (memory, event loop, CPU)
client.collectDefaultMetrics({ register });

export const jobsEnqueuedCounter = new client.Counter({
  name: 'aethersched_jobs_enqueued_total',
  help: 'Total count of jobs enqueued into the system',
  labelNames: ['priority', 'task_type'],
  registers: [register],
});

export const jobsDispatchedCounter = new client.Counter({
  name: 'aethersched_jobs_dispatched_total',
  help: 'Total count of jobs atomically claimed and dispatched by schedulers',
  labelNames: ['scheduler_id'],
  registers: [register],
});

export const jobsCompletedCounter = new client.Counter({
  name: 'aethersched_jobs_completed_total',
  help: 'Total count of completed job executions by status',
  labelNames: ['status', 'worker_id'],
  registers: [register],
});

export const jobExecutionDuration = new client.Histogram({
  name: 'aethersched_job_execution_duration_seconds',
  help: 'Histogram of job execution duration in seconds',
  labelNames: ['task_type'],
  buckets: [0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
  registers: [register],
});

export const fencingRejectionsCounter = new client.Counter({
  name: 'aethersched_fencing_rejections_total',
  help: 'Total count of stale writes rejected due to outdated fencing tokens',
  registers: [register],
});

export const zombieReclaimedCounter = new client.Counter({
  name: 'aethersched_zombie_reclaimed_total',
  help: 'Total count of zombie/expired leases reclaimed by the scavenger',
  registers: [register],
});

export const queueDepthGauge = new client.Gauge({
  name: 'aethersched_queue_depth',
  help: 'Current number of items in a queue',
  labelNames: ['queue_name'],
  registers: [register],
});
