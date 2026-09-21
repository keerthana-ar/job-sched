import express, { Request, Response } from 'express';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';
import { ClusterManager } from '../core/ClusterManager.js';
import { ClusterBus } from '../core/ClusterBus.js';
import { ChaosEngine } from '../core/chaos/ChaosEngine.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export function createApp() {
  const app = express();
  const cluster = ClusterManager.getInstance();
  const chaos = ChaosEngine.getInstance();

  app.use(express.json());

  // Serve static UI files
  let uiPath = path.join(__dirname, '../ui');
  if (!fs.existsSync(uiPath)) {
    uiPath = path.join(process.cwd(), 'src/ui');
  }
  app.use(express.static(uiPath));

  // --- Job Endpoints ---
  app.get('/api/v1/jobs', async (_req: Request, res: Response) => {
    try {
      const jobs = await cluster.getAllJobs();
      res.json({ success: true, count: jobs.length, jobs });
    } catch (err: any) {
      res.status(500).json({ success: false, error: err.message });
    }
  });

  app.post('/api/v1/jobs', async (req: Request, res: Response) => {
    try {
      const { name, taskType, payload, priority, schedule, retryPolicy, timeoutMs } = req.body;
      if (!name || !schedule || !schedule.type) {
        return res.status(400).json({ success: false, error: 'Missing required fields: name, schedule.type' });
      }

      const job = await cluster.createJob({
        name,
        taskType,
        payload,
        priority,
        schedule,
        retryPolicy,
        timeoutMs,
      });

      res.status(201).json({ success: true, job });
    } catch (err: any) {
      res.status(500).json({ success: false, error: err.message });
    }
  });

  app.get('/api/v1/jobs/:id', async (req: Request, res: Response) => {
    try {
      const jobId = String(req.params.id);
      const job = await cluster.getJob(jobId);
      if (!job) return res.status(404).json({ success: false, error: 'Job not found' });
      res.json({ success: true, job });
    } catch (err: any) {
      res.status(500).json({ success: false, error: err.message });
    }
  });

  app.post('/api/v1/jobs/:id/run', async (req: Request, res: Response) => {
    try {
      const jobId = String(req.params.id);
      const job = await cluster.runNow(jobId);
      if (!job) return res.status(404).json({ success: false, error: 'Job not found' });
      res.json({ success: true, job });
    } catch (err: any) {
      res.status(500).json({ success: false, error: err.message });
    }
  });

  app.post('/api/v1/jobs/:id/pause', async (req: Request, res: Response) => {
    try {
      const jobId = String(req.params.id);
      const ok = await cluster.pauseJob(jobId);
      res.json({ success: ok });
    } catch (err: any) {
      res.status(500).json({ success: false, error: err.message });
    }
  });

  app.post('/api/v1/jobs/:id/resume', async (req: Request, res: Response) => {
    try {
      const jobId = String(req.params.id);
      const ok = await cluster.resumeJob(jobId);
      res.json({ success: ok });
    } catch (err: any) {
      res.status(500).json({ success: false, error: err.message });
    }
  });

  app.delete('/api/v1/jobs/:id', async (req: Request, res: Response) => {
    try {
      const jobId = String(req.params.id);
      const ok = await cluster.cancelJob(jobId);
      res.json({ success: ok });
    } catch (err: any) {
      res.status(500).json({ success: false, error: err.message });
    }
  });

  // --- Dead Letter Queue (DLQ) ---
  app.get('/api/v1/dlq', async (_req: Request, res: Response) => {
    try {
      const jobs = await cluster.getDlqJobs();
      res.json({ success: true, count: jobs.length, jobs });
    } catch (err: any) {
      res.status(500).json({ success: false, error: err.message });
    }
  });

  app.post('/api/v1/dlq/:id/replay', async (req: Request, res: Response) => {
    try {
      const jobId = String(req.params.id);
      const job = await cluster.replayDlqJob(jobId);
      if (!job) return res.status(404).json({ success: false, error: 'Job not found in DLQ' });
      res.json({ success: true, job });
    } catch (err: any) {
      res.status(500).json({ success: false, error: err.message });
    }
  });

  // --- Chaos Injection Endpoints ---
  app.post('/api/v1/chaos/:action', async (req: Request, res: Response) => {
    const action = String(req.params.action);
    const { targetId, latencyMs, count } = req.body || {};

    switch (action) {
      case 'kill-worker':
        chaos.killWorker(targetId);
        return res.json({ success: true, message: `Worker ${targetId} killed.` });
      case 'revive-worker':
        chaos.reviveWorker(targetId);
        return res.json({ success: true, message: `Worker ${targetId} revived.` });
      case 'crash-scheduler':
        chaos.crashScheduler(targetId);
        return res.json({ success: true, message: `Scheduler ${targetId} crashed.` });
      case 'revive-scheduler':
        chaos.reviveScheduler(targetId);
        return res.json({ success: true, message: `Scheduler ${targetId} revived.` });
      case 'set-latency':
        chaos.setStorageLatency(Number(latencyMs) || 0);
        return res.json({ success: true, message: `Storage latency set to ${latencyMs}ms.` });
      case 'job-burst':
        await chaos.triggerJobSpike(Number(count) || 25);
        return res.json({ success: true, message: `Dispatched burst of jobs.` });
      case 'flaky-cascade':
        await chaos.triggerFlakyCascade();
        return res.json({ success: true, message: `Flaky cascade simulation launched.` });
      default:
        return res.status(400).json({ success: false, error: `Unknown chaos action: ${action}` });
    }
  });

  // --- Real-time Telemetry & SSE Stream ---
  app.get('/api/v1/telemetry', async (_req: Request, res: Response) => {
    try {
      const telemetry = await cluster.getTelemetry();
      res.json({ success: true, telemetry });
    } catch (err: any) {
      res.status(500).json({ success: false, error: err.message });
    }
  });

  app.get('/api/v1/telemetry/stream', (req: Request, res: Response) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.flushHeaders();

    // Stream live cluster logs
    const unsubscribe = ClusterBus.getInstance().subscribe((event) => {
      res.write(`event: log\ndata: ${JSON.stringify(event)}\n\n`);
    });

    // Stream full telemetry updates every 1000ms
    const interval = setInterval(async () => {
      try {
        const telemetry = await cluster.getTelemetry();
        res.write(`event: telemetry\ndata: ${JSON.stringify(telemetry)}\n\n`);
      } catch (err) {
        // stream might be closed
      }
    }, 1000);

    req.on('close', () => {
      clearInterval(interval);
      unsubscribe();
    });
  });

  return app;
}
