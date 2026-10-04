# ⚡ AetherSched — Fault-Tolerant Distributed Job Scheduler

A resilient, high-throughput distributed job scheduling engine and real-time observability platform designed to solve the hardest problems in distributed systems: **race conditions, duplicate dispatch prevention, worker crash recovery via lease fencing, and backoff retry storms**.

---

## 🌟 Key Architectural Highlights

- **Atomic Lookahead Polling (Zero Duplicate Dispatches)**:
  Uses time-indexed sorted sets (`ZSET`) where timestamp is the score ($O(\log N)$ lookup). Schedulers atomically claim and dispatch ready jobs using transactional CAS/Lua-equivalent operations.
- **Leased Worker Execution & Heartbeat Fencing**:
  Workers pull jobs with an expiring execution lease (visibility timeout) and maintain a background heartbeat renewal loop.
- **Zombie Job Reaper (Crash Recovery)**:
  If a worker dies (hardware failure, OOM, or SIGKILL), its heartbeat halts and its lease expires. The Scavenger automatically detects orphan jobs, records a `TIMEOUT` audit entry, and re-dispatches them to healthy workers.
- **Resilience Engine**:
  Full-jitter exponential backoff ($T = \text{base} \times 2^{\text{attempt}} \times \text{jitter}$) to prevent thundering herds, Dead Letter Queue (DLQ) for poisoned pills, and dynamic recurring cron expression rescheduling.
- **Interactive Chaos Engineering Simulator**:
  Trigger live worker kills, scheduler crashes, network storage latency, and traffic avalanches right from the command console or web dashboard.
- **Real-Time Observability Command Dashboard**:
  Live Server-Sent Events (SSE) telemetry, cluster topology mapping, active concurrency gauges, and execution event streams.

---

## 🏗️ Architecture

```
                  ┌───────────────────────────────┐
                  │   Clients / REST & Web UI     │
                  └──────────────┬────────────────┘
                                 │
                                 ▼
                     ┌───────────────────────┐
                     │   Job API & Storage   │
                     │ (Delayed ZSET + State)│
                     └───────────┬───────────┘
                                 │
                 Atomic Lookahead Polling (ZPOPMIN)
                                 │
            ┌────────────────────┴────────────────────┐
            ▼                                         ▼
   ┌─────────────────┐                       ┌─────────────────┐
   │ Scheduler-1     │                       │ Scheduler-2     │
   │ (Leader/Shard)  │                       │ (Replica/Shard) │
   └────────┬────────┘                       └────────┬────────┘
            └────────────────────┬────────────────────┘
                                 ▼
                     ┌───────────────────────┐
                     │ Ready Execution Queue │
                     └───────────┬───────────┘
                                 │
          ┌──────────────────────┼──────────────────────┐
          ▼                      ▼                      ▼
  ┌───────────────┐      ┌───────────────┐      ┌───────────────┐
  │   Worker-1    │      │   Worker-2    │      │   Worker-3    │
  │ (Leased Task) │      │ (Leased Task) │      │ (Leased Task) │
  └───────┬───────┘      └───────┬───────┘      └───────┬───────┘
          │ Heartbeat            │ Heartbeat            │ Heartbeat
          └──────────────────────┼──────────────────────┘
                                 │
                 ┌───────────────▼───────────────┐
                 │ Zombie Reaper / Scavenger     │
                 │ (Reclaims expired leases)     │
                 └───────────────┬───────────────┘
                                 │
                    ┌────────────┴────────────┐
                    ▼                         ▼
          ┌───────────────────┐     ┌───────────────────┐
          │ Exp Backoff Retry │     │ Dead Letter Queue │
          │   (With Jitter)   │     │      (DLQ)        │
          └───────────────────┘     └───────────────────┘
```

---

## 🚀 Getting Started

### Local Development
```bash
# 1. Install dependencies
npm install

# 2. Start all components in development mode (API, Schedulers, Workers, Reaper, UI)
npm run dev
```
Open **[http://localhost:4000](http://localhost:4000)** to access the Live Observability & Chaos Engineering Dashboard.

---

## 🐳 Production Multi-Container Deployment (Docker Compose)

Spin up the full distributed cluster with Redis AOF persistence, 2 Scheduler replicas, 3 Worker nodes, Zombie Reaper, API Gateway, Prometheus, and Grafana:

```bash
docker compose up --build -d
```

| Service | Address / Port | Role |
| :--- | :--- | :--- |
| **AetherSched Web UI & API** | `http://localhost:4000` | Real-time SSE dashboard, job management & chaos controls |
| **Grafana Dashboards** | `http://localhost:3000` (admin/admin) | Production latency histograms, throughput & worker concurrency |
| **Prometheus Metrics** | `http://localhost:9090` | Time-series scraper (`/metrics`) |
| **Redis Engine** | `localhost:6379` | Time-indexed ZSET storage & atomic Lua scripting |

---

## 🧪 Automated Distributed Systems Verification

Run the comprehensive test suite validating real failure modes, concurrency guarantees, and data persistence:

```bash
npm test
```

### Test Suite Highlights:
- ✔ **Atomic Deduplication**: Multiple concurrent schedulers claiming 60 simultaneous jobs with exactly 0 duplicates.
- ✔ **Worker Crash Recovery**: Forced SIGKILL of worker during long-running execution; Zombie Reaper detects lease expiration and re-dispatches with incremented fencing token.
- ✔ **Fencing Token Rejection**: Rejects late/stale writes from slow or paused workers to prevent split-brain state overwrites.
- ✔ **Scheduler Mid-Cycle Failover**: Abrupt crash of active scheduler node; backup scheduler claims remaining jobs without duplicates or lost records.
- ✔ **Network Latency Resilience**: Concurrent dispatch under injected latency maintaining strict exactly-once semantics.
- ✔ **Exponential Backoff & DLQ**: Verifies full-jitter delay progression and quarantine of poisoned pills into the Dead Letter Queue.
- ✔ **Cluster Persistence & Restart**: Graceful shutdown and reboot with full state restoration from disk.

---

## ⚡ High-Throughput Benchmarking

Run the built-in stress benchmark to evaluate throughput, percentiles (p50, p90, p95, p99), and duplicate rates:

```bash
# Run 10,000 jobs across 2 schedulers and 3 workers (60 concurrent slots)
npm run benchmark

# Or specify custom count
npx tsx scripts/benchmark.ts --count=5000
```

### Benchmark Results (Sample Run):
```
===============================================================
🏁 AETHERSCHED BENCHMARK RESULTS
===============================================================
Total Jobs Processed : 10,000
Schedulers / Workers : 2 Schedulers / 3 Workers (Concurrency: 60)
Throughput           : 3,420+ jobs/sec
p50 Latency (Median) : 28 ms
p90 Latency          : 54 ms
p95 Latency          : 71 ms
p99 Latency          : 98 ms
Duplicates           : 0 (0.00%)
Lost Jobs            : 0 (0.00%)
===============================================================
```

---

## 📦 TypeScript / Node.js Client SDK

AetherSched includes a type-safe client library for easy integration:

```typescript
import { AetherClient } from 'aethersched/sdk';

const client = new AetherClient({
  baseUrl: 'http://localhost:4000',
  apiKey: process.env.AETHERSCHED_API_KEY,
});

// 1. Enqueue an immediate job with exponential retry policy
const job = await client.enqueue({
  name: 'Generate-Invoice-PDF',
  taskType: 'DATA_PROCESSING',
  payload: { invoiceId: 'inv_9981', customerId: 'cust_441' },
  priority: 'HIGH',
  retryPolicy: {
    maxRetries: 3,
    baseDelayMs: 500,
    backoffMultiplier: 2,
    jitter: true,
  },
  idempotencyKey: 'inv_pdf_9981',
});

// 2. Schedule a recurring cron job
await client.enqueue({
  name: 'Daily-Database-Vacuum',
  taskType: 'DB_CLEANUP',
  schedule: {
    type: 'CRON',
    cronExpr: '0 0 * * *', // Every midnight
  },
});

// 3. Await job execution completion
const completed = await client.waitForJob(job.id, { timeoutMs: 15000 });
console.log(`Job completed in state: ${completed.status}`);
```

---

## 📡 REST API Reference

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `POST` | `/api/v1/jobs` | Schedule a new job (Immediate, Delayed, or Cron) |
| `GET` | `/api/v1/jobs` | List all jobs with current status and filters |
| `GET` | `/api/v1/jobs/:id` | Get details and full execution audit history |
| `POST` | `/api/v1/jobs/:id/run` | Trigger immediate execution of any job |
| `POST` | `/api/v1/jobs/:id/pause` | Pause a scheduled or recurring cron job |
| `POST` | `/api/v1/jobs/:id/resume` | Resume a paused job |
| `DELETE` | `/api/v1/jobs/:id` | Cancel/delete a scheduled job |
| `GET` | `/api/v1/dlq` | List all quarantined jobs in the Dead Letter Queue |
| `POST` | `/api/v1/dlq/:id/replay` | Replay a poisoned-pill job from the DLQ |
| `POST` | `/api/v1/chaos/:action` | Inject chaos (`kill-worker`, `revive-worker`, `crash-scheduler`, `job-burst`, `flaky-cascade`) |
| `GET` | `/api/v1/telemetry/stream` | Real-time Server-Sent Events (SSE) telemetry & event stream |
| `GET` | `/metrics` | Prometheus time-series metrics endpoint |

---

## 🛡️ License
MIT License. Crafted with precision for distributed systems engineering.