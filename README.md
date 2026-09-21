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

## 🚀 Quick Start

### 1. Install Dependencies
```bash
npm install
```

### 2. Run the Development Server & Live Dashboard
```bash
npm run dev
```
Open **[http://localhost:4000](http://localhost:4000)** in your browser.

### 3. Run the Automated Distributed Systems Test Suite
```bash
npm test
```

Verifies:
1. **Multi-Scheduler Concurrency**: 3 concurrent schedulers processing 30 simultaneous jobs with zero duplicate dispatches.
2. **Worker Crash Recovery**: Automatic orphan job reclamation upon sudden worker SIGKILL.
3. **Retry & DLQ**: Exponential backoff progression and poisoned-pill quarantine in the Dead Letter Queue.

---

## 📡 REST API Reference

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `POST` | `/api/v1/jobs` | Schedule a new job (Immediate, Delayed, or Cron) |
| `GET` | `/api/v1/jobs` | List all jobs with their current status |
| `GET` | `/api/v1/jobs/:id` | Get details and execution history for a job |
| `POST` | `/api/v1/jobs/:id/run` | Trigger immediate execution of a job |
| `POST` | `/api/v1/jobs/:id/pause` | Pause a scheduled or recurring job |
| `POST` | `/api/v1/jobs/:id/resume` | Resume a paused job |
| `DELETE` | `/api/v1/jobs/:id` | Cancel/delete a scheduled job |
| `GET` | `/api/v1/dlq` | List all quarantined jobs in the Dead Letter Queue |
| `POST` | `/api/v1/dlq/:id/replay` | Replay a poisoned-pill job from the DLQ |
| `POST` | `/api/v1/chaos/:action` | Inject chaos (`kill-worker`, `revive-worker`, `crash-scheduler`, `job-burst`, `flaky-cascade`) |
| `GET` | `/api/v1/telemetry/stream` | Real-time Server-Sent Events (SSE) telemetry and log stream |

---

## 🛡️ License
MIT