// AetherSched Observability & Chaos Control Frontend

let eventSource = null;
let currentJobs = [];
let currentDlq = [];

document.addEventListener('DOMContentLoaded', () => {
  initClock();
  initSSE();
  initFormListeners();
  initChaosListeners();
  loadJobs();
});

// 1. UTC System Clock
function initClock() {
  const clockEl = document.getElementById('systemClock');
  setInterval(() => {
    const now = new Date();
    clockEl.textContent = now.toISOString().slice(11, 19) + ' UTC';
  }, 1000);
}

// 2. Server-Sent Events (SSE) Stream
function initSSE() {
  if (eventSource) eventSource.close();
  eventSource = new EventSource('/api/v1/telemetry/stream');

  eventSource.addEventListener('telemetry', (e) => {
    try {
      const data = JSON.parse(e.data);
      updateTelemetry(data);
    } catch (err) {
      console.error('Error parsing telemetry SSE:', err);
    }
  });

  eventSource.addEventListener('log', (e) => {
    try {
      const log = JSON.parse(e.data);
      appendLogEntry(log);
    } catch (err) {
      console.error('Error parsing log SSE:', err);
    }
  });

  eventSource.onerror = () => {
    document.getElementById('clusterStatusText').textContent = 'RECONNECTING...';
    document.querySelector('.cluster-pill').style.borderColor = 'var(--accent-amber)';
    document.querySelector('.cluster-pill').style.color = 'var(--accent-amber)';
    document.querySelector('.pulse-dot').style.backgroundColor = 'var(--accent-amber)';
  };

  eventSource.onopen = () => {
    document.getElementById('clusterStatusText').textContent = 'CLUSTER ACTIVE';
    document.querySelector('.cluster-pill').style.borderColor = 'rgba(16, 185, 129, 0.25)';
    document.querySelector('.cluster-pill').style.color = 'var(--accent-emerald)';
    document.querySelector('.pulse-dot').style.backgroundColor = 'var(--accent-emerald)';
  };
}

// 3. Update Dashboard from Telemetry Snapshot
function updateTelemetry(telemetry) {
  const m = telemetry.metrics;

  // Metric Cards
  document.getElementById('metricTotalJobs').textContent = m.totalJobs;
  document.getElementById('metricJobBreakdown').textContent = 
    `${m.scheduledCount} sched · ${m.runningCount} run · ${m.successCount} succ · ${m.failedCount} fail`;

  const activeSched = telemetry.schedulers.filter(s => s.status === 'ACTIVE').length;
  document.getElementById('metricSchedulers').textContent = `${activeSched} / ${telemetry.schedulers.length}`;

  const healthyWorkers = telemetry.workers.filter(w => w.status === 'HEALTHY' || w.status === 'BUSY').length;
  document.getElementById('metricWorkers').textContent = `${healthyWorkers} / ${telemetry.workers.length}`;

  document.getElementById('metricRunningLeases').textContent = m.runningCount;
  document.getElementById('metricDlqCount').textContent = m.dlqCount;
  document.getElementById('dlqBadge').textContent = `${m.dlqCount} Poisoned Jobs`;

  // Render Topology
  renderSchedulers(telemetry.schedulers);
  renderWorkers(telemetry.workers);

  // Render DLQ
  renderDlq(telemetry.dlqJobs);

  // Periodically re-sync jobs table
  loadJobs();
}

function renderSchedulers(schedulers) {
  const container = document.getElementById('schedulersContainer');
  container.innerHTML = '';

  schedulers.forEach(s => {
    const isCrashed = s.status === 'CRASHED';
    const div = document.createElement('div');
    div.className = `node-item ${isCrashed ? 'crashed' : ''}`;
    div.innerHTML = `
      <div class="node-main-info">
        <div class="node-status-dot"></div>
        <div>
          <div class="node-name">${s.id} <span style="font-size: 0.7rem; color: var(--text-muted)">(${s.hostname})</span></div>
          <div class="node-meta">Dispatched: ${s.jobsDispatched} jobs · Status: ${s.status}</div>
        </div>
      </div>
      <div class="node-actions">
        ${isCrashed 
          ? `<button class="btn btn-xs btn-emerald" onclick="reviveScheduler('${s.id}')">Revive</button>`
          : `<button class="btn btn-xs btn-danger" onclick="crashScheduler('${s.id}')">Crash</button>`
        }
      </div>
    `;
    container.appendChild(div);
  });
}

function renderWorkers(workers) {
  const container = document.getElementById('workersContainer');
  container.innerHTML = '';

  workers.forEach(w => {
    const isCrashed = w.status === 'CRASHED' || w.status === 'DEAD';
    const activeCount = w.activeJobs ? w.activeJobs.length : 0;
    const pct = Math.min(100, Math.round((activeCount / w.concurrency) * 100));

    const div = document.createElement('div');
    div.className = `node-item ${isCrashed ? 'crashed' : ''}`;
    div.innerHTML = `
      <div class="node-main-info">
        <div class="node-status-dot"></div>
        <div>
          <div class="node-name">${w.id} <span style="font-size: 0.7rem; color: var(--text-muted)">(${w.status})</span></div>
          <div class="node-meta">Executed: ${w.totalExecuted} · Failed: ${w.totalFailed}</div>
          <div class="concurrency-bar-wrap">
            <div class="concurrency-fill" style="width: ${pct}%"></div>
          </div>
          <div style="font-size:0.68rem; color:var(--text-muted); margin-top:2px;">Lease Load: ${activeCount}/${w.concurrency}</div>
        </div>
      </div>
      <div class="node-actions">
        ${isCrashed 
          ? `<button class="btn btn-xs btn-emerald" onclick="reviveWorker('${w.id}')">Revive</button>`
          : `<button class="btn btn-xs btn-danger" onclick="killWorker('${w.id}')">Kill (SIGKILL)</button>`
        }
      </div>
    `;
    container.appendChild(div);
  });
}

async function loadJobs() {
  try {
    const res = await fetch('/api/v1/jobs');
    const data = await res.json();
    if (data.success) {
      currentJobs = data.jobs;
      renderJobsTable(currentJobs);
    }
  } catch (err) {
    console.error('Failed to load jobs:', err);
  }
}

function renderJobsTable(jobs) {
  const tbody = document.getElementById('jobsTableBody');
  tbody.innerHTML = '';

  if (jobs.length === 0) {
    tbody.innerHTML = '<tr><td colspan="7" class="empty-state">No jobs found in cluster.</td></tr>';
    return;
  }

  jobs.slice(0, 15).forEach(job => {
    const tr = document.createElement('tr');
    const schedDesc = job.schedule.type === 'CRON' 
      ? `Cron (${job.schedule.cronExpr})` 
      : job.schedule.type === 'DELAYED' 
        ? `Delayed (In ${Math.max(0, Math.round((job.nextRunTime - Date.now()) / 1000))}s)`
        : 'Immediate';

    tr.innerHTML = `
      <td>
        <strong>${escapeHtml(job.name)}</strong>
        <div style="font-size:0.7rem; color:var(--text-muted); font-family:var(--font-mono);">${job.id}</div>
      </td>
      <td><span class="badge">${job.taskType}</span></td>
      <td><span style="font-weight:600; font-size:0.75rem;">${job.priority}</span></td>
      <td><span style="font-size:0.75rem; color:var(--text-secondary);">${schedDesc}</span></td>
      <td><span class="status-pill status-${job.status}">${job.status}</span></td>
      <td><span style="font-family:var(--font-mono); font-size:0.75rem;">${job.currentAttempt}/${job.maxRetries}</span></td>
      <td>
        <div style="display:flex; gap:0.3rem;">
          <button class="btn btn-xs btn-outline" onclick="runJobNow('${job.id}')">Run Now</button>
          ${job.status === 'PAUSED' 
            ? `<button class="btn btn-xs btn-outline" onclick="resumeJob('${job.id}')">Resume</button>`
            : `<button class="btn btn-xs btn-ghost" onclick="pauseJob('${job.id}')">Pause</button>`
          }
        </div>
      </td>
    `;
    tbody.appendChild(tr);
  });
}

function renderDlq(dlqJobs) {
  const container = document.getElementById('dlqContainer');
  if (!dlqJobs || dlqJobs.length === 0) {
    container.innerHTML = '<div class="empty-state">No failed poisoned pills in Dead Letter Queue. System is healthy!</div>';
    return;
  }

  container.innerHTML = '';
  dlqJobs.forEach(job => {
    const div = document.createElement('div');
    div.className = 'dlq-item';
    div.innerHTML = `
      <div class="dlq-info">
        <span class="dlq-name">${escapeHtml(job.name)} (${job.id})</span>
        <span class="dlq-error">${escapeHtml(job.lastError || 'Max retries exhausted')}</span>
      </div>
      <button class="btn btn-xs btn-rose" onclick="replayDlq('${job.id}')">Replay Job ➔</button>
    `;
    container.appendChild(div);
  });
}

function appendLogEntry(log) {
  const container = document.getElementById('logStream');
  const div = document.createElement('div');
  div.className = `log-entry ${log.level}`;
  const timeStr = new Date(log.timestamp).toLocaleTimeString();

  div.innerHTML = `
    <span class="log-time">[${timeStr}]</span>
    <span class="log-source">[${escapeHtml(log.source)}]</span>
    <span class="log-msg">${escapeHtml(log.message)}</span>
  `;

  container.prepend(div);
  if (container.children.length > 100) {
    container.removeChild(container.lastChild);
  }
}

// 4. Form & User Interactions
function initFormListeners() {
  const scheduleTypeSelect = document.getElementById('scheduleType');
  const delayGroup = document.getElementById('delayGroup');
  const cronGroup = document.getElementById('cronGroup');

  scheduleTypeSelect.addEventListener('change', () => {
    const val = scheduleTypeSelect.value;
    delayGroup.style.display = val === 'DELAYED' ? 'flex' : 'none';
    cronGroup.style.display = val === 'CRON' ? 'flex' : 'none';
  });

  document.getElementById('jobForm').addEventListener('submit', async (e) => {
    e.preventDefault();
    const name = document.getElementById('jobName').value;
    const taskType = document.getElementById('taskType').value;
    const priority = document.getElementById('jobPriority').value;
    const schedType = document.getElementById('scheduleType').value;
    const maxRetries = parseInt(document.getElementById('maxRetries').value, 10);
    const baseDelayMs = parseInt(document.getElementById('baseDelayMs').value, 10);

    const schedule = { type: schedType };
    if (schedType === 'DELAYED') {
      const delaySec = parseInt(document.getElementById('delaySeconds').value, 10);
      schedule.runAt = Date.now() + (delaySec * 1000);
    } else if (schedType === 'CRON') {
      schedule.cronExpr = document.getElementById('cronExpr').value;
    }

    try {
      const res = await fetch('/api/v1/jobs', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name,
          taskType,
          priority,
          schedule,
          retryPolicy: { maxRetries, baseDelayMs, backoffMultiplier: 2, jitter: true }
        })
      });
      const data = await res.json();
      if (data.success) {
        loadJobs();
      } else {
        alert('Failed: ' + data.error);
      }
    } catch (err) {
      alert('Error creating job: ' + err.message);
    }
  });

  document.getElementById('btnRefreshJobs').addEventListener('click', loadJobs);
  document.getElementById('btnClearLogs').addEventListener('click', () => {
    document.getElementById('logStream').innerHTML = '';
  });
}

// 5. Chaos Engineering Triggers
function initChaosListeners() {
  document.getElementById('btnChaosKillWorker').addEventListener('click', () => killWorker('Worker-1'));
  document.getElementById('btnChaosReviveWorker').addEventListener('click', () => reviveWorker('Worker-1'));
  document.getElementById('btnChaosSpike').addEventListener('click', async () => {
    await fetch('/api/v1/chaos/job-burst', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ count: 20 })
    });
  });
  document.getElementById('btnChaosFlaky').addEventListener('click', async () => {
    await fetch('/api/v1/chaos/flaky-cascade', { method: 'POST' });
  });
}

// Global actions invoked from inline onclicks
window.killWorker = async (id) => {
  await fetch('/api/v1/chaos/kill-worker', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ targetId: id })
  });
};

window.reviveWorker = async (id) => {
  await fetch('/api/v1/chaos/revive-worker', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ targetId: id })
  });
};

window.crashScheduler = async (id) => {
  await fetch('/api/v1/chaos/crash-scheduler', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ targetId: id })
  });
};

window.reviveScheduler = async (id) => {
  await fetch('/api/v1/chaos/revive-scheduler', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ targetId: id })
  });
};

window.runJobNow = async (id) => {
  await fetch(`/api/v1/jobs/${id}/run`, { method: 'POST' });
  loadJobs();
};

window.pauseJob = async (id) => {
  await fetch(`/api/v1/jobs/${id}/pause`, { method: 'POST' });
  loadJobs();
};

window.resumeJob = async (id) => {
  await fetch(`/api/v1/jobs/${id}/resume`, { method: 'POST' });
  loadJobs();
};

window.replayDlq = async (id) => {
  await fetch(`/api/v1/dlq/${id}/replay`, { method: 'POST' });
  loadJobs();
};

function escapeHtml(str) {
  if (!str) return '';
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}
