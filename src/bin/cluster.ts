import { fork, ChildProcess } from 'child_process';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

interface ManagedProcess {
  id: string;
  type: 'scheduler' | 'worker' | 'reaper' | 'api';
  process: ChildProcess;
  pid: number;
  args: string[];
}

export class ClusterSupervisor {
  private processes = new Map<string, ManagedProcess>();

  public spawnProcess(id: string, type: 'scheduler' | 'worker' | 'reaper' | 'api', customArgs: string[] = []): ManagedProcess {
    let scriptFile = '';
    switch (type) {
      case 'scheduler':
        scriptFile = path.join(__dirname, 'scheduler.ts');
        break;
      case 'worker':
        scriptFile = path.join(__dirname, 'worker.ts');
        break;
      case 'reaper':
        scriptFile = path.join(__dirname, 'reaper.ts');
        break;
      case 'api':
        scriptFile = path.join(__dirname, '../index.ts');
        break;
    }

    const fullArgs = [`--id=${id}`, ...customArgs];
    const child = fork(scriptFile, fullArgs, {
      execArgv: ['--loader', 'tsx'],
      env: {
        ...process.env,
        NODE_ID: id,
        USE_REDIS: 'true',
      },
      stdio: 'inherit',
    });

    const managed: ManagedProcess = {
      id,
      type,
      process: child,
      pid: child.pid!,
      args: customArgs,
    };

    this.processes.set(id, managed);

    child.on('exit', (code, signal) => {
      console.log(`[Supervisor] Process ${id} (PID: ${managed.pid}) exited with code ${code}, signal ${signal}`);
      if (this.processes.get(id)?.pid === managed.pid) {
        this.processes.delete(id);
      }
    });

    console.log(`[Supervisor] Spawned ${type.toUpperCase()} '${id}' as child process (PID: ${managed.pid})`);
    return managed;
  }

  // Real SIGKILL ("kill -9")
  public killProcess(id: string, signal: NodeJS.Signals | number = 'SIGKILL'): boolean {
    const proc = this.processes.get(id);
    if (!proc || !proc.process) {
      console.warn(`[Supervisor] No active process found for ${id}`);
      return false;
    }

    console.log(`[Supervisor] 💥 Sending ${signal} (kill -9) to ${id} (PID: ${proc.pid})!`);
    try {
      if (typeof signal === 'number') {
        process.kill(proc.pid, signal);
      } else {
        process.kill(proc.pid, signal);
      }
      this.processes.delete(id);
      return true;
    } catch (err: any) {
      console.error(`[Supervisor] Failed to kill ${id}:`, err.message);
      return false;
    }
  }

  public reviveProcess(id: string): ManagedProcess | null {
    if (this.processes.has(id)) {
      console.warn(`[Supervisor] Process ${id} is already running.`);
      return this.processes.get(id)!;
    }

    let type: 'scheduler' | 'worker' | 'reaper' | 'api' = 'worker';
    if (id.toLowerCase().includes('scheduler')) type = 'scheduler';
    else if (id.toLowerCase().includes('reaper')) type = 'reaper';
    else if (id.toLowerCase().includes('api')) type = 'api';

    console.log(`[Supervisor] 🔄 Reviving process ${id} (${type})...`);
    return this.spawnProcess(id, type);
  }

  public getStatus(): Record<string, { pid: number; type: string; alive: boolean }> {
    const res: Record<string, { pid: number; type: string; alive: boolean }> = {};
    for (const [id, item] of this.processes.entries()) {
      res[id] = {
        pid: item.pid,
        type: item.type,
        alive: !item.process.killed && item.process.connected,
      };
    }
    return res;
  }

  public shutdownAll(): void {
    console.log('[Supervisor] Shutting down all managed processes...');
    for (const [id, proc] of this.processes.entries()) {
      try {
        proc.process.kill('SIGTERM');
      } catch {}
    }
    this.processes.clear();
  }
}

// CLI runner if invoked directly
if (process.argv[1] && process.argv[1].endsWith('cluster.ts')) {
  const supervisor = new ClusterSupervisor();

  console.log('========================================================');
  console.log('⚡ AetherSched Multi-Process Cluster Supervisor Starting');
  console.log('Spawning 1 Reaper, 2 Schedulers, and 3 Workers as real OS processes');
  console.log('========================================================\n');

  // 1 Reaper
  supervisor.spawnProcess('Reaper-1', 'reaper');

  // 2 Schedulers
  supervisor.spawnProcess('Scheduler-1', 'scheduler');
  supervisor.spawnProcess('Scheduler-2', 'scheduler');

  // 3 Workers
  supervisor.spawnProcess('Worker-1', 'worker');
  supervisor.spawnProcess('Worker-2', 'worker');
  supervisor.spawnProcess('Worker-3', 'worker');

  process.on('SIGINT', () => {
    supervisor.shutdownAll();
    process.exit(0);
  });
}
