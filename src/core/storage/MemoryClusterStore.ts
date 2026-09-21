import { ExecutionLease } from '../types.js';
import { IStorageAdapter } from './IStorageAdapter.js';

interface LockEntry {
  ownerId: string;
  expiresAt: number;
}

export class MemoryClusterStore implements IStorageAdapter {
  private kvStore = new Map<string, { value: string; expiresAt?: number }>();
  private zsets = new Map<string, Map<string, number>>(); // key -> (member -> score)
  private queues = new Map<string, string[]>(); // key -> list of string items
  private locks = new Map<string, LockEntry>();
  private leases = new Map<string, ExecutionLease>(); // jobId -> ExecutionLease

  // Chaos injection support
  private simulatedLatencyMs = 0;

  public setSimulatedLatency(ms: number) {
    this.simulatedLatencyMs = ms;
  }

  private async applyChaosLatency(): Promise<void> {
    if (this.simulatedLatencyMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, this.simulatedLatencyMs));
    }
  }

  // --- KV Operations ---
  async get(key: string): Promise<string | null> {
    await this.applyChaosLatency();
    const entry = this.kvStore.get(key);
    if (!entry) return null;
    if (entry.expiresAt && Date.now() > entry.expiresAt) {
      this.kvStore.delete(key);
      return null;
    }
    return entry.value;
  }

  async set(key: string, value: string, ttlMs?: number): Promise<void> {
    await this.applyChaosLatency();
    const expiresAt = ttlMs ? Date.now() + ttlMs : undefined;
    this.kvStore.set(key, { value, expiresAt });
  }

  async del(key: string): Promise<boolean> {
    await this.applyChaosLatency();
    return this.kvStore.delete(key);
  }

  async keys(pattern: string): Promise<string[]> {
    await this.applyChaosLatency();
    const regex = new RegExp('^' + pattern.replace(/\*/g, '.*') + '$');
    const now = Date.now();
    const results: string[] = [];

    for (const [k, entry] of this.kvStore.entries()) {
      if (entry.expiresAt && now > entry.expiresAt) {
        this.kvStore.delete(k);
        continue;
      }
      if (regex.test(k)) results.push(k);
    }
    return results;
  }

  // --- ZSET Operations ---
  async zadd(key: string, score: number, member: string): Promise<number> {
    await this.applyChaosLatency();
    let zset = this.zsets.get(key);
    if (!zset) {
      zset = new Map<string, number>();
      this.zsets.set(key, zset);
    }
    const isNew = !zset.has(member);
    zset.set(member, score);
    return isNew ? 1 : 0;
  }

  async zrangebyscore(key: string, minScore: number, maxScore: number, limit?: number): Promise<string[]> {
    await this.applyChaosLatency();
    const zset = this.zsets.get(key);
    if (!zset) return [];

    const matches: { member: string; score: number }[] = [];
    for (const [member, score] of zset.entries()) {
      if (score >= minScore && score <= maxScore) {
        matches.push({ member, score });
      }
    }

    matches.sort((a, b) => a.score - b.score);
    const sliced = limit ? matches.slice(0, limit) : matches;
    return sliced.map((m) => m.member);
  }

  async zrem(key: string, member: string): Promise<boolean> {
    await this.applyChaosLatency();
    const zset = this.zsets.get(key);
    if (!zset) return false;
    return zset.delete(member);
  }

  async zscore(key: string, member: string): Promise<number | null> {
    await this.applyChaosLatency();
    const zset = this.zsets.get(key);
    if (!zset || !zset.has(member)) return null;
    return zset.get(member)!;
  }

  async zcount(key: string, minScore: number, maxScore: number): Promise<number> {
    await this.applyChaosLatency();
    const zset = this.zsets.get(key);
    if (!zset) return 0;
    let count = 0;
    for (const score of zset.values()) {
      if (score >= minScore && score <= maxScore) count++;
    }
    return count;
  }

  async zcard(key: string): Promise<number> {
    await this.applyChaosLatency();
    const zset = this.zsets.get(key);
    return zset ? zset.size : 0;
  }

  // --- Queue Operations ---
  async rpush(key: string, value: string): Promise<number> {
    await this.applyChaosLatency();
    let queue = this.queues.get(key);
    if (!queue) {
      queue = [];
      this.queues.set(key, queue);
    }
    queue.push(value);
    return queue.length;
  }

  async lpop(key: string): Promise<string | null> {
    await this.applyChaosLatency();
    const queue = this.queues.get(key);
    if (!queue || queue.length === 0) return null;
    return queue.shift() || null;
  }

  async llen(key: string): Promise<number> {
    await this.applyChaosLatency();
    const queue = this.queues.get(key);
    return queue ? queue.length : 0;
  }

  async lrange(key: string, start: number, stop: number): Promise<string[]> {
    await this.applyChaosLatency();
    const queue = this.queues.get(key);
    if (!queue) return [];
    if (stop === -1) return queue.slice(start);
    return queue.slice(start, stop + 1);
  }

  // --- Distributed Lock Primitives ---
  async acquireLock(lockKey: string, ownerId: string, ttlMs: number): Promise<boolean> {
    await this.applyChaosLatency();
    const now = Date.now();
    const existing = this.locks.get(lockKey);

    if (existing && existing.expiresAt > now && existing.ownerId !== ownerId) {
      return false; // Lock already held by someone else
    }

    this.locks.set(lockKey, {
      ownerId,
      expiresAt: now + ttlMs,
    });
    return true;
  }

  async releaseLock(lockKey: string, ownerId: string): Promise<boolean> {
    await this.applyChaosLatency();
    const existing = this.locks.get(lockKey);
    if (!existing) return true;
    if (existing.ownerId === ownerId) {
      this.locks.delete(lockKey);
      return true;
    }
    return false; // Cannot release someone else's lock
  }

  async renewLock(lockKey: string, ownerId: string, ttlMs: number): Promise<boolean> {
    await this.applyChaosLatency();
    const existing = this.locks.get(lockKey);
    if (!existing || existing.ownerId !== ownerId) {
      return false;
    }
    existing.expiresAt = Date.now() + ttlMs;
    return true;
  }

  // --- Worker Lease Management ---
  async setLease(lease: ExecutionLease): Promise<void> {
    await this.applyChaosLatency();
    this.leases.set(lease.jobId, { ...lease });
  }

  async getLease(jobId: string): Promise<ExecutionLease | null> {
    await this.applyChaosLatency();
    return this.leases.get(jobId) || null;
  }

  async getAllLeases(): Promise<ExecutionLease[]> {
    await this.applyChaosLatency();
    return Array.from(this.leases.values());
  }

  async removeLease(jobId: string): Promise<boolean> {
    await this.applyChaosLatency();
    return this.leases.delete(jobId);
  }

  // --- Atomic Lookahead Dispatch (Lua Script Equivalent) ---
  // Guarantees atomic transition from Delayed ZSET to Ready Queue
  async atomicClaimDueJobs(
    delayedZSetKey: string,
    readyQueueKey: string,
    maxTimestamp: number,
    limit: number,
    _schedulerId: string
  ): Promise<string[]> {
    await this.applyChaosLatency();
    const zset = this.zsets.get(delayedZSetKey);
    if (!zset) return [];

    const dueJobs: { member: string; score: number }[] = [];
    for (const [member, score] of zset.entries()) {
      if (score <= maxTimestamp) {
        dueJobs.push({ member, score });
      }
    }

    dueJobs.sort((a, b) => a.score - b.score);
    const claimed = dueJobs.slice(0, limit);
    const claimedJobIds: string[] = [];

    let readyQueue = this.queues.get(readyQueueKey);
    if (!readyQueue) {
      readyQueue = [];
      this.queues.set(readyQueueKey, readyQueue);
    }

    for (const item of claimed) {
      // Atomically remove from delayed set
      zset.delete(item.member);
      // Push to ready queue
      readyQueue.push(item.member);
      claimedJobIds.push(item.member);
    }

    return claimedJobIds;
  }

  async clear(): Promise<void> {
    this.kvStore.clear();
    this.zsets.clear();
    this.queues.clear();
    this.locks.clear();
    this.leases.clear();
  }
}
