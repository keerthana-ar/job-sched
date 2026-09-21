import { Job, ExecutionLease } from '../types.js';

export interface ZSetMember {
  score: number;
  member: string;
}

export interface IStorageAdapter {
  // KV Operations
  get(key: string): Promise<string | null>;
  set(key: string, value: string, ttlMs?: number): Promise<void>;
  del(key: string): Promise<boolean>;
  keys(pattern: string): Promise<string[]>;

  // Sorted Set (ZSET) Operations for Time-Indexed Delayed Queue
  zadd(key: string, score: number, member: string): Promise<number>;
  zrangebyscore(key: string, minScore: number, maxScore: number, limit?: number): Promise<string[]>;
  zrem(key: string, member: string): Promise<boolean>;
  zscore(key: string, member: string): Promise<number | null>;
  zcount(key: string, minScore: number, maxScore: number): Promise<number>;
  zcard(key: string): Promise<number>;

  // Queue (FIFO / Priority) Operations
  rpush(key: string, value: string): Promise<number>;
  lpop(key: string): Promise<string | null>;
  llen(key: string): Promise<number>;
  lrange(key: string, start: number, stop: number): Promise<string[]>;

  // Distributed Lock Primitives (Lease-based Mutex)
  acquireLock(lockKey: string, ownerId: string, ttlMs: number): Promise<boolean>;
  releaseLock(lockKey: string, ownerId: string): Promise<boolean>;
  renewLock(lockKey: string, ownerId: string, ttlMs: number): Promise<boolean>;

  // Worker Lease Management
  setLease(lease: ExecutionLease): Promise<void>;
  getLease(jobId: string): Promise<ExecutionLease | null>;
  getAllLeases(): Promise<ExecutionLease[]>;
  removeLease(jobId: string): Promise<boolean>;

  // Atomic Scheduler Lookahead Dispatch (Lua Script equivalent)
  // Atomically pops jobs with score <= now from delayed ZSET, changes their status to DISPATCHED,
  // and pushes them onto ready queue, preventing double-dispatch across multiple schedulers
  atomicClaimDueJobs(
    delayedZSetKey: string,
    readyQueueKey: string,
    maxTimestamp: number,
    limit: number,
    schedulerId: string
  ): Promise<string[]>;

  // Reset / Clear
  clear(): Promise<void>;
}
