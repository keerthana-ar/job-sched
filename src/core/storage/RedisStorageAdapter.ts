import { Redis, RedisOptions } from 'ioredis';
import { IStorageAdapter } from './IStorageAdapter.js';
import { ExecutionLease } from '../types.js';

export class RedisStorageAdapter implements IStorageAdapter {
  private client: Redis;
  private isConnected = false;

  constructor(urlOrOptions: string | RedisOptions = process.env.REDIS_URL || 'redis://127.0.0.1:6379') {
    if (typeof urlOrOptions === 'string') {
      this.client = new Redis(urlOrOptions, {
        maxRetriesPerRequest: 3,
        retryStrategy: (times) => Math.min(times * 100, 2000),
        enableReadyCheck: true,
      });
    } else {
      this.client = new Redis({
        ...urlOrOptions,
        maxRetriesPerRequest: 3,
        retryStrategy: (times) => Math.min(times * 100, 2000),
      });
    }

    this.client.on('connect', () => {
      this.isConnected = true;
    });

    this.client.on('error', (err) => {
      // Handled via logging or connection tracking
    });
  }

  public getRawClient(): Redis {
    return this.client;
  }

  // --- KV Operations ---
  async get(key: string): Promise<string | null> {
    return this.client.get(key);
  }

  async set(key: string, value: string, ttlMs?: number): Promise<void> {
    if (ttlMs && ttlMs > 0) {
      await this.client.set(key, value, 'PX', ttlMs);
    } else {
      await this.client.set(key, value);
    }
  }

  async del(key: string): Promise<boolean> {
    const res = await this.client.del(key);
    return res > 0;
  }

  async keys(pattern: string): Promise<string[]> {
    return this.client.keys(pattern);
  }

  // --- ZSET Operations ---
  async zadd(key: string, score: number, member: string): Promise<number> {
    return this.client.zadd(key, score, member);
  }

  async zrangebyscore(key: string, minScore: number, maxScore: number, limit?: number): Promise<string[]> {
    if (limit) {
      return this.client.zrangebyscore(key, minScore, maxScore, 'LIMIT', 0, limit);
    }
    return this.client.zrangebyscore(key, minScore, maxScore);
  }

  async zrem(key: string, member: string): Promise<boolean> {
    const res = await this.client.zrem(key, member);
    return res > 0;
  }

  async zscore(key: string, member: string): Promise<number | null> {
    const score = await this.client.zscore(key, member);
    return score !== null ? parseFloat(score) : null;
  }

  async zcount(key: string, minScore: number, maxScore: number): Promise<number> {
    return this.client.zcount(key, minScore, maxScore);
  }

  async zcard(key: string): Promise<number> {
    return this.client.zcard(key);
  }

  // --- Queue Operations ---
  async rpush(key: string, value: string): Promise<number> {
    return this.client.rpush(key, value);
  }

  async lpop(key: string): Promise<string | null> {
    return this.client.lpop(key);
  }

  async llen(key: string): Promise<number> {
    return this.client.llen(key);
  }

  async lrange(key: string, start: number, stop: number): Promise<string[]> {
    return this.client.lrange(key, start, stop);
  }

  // Priority Queue Pop: Checks queues in order (e.g. CRITICAL -> HIGH -> NORMAL -> LOW)
  async lpopPriority(queueKeys: string[]): Promise<{ queue: string; item: string } | null> {
    for (const key of queueKeys) {
      const item = await this.client.lpop(key);
      if (item) {
        return { queue: key, item };
      }
    }
    return null;
  }

  // --- Distributed Locks (Redlock Primitives) ---
  async acquireLock(lockKey: string, ownerId: string, ttlMs: number): Promise<boolean> {
    const result = await this.client.set(lockKey, ownerId, 'PX', ttlMs, 'NX');
    return result === 'OK';
  }

  async releaseLock(lockKey: string, ownerId: string): Promise<boolean> {
    // Atomic release only if current value matches ownerId
    const script = `
      if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("del", KEYS[1])
      else
        return 0
      end
    `;
    const res = await this.client.eval(script, 1, lockKey, ownerId);
    return res === 1;
  }

  async renewLock(lockKey: string, ownerId: string, ttlMs: number): Promise<boolean> {
    const script = `
      if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("pexpire", KEYS[1], ARGV[2])
      else
        return 0
      end
    `;
    const res = await this.client.eval(script, 1, lockKey, ownerId, ttlMs);
    return res === 1;
  }

  // --- Leases & Fencing Tokens ---
  async incrementFencingToken(jobId: string): Promise<number> {
    return this.client.incr(`fencing_token:${jobId}`);
  }

  async getFencingToken(jobId: string): Promise<number> {
    const raw = await this.client.get(`fencing_token:${jobId}`);
    return raw ? parseInt(raw, 10) : 0;
  }

  async setLease(lease: ExecutionLease): Promise<void> {
    const leaseKey = `lease:${lease.jobId}`;
    // Use generous safety TTL (1 hour) so Redis doesn't delete the record before the reaper inspects it
    const safetyTtlMs = 3600 * 1000;
    const multi = this.client.multi();
    multi.set(leaseKey, JSON.stringify(lease), 'PX', safetyTtlMs);
    multi.sadd('leases:active', lease.jobId);
    multi.zadd('leases:expirations', lease.expiresAt, lease.jobId);
    await multi.exec();
  }

  async getLease(jobId: string): Promise<ExecutionLease | null> {
    const raw = await this.client.get(`lease:${jobId}`);
    return raw ? JSON.parse(raw) : null;
  }

  async getAllLeases(): Promise<ExecutionLease[]> {
    const jobIds = await this.client.smembers('leases:active');
    if (jobIds.length === 0) return [];

    const keys = jobIds.map((id) => `lease:${id}`);
    const results = await this.client.mget(keys);
    const leases: ExecutionLease[] = [];
    const missingJobIds: string[] = [];

    for (let i = 0; i < results.length; i++) {
      const raw = results[i];
      if (raw) {
        leases.push(JSON.parse(raw));
      } else {
        missingJobIds.push(jobIds[i]);
      }
    }

    if (missingJobIds.length > 0) {
      await this.client.srem('leases:active', ...missingJobIds);
      await this.client.zrem('leases:expirations', ...missingJobIds);
    }

    return leases;
  }

  async removeLease(jobId: string): Promise<boolean> {
    const multi = this.client.multi();
    multi.del(`lease:${jobId}`);
    multi.srem('leases:active', jobId);
    multi.zrem('leases:expirations', jobId);
    const results = await multi.exec();
    return (results?.[0]?.[1] as number) > 0;
  }

  // --- Atomic Job Completion with Fencing Token Verification ---
  async completeJobAtomic(
    jobId: string,
    fencingToken: number,
    jobData: string,
    idempotencyKey?: string,
    idempotencyResult?: string
  ): Promise<{ success: boolean; reason?: string }> {
    const script = `
      local tokenKey = KEYS[1]
      local leaseKey = KEYS[2]
      local jobKey = KEYS[3]
      local activeSet = KEYS[4]
      local idempKey = KEYS[5]

      local submittedToken = tonumber(ARGV[1])
      local currentToken = tonumber(redis.call('GET', tokenKey) or '0')

      if submittedToken < currentToken then
        return {0, "STALE_FENCING_TOKEN: submitted " .. submittedToken .. " < current " .. currentToken}
      end

      -- Token valid: remove lease
      redis.call('DEL', leaseKey)
      redis.call('SREM', activeSet, ARGV[2])
      redis.call('ZREM', 'leases:expirations', ARGV[2])

      -- Write job state
      redis.call('SET', jobKey, ARGV[3])

      -- Record idempotency cache if specified
      if idempKey and idempKey ~= "" and ARGV[4] and ARGV[4] ~= "" then
        redis.call('SETEX', idempKey, tonumber(ARGV[5] or 86400), ARGV[4])
      end

      return {1, "OK"}
    `;

    const tokenKey = `fencing_token:${jobId}`;
    const leaseKey = `lease:${jobId}`;
    const jobKey = `job:${jobId}`;
    const activeSet = 'leases:active';
    const idempKey = idempotencyKey ? `idempotency:${idempotencyKey}` : '';

    const res = (await this.client.eval(
      script,
      5,
      tokenKey,
      leaseKey,
      jobKey,
      activeSet,
      idempKey,
      fencingToken,
      jobId,
      jobData,
      idempotencyResult || '',
      86400
    )) as [number, string];

    if (res[0] === 1) {
      return { success: true };
    } else {
      return { success: false, reason: res[1] };
    }
  }

  // --- Idempotency Store ---
  async getIdempotency(key: string): Promise<string | null> {
    return this.client.get(`idempotency:${key}`);
  }

  async setIdempotency(key: string, value: string, ttlSeconds = 86400): Promise<void> {
    await this.client.set(`idempotency:${key}`, value, 'EX', ttlSeconds);
  }

  // --- Sliding Window Rate Limiting ---
  async checkRateLimit(key: string, limitPerSecond: number): Promise<boolean> {
    const script = `
      local rateKey = KEYS[1]
      local now = tonumber(ARGV[1])
      local windowMs = tonumber(ARGV[2])
      local limit = tonumber(ARGV[3])
      local clearBefore = now - windowMs

      redis.call('ZREMRANGEBYSCORE', rateKey, 0, clearBefore)
      local currentCount = redis.call('ZCARD', rateKey)

      if currentCount < limit then
        redis.call('ZADD', rateKey, now, now .. '-' .. redis.call('INCR', rateKey .. ':seq'))
        redis.call('EXPIRE', rateKey, math.ceil(windowMs / 1000) + 2)
        return 1
      else
        return 0
      end
    `;

    const res = await this.client.eval(script, 1, `ratelimit:${key}`, Date.now(), 1000, limitPerSecond);
    return res === 1;
  }

  // --- Atomic Scheduler Lookahead Dispatch (Lua Script) ---
  // Guarantees zero duplicate dispatches across multiple concurrent schedulers!
  async atomicClaimDueJobs(
    delayedZSetKey: string,
    readyQueueKey: string,
    maxTimestamp: number,
    limit: number,
    _schedulerId: string
  ): Promise<string[]> {
    const script = `
      local delayedKey = KEYS[1]
      local now = tonumber(ARGV[1])
      local maxLimit = tonumber(ARGV[2])
      local readyBaseKey = ARGV[3]

      -- 1. Atomically query due jobs
      local due = redis.call('ZRANGEBYSCORE', delayedKey, 0, now, 'LIMIT', 0, maxLimit)
      if #due == 0 then
        return {}
      end

      local claimed = {}
      for i, jobId in ipairs(due) do
        -- 2. Atomically remove from delayed queue
        local removed = redis.call('ZREM', delayedKey, jobId)
        if removed > 0 then
          -- 3. Check priority if present in job JSON
          local jobKey = 'job:' .. jobId
          local jobRaw = redis.call('GET', jobKey)
          local targetQueue = readyBaseKey

          if jobRaw then
            local priority = string.match(jobRaw, '"priority"%s*:%s*"([^"]+)"')
            if priority and priority ~= "" then
              targetQueue = readyBaseKey .. ':' .. priority
            end
          end

          -- 4. Push into ready queue
          redis.call('RPUSH', targetQueue, jobId)
          table.insert(claimed, jobId)
        end
      end

      return claimed
    `;

    const results = (await this.client.eval(
      script,
      1,
      delayedZSetKey,
      maxTimestamp,
      limit,
      readyQueueKey
    )) as string[];

    return results || [];
  }

  // --- Reset & Cleanup ---
  async clear(): Promise<void> {
    await this.client.flushdb();
  }

  async disconnect(): Promise<void> {
    await this.client.quit();
  }
}
