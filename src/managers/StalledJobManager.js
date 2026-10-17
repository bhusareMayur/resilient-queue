/**
 * Handles crash durability, recovery of in-flight stalled jobs, and fencing tokens.
 */
class StalledJobManager {
  constructor(producer, options = {}) {
    if (!producer) {
      throw new Error("Redis producer client is required for StalledJobManager");
    }

    this.producer = producer;
    this.keyPrefix = options.keyPrefix || "rq";
    this.mainQueueKey = `${this.keyPrefix}:main`;
    this.activeQueueKey = `${this.keyPrefix}:active`;
    this.locksKey = `${this.keyPrefix}:locks`;
    this.tokensKey = `${this.keyPrefix}:lock_tokens`;
    this.stalledMetaKey = `${this.keyPrefix}:stalled_meta`;
    this.dlqKey = `${this.keyPrefix}:dlq`;

    this.lockDuration = options.lockDuration ?? 30000;
    this.stalledInterval = options.stalledInterval ?? 15000;
    this.maxStalledCount = options.maxStalledCount ?? 1;
    this.maxRetries = options.maxRetries ?? 3;
    this.onStalled = options.onStalled || null;
    this.onDLQ = options.onDLQ || null;
    this.jobStateManager = options.jobStateManager || null;

    // Heartbeat runs roughly at 1/3 of the lock duration.
    this.heartbeatInterval = Math.max(100, Math.floor(this.lockDuration / 3));
    this.heartbeats = new Map();
    this.useBlmove = true;
  }

  /**
   * Atomically dequeue a job from main queue to active list.
   * Uses BLMOVE (Redis 6.2+) with fallback to BRPOPLPUSH for older Redis versions.
   */
  async dequeue(consumer, timeoutSec = 1) {
    if (this.useBlmove) {
      try {
        return await consumer.blmove(
          this.mainQueueKey,
          this.activeQueueKey,
          "LEFT",
          "RIGHT",
          timeoutSec
        );
      } catch (err) {
        if (err.message && err.message.includes("unknown command")) {
          this.useBlmove = false;
          return await consumer.brpoplpush(
            this.mainQueueKey,
            this.activeQueueKey,
            timeoutSec
          );
        }
        throw err;
      }
    } else {
      return await consumer.brpoplpush(
        this.mainQueueKey,
        this.activeQueueKey,
        timeoutSec
      );
    }
  }

  /**
   * Acquire a visibility lock with a per-dequeue fencing token for the in-flight job.
   */
  async acquireLock(jobId, lockToken = null) {
    if (!jobId) return;
    const expiry = Date.now() + this.lockDuration;
    const pipeline = this.producer.pipeline();
    if (lockToken) {
      pipeline.hset(this.tokensKey, jobId, lockToken);
    }
    pipeline.zadd(this.locksKey, expiry, jobId);
    await pipeline.exec();
  }

  /**
   * Renew the visibility lock using the fencing token.
   * If the token does not match (lease was lost or recovered), renewal fails.
   */
  async renewLock(jobId, lockToken) {
    if (!jobId || !lockToken) return false;

    const script = `
      local currentToken = redis.call('HGET', KEYS[1], ARGV[1])
      if currentToken == ARGV[2] then
        redis.call('ZADD', KEYS[2], ARGV[3], ARGV[1])
        return 1
      else
        return 0
      end
    `;

    const newExpiry = Date.now() + this.lockDuration;
    const result = await this.producer.eval(
      script,
      2,
      this.tokensKey,
      this.locksKey,
      jobId,
      lockToken,
      newExpiry
    );

    return result === 1;
  }

  /**
   * Start a recurring heartbeat to extend the visibility lock while the job is active.
   * Calls onLeaseLost callback if the lock token is no longer valid.
   */
  startHeartbeat(jobId, lockToken = null, onLeaseLost = null) {
    if (!jobId) return;
    this.stopHeartbeat(jobId);

    const timer = setInterval(async () => {
      try {
        if (lockToken) {
          const renewed = await this.renewLock(jobId, lockToken);
          if (!renewed) {
            // Lease lost! Another worker or recovery scheduler has taken over.
            this.stopHeartbeat(jobId);
            if (typeof onLeaseLost === "function") {
              onLeaseLost();
            }
          }
        } else {
          const expiry = Date.now() + this.lockDuration;
          await this.producer.zadd(this.locksKey, expiry, jobId);
        }
      } catch (err) {
        // Library should not crash on heartbeat network blips
      }
    }, this.heartbeatInterval);

    if (timer.unref) {
      timer.unref();
    }

    this.heartbeats.set(jobId, timer);
  }

  /**
   * Stop the heartbeat for a specific job.
   */
  stopHeartbeat(jobId) {
    if (!jobId) return;
    const timer = this.heartbeats.get(jobId);
    if (timer) {
      clearInterval(timer);
      this.heartbeats.delete(jobId);
    }
  }

  /**
   * Stop all active heartbeats (e.g. on shutdown).
   */
  stopAllHeartbeats() {
    for (const timer of this.heartbeats.values()) {
      clearInterval(timer);
    }
    this.heartbeats.clear();
  }

  /**
   * Release lock and remove job from active queue upon completion or failure.
   * Fenced by lockToken to ensure stale workers cannot release successor workers' locks.
   */
  async releaseLock(rawJob, jobId, lockToken = null) {
    if (jobId) {
      this.stopHeartbeat(jobId);
    }

    if (!jobId || !lockToken) {
      // Fallback if no token was passed
      const pipeline = this.producer.pipeline();
      if (rawJob) {
        pipeline.lrem(this.activeQueueKey, 1, rawJob);
      }
      if (jobId) {
        pipeline.zrem(this.locksKey, jobId);
        pipeline.hdel(this.tokensKey, jobId);
        pipeline.hdel(this.stalledMetaKey, jobId);
      }
      await pipeline.exec();
      return;
    }

    const script = `
      local currentToken = redis.call('HGET', KEYS[1], ARGV[1])
      if currentToken == ARGV[2] then
        redis.call('HDEL', KEYS[1], ARGV[1])
        redis.call('ZREM', KEYS[2], ARGV[1])
        redis.call('HDEL', KEYS[4], ARGV[1])
        if ARGV[3] and #ARGV[3] > 0 then
          redis.call('LREM', KEYS[3], 1, ARGV[3])
        end
        return 1
      else
        -- Fenced: lock token mismatch or lease already lost
        return 0
      end
    `;

    return this.producer.eval(
      script,
      4,
      this.tokensKey,
      this.locksKey,
      this.activeQueueKey,
      this.stalledMetaKey,
      jobId,
      lockToken,
      rawJob || ""
    );
  }

  /**
   * Remove a raw job directly from active queue (e.g., malformed JSON).
   */
  async removeFromActive(rawJob) {
    if (!rawJob) return;
    await this.producer.lrem(this.activeQueueKey, 1, rawJob);
  }

  /**
   * Get stalled metadata stored separately from the raw job payload.
   */
  async getStalledMetadata(jobId) {
    if (!jobId) return null;
    const raw = await this.producer.hget(this.stalledMetaKey, jobId);
    if (!raw) return null;
    try {
      return JSON.parse(raw);
    } catch {
      return null;
    }
  }

  /**
   * Recover stalled jobs whose locks have expired or active jobs that were orphaned.
   * Atomically moves the UNTOUCHED raw job string (preventing Lua JSON serialization corruption)
   * while tracking stalled recovery metadata outside the job payload.
   */
  async recoverStalledJobs() {
    const script = `
      local now = tonumber(ARGV[1])
      local maxStalledCount = tonumber(ARGV[2])
      local lockDuration = tonumber(ARGV[3])
      local maxRetries = tonumber(ARGV[4])
      local rec = 0

      local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now)
      local active = redis.call('LRANGE', KEYS[2], 0, -1)
      local recoveredSet = {}
      local events = {}

      for _, id in ipairs(expired) do
        for _, j in ipairs(active) do
          local ok, obj = pcall(cjson.decode, j)
          if ok and obj and obj.id == id then
            redis.call('LREM', KEYS[2], 1, j)
            redis.call('ZREM', KEYS[1], id)
            redis.call('HDEL', KEYS[5], id)
            recoveredSet[id] = true

            -- Retrieve any prior stalled count tracked in separate metadata Hash
            local currentMetaRaw = redis.call('HGET', KEYS[6], id)
            local currentCount = 0
            if currentMetaRaw then
              local okMeta, metaObj = pcall(cjson.decode, currentMetaRaw)
              if okMeta and metaObj and metaObj.stalledCount then
                currentCount = tonumber(metaObj.stalledCount)
              end
            elseif obj.stalledCount then
              currentCount = tonumber(obj.stalledCount)
            end

            local count = currentCount + 1
            local nextAttempt = (obj.attempt or 0) + 1

            local metaData = {
              stalledCount = count,
              lastStalledAt = now,
              attempt = nextAttempt
            }

            if count > maxStalledCount or nextAttempt > maxRetries then
              metaData.failedAt = now
              if count > maxStalledCount then
                metaData.failureReason = 'Job stalled more than maxStalledCount times (' .. tostring(maxStalledCount) .. ')'
              else
                metaData.failureReason = 'Job stalled and exceeded maxRetries (' .. tostring(maxRetries) .. ')'
              end
              -- Move the pristine raw string j directly to DLQ without re-encoding
              redis.call('RPUSH', KEYS[4], j)
              table.insert(events, {'dlq', j, cjson.encode(metaData)})
            else
              -- Move the pristine raw string j directly to main queue without re-encoding
              redis.call('RPUSH', KEYS[3], j)
              table.insert(events, {'stalled', j, cjson.encode(metaData)})
            end

            -- Persist recovery metadata outside the raw job data
            redis.call('HSET', KEYS[6], id, cjson.encode(metaData))

            if KEYS[7] and KEYS[7] ~= '' then
              local jobKey = KEYS[7] .. ':job:' .. id
              if redis.call('EXISTS', jobKey) == 1 then
                if count > maxStalledCount or nextAttempt > maxRetries then
                  redis.call('HSET', jobKey, 'state', 'failed', 'failedAt', now, 'failureReason', metaData.failureReason, 'stalledCount', count, 'attempt', nextAttempt)
                else
                  redis.call('HSET', jobKey, 'state', 'waiting', 'stalledCount', count, 'lastStalledAt', now, 'attempt', nextAttempt)
                  redis.call('PERSIST', jobKey)
                end
              end
            end

            rec = rec + 1
            break
          end
        end
        redis.call('ZREM', KEYS[1], id)
        redis.call('HDEL', KEYS[5], id)
      end

      -- Ensure any active job without a lock gets a provisional lock so it cannot be orphaned
      for _, j in ipairs(active) do
        local ok, obj = pcall(cjson.decode, j)
        if ok and obj and obj.id and not recoveredSet[obj.id] then
          local score = redis.call('ZSCORE', KEYS[1], obj.id)
          if not score then
            redis.call('ZADD', KEYS[1], 'NX', now + lockDuration, obj.id)
          end
        end
      end

      return { rec, events }
    `;

    const result = await this.producer.eval(
      script,
      7,
      this.locksKey,
      this.activeQueueKey,
      this.mainQueueKey,
      this.dlqKey,
      this.tokensKey,
      this.stalledMetaKey,
      this.keyPrefix,
      Date.now(),
      this.maxStalledCount,
      this.lockDuration,
      this.maxRetries
    );

    let rec = 0;
    let events = [];

    if (Array.isArray(result)) {
      rec = Number(result[0]) || 0;
      events = Array.isArray(result[1]) ? result[1] : [];
    } else {
      rec = Number(result) || 0;
    }

    for (const item of events) {
      if (!Array.isArray(item) || item.length < 3) continue;
      const [type, rawJob, metaRaw] = item;
      let job;
      try {
        job = JSON.parse(rawJob);
      } catch {
        job = { raw: rawJob };
      }
      let meta = {};
      try {
        meta = JSON.parse(metaRaw);
      } catch {}

      Object.assign(job, meta);

      if (type === "stalled" && typeof this.onStalled === "function") {
        this.onStalled(job);
      } else if (type === "dlq") {
        if (this.jobStateManager && job && job.id) {
          await this.jobStateManager.applyRetention(job.id, "failed", job.removeOnFail, Date.now());
        }
        if (typeof this.onDLQ === "function") {
          this.onDLQ(job, meta.failureReason || "Job stalled");
        }
      }
    }

    return rec;
  }
}

module.exports = StalledJobManager;
