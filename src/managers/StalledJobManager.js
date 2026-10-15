/**
 * Handles crash durability and recovery of in-flight stalled jobs.
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
    this.dlqKey = `${this.keyPrefix}:dlq`;

    this.lockDuration = options.lockDuration ?? 30000;
    this.stalledInterval = options.stalledInterval ?? 15000;
    this.maxStalledCount = options.maxStalledCount ?? 1;
    this.maxRetries = options.maxRetries ?? 3;

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
   * Acquire a visibility lock for the in-flight job.
   */
  async acquireLock(jobId) {
    if (!jobId) return;
    const expiry = Date.now() + this.lockDuration;
    await this.producer.zadd(this.locksKey, expiry, jobId);
  }

  /**
   * Start a recurring heartbeat to extend the visibility lock while the job is active.
   */
  startHeartbeat(jobId) {
    if (!jobId) return;
    this.stopHeartbeat(jobId);

    const timer = setInterval(async () => {
      try {
        const expiry = Date.now() + this.lockDuration;
        await this.producer.zadd(this.locksKey, expiry, jobId);
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
   */
  async releaseLock(rawJob, jobId) {
    if (jobId) {
      this.stopHeartbeat(jobId);
    }

    const pipeline = this.producer.pipeline();
    if (rawJob) {
      pipeline.lrem(this.activeQueueKey, 1, rawJob);
    }
    if (jobId) {
      pipeline.zrem(this.locksKey, jobId);
    }
    await pipeline.exec();
  }

  /**
   * Remove a raw job directly from active queue (e.g., malformed JSON).
   */
  async removeFromActive(rawJob) {
    if (!rawJob) return;
    await this.producer.lrem(this.activeQueueKey, 1, rawJob);
  }

  /**
   * Recover stalled jobs whose locks have expired or active jobs that were orphaned.
   * Atomically re-queues them back to rq:main or moves to DLQ if maxStalledCount/maxRetries is exceeded.
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

      for _, id in ipairs(expired) do
        for _, j in ipairs(active) do
          local ok, obj = pcall(cjson.decode, j)
          if ok and obj and obj.id == id then
            redis.call('LREM', KEYS[2], 1, j)
            redis.call('ZREM', KEYS[1], id)
            recoveredSet[id] = true

            local count = (obj.stalledCount or 0) + 1
            obj.stalledCount = count
            obj.lastStalledAt = now

            local nextAttempt = (obj.attempt or 0) + 1
            obj.attempt = nextAttempt

            if count > maxStalledCount or nextAttempt > maxRetries then
              obj.failedAt = now
              if count > maxStalledCount then
                obj.failureReason = 'Job stalled more than maxStalledCount times (' .. tostring(maxStalledCount) .. ')'
              else
                obj.failureReason = 'Job stalled and exceeded maxRetries (' .. tostring(maxRetries) .. ')'
              end
              redis.call('RPUSH', KEYS[4], cjson.encode(obj))
            else
              redis.call('RPUSH', KEYS[3], cjson.encode(obj))
            end
            rec = rec + 1
            break
          end
        end
        redis.call('ZREM', KEYS[1], id)
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

      return rec
    `;

    return this.producer.eval(
      script,
      4,
      this.locksKey,
      this.activeQueueKey,
      this.mainQueueKey,
      this.dlqKey,
      Date.now(),
      this.maxStalledCount,
      this.lockDuration,
      this.maxRetries
    );
  }
}

module.exports = StalledJobManager;
