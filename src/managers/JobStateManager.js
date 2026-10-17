/**
 * Validates and normalizes retention policy configuration.
 */
function normalizeRetentionPolicy(policy) {
  if (policy === undefined || policy === null) {
    return null;
  }
  if (typeof policy === "boolean") {
    return policy ? { count: 0 } : false;
  }
  if (typeof policy === "number") {
    if (!Number.isFinite(policy) || policy < 0) {
      throw new Error("Retention count must be a non-negative number");
    }
    return { count: Math.floor(policy) };
  }
  if (typeof policy === "object" && !Array.isArray(policy)) {
    const result = {};
    if (policy.age !== undefined && policy.age !== null) {
      if (typeof policy.age !== "number" || !Number.isFinite(policy.age) || policy.age < 0) {
        throw new Error("Retention age must be a non-negative number of seconds");
      }
      result.age = policy.age;
    }
    if (policy.count !== undefined && policy.count !== null) {
      if (typeof policy.count !== "number" || !Number.isFinite(policy.count) || policy.count < 0) {
        throw new Error("Retention count must be a non-negative number");
      }
      result.count = Math.floor(policy.count);
    }
    if (result.age === undefined && result.count === undefined) {
      throw new Error("Retention policy object must specify 'age' and/or 'count'");
    }
    return result;
  }
  throw new Error("Retention policy must be a boolean, number, or object with age and/or count");
}

function safeStringify(val) {
  if (val === undefined) return undefined;
  try {
    return JSON.stringify(val);
  } catch {
    return String(val);
  }
}

function safeParse(val) {
  if (val === undefined || val === null || val === "") return undefined;
  try {
    return JSON.parse(val);
  } catch {
    return val;
  }
}

function serializeHashFields(fields) {
  const result = {};
  for (const [k, v] of Object.entries(fields)) {
    if (v === undefined || v === null) continue;
    if (typeof v === "object") {
      result[k] = JSON.stringify(v);
    } else {
      result[k] = String(v);
    }
  }
  return result;
}

const RETENTION_LUA = `
  local zsetKey = KEYS[1]
  local jobKey = KEYS[2]
  local jobId = ARGV[1]
  local now = tonumber(ARGV[2])
  local removeImmediately = ARGV[3] == '1'
  local maxAgeMs = tonumber(ARGV[4]) or 0
  local maxCount = tonumber(ARGV[5]) or -1
  local prefix = ARGV[6]

  if removeImmediately then
    redis.call('DEL', jobKey)
    redis.call('ZREM', zsetKey, jobId)
    return 1
  end

  redis.call('ZADD', zsetKey, now, jobId)

  if maxAgeMs > 0 then
    local ttlSec = math.ceil(maxAgeMs / 1000)
    if ttlSec > 0 then
      redis.call('EXPIRE', jobKey, ttlSec)
    end
    local cutoff = now - maxAgeMs
    local expired = redis.call('ZRANGEBYSCORE', zsetKey, '-inf', cutoff)
    for _, id in ipairs(expired) do
      local oldJobKey = prefix .. ':job:' .. id
      local st = redis.call('HGET', oldJobKey, 'state')
      if not st or st == 'completed' or st == 'failed' then
        redis.call('DEL', oldJobKey)
      end
      redis.call('ZREM', zsetKey, id)
    end
  end

  if maxCount >= 0 then
    local total = redis.call('ZCARD', zsetKey)
    if total > maxCount then
      local excess = total - maxCount
      local oldIds = redis.call('ZRANGE', zsetKey, 0, excess - 1)
      for _, id in ipairs(oldIds) do
        local oldJobKey = prefix .. ':job:' .. id
        local st = redis.call('HGET', oldJobKey, 'state')
        if not st or st == 'completed' or st == 'failed' then
          redis.call('DEL', oldJobKey)
        end
        redis.call('ZREM', zsetKey, id)
      end
    end
  end

  return 0
`;

/**
 * Manages persistent job state hashes and retention policies in Redis.
 */
class JobStateManager {
  constructor(redisClient, keyPrefix = "rq", options = {}) {
    if (!redisClient) {
      throw new Error("Redis client is required for JobStateManager");
    }

    this.redis = redisClient;
    this.keyPrefix = keyPrefix || "rq";
    this.completedKey = `${this.keyPrefix}:completed`;
    this.failedKey = `${this.keyPrefix}:failed`;

    this.defaultRemoveOnComplete = normalizeRetentionPolicy(options.removeOnComplete);
    this.defaultRemoveOnFail = normalizeRetentionPolicy(options.removeOnFail);
  }

  jobKey(jobId) {
    return `${this.keyPrefix}:job:${jobId}`;
  }

  resolveRetentionPolicy(state, itemPolicy) {
    if (itemPolicy !== undefined && itemPolicy !== null) {
      return normalizeRetentionPolicy(itemPolicy);
    }
    if (state === "completed") {
      return this.defaultRemoveOnComplete;
    }
    if (state === "failed") {
      return this.defaultRemoveOnFail;
    }
    return null;
  }

  /**
   * Persists initial job metadata into Redis hash.
   */
  async saveJob(job, state = "waiting", options = {}) {
    if (!job || !job.id) return;
    const opts = options || {};
    const jobKey = this.jobKey(job.id);
    const fields = {
      id: job.id,
      state,
      data: job.data !== undefined ? safeStringify(job.data) : undefined,
      progress: 0,
      attempt: job.attempt ?? 0,
      stalledCount: job.stalledCount ?? 0,
      createdAt: job.createdAt || Date.now(),
      delay: job.delay,
      runAt: job.runAt,
      idempotencyKey: job.idempotencyKey,
      ttlSeconds: job.ttlSeconds,
      jobTimeout: job.jobTimeout
    };

    const removeOnComplete =
      job.removeOnComplete !== undefined
        ? job.removeOnComplete
        : opts.removeOnComplete !== undefined
        ? normalizeRetentionPolicy(opts.removeOnComplete)
        : this.defaultRemoveOnComplete;

    if (removeOnComplete !== null && removeOnComplete !== undefined) {
      fields.removeOnComplete = removeOnComplete;
    }

    const removeOnFail =
      job.removeOnFail !== undefined
        ? job.removeOnFail
        : opts.removeOnFail !== undefined
        ? normalizeRetentionPolicy(opts.removeOnFail)
        : this.defaultRemoveOnFail;

    if (removeOnFail !== null && removeOnFail !== undefined) {
      fields.removeOnFail = removeOnFail;
    }

    const serialized = serializeHashFields(fields);
    await this.redis.hset(jobKey, serialized);
  }

  /**
   * Appends initial job metadata persistence to an ioredis pipeline.
   */
  appendSaveJob(pipeline, job, state = "waiting", options = {}) {
    if (!job || !job.id) return;
    const opts = options || {};
    const jobKey = this.jobKey(job.id);
    const fields = {
      id: job.id,
      state,
      data: job.data !== undefined ? safeStringify(job.data) : undefined,
      progress: 0,
      attempt: job.attempt ?? 0,
      stalledCount: job.stalledCount ?? 0,
      createdAt: job.createdAt || Date.now(),
      delay: job.delay,
      runAt: job.runAt,
      idempotencyKey: job.idempotencyKey,
      ttlSeconds: job.ttlSeconds,
      jobTimeout: job.jobTimeout
    };

    const removeOnComplete =
      job.removeOnComplete !== undefined
        ? job.removeOnComplete
        : opts.removeOnComplete !== undefined
        ? normalizeRetentionPolicy(opts.removeOnComplete)
        : this.defaultRemoveOnComplete;

    if (removeOnComplete !== null && removeOnComplete !== undefined) {
      fields.removeOnComplete = removeOnComplete;
    }

    const removeOnFail =
      job.removeOnFail !== undefined
        ? job.removeOnFail
        : opts.removeOnFail !== undefined
        ? normalizeRetentionPolicy(opts.removeOnFail)
        : this.defaultRemoveOnFail;

    if (removeOnFail !== null && removeOnFail !== undefined) {
      fields.removeOnFail = removeOnFail;
    }

    pipeline.hset(jobKey, serializeHashFields(fields));
  }

  /**
   * Transitions job state to 'active' and records startedAt timestamp.
   */
  async transitionToActive(jobId, attempt = 0) {
    if (!jobId) return;
    const jobKey = this.jobKey(jobId);
    const now = Date.now();
    await this.redis.hset(jobKey, {
      state: "active",
      startedAt: String(now),
      attempt: String(attempt)
    });
    await this.redis.persist(jobKey);
  }

  /**
   * Transitions job state to 'completed', records result and timestamps, and applies retention policy.
   */
  async transitionToCompleted(jobId, result, retentionOptions) {
    if (!jobId) return;
    const jobKey = this.jobKey(jobId);
    const now = Date.now();
    const serializedResult = result !== undefined ? safeStringify(result) : "";

    const pipeline = this.redis.pipeline();
    pipeline.hset(jobKey, {
      state: "completed",
      completedAt: String(now),
      result: serializedResult,
      returnValue: serializedResult
    });
    pipeline.hdel(jobKey, "failedAt", "failureReason", "lastError");
    await pipeline.exec();

    await this.applyRetention(jobId, "completed", retentionOptions, now);
  }

  /**
   * Transitions job state to 'delayed' (during retry scheduling), records error and attempt.
   */
  async transitionToDelayed(jobId, attempt, error, executeAt) {
    if (!jobId) return;
    const jobKey = this.jobKey(jobId);
    const now = Date.now();
    const errorMsg = error?.message || (typeof error === "string" ? error : "Unknown error");
    const fields = {
      state: "delayed",
      attempt: String(attempt),
      retriedAt: String(now),
      failedAt: String(now),
      lastError: errorMsg,
      failureReason: errorMsg
    };
    if (executeAt) {
      fields.executeAt = String(executeAt);
    }
    await this.redis.hset(jobKey, fields);
  }

  /**
   * Transitions job state to 'failed', records failure reason and timestamps, and applies retention policy.
   */
  async transitionToFailed(jobId, reason, retentionOptions) {
    if (!jobId) return;
    const jobKey = this.jobKey(jobId);
    const now = Date.now();
    const reasonMsg = reason?.message || (typeof reason === "string" ? reason : "Unknown error");

    await this.redis.hset(jobKey, {
      state: "failed",
      failedAt: String(now),
      failureReason: reasonMsg,
      lastError: reasonMsg
    });

    await this.applyRetention(jobId, "failed", retentionOptions, now);
  }

  /**
   * Transitions job state back to 'waiting' (e.g. on retry promotion, DLQ retry, or stalled recovery).
   */
  async transitionToWaiting(jobId, resetMeta = {}) {
    if (!jobId) return;
    const jobKey = this.jobKey(jobId);
    const { clearError, ...otherFields } = resetMeta;
    const fields = {
      state: "waiting",
      ...otherFields
    };
    await this.redis.hset(jobKey, serializeHashFields(fields));
    if (clearError) {
      await this.redis.hdel(jobKey, "failureReason", "lastError", "failedAt");
    }
  }

  /**
   * Updates progress (0–100) for a job in Redis.
   */
  async updateProgress(jobId, progress) {
    if (!jobId || (typeof jobId !== "string" && typeof jobId !== "number")) {
      throw new Error("jobId is required for updateProgress");
    }

    if (
      progress === undefined ||
      progress === null ||
      typeof progress !== "number" ||
      !Number.isFinite(progress) ||
      progress < 0 ||
      progress > 100
    ) {
      throw new Error("Progress must be a number between 0 and 100");
    }

    const jobKey = this.jobKey(String(jobId));
    const result = await this.redis.eval(
      `if redis.call('EXISTS', KEYS[1]) == 1 then
         redis.call('HSET', KEYS[1], 'progress', ARGV[1])
         return 1
       else
         return 0
       end`,
      1,
      jobKey,
      String(progress)
    );

    if (result === 0) {
      throw new Error(`Job ${jobId} not found`);
    }
  }

  /**
   * Evaluates and applies retention policy (immediate deletion, age-based expiration, count trimming).
   */
  async applyRetention(jobId, state, retentionOptions, now = Date.now()) {
    let effectivePolicy = null;
    if (retentionOptions !== undefined && retentionOptions !== null) {
      effectivePolicy = normalizeRetentionPolicy(retentionOptions);
    } else {
      // Retrieve stored policy on the job hash if available
      try {
        const fieldName = state === "completed" ? "removeOnComplete" : "removeOnFail";
        const stored = await this.redis.hget(this.jobKey(jobId), fieldName);
        if (stored !== null && stored !== undefined && stored !== "") {
          effectivePolicy = normalizeRetentionPolicy(safeParse(stored));
        }
      } catch {
        // Fall back to queue defaults
      }
      if (effectivePolicy === null) {
        effectivePolicy = state === "completed" ? this.defaultRemoveOnComplete : this.defaultRemoveOnFail;
      }
    }

    if (!effectivePolicy || effectivePolicy === false) {
      return;
    }

    const isCompleted = state === "completed";
    const zsetKey = isCompleted ? this.completedKey : this.failedKey;
    const jobKey = this.jobKey(jobId);

    const removeImmediately = effectivePolicy.count === 0 ? "1" : "0";
    const maxAgeMs = effectivePolicy.age && effectivePolicy.age > 0 ? effectivePolicy.age * 1000 : 0;
    const maxCount = effectivePolicy.count !== undefined ? effectivePolicy.count : -1;

    await this.redis.eval(
      RETENTION_LUA,
      2,
      zsetKey,
      jobKey,
      jobId,
      now,
      removeImmediately,
      maxAgeMs,
      maxCount,
      this.keyPrefix
    );
  }

  /**
   * Removes job hash and removes job ID from completed/failed sorted sets.
   */
  async cleanupJob(jobId, state) {
    if (!jobId) return;
    const jobKey = this.jobKey(jobId);
    const pipeline = this.redis.pipeline();
    pipeline.del(jobKey);
    if (state === "completed" || !state) {
      pipeline.zrem(this.completedKey, jobId);
    }
    if (state === "failed" || !state) {
      pipeline.zrem(this.failedKey, jobId);
    }
    await pipeline.exec();
  }

  /**
   * Retrieves persisted job details from Redis by ID.
   * Returns null if missing or expired.
   */
  async getJob(jobId, context = {}) {
    if (!jobId || (typeof jobId !== "string" && typeof jobId !== "number")) {
      return null;
    }
    const idStr = String(jobId);
    const jobKey = this.jobKey(idStr);
    const raw = await this.redis.hgetall(jobKey);

    if (!raw || Object.keys(raw).length === 0 || !raw.id) {
      return null;
    }

    // Check expiration based on retention policy
    const now = Date.now();
    if (raw.state === "completed" && raw.completedAt) {
      const storedPolicy = raw.removeOnComplete ? safeParse(raw.removeOnComplete) : undefined;
      const policy = this.resolveRetentionPolicy("completed", storedPolicy);
      if (policy && policy.age && policy.age > 0) {
        if (now - Number(raw.completedAt) > policy.age * 1000) {
          await this.cleanupJob(idStr, "completed");
          return null;
        }
      }
    } else if (raw.state === "failed" && raw.failedAt) {
      const storedPolicy = raw.removeOnFail ? safeParse(raw.removeOnFail) : undefined;
      const policy = this.resolveRetentionPolicy("failed", storedPolicy);
      if (policy && policy.age && policy.age > 0) {
        if (now - Number(raw.failedAt) > policy.age * 1000) {
          await this.cleanupJob(idStr, "failed");
          return null;
        }
      }
    }

    const parsedData = raw.data !== undefined ? safeParse(raw.data) : undefined;
    const parsedResult = raw.result !== undefined && raw.result !== "" ? safeParse(raw.result) : undefined;

    const isCompleted = raw.state === "completed";

    const job = {
      id: raw.id,
      data: parsedData,
      state: raw.state || "waiting",
      progress:
        raw.progress !== undefined && Number.isFinite(Number(raw.progress))
          ? Number(raw.progress)
          : 0,
      attempt:
        raw.attempt !== undefined && Number.isFinite(Number(raw.attempt))
          ? Number(raw.attempt)
          : 0,
      stalledCount:
        raw.stalledCount !== undefined && Number.isFinite(Number(raw.stalledCount))
          ? Number(raw.stalledCount)
          : 0,
      createdAt: raw.createdAt ? Number(raw.createdAt) : undefined,
      startedAt: raw.startedAt ? Number(raw.startedAt) : undefined,
      completedAt: raw.completedAt ? Number(raw.completedAt) : undefined,
      failedAt: !isCompleted && raw.failedAt ? Number(raw.failedAt) : undefined,
      retriedAt: raw.retriedAt ? Number(raw.retriedAt) : undefined,
      lastStalledAt: raw.lastStalledAt ? Number(raw.lastStalledAt) : undefined,
      result: parsedResult,
      returnValue: parsedResult,
      failureReason: !isCompleted ? (raw.failureReason || undefined) : undefined,
      lastError: !isCompleted ? (raw.lastError || raw.failureReason || undefined) : undefined,
      delay: raw.delay !== undefined && raw.delay !== "" ? Number(raw.delay) : undefined,
      runAt: raw.runAt ? Number(raw.runAt) : undefined,
      jobTimeout: raw.jobTimeout ? Number(raw.jobTimeout) : undefined,
      idempotencyKey: raw.idempotencyKey || null,
      ttlSeconds: raw.ttlSeconds ? Number(raw.ttlSeconds) : null
    };

    if (raw.removeOnComplete) {
      job.removeOnComplete = safeParse(raw.removeOnComplete);
    }
    if (raw.removeOnFail) {
      job.removeOnFail = safeParse(raw.removeOnFail);
    }

    // Attach asynchronous updateProgress method
    job.updateProgress = async (progress) => {
      try {
        await this.updateProgress(job.id, progress);
        job.progress = progress;
        if (typeof context.onUpdateProgress === "function") {
          context.onUpdateProgress(job, progress);
        }
      } catch (err) {
        if (typeof context.onError === "function") {
          context.onError(err);
        }
        throw err;
      }
    };

    return job;
  }
}

JobStateManager.normalizeRetentionPolicy = normalizeRetentionPolicy;
JobStateManager.serializeHashFields = serializeHashFields;
JobStateManager.safeParse = safeParse;
JobStateManager.safeStringify = safeStringify;

module.exports = JobStateManager;
