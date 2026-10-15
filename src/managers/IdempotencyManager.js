const REGISTER_AND_ENQUEUE_SCRIPT = `
  local existingJobId = redis.call('GET', KEYS[1])
  if existingJobId then
    return existingJobId
  end

  local destTypeTable = redis.call('TYPE', KEYS[2])
  local destinationType = type(destTypeTable) == 'table' and destTypeTable.ok or destTypeTable
  if destinationType ~= 'none' and destinationType ~= 'list' then
    return redis.error_reply('Main queue key must be a Redis list')
  end

  local ttl = tonumber(ARGV[3])
  if ttl and ttl > 0 then
    redis.call('SET', KEYS[1], ARGV[1], 'EX', ttl)
  else
    redis.call('SET', KEYS[1], ARGV[1])
  end

  redis.call('RPUSH', KEYS[2], ARGV[2])
  return ARGV[1]
`;

const REGISTER_AND_ENQUEUE_DELAYED_SCRIPT = `
  local existingJobId = redis.call('GET', KEYS[1])
  if existingJobId then
    return existingJobId
  end

  local destTypeTable = redis.call('TYPE', KEYS[2])
  local destinationType = type(destTypeTable) == 'table' and destTypeTable.ok or destTypeTable
  if destinationType ~= 'none' and destinationType ~= 'zset' then
    return redis.error_reply('Delayed queue key must be a Redis zset')
  end

  local ttl = tonumber(ARGV[3])
  if ttl and ttl > 0 then
    redis.call('SET', KEYS[1], ARGV[1], 'EX', ttl)
  else
    redis.call('SET', KEYS[1], ARGV[1])
  end

  local score = tonumber(ARGV[4])
  redis.call('ZADD', KEYS[2], score, ARGV[2])
  return ARGV[1]
`;

/**
 * Handles idempotency checks using Redis.
 */
class IdempotencyManager {
  constructor(redisClient, keyPrefix = "rq") {
    if (!redisClient) {
      throw new Error("Redis client is required for IdempotencyManager");
    }

    this.redis = redisClient;
    this.keyPrefix = keyPrefix;
  }

  /**
   * Atomically checks idempotency and enqueues the job to the main queue if not already present.
   * If the idempotency key already exists, returns the existing job ID without enqueuing.
   * If not present, registers the candidate job ID and pushes to the main queue atomically.
   */
  async registerAndEnqueue(mainQueueKey, idempotencyKey, job, ttlSeconds = null) {
    if (!idempotencyKey) {
      throw new Error("idempotencyKey is required for registerAndEnqueue");
    }

    if (!job) {
      throw new Error("job is required for registerAndEnqueue");
    }

    if (ttlSeconds !== undefined && ttlSeconds !== null) {
      if (typeof ttlSeconds !== "number" || !Number.isInteger(ttlSeconds)) {
        throw new Error("ttlSeconds must be an integer");
      }
    }

    const redisKey = `${this.keyPrefix}:idempotency:${idempotencyKey}`;
    const queueKey = mainQueueKey || `${this.keyPrefix}:main`;
    const candidateJobId = typeof job === "string" ? JSON.parse(job).id : job.id;
    const serializedJob = typeof job === "string" ? job : JSON.stringify(job);

    const ttlArg =
      typeof ttlSeconds === "number" && Number.isFinite(ttlSeconds) && ttlSeconds > 0
        ? ttlSeconds
        : 0;

    return await this.redis.eval(
      REGISTER_AND_ENQUEUE_SCRIPT,
      2,
      redisKey,
      queueKey,
      candidateJobId,
      serializedJob,
      ttlArg
    );
  }

  /**
   * Atomically checks idempotency and enqueues the job to the delayed sorted set if not already present.
   */
  async registerAndEnqueueDelayed(delayedQueueKey, idempotencyKey, job, executeAt, ttlSeconds = null) {
    if (!idempotencyKey) {
      throw new Error("idempotencyKey is required for registerAndEnqueueDelayed");
    }

    if (!job) {
      throw new Error("job is required for registerAndEnqueueDelayed");
    }

    if (ttlSeconds !== undefined && ttlSeconds !== null) {
      if (typeof ttlSeconds !== "number" || !Number.isInteger(ttlSeconds)) {
        throw new Error("ttlSeconds must be an integer");
      }
    }

    const redisKey = `${this.keyPrefix}:idempotency:${idempotencyKey}`;
    const queueKey = delayedQueueKey || `${this.keyPrefix}:delayed`;
    const candidateJobId = typeof job === "string" ? JSON.parse(job).id : job.id;
    const serializedJob = typeof job === "string" ? job : JSON.stringify(job);

    const ttlArg =
      typeof ttlSeconds === "number" && Number.isFinite(ttlSeconds) && ttlSeconds > 0
        ? ttlSeconds
        : 0;

    return await this.redis.eval(
      REGISTER_AND_ENQUEUE_DELAYED_SCRIPT,
      2,
      redisKey,
      queueKey,
      candidateJobId,
      serializedJob,
      ttlArg,
      executeAt
    );
  }

  /**
   * Appends a registerAndEnqueue command to an ioredis pipeline.
   */
  appendRegisterAndEnqueue(pipeline, mainQueueKey, idempotencyKey, job, ttlSeconds = null) {
    if (!idempotencyKey) {
      throw new Error("idempotencyKey is required for appendRegisterAndEnqueue");
    }

    if (!job) {
      throw new Error("job is required for appendRegisterAndEnqueue");
    }

    if (ttlSeconds !== undefined && ttlSeconds !== null) {
      if (typeof ttlSeconds !== "number" || !Number.isInteger(ttlSeconds)) {
        throw new Error("ttlSeconds must be an integer");
      }
    }

    const redisKey = `${this.keyPrefix}:idempotency:${idempotencyKey}`;
    const queueKey = mainQueueKey || `${this.keyPrefix}:main`;
    const candidateJobId = typeof job === "string" ? JSON.parse(job).id : job.id;
    const serializedJob = typeof job === "string" ? job : JSON.stringify(job);

    const ttlArg =
      typeof ttlSeconds === "number" && Number.isFinite(ttlSeconds) && ttlSeconds > 0
        ? ttlSeconds
        : 0;

    pipeline.eval(
      REGISTER_AND_ENQUEUE_SCRIPT,
      2,
      redisKey,
      queueKey,
      candidateJobId,
      serializedJob,
      ttlArg
    );
  }

  /**
   * Appends a registerAndEnqueueDelayed command to an ioredis pipeline.
   */
  appendRegisterAndEnqueueDelayed(pipeline, delayedQueueKey, idempotencyKey, job, executeAt, ttlSeconds = null) {
    if (!idempotencyKey) {
      throw new Error("idempotencyKey is required for appendRegisterAndEnqueueDelayed");
    }

    if (!job) {
      throw new Error("job is required for appendRegisterAndEnqueueDelayed");
    }

    if (ttlSeconds !== undefined && ttlSeconds !== null) {
      if (typeof ttlSeconds !== "number" || !Number.isInteger(ttlSeconds)) {
        throw new Error("ttlSeconds must be an integer");
      }
    }

    const redisKey = `${this.keyPrefix}:idempotency:${idempotencyKey}`;
    const queueKey = delayedQueueKey || `${this.keyPrefix}:delayed`;
    const candidateJobId = typeof job === "string" ? JSON.parse(job).id : job.id;
    const serializedJob = typeof job === "string" ? job : JSON.stringify(job);

    const ttlArg =
      typeof ttlSeconds === "number" && Number.isFinite(ttlSeconds) && ttlSeconds > 0
        ? ttlSeconds
        : 0;

    pipeline.eval(
      REGISTER_AND_ENQUEUE_DELAYED_SCRIPT,
      2,
      redisKey,
      queueKey,
      candidateJobId,
      serializedJob,
      ttlArg,
      executeAt
    );
  }

  /**
   * Returns true if job should proceed.
   * Returns false if duplicate.
   *
   * Supports both legacy signature (idempotencyKey, ttlSeconds)
   * and job-aware verification (idempotencyKey, ttlSeconds, jobId).
   */
  async shouldProcess(idempotencyKey, ttlSeconds = null, jobId = null) {
    if (typeof idempotencyKey === "object" && idempotencyKey !== null) {
      jobId = idempotencyKey.id;
      ttlSeconds = idempotencyKey.ttlSeconds ?? null;
      idempotencyKey = idempotencyKey.idempotencyKey;
    }

    if (!idempotencyKey) {
      return true;
    }

    if (ttlSeconds !== undefined && ttlSeconds !== null) {
      if (typeof ttlSeconds !== "number" || !Number.isInteger(ttlSeconds)) {
        throw new Error("ttlSeconds must be an integer");
      }
    }

    const redisKey = `${this.keyPrefix}:idempotency:${idempotencyKey}`;

    const script = `
      local current = redis.call('GET', KEYS[1])
      if not current then
        local candidate = ARGV[1]
        if not candidate or candidate == '' then
          candidate = '1'
        end
        local ttl = tonumber(ARGV[2])
        if ttl and ttl > 0 then
          redis.call('SET', KEYS[1], candidate, 'EX', ttl)
        else
          redis.call('SET', KEYS[1], candidate)
        end
        return 1
      end

      if ARGV[1] and ARGV[1] ~= '' and current == ARGV[1] then
        return 1
      end

      return 0
    `;

    const ttlArg =
      typeof ttlSeconds === "number" && Number.isFinite(ttlSeconds) && ttlSeconds > 0
        ? ttlSeconds
        : 0;

    const result = await this.redis.eval(
      script,
      1,
      redisKey,
      jobId || "",
      ttlArg
    );

    return result === 1;
  }
}

module.exports = IdempotencyManager;
