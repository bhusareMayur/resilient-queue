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

    const script = `
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

    const ttlArg =
      typeof ttlSeconds === "number" && Number.isFinite(ttlSeconds) && ttlSeconds > 0
        ? ttlSeconds
        : 0;

    return await this.redis.eval(
      script,
      2,
      redisKey,
      queueKey,
      candidateJobId,
      serializedJob,
      ttlArg
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
