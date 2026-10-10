const { RetryableError } = require("../errors/Errors");

/**
 * Handles retry logic with exponential backoff.
 */
class RetryManager {
  constructor(redisClient, dlqManager, options = {}) {
    if (!redisClient) {
      throw new Error("Redis client is required for RetryManager");
    }

    if (!dlqManager) {
      throw new Error("DLQManager is required for RetryManager");
    }

    this.redis = redisClient;
    this.dlqManager = dlqManager;

    this.maxRetries = options.maxRetries ?? 3;
    this.baseDelay = options.baseDelay ?? 500;
    this.keyPrefix = options.keyPrefix ?? "rq";
  }

  /**
   * Handle job failure and decide whether to retry or send to DLQ.
   */
  async handleFailure(job, error) {
    const mainQueueKey = `${this.keyPrefix}:main`;

    // If error is not retryable → send to DLQ immediately
    if (!(error instanceof RetryableError)) {
      await this.dlqManager.moveToDLQ(job, error);
      return;
    }

    const nextAttempt = (job.attempt || 0) + 1;

    // If max retries exceeded → send to DLQ
    if (nextAttempt > this.maxRetries) {
      await this.dlqManager.moveToDLQ(
        { ...job, attempt: nextAttempt },
        error
      );
      return;
    }

    const delay = this.baseDelay * Math.pow(2, nextAttempt);

    const retryJob = {
      ...job,
      attempt: nextAttempt,
      lastError: error.message,
      retriedAt: Date.now()
    };

    // Persist the retry in Redis until it is due.
   const delayedQueueKey = `${this.keyPrefix}:delayed`;
   const executeAt = Date.now() + delay;

    await this.redis.zadd(
      delayedQueueKey,
      executeAt,
      JSON.stringify(retryJob)
    ); 
  }
  /**
 * Move due retries from the delayed ZSET to the main queue.
 * The Lua script makes the move atomic.
 */
async promoteDueRetries() {
  const delayedQueueKey = `${this.keyPrefix}:delayed`;
  const mainQueueKey = `${this.keyPrefix}:main`;

  const script = `
    local jobs = redis.call(
      'ZRANGEBYSCORE',
      KEYS[1],
      '-inf',
      ARGV[1],
      'LIMIT',
      0,
      ARGV[2]
    )

    for _, job in ipairs(jobs) do
      if redis.call('ZREM', KEYS[1], job) == 1 then
        redis.call('RPUSH', KEYS[2], job)
      end
    end

    return #jobs
  `;

  return this.redis.eval(
    script,
    2,
    delayedQueueKey,
    mainQueueKey,
    Date.now(),
    100
  );
}
}

module.exports = RetryManager;
