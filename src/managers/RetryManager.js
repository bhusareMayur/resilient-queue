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

  async handleFailure(job, error) {
    const mainQueueKey = `${this.keyPrefix}:main`;

    // If not retryable → send to DLQ immediately
    if (!(error instanceof RetryableError)) {
      await this.dlqManager.moveToDLQ(job, error);
      return;
    }

    const nextAttempt = (job.attempt || 0) + 1;

    // If max retries exceeded → DLQ
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

    // Delay requeue using setTimeout (v1 simple design)
    setTimeout(async () => {
      try {
        await this.redis.rpush(mainQueueKey, JSON.stringify(retryJob));
      } catch (err) {
        // If retry push fails → send to DLQ
        await this.dlqManager.moveToDLQ(retryJob, err);
      }
    }, delay);
  }
}

module.exports = RetryManager;
