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

    // Requeue job after exponential delay
    setTimeout(() => {
      this.redis
        .rpush(mainQueueKey, JSON.stringify(retryJob))
        .catch(err => {
          // If requeue fails, move job to DLQ
          this.dlqManager.moveToDLQ(retryJob, err);
        });
    }, delay);
  }
}

module.exports = RetryManager;
