/**
 * Handles Dead Letter Queue (DLQ) operations.
 */
class DLQManager {
  constructor(redisClient, keyPrefix = "rq") {
    if (!redisClient) {
      throw new Error("Redis client is required for DLQManager");
    }

    this.redis = redisClient;
    this.keyPrefix = keyPrefix;
  }

  /**
   * Moves a failed job to DLQ.
   */
  async moveToDLQ(job, reason) {
    const dlqKey = `${this.keyPrefix}:dlq`;

    const failedJob = {
      ...job,
      failedAt: Date.now(),
      failureReason: reason?.message || "Unknown error"
    };

    await this.redis.rpush(dlqKey, JSON.stringify(failedJob));
  }
}

module.exports = DLQManager;
