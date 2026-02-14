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
   * Returns true if job should proceed.
   * Returns false if duplicate.
   */
  async shouldProcess(idempotencyKey, ttlSeconds = null) {
    if (!idempotencyKey) {
      return true; // No idempotency enforcement
    }

    const redisKey = `${this.keyPrefix}:idempotency:${idempotencyKey}`;

    const result = await this.redis.set(
      redisKey,
      "1",
      ttlSeconds ? "EX" : undefined,
      ttlSeconds || undefined,
      "NX"
    );

    return result === "OK";
  }
}

module.exports = IdempotencyManager;
