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
      return true;
    }

    const redisKey = `${this.keyPrefix}:idempotency:${idempotencyKey}`;

    let result;

    if (ttlSeconds) {
      result = await this.redis.set(
        redisKey,
        "1",
        "EX",
        ttlSeconds,
        "NX"
      );
    } else {
      result = await this.redis.set(
        redisKey,
        "1",
        "NX"
      );
    }

    return result === "OK";
  }
}

module.exports = IdempotencyManager;
