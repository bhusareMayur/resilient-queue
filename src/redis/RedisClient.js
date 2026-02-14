const Redis = require("ioredis");

/**
 * Redis client wrapper.
 * Responsible only for managing the Redis connection.
 */
class RedisClient {
  constructor(redisUrl) {
    if (!redisUrl) {
      throw new Error("redisUrl is required to initialize RedisClient");
    }

    this.redis = new Redis(redisUrl);

    this.redis.on("connect", () => {
      // Silent by default — no noisy logs in library
    });

    this.redis.on("error", (err) => {
      // Do not crash application
      console.error("[resilient-queue] Redis connection error:", err.message);
    });
  }

  getClient() {
    return this.redis;
  }
}

module.exports = RedisClient;
