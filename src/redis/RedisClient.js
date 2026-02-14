const Redis = require("ioredis");

/**
 * RedisClient
 *
 * Creates separate Redis connections for:
 *  - Producer (writes: RPUSH, SET, etc.)
 *  - Consumer (blocking reads: BLPOP)
 *
 * This separation is critical because a connection
 * blocked by BLPOP cannot execute other commands.
 */
class RedisClient {
  constructor(redisUrl) {
    if (!redisUrl) {
      throw new Error("redisUrl is required to initialize RedisClient");
    }

    // Producer connection (writes)
    this.producer = new Redis(redisUrl, {
      maxRetriesPerRequest: null
    });

    // Consumer connection (blocking reads)
    this.consumer = new Redis(redisUrl, {
      maxRetriesPerRequest: null
    });

    this._attachEventHandlers();
  }

  /**
   * Attach safe error handlers.
   * Library should not log by default.
   * Host application can manage Redis errors.
   */
  _attachEventHandlers() {
    this.producer.on("error", () => {});
    this.consumer.on("error", () => {});
  }

  /**
   * Returns producer connection.
   */
  getProducer() {
    return this.producer;
  }

  /**
   * Returns consumer connection.
   */
  getConsumer() {
    return this.consumer;
  }

  /**
   * Gracefully close connections.
   */
  async disconnect() {
    await Promise.all([
      this.producer.quit(),
      this.consumer.quit()
    ]);
  }
}

module.exports = RedisClient;
