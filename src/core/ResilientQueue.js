const { v4: uuidv4 } = require("uuid");

const RedisClient = require("../redis/RedisClient");
const IdempotencyManager = require("../managers/IdempotencyManager");
const DLQManager = require("../managers/DLQManager");
const RetryManager = require("../managers/RetryManager");
const { FatalError } = require("../errors/Errors");

/**
 * Main queue class.
 */
class ResilientQueue {
  constructor(options = {}) {
    const {
      redisUrl,
      maxRetries = 3,
      baseDelay = 500,
      keyPrefix = "rq"
    } = options;

    if (!redisUrl) {
      throw new Error("redisUrl is required to initialize ResilientQueue");
    }

    this.keyPrefix = keyPrefix;
    this.mainQueueKey = `${keyPrefix}:main`;

    const redisClientWrapper = new RedisClient(redisUrl);
    this.redis = redisClientWrapper.getClient();

    this.idempotencyManager = new IdempotencyManager(
      this.redis,
      keyPrefix
    );

    this.dlqManager = new DLQManager(
      this.redis,
      keyPrefix
    );

    this.retryManager = new RetryManager(
      this.redis,
      this.dlqManager,
      { maxRetries, baseDelay, keyPrefix }
    );
  }

  /**
   * Enqueue a new job.
   */
  async enqueue(data, options = {}) {
    const { idempotencyKey, ttlSeconds } = options;

    const job = {
      id: uuidv4(),
      data,
      attempt: 0,
      idempotencyKey: idempotencyKey || null,
      ttlSeconds: ttlSeconds || null,
      createdAt: Date.now()
    };

    await this.redis.rpush(this.mainQueueKey, JSON.stringify(job));

    return job.id;
  }

  /**
   * Start processing jobs.
   */
  async process(handler) {
    if (typeof handler !== "function") {
      throw new Error("A handler function must be provided to process()");
    }

    while (true) {
      try {
        const result = await this.redis.blpop(
          this.mainQueueKey,
          0
        );

        if (!result || result.length < 2) {
          continue;
        }

        const rawJob = result[1];
        let job;

        try {
          job = JSON.parse(rawJob);
        } catch (err) {
          // Malformed job — skip safely
          continue;
        }

        const shouldProceed =
          await this.idempotencyManager.shouldProcess(
            job.idempotencyKey,
            job.ttlSeconds
          );

        if (!shouldProceed) {
          continue;
        }

        try {
          await handler(job);
        } catch (error) {
          await this.retryManager.handleFailure(job, error);
        }

      } catch (err) {
        // Safety net — never crash worker
        console.error(
          "[resilient-queue] Worker loop error:",
          err.message
        );
      }
    }
  }
}

module.exports = ResilientQueue;
