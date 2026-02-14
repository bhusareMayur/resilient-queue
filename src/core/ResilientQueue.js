const { v4: uuidv4 } = require("uuid");

const RedisClient = require("../redis/RedisClient");
const IdempotencyManager = require("../managers/IdempotencyManager");
const DLQManager = require("../managers/DLQManager");
const RetryManager = require("../managers/RetryManager");

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
    this.isRunning = false;

    const redisClientWrapper = new RedisClient(redisUrl);

    this.producer = redisClientWrapper.getProducer();
    this.consumer = redisClientWrapper.getConsumer();

    this.idempotencyManager = new IdempotencyManager(
      this.producer,
      keyPrefix
    );

    this.dlqManager = new DLQManager(
      this.producer,
      keyPrefix
    );

    this.retryManager = new RetryManager(
      this.producer,
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

    await this.producer.rpush(
      this.mainQueueKey,
      JSON.stringify(job)
    );

    return job.id;
  }

  /**
   * Start processing jobs.
   */
  async process(handler) {
    if (typeof handler !== "function") {
      throw new Error("A handler function must be provided to process()");
    }

    this.isRunning = true;

    while (this.isRunning) {
      try {
        // IMPORTANT: timeout = 1 (not 0)
        const result = await this.consumer.blpop(
          this.mainQueueKey,
          1
        );

        if (!result || result.length < 2) {
          continue;
        }

        const rawJob = result[1];
        let job;

        try {
          job = JSON.parse(rawJob);
        } catch {
          continue;
        }

        let shouldProceed = true;

        // Apply idempotency only on first attempt
        if (job.attempt === 0) {
          shouldProceed =
            await this.idempotencyManager.shouldProcess(
              job.idempotencyKey,
              job.ttlSeconds
            );
        }

        if (!shouldProceed) {
          continue;
        }

        try {
          await handler(job);
        } catch (error) {
          // Prevent retry during shutdown
          if (this.isRunning) {
            await this.retryManager.handleFailure(job, error);
          }
        }

      } catch {
        // Silent safety net
      }
    }
  }

  /**
   * Stop the worker loop.
   */
  async stop() {
    this.isRunning = false;
  }

  /**
   * Stop worker and close Redis connections safely.
   */
  async close() {
    this.isRunning = false;

    // Wait for BLPOP (timeout 1s) to exit naturally
    await new Promise(resolve => setTimeout(resolve, 1100));

    await Promise.all([
      this.producer.quit(),
      this.consumer.quit()
    ]);
  }
}

module.exports = ResilientQueue;
