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
    this.workerCount = 0;
    this.retryScheduler = null;
    this.retrySchedulerTask = null;

  this.redisClientWrapper = new RedisClient(redisUrl);

  this.producer = this.redisClientWrapper.getProducer();
  this.consumer = this.redisClientWrapper.getConsumer();

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
  async process(handler, options = {}) {
    if (typeof handler !== "function") {
      throw new Error("A handler function must be provided to process()");
    }

    const { concurrency = 1 } = options;

    if (!Number.isInteger(concurrency) || concurrency < 1) {
      throw new Error("Concurrency must be a positive integer");
    }

    this.isRunning = true;
    this.workerCount = concurrency;
    this.workers = [];
// Recover persisted retries without overlapping scheduler calls.
if (this.retryScheduler) {
  clearTimeout(this.retryScheduler);
  this.retryScheduler = null;
}

const promoteRetries = async () => {
  if (!this.isRunning) {
    return;
  }

  this.retrySchedulerTask = (async () => {
    try {
      await this.retryManager.promoteDueRetries();
    } catch (error) {
      if (this.isRunning) {
        console.error("Retry scheduler encountered an error:", error);
      }
    }
  })();

  try {
    await this.retrySchedulerTask;
  } finally {
    this.retrySchedulerTask = null;

    if (this.isRunning) {
      this.retryScheduler = setTimeout(promoteRetries, 100);
    }
  }
};

// Recover due jobs immediately.
this.retryScheduler = setTimeout(promoteRetries, 0);

    const startWorker = async () => {
      while (this.isRunning) {
        try {
          // Timeout = 1 second for graceful shutdown.
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
          } catch (error) {
            // Skip malformed jobs.
            continue;
          }

          let shouldProceed = true;

          // Apply idempotency only on the first attempt.
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
            await this.retryManager.handleFailure(job, error);
          }
        } catch (error) {
          // Keep the worker alive if a queue operation fails.
          if (this.isRunning) {
            console.error("Worker encountered an error:", error);
          }
        }
      }
    };

    // Start the worker pool.
    for (let i = 0; i < concurrency; i++) {
      this.workers.push(startWorker());
    }
  }


  /**
   * Stop the worker loop.
   */

async stop() {
  this.isRunning = false;

  if (this.retryScheduler) {
    clearTimeout(this.retryScheduler);
    this.retryScheduler = null;
  }

  const schedulerTask = this.retrySchedulerTask;

  if (schedulerTask) {
    await schedulerTask;
  }
}


  /**
   * Stop worker and close Redis connections safely.
   */

async close() {
  await this.stop();

  try {
    if (this.workers && this.workers.length > 0) {
      await Promise.all(this.workers);
    }
  } finally {
    await this.redisClientWrapper.disconnect();
  }
}

}

module.exports = ResilientQueue;
 