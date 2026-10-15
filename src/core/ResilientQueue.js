const { v4: uuidv4 } = require("uuid");

const RedisClient = require("../redis/RedisClient");
const IdempotencyManager = require("../managers/IdempotencyManager");
const DLQManager = require("../managers/DLQManager");
const RetryManager = require("../managers/RetryManager");
const StalledJobManager = require("../managers/StalledJobManager");

/**
 * Main queue class.
 */
class ResilientQueue {
  constructor(options = {}) {
    const {
      redisUrl,
      maxRetries = 3,
      baseDelay = 500,
      keyPrefix = "rq",
      stalledInterval = 15000,
      lockDuration = 30000,
      maxStalledCount = 1
    } = options;

    if (!redisUrl) {
      throw new Error("redisUrl is required to initialize ResilientQueue");
    }

    this.keyPrefix = keyPrefix;
    this.mainQueueKey = `${keyPrefix}:main`;
    this.isRunning = false;
    this.isClosed = false;
    this.workerCount = 0;
    this.retryScheduler = null;
    this.retrySchedulerTask = null;
    this.stalledScheduler = null;
    this.stalledSchedulerTask = null;
    this.activeJobs = new Set();
    this.fetcherPromise = null;
    this.workers = [];

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

    this.stalledJobManager = new StalledJobManager(
      this.producer,
      {
        keyPrefix,
        stalledInterval,
        lockDuration,
        maxStalledCount,
        maxRetries
      }
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
      stalledCount: 0,
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

    if (this.isRunning) {
      throw new Error("Queue is already processing");
    }

    this.isRunning = true;
    this.workerCount = concurrency;
    this.activeJobs = new Set();

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

    // Stalled jobs recovery scheduler
    if (this.stalledScheduler) {
      clearTimeout(this.stalledScheduler);
      this.stalledScheduler = null;
    }

    const promoteStalled = async () => {
      if (!this.isRunning) {
        return;
      }

      this.stalledSchedulerTask = (async () => {
        try {
          await this.stalledJobManager.recoverStalledJobs();
        } catch (error) {
          if (this.isRunning) {
            console.error("Stalled job scheduler encountered an error:", error);
          }
        }
      })();

      try {
        await this.stalledSchedulerTask;
      } finally {
        this.stalledSchedulerTask = null;

        if (this.isRunning) {
          this.stalledScheduler = setTimeout(
            promoteStalled,
            this.stalledJobManager.stalledInterval
          );
        }
      }
    };

    // Check stalled jobs immediately on startup to recover any crashed worker jobs.
    this.stalledScheduler = setTimeout(promoteStalled, 0);

    const executeJob = async (job) => {
      let shouldProceed = true;

      // Apply idempotency only on the first attempt if it hasn't stalled.
      if (job.attempt === 0 && !job.stalledCount) {
        shouldProceed =
          await this.idempotencyManager.shouldProcess(
            job.idempotencyKey,
            job.ttlSeconds
          );
      }

      if (!shouldProceed) {
        return;
      }

      try {
        await handler(job);
      } catch (error) {
        // If the lease was lost while handler was running, the recovery scheduler
        // already re-queued or dead-lettered this job. Do not duplicate failure handling.
        if (!job.isLeaseLost) {
          await this.retryManager.handleFailure(job, error);
        }
      }
    };

    const runFetcher = async () => {
      while (this.isRunning) {
        // Wait if in-memory concurrency limit is reached
        if (this.activeJobs.size >= concurrency) {
          await Promise.race(Array.from(this.activeJobs));
          continue;
        }

        try {
          // Timeout = 1 second for graceful shutdown.
          // Atomically moves job from rq:main to rq:active
          const rawJob = await this.stalledJobManager.dequeue(
            this.consumer,
            1
          );

          if (!rawJob) {
            continue;
          }

          let job;

          try {
            job = JSON.parse(rawJob);
          } catch (error) {
            // Remove malformed raw job from active queue to prevent queue clogging.
            await this.stalledJobManager.removeFromActive(rawJob);
            continue;
          }

          // Attach recovery metadata stored outside the raw payload
          const meta = await this.stalledJobManager.getStalledMetadata(job.id);
          if (meta) {
            job.stalledCount = meta.stalledCount;
            job.lastStalledAt = meta.lastStalledAt;
            if (meta.attempt !== undefined) {
              job.attempt = meta.attempt;
            }
          } else {
            job.stalledCount = job.stalledCount || 0;
          }

          // Generate per-dequeue fencing token
          const lockToken = uuidv4();
          job.lockToken = lockToken;

          // Surface lease loss to the handler using AbortController and flags
          const abortController = new AbortController();
          job.signal = abortController.signal;
          job.isLeaseLost = false;

          // Acquire lock with fencing token
          await this.stalledJobManager.acquireLock(job.id, lockToken);

          // Start heartbeat with fencing token and lease loss callback
          this.stalledJobManager.startHeartbeat(job.id, lockToken, () => {
            job.isLeaseLost = true;
            if (!abortController.signal.aborted) {
              abortController.abort(
                new Error(
                  "Job lease lost: lock expired or was recovered as stalled by another worker"
                )
              );
            }
          });

          // Dispatch job to the in-memory pool
          let jobPromise;
          jobPromise = (async () => {
            try {
              await executeJob(job);
            } catch (error) {
              if (this.isRunning) {
                console.error("Worker encountered an error:", error);
              }
            } finally {
              try {
                await this.stalledJobManager.releaseLock(rawJob, job.id, lockToken);
              } catch (cleanupError) {
                if (this.isRunning) {
                  console.error("Failed to release stalled job lock:", cleanupError);
                }
              }
              this.activeJobs.delete(jobPromise);
            }
          })();

          this.activeJobs.add(jobPromise);
        } catch (error) {
          // Keep the fetcher alive if a queue operation fails.
          if (this.isRunning) {
            console.error("Worker encountered an error:", error);
          }
        }
      }
    };

    this.fetcherPromise = runFetcher();
    this.workers = [this.fetcherPromise];
  }

  /**
   * Stop the worker loop and wait for in-flight tasks to finish.
   */
  async stop() {
    this.isRunning = false;

    if (this.retryScheduler) {
      clearTimeout(this.retryScheduler);
      this.retryScheduler = null;
    }

    if (this.stalledScheduler) {
      clearTimeout(this.stalledScheduler);
      this.stalledScheduler = null;
    }

    const schedulerTask = this.retrySchedulerTask;
    if (schedulerTask) {
      await schedulerTask;
    }

    const stalledTask = this.stalledSchedulerTask;
    if (stalledTask) {
      await stalledTask;
    }

    if (this.fetcherPromise) {
      await this.fetcherPromise;
      this.fetcherPromise = null;
    }

    if (this.activeJobs && this.activeJobs.size > 0) {
      await Promise.all(Array.from(this.activeJobs));
    }

    if (this.stalledJobManager) {
      this.stalledJobManager.stopAllHeartbeats();
    }
  }

  /**
   * Stop worker and close Redis connections safely.
   */
  async close() {
    if (this.isClosed) {
      return;
    }
    this.isClosed = true;

    await this.stop();

    try {
      if (this.workers && this.workers.length > 0) {
        await Promise.all(this.workers);
      }
    } finally {
      await this.redisClientWrapper.disconnect();
    }
  }

  /**
   * Get recovery or failure metadata stored for a job.
   */
  async getJobMetadata(jobId) {
    return await this.stalledJobManager.getStalledMetadata(jobId);
  }

}

module.exports = ResilientQueue;
 