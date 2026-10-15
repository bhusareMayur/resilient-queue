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
      redis,
      cluster,
      clusterNodes,
      clusterOptions,
      client,
      connection,
      producer,
      consumer,
      maxRetries = 3,
      baseDelay = 500,
      keyPrefix = "rq",
      stalledInterval = 15000,
      lockDuration = 30000,
      maxStalledCount = 1
    } = options;

    if (
      !redisUrl &&
      !redis &&
      !cluster &&
      !clusterNodes &&
      !client &&
      !connection &&
      !producer &&
      !consumer &&
      !options.host &&
      !options.port &&
      !options.sentinels
    ) {
      throw new Error("redisUrl or redis options are required to initialize ResilientQueue");
    }

    this.keyPrefix = keyPrefix;
    this.mainQueueKey = `${keyPrefix}:main`;
    this.delayedQueueKey = `${keyPrefix}:delayed`;
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

    this.redisClientWrapper = new RedisClient(options);

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
   * Validates options and creates the job descriptor.
   */
  _prepareJob(data, options = {}) {
    const { idempotencyKey, ttlSeconds, delay, runAt } = options || {};

    if (ttlSeconds !== undefined && ttlSeconds !== null) {
      if (typeof ttlSeconds !== "number" || !Number.isInteger(ttlSeconds)) {
        throw new Error("ttlSeconds must be an integer");
      }
    }

    let isDelayed = false;
    let executeAt = null;

    if (delay !== undefined && delay !== null) {
      if (typeof delay !== "number" || !Number.isFinite(delay) || delay < 0) {
        throw new Error("delay must be a non-negative number");
      }
      isDelayed = true;
      executeAt = Date.now() + delay;
    }

    if (runAt !== undefined && runAt !== null) {
      if (delay !== undefined && delay !== null) {
        throw new Error("Cannot specify both delay and runAt");
      }
      let timestamp;
      if (runAt instanceof Date) {
        timestamp = runAt.getTime();
        if (isNaN(timestamp)) {
          throw new Error("runAt must be a valid Date or timestamp");
        }
      } else if (typeof runAt === "number" && Number.isFinite(runAt)) {
        timestamp = runAt;
      } else {
        throw new Error("runAt must be a valid Date or timestamp");
      }
      isDelayed = true;
      executeAt = timestamp;
    }

    const job = {
      id: uuidv4(),
      data,
      attempt: 0,
      stalledCount: 0,
      idempotencyKey: idempotencyKey || null,
      ttlSeconds: ttlSeconds ?? null,
      createdAt: Date.now()
    };

    if (delay !== undefined && delay !== null) {
      job.delay = delay;
    }

    if (runAt !== undefined && runAt !== null) {
      job.runAt = executeAt;
    }

    return {
      job,
      isDelayed,
      executeAt,
      idempotencyKey: idempotencyKey || null,
      ttlSeconds: ttlSeconds ?? null
    };
  }

  /**
   * Enqueue a new job.
   */
  async enqueue(data, options = {}) {
    const { job, isDelayed, executeAt, idempotencyKey, ttlSeconds } =
      this._prepareJob(data, options);

    if (isDelayed) {
      if (!idempotencyKey) {
        await this.producer.zadd(
          this.delayedQueueKey,
          executeAt,
          JSON.stringify(job)
        );

        return job.id;
      }

      return await this.idempotencyManager.registerAndEnqueueDelayed(
        this.delayedQueueKey,
        idempotencyKey,
        job,
        executeAt,
        ttlSeconds
      );
    }

    if (!idempotencyKey) {
      await this.producer.rpush(
        this.mainQueueKey,
        JSON.stringify(job)
      );

      return job.id;
    }

    return await this.idempotencyManager.registerAndEnqueue(
      this.mainQueueKey,
      idempotencyKey,
      job,
      ttlSeconds
    );
  }

  /**
   * High-throughput batch enqueuing using an ioredis pipeline.
   */
  async enqueueMany(jobs, defaultOptions = {}) {
    if (!Array.isArray(jobs)) {
      throw new Error("jobs must be an array");
    }

    if (jobs.length === 0) {
      return [];
    }

    const prepared = [];
    for (const item of jobs) {
      let data;
      let itemOptions = {};

      if (item && typeof item === "object" && "data" in item) {
        data = item.data;
        const { data: _, options: itemOpts, opts: itemOpts2, ...directOpts } = item;
        itemOptions = Object.assign({}, directOpts, itemOpts2, itemOpts);
      } else {
        data = item;
      }

      const mergedOptions = Object.assign({}, defaultOptions || {}, itemOptions);
      prepared.push(this._prepareJob(data, mergedOptions));
    }

    const pipeline = this.producer.pipeline();

    for (const item of prepared) {
      const { job, isDelayed, executeAt, idempotencyKey, ttlSeconds } = item;
      const serialized = JSON.stringify(job);

      if (isDelayed) {
        if (!idempotencyKey) {
          pipeline.zadd(this.delayedQueueKey, executeAt, serialized);
        } else {
          this.idempotencyManager.appendRegisterAndEnqueueDelayed(
            pipeline,
            this.delayedQueueKey,
            idempotencyKey,
            job,
            executeAt,
            ttlSeconds
          );
        }
      } else {
        if (!idempotencyKey) {
          pipeline.rpush(this.mainQueueKey, serialized);
        } else {
          this.idempotencyManager.appendRegisterAndEnqueue(
            pipeline,
            this.mainQueueKey,
            idempotencyKey,
            job,
            ttlSeconds
          );
        }
      }
    }

    const results = await pipeline.exec();

    if (!results) {
      throw new Error("Pipeline execution failed: no results returned");
    }

    const jobIds = [];
    for (let i = 0; i < results.length; i++) {
      const [err, res] = results[i];
      if (err) {
        throw err;
      }

      const item = prepared[i];
      if (item.idempotencyKey) {
        jobIds.push(res || item.job.id);
      } else {
        jobIds.push(item.job.id);
      }
    }

    return jobIds;
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
            job.ttlSeconds,
            job.id
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

  /**
   * Retrieves paginated DLQ jobs.
   */
  async getDLQ(options = {}) {
    return await this.dlqManager.getDLQ(options);
  }

  /**
   * Returns exact count of DLQ jobs.
   */
  async getDLQCount() {
    return await this.dlqManager.getDLQCount();
  }

  /**
   * Atomically retries a specific DLQ job by moving it back to the main queue.
   */
  async retryDLQJob(jobId) {
    return await this.dlqManager.retryDLQJob(jobId);
  }

  /**
   * Atomically retries DLQ jobs in batches by moving them back to the main queue.
   */
  async retryAllDLQ(options = {}) {
    return await this.dlqManager.retryAllDLQ(options);
  }

  /**
   * Removes a specific failed job from DLQ.
   */
  async removeDLQJob(jobId) {
    return await this.dlqManager.removeDLQJob(jobId);
  }

  /**
   * Purges all jobs from DLQ and cleans associated metadata.
   */
  async purgeDLQ() {
    return await this.dlqManager.purgeDLQ();
  }

  /**
   * Manually promotes due delayed and retry jobs from the delayed sorted set to the main queue.
   */
  async promoteDelayedJobs() {
    return await this.retryManager.promoteDueRetries();
  }
}

module.exports = ResilientQueue;
 