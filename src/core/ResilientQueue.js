const { EventEmitter } = require("events");
const { v4: uuidv4 } = require("uuid");

const RedisClient = require("../redis/RedisClient");
const IdempotencyManager = require("../managers/IdempotencyManager");
const DLQManager = require("../managers/DLQManager");
const RetryManager = require("../managers/RetryManager");
const StalledJobManager = require("../managers/StalledJobManager");
const { JobTimeoutError } = require("../errors/Errors");

/**
 * Main queue class.
 */
class ResilientQueue extends EventEmitter {
  constructor(options = {}) {
    super();

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
      maxRetries,
      baseDelay,
      maxDelay,
      backoff,
      strategy,
      jitter,
      fn,
      keyPrefix = "rq",
      stalledInterval = 15000,
      lockDuration = 30000,
      maxStalledCount = 1,
      jobTimeout = null
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

    if (jobTimeout !== undefined && jobTimeout !== null) {
      if (typeof jobTimeout !== "number" || !Number.isFinite(jobTimeout) || jobTimeout <= 0) {
        throw new Error("jobTimeout must be a positive number");
      }
    }

    RetryManager.validateBackoffOptions({
      maxRetries,
      baseDelay,
      maxDelay,
      jitter,
      strategy,
      backoff,
      fn
    });

    const resolvedBaseDelay =
      baseDelay ??
      (typeof backoff === "object" && backoff !== null ? backoff.baseDelay : undefined) ??
      500;
    const resolvedMaxRetries =
      maxRetries ??
      (typeof backoff === "object" && backoff !== null ? backoff.maxRetries : undefined) ??
      3;
    const resolvedMaxDelay =
      maxDelay ??
      (typeof backoff === "object" && backoff !== null ? backoff.maxDelay : undefined) ??
      60000;

    this.jobTimeout = jobTimeout || null;
    this.maxRetries = resolvedMaxRetries;
    this.baseDelay = resolvedBaseDelay;
    this.maxDelay = resolvedMaxDelay;
    this.backoff = backoff;
    this.strategy = strategy;
    this.jitter = jitter;
    this.customJobBackoffFns = new Map();
    this.lockDuration = lockDuration;
    this.keyPrefix = keyPrefix;
    this.mainQueueKey = `${keyPrefix}:main`;
    this.delayedQueueKey = `${keyPrefix}:delayed`;
    this.activeQueueKey = `${keyPrefix}:active`;
    this.dlqKey = `${keyPrefix}:dlq`;
    this.pausedKey = `${keyPrefix}:paused`;
    this._isLocallyPaused = false;
    this._resumeWaiters = new Set();
    this._pausePollInterval =
      typeof options.pausePollInterval === "number" &&
      Number.isFinite(options.pausePollInterval) &&
      options.pausePollInterval > 0
        ? options.pausePollInterval
        : 50;
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
      keyPrefix,
      {
        onDLQ: (job, reason) => {
          if (job && job.id) {
            this.customJobBackoffFns.delete(job.id);
          }
          this._safeEmit("dlq", job, reason);
        }
      }
    );

    this.retryManager = new RetryManager(
      this.producer,
      this.dlqManager,
      {
        maxRetries: resolvedMaxRetries,
        baseDelay: resolvedBaseDelay,
        maxDelay: resolvedMaxDelay,
        backoff: options.backoff,
        strategy: options.strategy,
        jitter: options.jitter,
        fn: options.fn,
        keyPrefix,
        onRetrying: (job, error, attempt, delayMs) =>
          this._safeEmit("retrying", job, error, attempt, delayMs)
      }
    );

    this.stalledJobManager = new StalledJobManager(
      this.producer,
      {
        keyPrefix,
        stalledInterval,
        lockDuration,
        maxStalledCount,
        maxRetries: resolvedMaxRetries,
        onStalled: (job) => this._safeEmit("stalled", job),
        onDLQ: (job, reason) => this._safeEmit("dlq", job, reason)
      }
    );
  }

  /**
   * Safely invoke event listeners so listener exceptions or rejected promises
   * never corrupt queue processing or trigger unhandled rejections.
   */
  _safeEmit(event, ...args) {
    try {
      const listeners = this.rawListeners(event);
      for (const listener of listeners) {
        try {
          const res = listener.apply(this, args);
          if (res && typeof res.catch === "function") {
            res.catch((err) => {
              if (event !== "error") {
                this._safeEmit("error", err);
              }
            });
          }
        } catch (listenerErr) {
          if (event !== "error") {
            this._safeEmit("error", listenerErr);
          }
        }
      }
    } catch {
      // Safety net to protect worker loop
    }
  }

  /**
   * Validates options and creates the job descriptor.
   */
  _prepareJob(data, options = {}) {
    const {
      idempotencyKey,
      ttlSeconds,
      delay,
      runAt,
      jobTimeout,
      maxRetries,
      baseDelay,
      maxDelay,
      strategy,
      jitter,
      backoff,
      fn
    } = options || {};

    if (ttlSeconds !== undefined && ttlSeconds !== null) {
      if (typeof ttlSeconds !== "number" || !Number.isInteger(ttlSeconds)) {
        throw new Error("ttlSeconds must be an integer");
      }
    }

    if (jobTimeout !== undefined && jobTimeout !== null) {
      if (typeof jobTimeout !== "number" || !Number.isFinite(jobTimeout) || jobTimeout <= 0) {
        throw new Error("jobTimeout must be a positive number");
      }
    }

    RetryManager.validateBackoffOptions({
      maxRetries,
      baseDelay,
      maxDelay,
      strategy,
      jitter,
      backoff,
      fn
    });

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

    if (jobTimeout !== undefined && jobTimeout !== null) {
      job.jobTimeout = jobTimeout;
    }

    if (maxRetries !== undefined && maxRetries !== null) {
      job.maxRetries = maxRetries;
    }

    if (baseDelay !== undefined && baseDelay !== null) {
      job.baseDelay = baseDelay;
    }

    if (maxDelay !== undefined && maxDelay !== null) {
      job.maxDelay = maxDelay;
    }

    if (strategy !== undefined && strategy !== null) {
      job.strategy = strategy;
    }

    if (jitter !== undefined && jitter !== null) {
      job.jitter = jitter;
    }

    if (backoff !== undefined && backoff !== null) {
      if (typeof backoff === "function") {
        job.backoff = { strategy: "custom" };
        this.customJobBackoffFns.set(job.id, backoff);
      } else if (typeof backoff === "string") {
        job.backoff = { strategy: backoff };
      } else if (typeof backoff === "object") {
        const { fn: customFn, ...serializableBackoff } = backoff;
        job.backoff = { ...serializableBackoff };
        if (typeof customFn === "function") {
          this.customJobBackoffFns.set(job.id, customFn);
        }
      }
    }

    if (typeof fn === "function") {
      this.customJobBackoffFns.set(job.id, fn);
    }

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

    const defOpts =
      defaultOptions && typeof defaultOptions === "object" && !Array.isArray(defaultOptions)
        ? defaultOptions
        : {};

    const prepared = [];
    for (const item of jobs) {
      let data;
      let itemOptions = {};

      if (item && typeof item === "object" && !Array.isArray(item) && "data" in item) {
        data = item.data;
        const { data: _, options: itemOpts, opts: itemOpts2, ...directOpts } = item;
        const safeOpts =
          itemOpts && typeof itemOpts === "object" && !Array.isArray(itemOpts)
            ? itemOpts
            : {};
        const safeOpts2 =
          itemOpts2 && typeof itemOpts2 === "object" && !Array.isArray(itemOpts2)
            ? itemOpts2
            : {};
        itemOptions = Object.assign({}, directOpts, safeOpts2, safeOpts);
      } else {
        data = item;
      }

      const mergedOptions = Object.assign({}, defOpts, itemOptions);
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

    const { concurrency = 1, jobTimeout = null } = options;

    if (!Number.isInteger(concurrency) || concurrency < 1) {
      throw new Error("Concurrency must be a positive integer");
    }

    if (jobTimeout !== undefined && jobTimeout !== null) {
      if (typeof jobTimeout !== "number" || !Number.isFinite(jobTimeout) || jobTimeout <= 0) {
        throw new Error("jobTimeout must be a positive number");
      }
    }

    RetryManager.validateBackoffOptions(options);

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

    const executeJob = async (job, abortController) => {
      if (this.customJobBackoffFns.has(job.id)) {
        job._customBackoffFn = this.customJobBackoffFns.get(job.id);
      }

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

      const timeoutMs =
        job.jobTimeout ??
        options.jobTimeout ??
        this.jobTimeout ??
        null;

      let completedEmitted = false;
      let failedEmitted = false;

      if (!timeoutMs || timeoutMs <= 0) {
        try {
          const result = await handler(job);
          if (!job.isLeaseLost && !completedEmitted) {
            completedEmitted = true;
            this.customJobBackoffFns.delete(job.id);
            this._safeEmit("completed", job, result);
          }
        } catch (error) {
          if (!job.isLeaseLost && !failedEmitted) {
            failedEmitted = true;
            this._safeEmit("failed", job, error);
            await this.retryManager.handleFailure(job, error, options);
          }
        }
        return;
      }

      let timeoutTimer = null;
      let timedOut = false;
      job.isTimedOut = false;

      let handlerPromise;
      try {
        handlerPromise = Promise.resolve().then(() => handler(job));
      } catch (syncError) {
        handlerPromise = Promise.reject(syncError);
      }

      // Prevent unhandled rejections on late handler rejection
      handlerPromise.catch(() => {});

      const timeoutPromise = new Promise((_, reject) => {
        timeoutTimer = setTimeout(() => {
          timedOut = true;
          job.isTimedOut = true;
          const timeoutErr = new JobTimeoutError(
            `Job execution timed out after ${timeoutMs}ms`,
            timeoutMs
          );
          if (abortController && !abortController.signal.aborted) {
            abortController.abort(timeoutErr);
          }
          reject(timeoutErr);
        }, timeoutMs);
      });

      try {
        const result = await Promise.race([handlerPromise, timeoutPromise]);
        if (!job.isLeaseLost && !timedOut && !completedEmitted) {
          completedEmitted = true;
          this.customJobBackoffFns.delete(job.id);
          this._safeEmit("completed", job, result);
        }
      } catch (error) {
        if (!job.isLeaseLost && !failedEmitted) {
          failedEmitted = true;
          this._safeEmit("failed", job, error);
        }

        if (timedOut) {
          // Safe wait for late handler settlement bounded by lock duration
          // before scheduling retry or releasing lock, preventing concurrent
          // execution with retried job instances and premature lock expiration.
          const maxWaitMs = this.lockDuration || 30000;
          await Promise.race([
            handlerPromise,
            new Promise((resolve) => setTimeout(resolve, maxWaitMs))
          ]).catch(() => {});
        }

        if (!job.isLeaseLost) {
          await this.retryManager.handleFailure(job, error, options);
        }
      } finally {
        if (timeoutTimer) {
          clearTimeout(timeoutTimer);
          timeoutTimer = null;
        }

        this.stalledJobManager.stopHeartbeat(job.id);
      }
    };

    const runFetcher = async () => {
      while (this.isRunning) {
        // Wait if in-memory concurrency limit is reached
        if (this.activeJobs.size >= concurrency) {
          await Promise.race(Array.from(this.activeJobs));
          continue;
        }

        if (await this.isPaused()) {
          await this._waitForResume();
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

          if (await this.isPaused()) {
            try {
              await this.producer.eval(
                `
                local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
                if removed > 0 then
                  redis.call('LPUSH', KEYS[2], ARGV[1])
                  return 1
                end
                return 0
                `,
                2,
                this.activeQueueKey,
                this.mainQueueKey,
                rawJob
              );
            } catch (err) {
              this._safeEmit("error", err);
            }
            await this._waitForResume();
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
              await executeJob(job, abortController);
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
    this._wakeResumeWaiters();

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
    this._cleanupPauseState();

    try {
      if (this.workers && this.workers.length > 0) {
        await Promise.all(this.workers);
      }
    } finally {
      if (this.customJobBackoffFns) {
        this.customJobBackoffFns.clear();
      }
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

  /**
   * Waits until the queue is resumed or shut down.
   */
  _waitForResume() {
    if (!this.isRunning || this.isClosed) {
      return Promise.resolve();
    }

    return new Promise((resolve) => {
      let timer = null;

      const cleanupAndResolve = () => {
        if (timer) {
          clearTimeout(timer);
          timer = null;
        }
        this._resumeWaiters.delete(cleanupAndResolve);
        resolve();
      };

      this._resumeWaiters.add(cleanupAndResolve);

      timer = setTimeout(() => {
        cleanupAndResolve();
      }, this._pausePollInterval);

      if (timer && timer.unref) {
        timer.unref();
      }
    });
  }

  /**
   * Wakes up any pending resume waiters.
   */
  _wakeResumeWaiters() {
    if (this._resumeWaiters && this._resumeWaiters.size > 0) {
      const waiters = Array.from(this._resumeWaiters);
      this._resumeWaiters.clear();
      for (const waiter of waiters) {
        try {
          waiter();
        } catch {
          // Safety net
        }
      }
    }
  }

  /**
   * Cleans up pause waiters and state on close.
   */
  _cleanupPauseState() {
    this._wakeResumeWaiters();
    this._isLocallyPaused = false;
  }

  /**
   * Returns real-time metrics for waiting, active, delayed, and dead-letter queue jobs.
   * Uses an ioredis pipeline to query all four states in a single network round-trip.
   *
   * @returns {Promise<{ waiting: number, active: number, delayed: number, dlq: number }>}
   */
  async getMetrics() {
    if (this.isClosed) {
      throw new Error("Queue is closed");
    }

    const pipeline = this.producer.pipeline();
    pipeline.llen(this.mainQueueKey);
    pipeline.llen(this.activeQueueKey);
    pipeline.zcard(this.delayedQueueKey);
    pipeline.llen(this.dlqKey);

    const results = await pipeline.exec();

    if (!results) {
      throw new Error("Pipeline execution failed: no results returned");
    }

    for (let i = 0; i < results.length; i++) {
      const [err] = results[i];
      if (err) {
        throw err;
      }
    }

    return {
      waiting: Number(results[0][1]) || 0,
      active: Number(results[1][1]) || 0,
      delayed: Number(results[2][1]) || 0,
      dlq: Number(results[3][1]) || 0
    };
  }

  /**
   * Pauses queue job consumption.
   * Active jobs continue executing until finished, but no new jobs are dequeued.
   * Idempotent and coordinates across distributed workers using Redis.
   */
  async pause() {
    if (this.isClosed) {
      throw new Error("Queue is closed");
    }

    const wasPaused = this._isLocallyPaused;
    this._isLocallyPaused = true;

    try {
      await this.producer.set(this.pausedKey, "1");
      if (!wasPaused) {
        this._safeEmit("paused");
      }
    } catch (err) {
      this._isLocallyPaused = wasPaused;
      throw err;
    }
  }

  /**
   * Resumes queue job consumption.
   * Dequeuing resumes promptly locally and across distributed workers.
   * Idempotent and coordinates across distributed workers using Redis.
   */
  async resume() {
    if (this.isClosed) {
      throw new Error("Queue is closed");
    }

    const wasPaused = this._isLocallyPaused;
    this._isLocallyPaused = false;

    try {
      const deleted = await this.producer.del(this.pausedKey);
      this._wakeResumeWaiters();
      if (wasPaused || deleted > 0) {
        this._safeEmit("resumed");
      }
    } catch (err) {
      this._isLocallyPaused = wasPaused;
      throw err;
    }
  }

  /**
   * Returns whether the queue is currently paused.
   * Checks Redis state by default for distributed coordination,
   * with fallback to local state if Redis errors.
   *
   * @param {object|boolean} [options] Optional options or boolean localOnly flag.
   * @returns {Promise<boolean>}
   */
  async isPaused(options = {}) {
    const localOnly =
      typeof options === "boolean" ? options : Boolean(options && options.localOnly);

    if (localOnly || this.isClosed) {
      return this._isLocallyPaused;
    }

    try {
      const val = await this.producer.get(this.pausedKey);
      const redisPaused = val !== null && val !== undefined && val !== "0";

      if (this._isLocallyPaused !== redisPaused) {
        this._isLocallyPaused = redisPaused;
        if (redisPaused) {
          this._safeEmit("paused");
        } else {
          this._wakeResumeWaiters();
          this._safeEmit("resumed");
        }
      }

      return redisPaused;
    } catch (err) {
      return this._isLocallyPaused;
    }
  }
}

module.exports = ResilientQueue;
 