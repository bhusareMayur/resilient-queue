const { RetryableError } = require("../errors/Errors");

/**
 * Validates retry and backoff options.
 * Throws a descriptive Error if any option is invalid.
 */
function validateBackoffOptions(options = {}) {
  if (!options || typeof options !== "object") {
    return;
  }

  const { maxRetries, baseDelay, maxDelay, jitter, strategy, backoff, fn } = options;

  if (maxRetries !== undefined && maxRetries !== null) {
    if (typeof maxRetries !== "number" || !Number.isInteger(maxRetries) || maxRetries < 0) {
      throw new Error("maxRetries must be a non-negative integer");
    }
  }

  if (baseDelay !== undefined && baseDelay !== null) {
    if (typeof baseDelay !== "number" || !Number.isFinite(baseDelay) || baseDelay < 0) {
      throw new Error("baseDelay must be a non-negative number");
    }
  }

  if (maxDelay !== undefined && maxDelay !== null) {
    if (typeof maxDelay !== "number" || !Number.isFinite(maxDelay) || maxDelay < 0) {
      throw new Error("maxDelay must be a non-negative number");
    }
  }

  if (jitter !== undefined && jitter !== null) {
    if (typeof jitter !== "boolean") {
      throw new Error("jitter must be a boolean");
    }
  }

  if (strategy !== undefined && strategy !== null) {
    if (!["exponential", "fixed", "custom"].includes(strategy)) {
      throw new Error(`Invalid backoff strategy: "${strategy}". Must be "exponential", "fixed", or "custom"`);
    }
    if (strategy === "custom") {
      const customFn =
        fn ||
        (backoff && typeof backoff === "object" ? backoff.fn : null) ||
        (typeof backoff === "function" ? backoff : null);
      if (typeof customFn !== "function") {
        throw new Error("Custom backoff strategy requires a fn callback function");
      }
    }
  }

  if (fn !== undefined && fn !== null) {
    if (typeof fn !== "function") {
      throw new Error("backoff fn must be a function");
    }
  }

  if (backoff !== undefined && backoff !== null) {
    if (typeof backoff === "function") {
      // valid custom function shorthand
    } else if (typeof backoff === "string") {
      if (!["exponential", "fixed", "custom"].includes(backoff)) {
        throw new Error(`Invalid backoff strategy: "${backoff}". Must be "exponential", "fixed", or "custom"`);
      }
      if (backoff === "custom" && typeof fn !== "function") {
        throw new Error("Custom backoff strategy requires a fn callback function");
      }
    } else if (typeof backoff === "object" && !Array.isArray(backoff)) {
      if (backoff.maxRetries !== undefined && backoff.maxRetries !== null) {
        if (typeof backoff.maxRetries !== "number" || !Number.isInteger(backoff.maxRetries) || backoff.maxRetries < 0) {
          throw new Error("maxRetries must be a non-negative integer");
        }
      }
      if (backoff.baseDelay !== undefined && backoff.baseDelay !== null) {
        if (typeof backoff.baseDelay !== "number" || !Number.isFinite(backoff.baseDelay) || backoff.baseDelay < 0) {
          throw new Error("baseDelay must be a non-negative number");
        }
      }
      if (backoff.maxDelay !== undefined && backoff.maxDelay !== null) {
        if (typeof backoff.maxDelay !== "number" || !Number.isFinite(backoff.maxDelay) || backoff.maxDelay < 0) {
          throw new Error("maxDelay must be a non-negative number");
        }
      }
      if (backoff.jitter !== undefined && backoff.jitter !== null) {
        if (typeof backoff.jitter !== "boolean") {
          throw new Error("jitter must be a boolean");
        }
      }
      if (backoff.strategy !== undefined && backoff.strategy !== null) {
        if (!["exponential", "fixed", "custom"].includes(backoff.strategy)) {
          throw new Error(`Invalid backoff strategy: "${backoff.strategy}". Must be "exponential", "fixed", or "custom"`);
        }
        if (backoff.strategy === "custom" && typeof backoff.fn !== "function" && typeof fn !== "function") {
          throw new Error("Custom backoff strategy requires a fn callback function");
        }
      }
      if (backoff.fn !== undefined && backoff.fn !== null) {
        if (typeof backoff.fn !== "function") {
          throw new Error("backoff fn must be a function");
        }
      }
    } else {
      throw new Error("backoff must be an object, string, or function");
    }
  }
}

/**
 * Extracts normalized backoff properties from string, function, or object backoff definitions.
 */
function extractBackoffOptions(input) {
  if (!input) return {};
  if (typeof input === "function") {
    return { strategy: "custom", fn: input };
  }
  if (typeof input === "string") {
    return { strategy: input };
  }
  if (typeof input === "object" && !Array.isArray(input)) {
    const res = {};
    if (input.strategy !== undefined) res.strategy = input.strategy;
    if (input.baseDelay !== undefined) res.baseDelay = input.baseDelay;
    if (input.maxDelay !== undefined) res.maxDelay = input.maxDelay;
    if (input.jitter !== undefined) res.jitter = input.jitter;
    if (input.fn !== undefined) res.fn = input.fn;
    if (input.maxRetries !== undefined) res.maxRetries = input.maxRetries;
    return res;
  }
  return {};
}

/**
 * Handles retry logic with configurable backoff strategies (exponential, fixed, custom).
 */
class RetryManager {
  constructor(redisClient, dlqManager, options = {}) {
    if (!redisClient) {
      throw new Error("Redis client is required for RetryManager");
    }

    if (!dlqManager) {
      throw new Error("DLQManager is required for RetryManager");
    }

    validateBackoffOptions(options);

    this.redis = redisClient;
    this.dlqManager = dlqManager;

    const extractedBackoff = extractBackoffOptions(options.backoff);

    this.maxRetries = options.maxRetries ?? extractedBackoff.maxRetries ?? 3;
    this.baseDelay = options.baseDelay ?? extractedBackoff.baseDelay ?? 500;
    this.maxDelay = options.maxDelay ?? extractedBackoff.maxDelay ?? 60000;
    this.strategy = options.strategy ?? extractedBackoff.strategy ?? "exponential";
    this.jitter = options.jitter ?? extractedBackoff.jitter ?? false;
    this.fn = options.fn ?? extractedBackoff.fn ?? null;
    this.backoff = options.backoff;

    this.keyPrefix = options.keyPrefix ?? "rq";
    this.onRetrying = options.onRetrying ?? null;
  }

  /**
   * Resolves effective backoff and retry settings using three-level configuration precedence:
   * per-job > worker-level > queue-level defaults.
   */
  resolveBackoffConfig(job, workerOptions = {}) {
    const jobBackoff = extractBackoffOptions(job?.backoff);
    const workerBackoff = extractBackoffOptions(workerOptions?.backoff);
    const queueBackoff = extractBackoffOptions(this.backoff);

    const jobOpts = {
      strategy: jobBackoff.strategy ?? job?.strategy,
      baseDelay: jobBackoff.baseDelay ?? job?.baseDelay,
      maxDelay: jobBackoff.maxDelay ?? job?.maxDelay,
      jitter: jobBackoff.jitter ?? job?.jitter,
      fn: jobBackoff.fn ?? job?._customBackoffFn ?? job?.fn,
      maxRetries: jobBackoff.maxRetries ?? job?.maxRetries
    };

    const workerOpts = {
      strategy: workerBackoff.strategy ?? workerOptions?.strategy,
      baseDelay: workerBackoff.baseDelay ?? workerOptions?.baseDelay,
      maxDelay: workerBackoff.maxDelay ?? workerOptions?.maxDelay,
      jitter: workerBackoff.jitter ?? workerOptions?.jitter,
      fn: workerBackoff.fn ?? workerOptions?.fn,
      maxRetries: workerBackoff.maxRetries ?? workerOptions?.maxRetries
    };

    const queueOpts = {
      strategy: queueBackoff.strategy ?? this.strategy ?? "exponential",
      baseDelay: queueBackoff.baseDelay ?? this.baseDelay ?? 500,
      maxDelay: queueBackoff.maxDelay ?? this.maxDelay ?? 60000,
      jitter: queueBackoff.jitter ?? this.jitter ?? false,
      fn: queueBackoff.fn ?? this.fn ?? null,
      maxRetries: queueBackoff.maxRetries ?? this.maxRetries ?? 3
    };

    const strategy = jobOpts.strategy ?? workerOpts.strategy ?? queueOpts.strategy ?? "exponential";
    const baseDelay = jobOpts.baseDelay ?? workerOpts.baseDelay ?? queueOpts.baseDelay ?? 500;
    const maxDelay = jobOpts.maxDelay ?? workerOpts.maxDelay ?? queueOpts.maxDelay ?? 60000;
    const jitter = jobOpts.jitter ?? workerOpts.jitter ?? queueOpts.jitter ?? false;
    const fn = jobOpts.fn ?? workerOpts.fn ?? queueOpts.fn ?? null;
    const maxRetries = jobOpts.maxRetries ?? workerOpts.maxRetries ?? queueOpts.maxRetries ?? 3;

    return {
      strategy,
      baseDelay,
      maxDelay,
      jitter,
      fn,
      maxRetries
    };
  }

  /**
   * Computes the retry delay in milliseconds based on attempt, error, job, and backoff configuration.
   *
   * @param {number} attempt Current retry attempt number (1 for first retry, etc.)
   * @param {Error} error The error that caused the failure
   * @param {object} job The job descriptor
   * @param {object} [workerOptions] Optional worker-level options
   * @returns {Promise<number>} Delay in milliseconds
   */
  async calculateDelay(attempt, error, job, workerOptions = {}) {
    const config = this.resolveBackoffConfig(job, workerOptions);
    const { strategy, baseDelay, maxDelay, jitter, fn } = config;

    let delay;

    if (strategy === "exponential") {
      const uncapped = baseDelay === 0 ? 0 : baseDelay * Math.pow(2, attempt);
      const capped = Math.min(maxDelay, uncapped);
      if (jitter) {
        // Equal Jitter: sleep = capped / 2 + rand(0, capped / 2)
        const half = capped / 2;
        delay = Math.round(half + Math.random() * half);
      } else {
        delay = capped;
      }
    } else if (strategy === "fixed") {
      if (jitter) {
        // Fixed jitter: baseDelay ± 20%
        delay = Math.round(baseDelay * (0.8 + Math.random() * 0.4));
      } else {
        delay = baseDelay;
      }
    } else if (strategy === "custom") {
      if (typeof fn !== "function") {
        throw new Error("Custom backoff strategy requires a fn callback function");
      }
      delay = await fn(attempt, error, job);
    } else {
      throw new Error(`Invalid backoff strategy: "${strategy}". Must be "exponential", "fixed", or "custom"`);
    }

    if (typeof delay !== "number" || !Number.isFinite(delay) || delay < 0) {
      throw new Error(`Invalid retry delay: ${delay}. Delay must be a finite non-negative number`);
    }

    return Math.round(delay) || 0;
  }

  /**
   * Handle job failure and decide whether to retry or send to DLQ.
   *
   * @param {object} job The failed job
   * @param {Error} error The error that caused failure
   * @param {object} [workerOptions] Worker-level options for configuration precedence
   */
  async handleFailure(job, error, workerOptions = {}) {
    // If error is not retryable → send to DLQ immediately
    if (!(error instanceof RetryableError)) {
      await this.dlqManager.moveToDLQ(job, error);
      return;
    }

    const nextAttempt = (job.attempt || 0) + 1;
    const config = this.resolveBackoffConfig(job, workerOptions);

    // If max retries exceeded → send to DLQ
    if (nextAttempt > config.maxRetries) {
      await this.dlqManager.moveToDLQ(
        { ...job, attempt: nextAttempt },
        error
      );
      return;
    }

    // Compute backoff delay
    let delay;
    try {
      delay = await this.calculateDelay(nextAttempt, error, job, workerOptions);
    } catch (delayError) {
      // Failed to compute a valid delay (e.g. invalid custom callback or result).
      // Safely move job to DLQ with the error and DO NOT silently schedule invalid delay.
      await this.dlqManager.moveToDLQ(
        { ...job, attempt: nextAttempt },
        delayError
      );
      return;
    }

    const retryJob = {
      ...job,
      attempt: nextAttempt,
      lastError: error.message,
      retriedAt: Date.now()
    };

    // Persist the retry in Redis until it is due.
    const delayedQueueKey = `${this.keyPrefix}:delayed`;
    const executeAt = Date.now() + delay;

    try {
      await this.redis.zadd(
        delayedQueueKey,
        executeAt,
        JSON.stringify(retryJob)
      );
      if (typeof this.onRetrying === "function") {
        this.onRetrying(retryJob, error, nextAttempt, delay);
      }
    } catch (storageError) {
      try {
        await this.dlqManager.moveToDLQ(retryJob, storageError);
      } catch (dlqError) {
        const err = new Error(
          "Failed to persist retry and failed to move job to DLQ"
        );
        err.storageError = storageError;
        err.dlqError = dlqError;
        throw err;
      }
    }
  }
  /**
 * Move due retries from the delayed ZSET to the main queue.
 * The Lua script makes the move atomic.
 */
async promoteDueRetries() {
  const delayedQueueKey = `${this.keyPrefix}:delayed`;
  const mainQueueKey = `${this.keyPrefix}:main`;

 const script = `
  -- Validate the destination before modifying the delayed queue.
  local destinationType = redis.call('TYPE', KEYS[2]).ok

  if destinationType ~= 'none' and destinationType ~= 'list' then
    return redis.error_reply(
      'Main queue key must be a Redis list'
    )
  end

  local jobs = redis.call(
    'ZRANGEBYSCORE',
    KEYS[1],
    '-inf',
    ARGV[1],
    'LIMIT',
    0,
    ARGV[2]
  )

  for _, job in ipairs(jobs) do
    if redis.call('ZREM', KEYS[1], job) == 1 then
      redis.call('RPUSH', KEYS[2], job)
    end
  end

  return #jobs
`;

  return this.redis.eval(
    script,
    2,
    delayedQueueKey,
    mainQueueKey,
    Date.now(),
    100
  );
}
}

RetryManager.validateBackoffOptions = validateBackoffOptions;
RetryManager.extractBackoffOptions = extractBackoffOptions;

module.exports = RetryManager;
