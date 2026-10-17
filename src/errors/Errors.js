/**
 * Error used to indicate a job can be retried.
 */
class RetryableError extends Error {
  constructor(message) {
    super(message);
    this.name = "RetryableError";
    Error.captureStackTrace(this, this.constructor);
  }
}

/**
 * Error used to indicate a job should not be retried.
 */
class FatalError extends Error {
  constructor(message) {
    super(message);
    this.name = "FatalError";
    Error.captureStackTrace(this, this.constructor);
  }
}

/**
 * Error used to indicate a job execution timed out.
 * Extends RetryableError so timeout failures can be retried with exponential
 * backoff before routing to the DLQ.
 */
class JobTimeoutError extends RetryableError {
  constructor(message = "Job execution timed out", timeoutMs = null) {
    super(message);
    this.name = "JobTimeoutError";
    this.timeoutMs = timeoutMs;
    Error.captureStackTrace(this, this.constructor);
  }
}

module.exports = {
  RetryableError,
  FatalError,
  JobTimeoutError
};
