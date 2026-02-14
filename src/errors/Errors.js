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

module.exports = {
  RetryableError,
  FatalError
};
