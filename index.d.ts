/**
 * Represents a queued job.
 */
export interface Job<T = any> {
  /**
   * Unique UUID v4 identifying the job.
   */
  id: string;

  /**
   * The user-provided job payload.
   */
  data: T;

  /**
   * The number of retry attempts executed so far (starts at 0 on initial try).
   */
  attempt: number;

  /**
   * Optional idempotency key supplied during enqueue.
   */
  idempotencyKey: string | null;

  /**
   * Optional TTL in seconds for the idempotency key in Redis.
   */
  ttlSeconds: number | null;

  /**
   * Unix timestamp (in milliseconds) when the job was enqueued.
   */
  createdAt: number;

  /**
   * Error message from the previous failed attempt (available on retries).
   */
  lastError?: string;

  /**
   * Unix timestamp (in milliseconds) of the most recent retry attempt.
   */
  retriedAt?: number;

  /**
   * The number of times this job stalled (due to worker crash or timeout) and was recovered.
   */
  stalledCount?: number;

  /**
   * Unix timestamp (in milliseconds) when the job was most recently recovered from a stalled state.
   */
  lastStalledAt?: number;

  /**
   * Standard AbortSignal triggered if the job lease is lost (e.g. stalled or recovered by another worker).
   */
  signal?: AbortSignal;

  /**
   * Boolean flag indicating whether this worker's visibility lease for the job has expired or was revoked.
   */
  isLeaseLost?: boolean;

  /**
   * Per-dequeue fencing token identifying this specific execution lease.
   */
  lockToken?: string;
}

/**
 * Configuration options for initializing ResilientQueue.
 */
export interface QueueOptions {
  /**
   * Redis connection URL (e.g. "redis://127.0.0.1:6379").
   */
  redisUrl: string;

  /**
   * Maximum number of retry attempts before routing a failed job to the DLQ.
   * @default 3
   */
  maxRetries?: number;

  /**
   * Base retry delay in milliseconds for exponential backoff (baseDelay * 2^attempt).
   * @default 500
   */
  baseDelay?: number;

  /**
   * Key namespace prefix for Redis keys (e.g. "rq" produces "rq:main", "rq:delayed", etc.).
   * @default "rq"
   */
  keyPrefix?: string;

  /**
   * Interval in milliseconds at which the stalled job recovery scheduler runs.
   * @default 15000
   */
  stalledInterval?: number;

  /**
   * Visibility timeout duration in milliseconds before an unacknowledged active job is considered stalled.
   * @default 30000
   */
  lockDuration?: number;

  /**
   * Maximum times a job can stall (crashed worker) before being moved directly to DLQ.
   * @default 1
   */
  maxStalledCount?: number;
}

/**
 * Options for enqueuing a job.
 */
export interface EnqueueOptions {
  /**
   * Unique idempotency key to prevent duplicate job processing.
   */
  idempotencyKey?: string | null;

  /**
   * Time-to-live in seconds for the idempotency key in Redis.
   */
  ttlSeconds?: number | null;
}

/**
 * Options for worker job processing.
 */
export interface ProcessOptions {
  /**
   * Maximum number of jobs to process concurrently in this worker instance.
   * @default 1
   */
  concurrency?: number;
}

/**
 * Handler function invoked for each dequeued job.
 *
 * @remarks
 * Handlers and downstream external operations must be designed to be idempotent
 * or use an idempotency key. While per-dequeue fencing tokens prevent a stale worker
 * from altering Redis locks or active queue state, lease expiration cannot forcibly
 * interrupt external side-effects already in progress. Handlers can inspect `job.signal`
 * or pass it to abort-aware APIs (such as `fetch`) to be notified immediately upon lease loss.
 */
export type JobHandler<T = any> = (job: Job<T>) => Promise<void> | void;

/**
 * Main ResilientQueue class.
 */
export class ResilientQueue {
  /**
   * Creates a new ResilientQueue instance.
   *
   * @param options Configuration options.
   */
  constructor(options: QueueOptions);

  /**
   * Enqueues a new job.
   *
   * @param data Any JSON-serializable job payload.
   * @param options Optional enqueue settings (idempotency key and TTL).
   * @returns Resolves to the unique UUID v4 generated for the job, or existing job ID if idempotencyKey was already registered.
   */
  enqueue<T = any>(data: T, options?: EnqueueOptions): Promise<string>;

  /**
   * Starts processing jobs using the provided worker handler function.
   *
   * @param handler Async function to process each job.
   * @param options Processing configuration including concurrency.
   */
  process<T = any>(handler: JobHandler<T>, options?: ProcessOptions): Promise<void>;

  /**
   * Stops the worker polling loop and waits for currently active in-flight jobs to finish.
   */
  stop(): Promise<void>;

  /**
   * Stops worker processing and gracefully closes Redis connections.
   */
  close(): Promise<void>;
}

/**
 * Error indicating that a failure is transient and the job should be retried with exponential backoff.
 */
export class RetryableError extends Error {
  constructor(message?: string);
}

/**
 * Error indicating that a failure is permanent. Bypasses retries and moves the job directly to the DLQ.
 */
export class FatalError extends Error {
  constructor(message?: string);
}
