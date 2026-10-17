import { EventEmitter } from "events";
import type { Redis, Cluster, RedisOptions, ClusterOptions, ClusterNode } from "ioredis";

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
   * Standard AbortSignal triggered if the job lease is lost (e.g. stalled or recovered by another worker) or on timeout.
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

  /**
   * Optional delay in milliseconds applied when the job was enqueued.
   */
  delay?: number;

  /**
   * Unix timestamp (in milliseconds) when the job is scheduled to execute.
   */
  runAt?: number;

  /**
   * Optional execution timeout in milliseconds applied to this job.
   */
  jobTimeout?: number;

  /**
   * Boolean flag indicating whether this job execution exceeded its configured execution timeout.
   */
  isTimedOut?: boolean;

  /**
   * Configured retry backoff options or strategy for this job.
   */
  backoff?: BackoffOptions<T> | BackoffStrategy | CustomBackoffFn<T>;

  /**
   * Maximum retry attempts for this job, overriding worker and queue defaults.
   */
  maxRetries?: number;

  /**
   * Base delay in milliseconds for this job, overriding worker and queue defaults.
   */
  baseDelay?: number;

  /**
   * Maximum delay cap in milliseconds for this job, overriding worker and queue defaults.
   */
  maxDelay?: number;

  /**
   * Backoff strategy for this job, overriding worker and queue defaults.
   */
  strategy?: BackoffStrategy;

  /**
   * Jitter flag for this job, overriding worker and queue defaults.
   */
  jitter?: boolean;
}

/**
 * Custom connection pair providing separate producer and consumer clients.
 */
export interface RedisConnectionPair {
  producer: Redis | Cluster;
  consumer?: Redis | Cluster;
}

/**
 * Redis Cluster configuration.
 */
export interface ClusterConfig {
  nodes: ClusterNode[];
  options?: ClusterOptions;
}

/**
 * Supported backoff strategies.
 */
export type BackoffStrategy = "exponential" | "fixed" | "custom";

/**
 * Custom backoff callback signature.
 * Receives the attempt number, error that caused failure, and job descriptor.
 * Can return a delay in milliseconds synchronously or asynchronously.
 */
export type CustomBackoffFn<T = any> = (
  attempt: number,
  error: Error,
  job: Job<T>
) => number | Promise<number>;

/**
 * Options configuring the retry backoff engine.
 */
export interface BackoffOptions<T = any> {
  /**
   * Backoff strategy: "exponential" (default), "fixed", or "custom".
   * @default "exponential"
   */
  strategy?: BackoffStrategy;

  /**
   * Base delay in milliseconds used by backoff strategies.
   * @default 500
   */
  baseDelay?: number;

  /**
   * Maximum retry delay cap in milliseconds for exponential backoff.
   * @default 60000
   */
  maxDelay?: number;

  /**
   * Enables random jitter to avoid thundering herds.
   * For exponential: AWS Equal Jitter [capped/2, capped].
   * For fixed: ±20% jitter [baseDelay * 0.8, baseDelay * 1.2].
   * @default false
   */
  jitter?: boolean;

  /**
   * Custom backoff callback when strategy is "custom".
   */
  fn?: CustomBackoffFn<T>;

  /**
   * Maximum number of retry attempts before routing to the DLQ.
   * @default 3
   */
  maxRetries?: number;
}

/**
 * Configuration options for initializing ResilientQueue.
 */
export interface QueueOptions {
  /**
   * Redis connection URL (e.g. "redis://127.0.0.1:6379").
   * Preserved for backward compatibility.
   */
  redisUrl?: string;

  /**
   * Flexible Redis connection options:
   * - Connection URL string (e.g. "redis://127.0.0.1:6379")
   * - Standard ioredis options object (supports host, port, db, tls, sentinels, etc.)
   * - Existing Redis or Cluster client instance
   * - Redis Cluster configuration with nodes and options
   * - Custom separate producer/consumer connection pair
   */
  redis?:
    | string
    | RedisOptions
    | Redis
    | Cluster
    | ClusterConfig
    | RedisConnectionPair;

  /**
   * Redis Cluster configuration or list of cluster nodes.
   */
  cluster?: ClusterNode[] | ClusterConfig;

  /**
   * List of Redis Cluster nodes.
   */
  clusterNodes?: ClusterNode[];

  /**
   * Options for Redis Cluster.
   */
  clusterOptions?: ClusterOptions;

  /**
   * Existing Redis or Cluster client instance.
   */
  client?: Redis | Cluster;

  /**
   * Producer connection or options.
   */
  producer?: Redis | Cluster | RedisOptions | string;

  /**
   * Consumer connection or options.
   */
  consumer?: Redis | Cluster | RedisOptions | string;

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
   * Maximum retry delay cap in milliseconds for exponential backoff.
   * @default 60000
   */
  maxDelay?: number;

  /**
   * Retry backoff strategy: "exponential" (default), "fixed", or "custom".
   * @default "exponential"
   */
  strategy?: BackoffStrategy;

  /**
   * Enable random jitter for retry backoff delays.
   * @default false
   */
  jitter?: boolean;

  /**
   * Configurable retry backoff engine options, strategy name, or custom function.
   */
  backoff?: BackoffOptions | BackoffStrategy | CustomBackoffFn;

  /**
   * Key namespace prefix for Redis keys (e.g. "rq" produces "rq:main", "rq:delayed", etc.).
   *
   * @remarks
   * For Redis Cluster deployments, `keyPrefix` MUST contain a Redis hash tag (e.g. `"{rq}"`),
   * ensuring that all multi-key Lua scripts and BLMOVE operations map to the same hash slot.
   *
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

  /**
   * Default execution timeout in milliseconds for jobs processed by this queue.
   */
  jobTimeout?: number;
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

  /**
   * Delay in milliseconds before the job becomes eligible for processing.
   */
  delay?: number;

  /**
   * Exact Date or Unix timestamp (in milliseconds) when the job should run.
   */
  runAt?: Date | number;

  /**
   * Optional execution timeout in milliseconds for this specific job, overriding worker and queue defaults.
   */
  jobTimeout?: number;

  /**
   * Configurable retry backoff engine options, strategy name, or custom function for this job.
   */
  backoff?: BackoffOptions | BackoffStrategy | CustomBackoffFn;

  /**
   * Per-job maximum retry attempts overriding worker and queue defaults.
   */
  maxRetries?: number;

  /**
   * Per-job base delay in milliseconds overriding worker and queue defaults.
   */
  baseDelay?: number;

  /**
   * Per-job maximum delay cap in milliseconds overriding worker and queue defaults.
   */
  maxDelay?: number;

  /**
   * Per-job backoff strategy overriding worker and queue defaults.
   */
  strategy?: BackoffStrategy;

  /**
   * Per-job jitter flag overriding worker and queue defaults.
   */
  jitter?: boolean;
}

/**
 * Item specification for enqueueMany.
 */
export interface EnqueueManyItem<T = any> extends EnqueueOptions {
  /**
   * The job data payload.
   */
  data: T;

  /**
   * Per-job options overriding batch defaults.
   */
  options?: EnqueueOptions;

  /**
   * Alias for options.
   */
  opts?: EnqueueOptions;
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

  /**
   * Optional worker-level execution timeout in milliseconds, overriding the queue default.
   */
  jobTimeout?: number;

  /**
   * Configurable retry backoff engine options, strategy name, or custom function for this worker.
   */
  backoff?: BackoffOptions | BackoffStrategy | CustomBackoffFn;

  /**
   * Worker-level maximum retry attempts overriding the queue default.
   */
  maxRetries?: number;

  /**
   * Worker-level base delay in milliseconds overriding the queue default.
   */
  baseDelay?: number;

  /**
   * Worker-level maximum delay cap in milliseconds overriding the queue default.
   */
  maxDelay?: number;

  /**
   * Worker-level backoff strategy overriding the queue default.
   */
  strategy?: BackoffStrategy;

  /**
   * Worker-level jitter flag overriding the queue default.
   */
  jitter?: boolean;
}

/**
 * Represents a failed job stored in the Dead-Letter Queue (DLQ).
 */
export interface DLQJob<T = any> extends Job<T> {
  /**
   * Unix timestamp (in milliseconds) when the job failed and was moved to DLQ.
   */
  failedAt?: number;

  /**
   * Diagnostic reason or error message describing why the job failed.
   */
  failureReason?: string;

  /**
   * Set to true if this entry was malformed or corrupted in Redis.
   */
  corrupted?: boolean;

  /**
   * Raw unparsed string if the entry could not be parsed as valid JSON.
   */
  raw?: string;

  /**
   * Parsing error message if the entry was corrupted.
   */
  error?: string;
}

/**
 * Options for querying the Dead-Letter Queue (DLQ).
 */
export interface DLQOptions {
  /**
   * Zero-based offset index to start retrieving jobs from.
   * @default 0
   */
  start?: number;

  /**
   * Maximum number of jobs to retrieve.
   * @default 50
   */
  limit?: number;

  /**
   * Sorting order: "asc" (oldest first, FIFO) or "desc" (newest first, LIFO).
   * @default "asc"
   */
  order?: "asc" | "desc";
}

/**
 * Result returned by getDLQ containing total count and paginated jobs.
 */
export interface DLQResult<T = any> {
  /**
   * Total number of jobs currently in the Dead-Letter Queue.
   */
  total: number;

  /**
   * Array of jobs retrieved for the requested page.
   */
  jobs: DLQJob<T>[];
}

/**
 * Options for bulk retrying DLQ jobs.
 */
export interface RetryAllDLQOptions {
  /**
   * Maximum number of DLQ jobs to requeue in this batch.
   * If omitted, requeues all eligible DLQ jobs.
   */
  limit?: number;
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
 * Event signatures for ResilientQueue lifecycle events.
 */
export interface ResilientQueueEvents<T = any> {
  /**
   * Emitted when a job has completed successfully.
   */
  completed: (job: Job<T>, result: any) => void;

  /**
   * Emitted when an execution attempt has failed.
   */
  failed: (job: Job<T>, error: Error) => void;

  /**
   * Emitted when a retry is scheduled with exponential backoff.
   */
  retrying: (job: Job<T>, error: Error, attempt: number, delayMs: number) => void;

  /**
   * Emitted when a job is moved to the Dead-Letter Queue (DLQ).
   */
  dlq: (job: DLQJob<T> | Job<T>, reason: Error | string) => void;

  /**
   * Emitted when a stalled job is successfully recovered.
   */
  stalled: (job: Job<T>) => void;

  /**
   * Emitted when an error is encountered in an event listener.
   */
  error: (error: Error) => void;
}

/**
 * Main ResilientQueue class.
 */
export class ResilientQueue extends EventEmitter {
  /**
   * Creates a new ResilientQueue instance.
   *
   * @param options Configuration options.
   */
  constructor(options: QueueOptions);

  on<E extends keyof ResilientQueueEvents<T>, T = any>(
    event: E,
    listener: ResilientQueueEvents<T>[E]
  ): this;
  on(event: string | symbol, listener: (...args: any[]) => void): this;

  once<E extends keyof ResilientQueueEvents<T>, T = any>(
    event: E,
    listener: ResilientQueueEvents<T>[E]
  ): this;
  once(event: string | symbol, listener: (...args: any[]) => void): this;

  emit<E extends keyof ResilientQueueEvents<T>, T = any>(
    event: E,
    ...args: Parameters<ResilientQueueEvents<T>[E]>
  ): boolean;
  emit(event: string | symbol, ...args: any[]): boolean;

  addListener<E extends keyof ResilientQueueEvents<T>, T = any>(
    event: E,
    listener: ResilientQueueEvents<T>[E]
  ): this;
  addListener(event: string | symbol, listener: (...args: any[]) => void): this;

  removeListener<E extends keyof ResilientQueueEvents<T>, T = any>(
    event: E,
    listener: ResilientQueueEvents<T>[E]
  ): this;
  removeListener(event: string | symbol, listener: (...args: any[]) => void): this;

  off<E extends keyof ResilientQueueEvents<T>, T = any>(
    event: E,
    listener: ResilientQueueEvents<T>[E]
  ): this;
  off(event: string | symbol, listener: (...args: any[]) => void): this;

  /**
   * Enqueues a new job.
   *
   * @param data Any JSON-serializable job payload.
   * @param options Optional enqueue settings (idempotency key, TTL, delay, and runAt).
   * @returns Resolves to the unique UUID v4 generated for the job, or existing job ID if idempotencyKey was already registered.
   */
  enqueue<T = any>(data: T, options?: EnqueueOptions): Promise<string>;

  /**
   * Enqueues multiple jobs in a single Redis network round-trip using an ioredis pipeline.
   *
   * @param jobs Array of job specifications ({ data, ...options } or plain data payloads).
   * @param defaultOptions Optional default enqueue settings applied to each job in the batch.
   * @returns Resolves to an array of job UUIDs in the same order as input jobs.
   */
  enqueueMany<T = any>(
    jobs: (EnqueueManyItem<T> | T)[],
    defaultOptions?: EnqueueOptions
  ): Promise<string[]>;

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

  /**
   * Retrieves a paginated list of failed jobs from the Dead-Letter Queue (DLQ).
   * Safely handles corrupted JSON entries and supports ascending/descending order.
   *
   * @param options Pagination and ordering options.
   * @returns Object containing the total DLQ count and array of jobs.
   */
  getDLQ<T = any>(options?: DLQOptions): Promise<DLQResult<T>>;

  /**
   * Returns the exact number of jobs currently waiting in the Dead-Letter Queue (DLQ).
   */
  getDLQCount(): Promise<number>;

  /**
   * Atomically moves a matching job from the DLQ back to the main queue for reprocessing,
   * resetting retry and failure metadata.
   *
   * @param jobId UUID of the job to retry.
   * @returns True if the job was found and retried, false otherwise.
   */
  retryDLQJob(jobId: string): Promise<boolean>;

  /**
   * Atomically moves jobs from the DLQ back to the main queue in controlled batches,
   * resetting retry and failure metadata.
   *
   * @param options Batch size options.
   * @returns Number of jobs successfully requeued.
   */
  retryAllDLQ(options?: RetryAllDLQOptions): Promise<number>;

  /**
   * Removes a specific failed job from the DLQ and cleans up associated metadata.
   *
   * @param jobId UUID of the job to remove.
   * @returns True if the job was found and removed, false otherwise.
   */
  removeDLQJob(jobId: string): Promise<boolean>;

  /**
   * Purges all jobs from the Dead-Letter Queue (DLQ) and cleans up associated metadata.
   *
   * @returns Total number of jobs purged.
   */
  purgeDLQ(): Promise<number>;

  /**
   * Manually promotes due delayed and retry jobs from the delayed sorted set to the main queue.
   *
   * @returns Number of jobs promoted to the main queue.
   */
  promoteDelayedJobs(): Promise<number>;
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

/**
 * Error indicating that a job execution timed out.
 * Extends RetryableError so timeout failures can be retried up to maxRetries before routing to DLQ.
 */
export class JobTimeoutError extends RetryableError {
  /**
   * The timeout threshold in milliseconds that was exceeded.
   */
  timeoutMs?: number | null;

  constructor(message?: string, timeoutMs?: number | null);
}
