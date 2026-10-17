# API Reference

This document provides a comprehensive reference for all exported classes, methods, errors, types, and Redis data structures in `@mayurbhusare/resilient-queue`.

---

## Table of Contents

- [Package Exports](#package-exports)
- [Class: ResilientQueue](#class-resilientqueue)
  - [constructor(options)](#constructoroptions)
  - [queue.enqueue(data, [options])](#queueenqueuedata-options)
  - [queue.process(handler, [options])](#queueprocesshandler-options)
  - [queue.stop()](#queuestop)
  - [queue.close()](#queueclose)
  - [queue.getJobMetadata(jobId)](#queuegetjobmetadatajobid)
  - [queue.getDLQ([options])](#queuegetdlqoptions)
  - [queue.getDLQCount()](#queuegetdlqcount)
  - [queue.retryDLQJob(jobId)](#queueretrydlqjobjobid)
  - [queue.retryAllDLQ([options])](#queueretryalldlqoptions)
  - [queue.removeDLQJob(jobId)](#queueremovedlqjobjobid)
  - [queue.purgeDLQ()](#queuepurgedlq)
  - [queue.getMetrics()](#queuegetmetrics)
  - [queue.pause()](#queuepause)
  - [queue.resume()](#queueresume)
  - [queue.isPaused([options])](#queueispausedoptions)
  - [Lifecycle Events](#lifecycle-events)
- [Error Classes](#error-classes)
  - [RetryableError](#retryableerror)
  - [FatalError](#fatalerror)
  - [JobTimeoutError](#jobtimeouterror)
- [Retry Backoff Engine](#retry-backoff-engine)
  - [Strategies](#backoff-strategies)
  - [Jitter and Bounds](#jitter-and-bounds)
  - [Three-Level Configuration Precedence](#three-level-configuration-precedence)
  - [Validation and DLQ Protection](#validation-and-dlq-protection)
- [TypeScript Types & Interfaces](#typescript-types-interfaces)
  - [Job<T>](#jobt)
  - [BackoffStrategy](#backoffstrategy)
  - [CustomBackoffFn<T>](#custombackofffnt)
  - [BackoffOptions<T>](#backoffoptionst)
  - [ResilientQueueEvents<T>](#resilientqueueeventst)
  - [QueueMetrics](#queuemetrics)
  - [DLQJob<T>](#dlqjobt)
  - [DLQOptions](#dlqoptions)
  - [DLQResult<T>](#dlqresultt)
  - [RetryAllDLQOptions](#retryalldlqoptions)
  - [QueueOptions](#queueoptions)
  - [EnqueueOptions](#enqueueoptions)
  - [ProcessOptions](#processoptions)
  - [JobHandler<T>](#jobhandlert)
  - [RedisConnectionPair](#redisconnectionpair)
  - [ClusterConfig](#clusterconfig)
- [Redis Data Structures](#redis-data-structures)

---

## Package Exports

The package exports four main constructs:

```javascript
const {
  ResilientQueue,
  RetryableError,
  FatalError,
  JobTimeoutError
} = require("@mayurbhusare/resilient-queue");
```

---

## Class: ResilientQueue

The primary entry point for managing job queues, enqueuing tasks, and registering workers.

### `constructor(options)`

Initializes a new `ResilientQueue` instance.

#### Parameters

- `options` (`QueueOptions`): Configuration options object. See [QueueOptions](#queueoptions) for all fields. Includes:
  - `jobTimeout` (`number`, optional): Default execution timeout in milliseconds for all jobs.
  - `maxRetries` (`number`, optional): Maximum retry attempts before routing to the DLQ. Default: `3`.
  - `baseDelay` (`number`, optional): Base retry delay in milliseconds. Default: `500`.
  - `maxDelay` (`number`, optional): Maximum delay cap in milliseconds for exponential backoff. Default: `60000`.
  - `backoff` (`BackoffOptions | BackoffStrategy | CustomBackoffFn`, optional): Configurable retry backoff engine settings.
  - `strategy` (`BackoffStrategy`, optional): Backoff strategy (`"exponential"`, `"fixed"`, or `"custom"`). Default: `"exponential"`.
  - `jitter` (`boolean`, optional): Enables Equal Jitter for exponential or ±20% jitter for fixed. Default: `false`.

#### Throws

- Throws an `Error` (`"redisUrl or redis options are required to initialize ResilientQueue"`) if no Redis connection configuration is provided.
- Throws an `Error` (`"jobTimeout must be a positive number"`) if `jobTimeout` is specified and is not a positive number.
- Throws an `Error` if backoff configuration options (`strategy`, `baseDelay`, `maxDelay`, `jitter`, `maxRetries`, `fn`) are invalid.

#### Example

```javascript
const queue = new ResilientQueue({
  redisUrl: "redis://127.0.0.1:6379",
  maxRetries: 3,
  baseDelay: 500,
  keyPrefix: "rq",
  jobTimeout: 5000, // default 5-second timeout
  backoff: {
    strategy: "exponential",
    maxDelay: 30000,
    jitter: true
  }
});
```

---

### `queue.enqueue(data, [options])`

Pushes a new job to the tail of the main queue or schedules it in the delayed sorted set.

#### Parameters

- `data` (`any`): Any JSON-serializable value representing the job payload.
- `options` (`EnqueueOptions`, optional):
  - `idempotencyKey` (`string | null`, optional): Unique key for deduplication.
  - `ttlSeconds` (`number | null`, optional): TTL in seconds for the idempotency key in Redis. Must be an integer if specified.
  - `delay` (`number`, optional): Delay in milliseconds before the job becomes eligible for processing. Must be a non-negative number.
  - `runAt` (`Date | number`, optional): Exact `Date` object or Unix timestamp in milliseconds when the job should run. Mutually exclusive with `delay`.
  - `jobTimeout` (`number`, optional): Execution timeout in milliseconds for this specific job, overriding worker and queue defaults.
  - `backoff` (`BackoffOptions | BackoffStrategy | CustomBackoffFn`, optional): Per-job retry backoff engine settings overriding worker and queue defaults.
  - `maxRetries` (`number`, optional): Per-job maximum retry attempts.
  - `baseDelay` (`number`, optional): Per-job base delay in milliseconds.
  - `maxDelay` (`number`, optional): Per-job maximum delay cap in milliseconds.
  - `strategy` (`BackoffStrategy`, optional): Per-job backoff strategy.
  - `jitter` (`boolean`, optional): Per-job jitter flag.

#### Returns

- `Promise<string>`: Resolves to the unique UUID v4 generated for the job. If `idempotencyKey` was supplied and is already registered in Redis, resolves to the existing job ID without enqueuing a duplicate job.

#### Throws

- Throws an `Error` (`"ttlSeconds must be an integer"`) if `ttlSeconds` is provided and is not an integer.
- Throws an `Error` (`"delay must be a non-negative number"`) if `delay` is negative or not a finite number.
- Throws an `Error` (`"runAt must be a valid Date or timestamp"`) if `runAt` is invalid.
- Throws an `Error` (`"Cannot specify both delay and runAt"`) if both options are provided.
- Throws an `Error` (`"jobTimeout must be a positive number"`) if `jobTimeout` is specified and is not a positive number.
- Throws an `Error` if backoff options are invalid.
- Throws an `Error` if the Redis destination key type does not match.

#### Example

```javascript
// Standard enqueue
const jobId = await queue.enqueue({ userId: 123, action: "generate-report" });

// Delayed enqueue (run after 5 seconds)
const delayedJobId = await queue.enqueue(
  { email: "user@example.com" },
  { delay: 5000 }
);

// Scheduled enqueue with Date
const scheduledJobId = await queue.enqueue(
  { email: "user@example.com" },
  { runAt: new Date(Date.now() + 60000) }
);

// Idempotent enqueue with TTL
const idempotentId = await queue.enqueue(
  { transactionId: "txn_789", amount: 100 },
  { idempotencyKey: "txn_789", ttlSeconds: 3600 }
);

// Enqueue with per-job execution timeout
const timeoutJobId = await queue.enqueue(
  { query: "SELECT * FROM large_table" },
  { jobTimeout: 2000 }
);
```

---

### `queue.enqueueMany(jobs, [defaultOptions])`

Enqueues multiple jobs in a single Redis network round-trip using an `ioredis` pipeline, boosting throughput by 50x–100x when enqueuing thousands of jobs. Supports mixed payloads, delayed scheduling, and idempotency.

#### Parameters

- `jobs` (`Array<EnqueueManyItem<T> | T>`): Array of job descriptors (e.g. `{ data, ...options }`) or direct values.
- `defaultOptions` (`EnqueueOptions`, optional): Default settings applied to every job in the batch. Individual job options override these defaults.

#### Returns

- `Promise<string[]>`: Resolves to an array of job UUIDs in the same order as the input jobs.

#### Throws

- Throws an `Error` (`"jobs must be an array"`) if `jobs` is not an array.
- Throws validation errors if any job in the batch contains invalid options before executing the pipeline.

#### Example

```javascript
// High-throughput batch enqueuing
const jobIds = await queue.enqueueMany([
  { data: { orderId: 1 } },
  { data: { orderId: 2 } },
  { data: { orderId: 3 } },
]);

// Batch with mixed delays and idempotency
const mixedIds = await queue.enqueueMany([
  { data: { task: "immediate" } },
  { data: { task: "delayed" }, delay: 5000 },
  { data: { task: "scheduled" }, runAt: new Date(Date.now() + 60000) },
  { data: { task: "idempotent" }, idempotencyKey: "task-unique-1" }
]);
```

---

### `queue.process(handler, [options])`

Starts worker polling and processing jobs from the queue.

#### Parameters

- `handler` (`JobHandler<T>`): An asynchronous or synchronous function invoked for each dequeued job. Receives a `job` object. Handlers can inspect `job.signal` (`AbortSignal`) to cancel ongoing work on timeout or lease loss.
- `options` (`ProcessOptions`, optional):
  - `concurrency` (`number`, optional): Positive integer specifying how many jobs can run concurrently in this worker instance. Default: `1`.
  - `jobTimeout` (`number`, optional): Worker-level execution timeout in milliseconds, overriding the queue default.
  - `maxRetries` (`number`, optional): Worker-level maximum retry attempts, overriding queue defaults.
  - `baseDelay` (`number`, optional): Worker-level base delay in milliseconds, overriding queue defaults.
  - `maxDelay` (`number`, optional): Worker-level maximum retry delay cap in milliseconds, overriding queue defaults.
  - `strategy` (`BackoffStrategy`, optional): Worker-level backoff strategy (`"exponential" | "fixed" | "custom"`).
  - `jitter` (`boolean`, optional): Worker-level toggle for random jitter.
  - `fn` (`CustomBackoffFn`, optional): Worker-level custom backoff callback.
  - `backoff` (`BackoffOptions | BackoffStrategy | CustomBackoffFn`, optional): Worker-level nested backoff configuration.

#### Returns

- `Promise<void>`: Resolves immediately after worker initialization.

#### Throws

- Throws an `Error` (`"A handler function must be provided to process()"`) if `handler` is not a function.
- Throws an `Error` (`"Concurrency must be a positive integer"`) if `concurrency` is not a positive integer.
- Throws an `Error` (`"jobTimeout must be a positive number"`) if `jobTimeout` is specified and is not a positive number.
- Throws an `Error` (`"strategy must be one of: exponential, fixed, custom"`) if strategy is unrecognized.
- Throws an `Error` (`"baseDelay must be a non-negative number"`) if `baseDelay` is negative or not a number.
- Throws an `Error` (`"maxDelay must be a positive number"`) if `maxDelay` is not a positive number.
- Throws an `Error` (`"jitter must be a boolean"`) if `jitter` is not a boolean.
- Throws an `Error` (`"fn must be a function"`) if custom strategy `fn` is not a function.
- Throws an `Error` (`"maxRetries must be a non-negative number"`) if `maxRetries` is invalid.
- Throws an `Error` (`"Queue is already processing"`) if `process()` was already called on this queue instance.

#### Example

```javascript
queue.process(async (job) => {
  console.log(`Processing job ${job.id}:`, job.data);
  // Pass job.signal to cancel on timeout or lease loss
  const response = await fetch("https://api.example.com", { signal: job.signal });
  return await response.json();
}, {
  concurrency: 4,
  jobTimeout: 3000,
  backoff: {
    strategy: "exponential",
    baseDelay: 1000,
    maxDelay: 30000,
    jitter: true
  }
});
```

---

### `queue.stop()`

Stops worker polling, clears background retry and stalled-job recovery schedulers, and waits for all active in-flight jobs to complete.

#### Returns

- `Promise<void>`: Resolves when all active jobs have settled and schedulers have cleared.

#### Example

```javascript
await queue.stop();
```

---

### `queue.close()`

Calls `queue.stop()` to settle active jobs and then gracefully closes queue-owned Redis connections (`producer.quit()` and `consumer.quit()`). Externally provided client instances are preserved.

#### Returns

- `Promise<void>`: Resolves when worker loops have stopped and connections have disconnected.

#### Example

```javascript
await queue.close();
```

---

### `queue.getJobMetadata(jobId)`

Retrieves recovery and failure metadata stored for a job outside its raw JSON payload in `<keyPrefix>:stalled_meta`.

#### Parameters

- `jobId` (`string`): The UUID of the job.

#### Returns

- `Promise<object | null>`: An object containing:
  - `stalledCount` (`number`): Number of times the job stalled.
  - `lastStalledAt` (`number`): Timestamp in ms of the most recent stall.
  - `attempt` (`number`): Attempt count at recovery time.
  - `failedAt` (`number`, optional): Timestamp in ms if moved to DLQ.
  - `failureReason` (`string`, optional): Description of failure reason if routed to DLQ.
  - Returns `null` if no metadata exists for the given ID.

#### Example

```javascript
const meta = await queue.getJobMetadata("550e8400-e29b-41d4-a716-446655440000");
if (meta) {
  console.log(`Job stalled ${meta.stalledCount} times: ${meta.failureReason}`);
}
```

---

### `queue.getDLQ([options])`

Retrieves a paginated list of failed jobs from the Dead-Letter Queue (`<keyPrefix>:dlq`). Supports ascending and descending order, and safely handles corrupted JSON entries without throwing.

#### Parameters

- `options` (`DLQOptions`, optional):
  - `start` (`number`, optional): Zero-based offset index to start retrieving jobs from. Default: `0`.
  - `limit` (`number`, optional): Maximum number of jobs to retrieve. Default: `50`.
  - `order` (`"asc" | "desc"`, optional): Sort order. `"asc"` returns oldest failed jobs first (FIFO); `"desc"` returns newest failed jobs first (LIFO). Default: `"asc"`.

#### Returns

- `Promise<DLQResult>`: An object containing:
  - `total` (`number`): The exact total number of jobs currently in the DLQ.
  - `jobs` (`DLQJob[]`): Array of failed jobs for the requested page. If an entry in Redis contains corrupted JSON, it is returned as `{ id: null, raw: string, corrupted: true, error: string }`.

#### Example

```javascript
const { total, jobs } = await queue.getDLQ({ start: 0, limit: 10, order: "desc" });
console.log(`Total failed jobs: ${total}`);
for (const job of jobs) {
  if (job.corrupted) {
    console.error("Corrupted DLQ payload:", job.raw);
  } else {
    console.log(`Failed job ${job.id}: ${job.failureReason}`);
  }
}
```

---

### `queue.getDLQCount()`

Returns the exact number of failed jobs currently stored in the Dead-Letter Queue.

#### Returns

- `Promise<number>`: The total count of entries in the DLQ list.

#### Example

```javascript
const count = await queue.getDLQCount();
console.log(`DLQ has ${count} failed jobs`);
```

---

### `queue.retryDLQJob(jobId)`

Atomically moves one matching failed job from the Dead-Letter Queue back to the tail of the main queue (`<keyPrefix>:main`), resetting retry attempts and failure metadata back to 0.

#### Parameters

- `jobId` (`string`): The UUID of the failed job to retry.

#### Returns

- `Promise<boolean>`: Resolves to `true` if the job was found in the DLQ and successfully moved back to the main queue; resolves to `false` if the job was not found.

#### Example

```javascript
const retried = await queue.retryDLQJob("550e8400-e29b-41d4-a716-446655440000");
if (retried) {
  console.log("Job moved back to main queue for reprocessing");
}
```

---

### `queue.retryAllDLQ([options])`

Atomically requeues failed jobs from the Dead-Letter Queue back to the main queue in controlled batches, resetting retry attempts and failure metadata back to 0. Corrupted entries are safely skipped and retained in the DLQ.

#### Parameters

- `options` (`RetryAllDLQOptions`, optional):
  - `limit` (`number`, optional): Maximum number of failed jobs to requeue in this batch. If omitted, all eligible jobs in the DLQ are requeued.

#### Returns

- `Promise<number>`: The number of jobs successfully requeued to the main queue.

#### Example

```javascript
// Requeue in controlled batches of 50
const requeued = await queue.retryAllDLQ({ limit: 50 });
console.log(`Requeued ${requeued} jobs`);
```

---

### `queue.removeDLQJob(jobId)`

Removes a specific failed job from the Dead-Letter Queue and cleans up its associated metadata in `<keyPrefix>:stalled_meta`.

#### Parameters

- `jobId` (`string`): The UUID of the failed job to remove.

#### Returns

- `Promise<boolean>`: Resolves to `true` if the job was found and removed; `false` otherwise.

#### Example

```javascript
const removed = await queue.removeDLQJob("550e8400-e29b-41d4-a716-446655440000");
```

---

### `queue.purgeDLQ()`

Atomically clears the entire Dead-Letter Queue and cleans associated metadata in `<keyPrefix>:stalled_meta`.

#### Returns

- `Promise<number>`: The total number of jobs purged from the DLQ.

#### Example

```javascript
const purged = await queue.purgeDLQ();
console.log(`Purged ${purged} failed jobs from DLQ`);
```

---

### `queue.getMetrics()`

Retrieves real-time counts across waiting, active, delayed, and dead-letter states in a single network round-trip using an `ioredis` pipeline.

#### Returns

- `Promise<QueueMetrics>`: Resolves to an object containing numeric counts:
  - `waiting` (`number`): Number of jobs waiting in the main FIFO queue (`<keyPrefix>:main`).
  - `active` (`number`): Number of in-flight jobs currently being processed by workers (`<keyPrefix>:active`).
  - `delayed` (`number`): Number of scheduled jobs and pending retries in the delayed sorted set (`<keyPrefix>:delayed`).
  - `dlq` (`number`): Number of failed jobs stored in the Dead-Letter Queue (`<keyPrefix>:dlq`).

#### Throws

- Throws an `Error` (`"Queue is closed"`) if called on a closed queue instance.
- Throws an `Error` if the pipeline execution fails or returns command errors.

#### Example

```javascript
const metrics = await queue.getMetrics();
console.log(`Waiting: ${metrics.waiting}, Active: ${metrics.active}, Delayed: ${metrics.delayed}, DLQ: ${metrics.dlq}`);
```

---

### `queue.pause()`

Pauses queue job consumption. In-flight active jobs continue executing until finished, but no new jobs are dequeued. The pause state is shared across all workers via Redis key `<keyPrefix>:paused`. Enqueuing remains fully functional while paused.

#### Returns

- `Promise<void>`: Resolves when local state is updated and the Redis pause key is set.

#### Throws

- Throws an `Error` (`"Queue is closed"`) if called on a closed queue instance.
- Throws an `Error` if Redis operations fail.

#### Example

```javascript
await queue.pause();
console.log("Queue paused across all worker instances");
```

---

### `queue.resume()`

Resumes queue job consumption. Dequeuing resumes promptly on the local worker and across distributed workers. Idempotent and coordinates via Redis key `<keyPrefix>:paused`.

#### Returns

- `Promise<void>`: Resolves when local state is updated and the Redis pause key is removed.

#### Throws

- Throws an `Error` (`"Queue is closed"`) if called on a closed queue instance.
- Throws an `Error` if Redis operations fail.

#### Example

```javascript
await queue.resume();
console.log("Queue processing resumed");
```

---

### `queue.isPaused([options])`

Returns whether the queue is currently paused. By default, queries the shared Redis key `<keyPrefix>:paused` for distributed coordination with safe fallback to in-memory state on Redis errors.

#### Parameters

- `options` (`{ localOnly?: boolean } | boolean`, optional): If `true` or `{ localOnly: true }`, inspects the in-memory flag without a Redis command round-trip.

#### Returns

- `Promise<boolean>`: Resolves to `true` if paused, `false` otherwise.

#### Example

```javascript
// Distributed check (queries Redis)
const paused = await queue.isPaused();

// Fast local in-memory check
const localPaused = await queue.isPaused({ localOnly: true });
```

---

## Lifecycle Events

`ResilientQueue` extends Node.js's native `EventEmitter`. All event listeners execute inside an isolated boundary; errors thrown synchronously or rejected asynchronously by listener functions do not corrupt queue state, interrupt job processing, or trigger unhandled rejections.

### Event: `'completed'`

- **Signature**: `(job: Job<T>, result: any) => void`
- **Description**: Emitted immediately when a job handler completes successfully.
- **Payload**:
  - `job` (`Job<T>`): The job object that finished processing.
  - `result` (`any`): The value returned by the worker handler function (or `undefined`).

### Event: `'failed'`

- **Signature**: `(job: Job<T>, error: Error) => void`
- **Description**: Emitted when a job execution attempt fails (handler throws an error or execution times out).
- **Payload**:
  - `job` (`Job<T>`): The job object that encountered the failure.
  - `error` (`Error`): The error thrown by the handler or a `JobTimeoutError`.

### Event: `'retrying'`

- **Signature**: `(job: Job<T>, error: Error, attempt: number, delayMs: number) => void`
- **Description**: Emitted when a failed job is scheduled for a delayed retry with exponential backoff.
- **Payload**:
  - `job` (`Job<T>`): The updated job object with the new attempt count.
  - `error` (`Error`): The error causing the retry.
  - `attempt` (`number`): The upcoming attempt number (e.g. `1`, `2`).
  - `delayMs` (`number`): The backoff delay in milliseconds before the retry executes.

### Event: `'dlq'`

- **Signature**: `(job: DLQJob<T> | Job<T>, reason: Error | string) => void`
- **Description**: Emitted when a job is permanently routed to the Dead-Letter Queue (`<keyPrefix>:dlq`).
- **Payload**:
  - `job` (`DLQJob<T>`): The job object moved to the DLQ.
  - `reason` (`Error | string`): The fatal error, exhausted error, or poison pill diagnostic reason.

### Event: `'stalled'`

- **Signature**: `(job: Job<T>) => void`
- **Description**: Emitted when an abandoned active job (due to worker crash or lost heartbeat) is successfully recovered from `<keyPrefix>:active` back to `<keyPrefix>:main`.
- **Payload**:
  - `job` (`Job<T>`): The recovered job object with incremented `stalledCount`.

### Event: `'error'`

- **Signature**: `(error: Error) => void`
- **Description**: Emitted when an event listener throws a synchronous error or returns a rejected promise, if an `'error'` listener is registered.

### Event: `'paused'`

- **Signature**: `() => void`
- **Description**: Emitted when queue consumption is paused.

### Event: `'resumed'`

- **Signature**: `() => void`
- **Description**: Emitted when queue consumption is resumed.

---

## Error Classes

### `RetryableError`

```javascript
class RetryableError extends Error
```

Indicates that a job encountered a transient, recoverable failure. When thrown inside a `queue.process()` handler:
1. The error message is recorded in `job.lastError`.
2. The retry count `job.attempt` is incremented.
3. If `attempt <= maxRetries`, the job is scheduled in `<keyPrefix>:delayed` with an exponential backoff score.
4. If `attempt > maxRetries`, the job is routed directly to `<keyPrefix>:dlq`.

#### Constructor

```javascript
new RetryableError(message?: string)
```

- `message`: Description of the transient failure.
- `name`: Set to `"RetryableError"`.

---

### `FatalError`

```javascript
class FatalError extends Error
```

Indicates that a job encountered a permanent, non-recoverable error (e.g. malformed data, schema violation). When thrown inside `queue.process()`:
1. Bypasses all remaining retries.
2. The job is immediately routed to `<keyPrefix>:dlq` with `failureReason` set to the error message.

#### Constructor

```javascript
new FatalError(message?: string)
```

- `message`: Description of the fatal error.
- `name`: Set to `"FatalError"`.

> [!NOTE]
> Standard unhandled errors (e.g. `throw new Error(...)`) that do not inherit from `RetryableError` are also treated as permanent failures and routed directly to the DLQ.

---

### `JobTimeoutError`

```javascript
class JobTimeoutError extends RetryableError
```

Indicates that a job's execution exceeded the configured `jobTimeout` threshold in milliseconds. Inherits from `RetryableError`, allowing timeouts to be retried with exponential backoff up to `maxRetries` before routing to the DLQ.

#### Constructor

```javascript
new JobTimeoutError(message?: string, timeoutMs?: number | null)
```

- `message`: Description of the timeout failure (defaults to `"Job execution timed out"`).
- `timeoutMs`: Optional timeout duration in milliseconds that was exceeded.
- `name`: Set to `"JobTimeoutError"`.

---

## Retry Backoff Engine

`ResilientQueue` provides an enterprise-grade retry backoff engine that automatically paces retries when transient failures (`RetryableError` or `JobTimeoutError`) occur.

### Backoff Strategies

The engine supports three distinct backoff strategies:

| Strategy | Delay Formula | Default Parameters | Description |
| :--- | :--- | :--- | :--- |
| `"exponential"` | `min(maxDelay, baseDelay * 2^attempt)` | `baseDelay = 500`, `maxDelay = 60000`, `jitter = false` | Standard exponential growth with doubling delay on each attempt, capped at `maxDelay`. Default strategy. |
| `"fixed"` | Constant `baseDelay` | `baseDelay = 500`, `jitter = false` | Constant delay intervals between retries. |
| `"custom"` | `fn(attempt, error, job) => number \| Promise<number>` | N/A (`fn` required) | Dynamic retry policies (e.g. HTTP `Retry-After` headers, rate-limit reset times). |

### Jitter and Bounds

To mitigate "thundering herd" spikes against downstream databases or external APIs, jitter can be enabled with `jitter: true`:

- **Exponential Jitter (AWS Equal Jitter)**:
  - Delay formula: `delay = Math.round(capped / 2 + Math.random() * (capped / 2))`
  - Where `capped = Math.min(maxDelay, baseDelay * Math.pow(2, attempt))`.
  - Guarantees delays are uniformly distributed within `[capped / 2, capped]`, ensuring deterministic lower bounds while diffusing concurrent retries.
- **Fixed Jitter (±20% Jitter)**:
  - Delay formula: `delay = Math.round(baseDelay * (0.8 + Math.random() * 0.4))`
  - Guarantees delays are uniformly distributed within `[0.8 * baseDelay, 1.2 * baseDelay]`.

When `jitter: false` (or omitted), retries are strictly deterministic to guarantee full backward compatibility for existing applications.

### Three-Level Configuration Precedence

Backoff configurations follow a 3-tier hierarchical resolution order:

$$\text{Per-Job Configuration} \succ \text{Worker-Level Configuration} \succ \text{Queue-Level Defaults}$$

1. **Per-Job (`queue.enqueue(..., { backoff: { ... } })`)**: Highest precedence. Individual parameters override worker and queue defaults.
2. **Worker-Level (`queue.process(handler, { backoff: { ... } })`)**: Applies to all jobs executed by that specific worker instance.
3. **Queue-Level (`new ResilientQueue({ backoff: { ... } })`)**: Global fallback defaults.

#### Resolution Rules
- Specific parameters (`strategy`, `baseDelay`, `maxDelay`, `jitter`, `fn`, `maxRetries`) are merged hierarchically across tiers.
- A flat property (e.g. `baseDelay: 1000`) or nested property within `backoff: { baseDelay: 1000 }` can be provided interchangeably.
- If `strategy` is omitted, it defaults to `"exponential"`.
- If `maxDelay` is omitted, it defaults to `60000ms`.
- If `baseDelay` is omitted, it defaults to `500ms`.
- If `maxRetries` is omitted, it defaults to `3`.

### Validation and DLQ Protection

The engine includes rigorous compile-time and runtime validation to prevent invalid retry delays from corrupting the Redis delayed sorted set:

- **Static Validation**: Strategies must be one of `"exponential"`, `"fixed"`, or `"custom"`. Delays must be non-negative numbers, `maxDelay` must be positive, `jitter` must be boolean, and `fn` must be a function.
- **Runtime Custom Function Protection**: If a custom backoff callback throws an error, rejects a promise, returns `NaN`, returns a negative number, or returns a non-number, the error is safely caught and the job is immediately routed to the Dead-Letter Queue (`<keyPrefix>:dlq`) with diagnostic error metadata, emitting the `dlq` and `failed` events. It will never silently crash or schedule invalid timestamps.

---

## TypeScript Types & Interfaces

The package includes TypeScript definitions (`index.d.ts`).

### `Job<T>`

Represents a job payload and execution metadata.

```typescript
export interface Job<T = any> {
  id: string;
  data: T;
  attempt: number;
  idempotencyKey: string | null;
  ttlSeconds: number | null;
  createdAt: number;
  lastError?: string;
  retriedAt?: number;
  stalledCount?: number;
  lastStalledAt?: number;
  signal?: AbortSignal;
  isLeaseLost?: boolean;
  isTimedOut?: boolean;
  jobTimeout?: number;
  lockToken?: string;
  delay?: number;
  runAt?: number;
  backoff?: BackoffOptions<T> | BackoffStrategy | CustomBackoffFn<T>;
  maxRetries?: number;
  baseDelay?: number;
  maxDelay?: number;
  strategy?: BackoffStrategy;
  jitter?: boolean;
}
```

| Field | Type | Description |
| :--- | :--- | :--- |
| `id` | `string` | Unique UUID v4 identifying the job. |
| `data` | `T` | User-provided job payload. |
| `attempt` | `number` | Number of retry attempts executed so far (`0` on initial try). |
| `idempotencyKey` | `string \| null` | Idempotency key supplied during enqueue, or `null`. |
| `ttlSeconds` | `number \| null` | Idempotency key TTL in seconds, or `null`. |
| `createdAt` | `number` | Unix timestamp in milliseconds when the job was enqueued. |
| `lastError` | `string \| undefined` | Error message from the previous failed attempt (available on retries). |
| `retriedAt` | `number \| undefined` | Unix timestamp in milliseconds of the most recent retry attempt. |
| `stalledCount` | `number \| undefined` | Number of times this job stalled and was recovered. |
| `lastStalledAt` | `number \| undefined` | Unix timestamp in milliseconds when most recently recovered from stalled state. |
| `signal` | `AbortSignal \| undefined` | Standard `AbortSignal` triggered if the job lease is lost or execution times out. |
| `isLeaseLost` | `boolean \| undefined` | Boolean indicating whether this worker's visibility lease has expired or was revoked. |
| `isTimedOut` | `boolean \| undefined` | Boolean indicating whether this job has timed out. |
| `jobTimeout` | `number \| undefined` | Configured execution timeout in milliseconds for this job. |
| `lockToken` | `string \| undefined` | Per-dequeue fencing token identifying this execution lease. |
| `delay` | `number \| undefined` | Optional delay in milliseconds configured during enqueue. |
| `runAt` | `number \| undefined` | Scheduled execution timestamp in milliseconds. |
| `backoff` | `BackoffOptions<T> \| BackoffStrategy \| CustomBackoffFn<T> \| undefined` | Configured retry backoff options or strategy for this job. |
| `maxRetries` | `number \| undefined` | Maximum retry attempts for this job, overriding worker and queue defaults. |
| `baseDelay` | `number \| undefined` | Base delay in milliseconds for this job, overriding worker and queue defaults. |
| `maxDelay` | `number \| undefined` | Maximum delay cap in milliseconds for this job, overriding worker and queue defaults. |
| `strategy` | `BackoffStrategy \| undefined` | Backoff strategy for this job, overriding worker and queue defaults. |
| `jitter` | `boolean \| undefined` | Jitter flag for this job, overriding worker and queue defaults. |

---

### `BackoffStrategy`

Supported backoff strategies.

```typescript
export type BackoffStrategy = "exponential" | "fixed" | "custom";
```

---

### `CustomBackoffFn<T>`

Custom backoff callback signature. Receives the retry attempt number, error that caused failure, and job descriptor. Can return a delay in milliseconds synchronously or asynchronously.

```typescript
export type CustomBackoffFn<T = any> = (
  attempt: number,
  error: Error,
  job: Job<T>
) => number | Promise<number>;
```

---

### `BackoffOptions<T>`

Configuration options for the retry backoff engine.

```typescript
export interface BackoffOptions<T = any> {
  strategy?: BackoffStrategy; // default: "exponential"
  baseDelay?: number;         // default: 500
  maxDelay?: number;          // default: 60000
  jitter?: boolean;           // default: false
  fn?: CustomBackoffFn<T>;
  maxRetries?: number;        // default: 3
}
```

| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `strategy` | `BackoffStrategy` | `"exponential"` | Backoff strategy (`"exponential"`, `"fixed"`, or `"custom"`). |
| `baseDelay` | `number` | `500` | Base delay in milliseconds used by backoff strategies. |
| `maxDelay` | `number` | `60000` | Maximum retry delay cap in milliseconds for exponential backoff. |
| `jitter` | `boolean` | `false` | Enables random jitter (AWS Equal Jitter for exponential; ±20% for fixed). |
| `fn` | `CustomBackoffFn<T>` | `undefined` | Custom backoff callback function when strategy is `"custom"`. |
| `maxRetries` | `number` | `3` | Maximum number of retry attempts before routing to the DLQ. |

---

### `ResilientQueueEvents<T>`

Event listener signatures supported by `ResilientQueue`.

```typescript
export interface ResilientQueueEvents<T = any> {
  completed: (job: Job<T>, result: any) => void;
  failed: (job: Job<T>, error: Error) => void;
  retrying: (job: Job<T>, error: Error, attempt: number, delayMs: number) => void;
  dlq: (job: DLQJob<T> | Job<T>, reason: Error | string) => void;
  stalled: (job: Job<T>) => void;
  error: (error: Error) => void;
  paused: () => void;
  resumed: () => void;
}
```

| Event | Signature | Description |
| :--- | :--- | :--- |
| `completed` | `(job: Job<T>, result: any) => void` | Emitted when a job handler completes successfully. |
| `failed` | `(job: Job<T>, error: Error) => void` | Emitted when a job execution attempt fails (includes retries, DLQ, and timeouts). |
| `retrying` | `(job: Job<T>, error: Error, attempt: number, delayMs: number) => void` | Emitted when a failed job is scheduled for retry in the delayed queue. |
| `dlq` | `(job: DLQJob<T> \| Job<T>, reason: Error \| string) => void` | Emitted when a job is permanently moved to the Dead-Letter Queue. |
| `stalled` | `(job: Job<T>) => void` | Emitted when an abandoned stalled job is recovered to the main queue. |
| `error` | `(error: Error) => void` | Emitted when an internal error occurs or a listener throws. |
| `paused` | `() => void` | Emitted when queue job consumption is paused. |
| `resumed` | `() => void` | Emitted when queue job consumption is resumed. |

---

### `QueueMetrics`

Real-time metric counts returned by `queue.getMetrics()`.

```typescript
export interface QueueMetrics {
  waiting: number;
  active: number;
  delayed: number;
  dlq: number;
}
```

| Field | Type | Description |
| :--- | :--- | :--- |
| `waiting` | `number` | Number of pending jobs waiting in the main FIFO queue (`<keyPrefix>:main`). |
| `active` | `number` | Number of in-flight jobs currently being processed by workers (`<keyPrefix>:active`). |
| `delayed` | `number` | Number of scheduled jobs and pending retries in the delayed sorted set (`<keyPrefix>:delayed`). |
| `dlq` | `number` | Number of failed jobs stored in the Dead-Letter Queue (`<keyPrefix>:dlq`). |

---

### `DLQJob<T>`

Represents a failed job stored in the Dead-Letter Queue. Extends `Job<T>`.

```typescript
export interface DLQJob<T = any> extends Job<T> {
  failedAt?: number;
  failureReason?: string;
  corrupted?: boolean;
  raw?: string;
  error?: string;
}
```

| Field | Type | Description |
| :--- | :--- | :--- |
| `failedAt` | `number \| undefined` | Unix timestamp in milliseconds when the job failed and was moved to DLQ. |
| `failureReason` | `string \| undefined` | Failure error message or diagnostic reason. |
| `corrupted` | `boolean \| undefined` | Flag set to `true` if this DLQ entry was malformed JSON in Redis. |
| `raw` | `string \| undefined` | Pristine unparsed string if the entry could not be parsed as JSON. |
| `error` | `string \| undefined` | Parse error message if corrupted. |

---

### `DLQOptions`

Options for querying the Dead-Letter Queue.

```typescript
export interface DLQOptions {
  start?: number;
  limit?: number;
  order?: "asc" | "desc";
}
```

| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `start` | `number` | `0` | Zero-based offset index to start retrieving jobs from. |
| `limit` | `number` | `50` | Maximum number of jobs to retrieve. |
| `order` | `"asc" \| "desc"` | `"asc"` | Sort order: `"asc"` (oldest first, FIFO) or `"desc"` (newest first, LIFO). |

---

### `DLQResult<T>`

Object returned by `queue.getDLQ()`.

```typescript
export interface DLQResult<T = any> {
  total: number;
  jobs: DLQJob<T>[];
}
```

---

### `RetryAllDLQOptions`

Options for batch requeuing DLQ jobs.

```typescript
export interface RetryAllDLQOptions {
  limit?: number;
}
```

| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `limit` | `number` | All | Maximum number of failed jobs to requeue in this batch. |

---

### `QueueOptions`

Options for configuring `ResilientQueue`.

```typescript
export interface QueueOptions {
  redisUrl?: string;
  redis?:
    | string
    | RedisOptions
    | Redis
    | Cluster
    | ClusterConfig
    | RedisConnectionPair;
  cluster?: ClusterNode[] | ClusterConfig;
  clusterNodes?: ClusterNode[];
  clusterOptions?: ClusterOptions;
  client?: Redis | Cluster;
  producer?: Redis | Cluster | RedisOptions | string;
  consumer?: Redis | Cluster | RedisOptions | string;
  maxRetries?: number;      // default: 3
  baseDelay?: number;       // default: 500
  maxDelay?: number;        // default: 60000
  strategy?: BackoffStrategy; // default: "exponential"
  jitter?: boolean;         // default: false
  backoff?: BackoffOptions | BackoffStrategy | CustomBackoffFn;
  keyPrefix?: string;       // default: "rq"
  stalledInterval?: number; // default: 15000
  lockDuration?: number;    // default: 30000
  maxStalledCount?: number; // default: 1
  jobTimeout?: number;      // default: undefined (disabled)
}
```

---

### `EnqueueOptions`

```typescript
export interface EnqueueOptions {
  idempotencyKey?: string | null;
  ttlSeconds?: number | null;
  delay?: number;
  runAt?: Date | number;
  jobTimeout?: number;
  backoff?: BackoffOptions | BackoffStrategy | CustomBackoffFn;
  maxRetries?: number;
  baseDelay?: number;
  maxDelay?: number;
  strategy?: BackoffStrategy;
  jitter?: boolean;
}
```

---

### `EnqueueManyItem<T>`

```typescript
export interface EnqueueManyItem<T = any> extends EnqueueOptions {
  data: T;
  options?: EnqueueOptions;
  opts?: EnqueueOptions;
}
```

---

### `ProcessOptions`

```typescript
export interface ProcessOptions {
  concurrency?: number; // default: 1
  jobTimeout?: number;
  backoff?: BackoffOptions | BackoffStrategy | CustomBackoffFn;
  maxRetries?: number;
  baseDelay?: number;
  maxDelay?: number;
  strategy?: BackoffStrategy;
  jitter?: boolean;
}
```

---

### `JobHandler<T>`

```typescript
export type JobHandler<T = any> = (job: Job<T>) => Promise<void> | void;
```

---

### `RedisConnectionPair`

```typescript
export interface RedisConnectionPair {
  producer: Redis | Cluster;
  consumer?: Redis | Cluster;
}
```

---

### `ClusterConfig`

```typescript
export interface ClusterConfig {
  nodes: ClusterNode[];
  options?: ClusterOptions;
}
```

---

## Redis Data Structures

All Redis keys are namespaced with the configured `keyPrefix` (default: `"rq"`):

| Redis Key | Redis Type | Description |
| :--- | :--- | :--- |
| `<keyPrefix>:main` | `List` | FIFO queue holding pending jobs awaiting consumption. |
| `<keyPrefix>:active` | `List` | In-flight jobs currently being processed by workers. |
| `<keyPrefix>:locks` | `Sorted Set` | Active job visibility locks. Member: `jobId`, Score: lease expiration timestamp (ms). |
| `<keyPrefix>:lock_tokens` | `Hash` | Fencing tokens for active jobs. Field: `jobId`, Value: `lockToken` (UUID). |
| `<keyPrefix>:stalled_meta` | `Hash` | External recovery metadata. Field: `jobId`, Value: JSON metadata (`stalledCount`, `attempt`, `failureReason`). |
| `<keyPrefix>:delayed` | `Sorted Set` | Scheduled retries. Member: serialized retry job, Score: execution timestamp (ms). |
| `<keyPrefix>:dlq` | `List` | Dead-Letter Queue storing permanently failed jobs with diagnostics. |
| `<keyPrefix>:idempotency:<key>` | `String` | Idempotency key mapping to registered job ID with optional TTL. |
| `<keyPrefix>:paused` | `String` | Shared pause flag (`"1"`) coordinating pause state across distributed workers. |
