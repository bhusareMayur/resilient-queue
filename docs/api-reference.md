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
- [Error Classes](#error-classes)
  - [RetryableError](#retryableerror)
  - [FatalError](#fatalerror)
- [TypeScript Types & Interfaces](#typescript-types-interfaces)
  - [Job<T>](#jobt)
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

The package exports three main constructs:

```javascript
const {
  ResilientQueue,
  RetryableError,
  FatalError
} = require("@mayurbhusare/resilient-queue");
```

---

## Class: ResilientQueue

The primary entry point for managing job queues, enqueuing tasks, and registering workers.

### `constructor(options)`

Initializes a new `ResilientQueue` instance.

#### Parameters

- `options` (`QueueOptions`): Configuration options object. See [QueueOptions](#queueoptions) for all fields.

#### Throws

- Throws an `Error` (`"redisUrl or redis options are required to initialize ResilientQueue"`) if no Redis connection configuration is provided.

#### Example

```javascript
const queue = new ResilientQueue({
  redisUrl: "redis://127.0.0.1:6379",
  maxRetries: 3,
  baseDelay: 500,
  keyPrefix: "rq"
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

#### Returns

- `Promise<string>`: Resolves to the unique UUID v4 generated for the job. If `idempotencyKey` was supplied and is already registered in Redis, resolves to the existing job ID without enqueuing a duplicate job.

#### Throws

- Throws an `Error` (`"ttlSeconds must be an integer"`) if `ttlSeconds` is provided and is not an integer.
- Throws an `Error` (`"delay must be a non-negative number"`) if `delay` is negative or not a finite number.
- Throws an `Error` (`"runAt must be a valid Date or timestamp"`) if `runAt` is invalid.
- Throws an `Error` (`"Cannot specify both delay and runAt"`) if both options are provided.
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

- `handler` (`JobHandler<T>`): An asynchronous or synchronous function invoked for each dequeued job. Receives a `job` object.
- `options` (`ProcessOptions`, optional):
  - `concurrency` (`number`, optional): Positive integer specifying how many jobs can run concurrently in this worker instance. Default: `1`.

#### Returns

- `Promise<void>`: Resolves immediately after worker initialization.

#### Throws

- Throws an `Error` (`"A handler function must be provided to process()"`) if `handler` is not a function.
- Throws an `Error` (`"Concurrency must be a positive integer"`) if `concurrency` is not a positive integer.
- Throws an `Error` (`"Queue is already processing"`) if `process()` was already called on this queue instance.

#### Example

```javascript
queue.process(async (job) => {
  console.log(`Processing job ${job.id}:`, job.data);
  // Perform work
}, { concurrency: 4 });
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
  lockToken?: string;
  delay?: number;
  runAt?: number;
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
| `signal` | `AbortSignal \| undefined` | Standard `AbortSignal` triggered if the job lease is lost or recovered by another worker. |
| `isLeaseLost` | `boolean \| undefined` | Boolean indicating whether this worker's visibility lease has expired or was revoked. |
| `lockToken` | `string \| undefined` | Per-dequeue fencing token identifying this execution lease. |
| `delay` | `number \| undefined` | Optional delay in milliseconds configured during enqueue. |
| `runAt` | `number \| undefined` | Scheduled execution timestamp in milliseconds. |

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
  keyPrefix?: string;       // default: "rq"
  stalledInterval?: number; // default: 15000
  lockDuration?: number;    // default: 30000
  maxStalledCount?: number; // default: 1
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
