# Getting Started

`@mayurbhusare/resilient-queue` is a lightweight, production-grade, Redis-backed job queue for Node.js. It provides crash-durable in-flight job recovery, persistent exponential retries, Dead-Letter Queue (DLQ) routing, atomic enqueue-time idempotency, and in-memory concurrency without framework bloat.

---

## Prerequisites

- **Node.js**: Node.js 18 or later.
- **Redis Server**: Redis 5.0 or later (Redis 6.2 or later is recommended for native `BLMOVE` support; Redis 5.x uses `BRPOPLPUSH` fallback).

---

## Installation

Install via npm:

```bash
npm install @mayurbhusare/resilient-queue
```

Or using yarn or pnpm:

```bash
# yarn
yarn add @mayurbhusare/resilient-queue

# pnpm
pnpm add @mayurbhusare/resilient-queue
```

The library has two runtime dependencies: [`ioredis`](https://www.npmjs.com/package/ioredis) and [`uuid`](https://www.npmjs.com/package/uuid).

---

## Core Concepts

Before writing code, consider the four primary Redis structures managed by the queue:

1. **Main Queue (`rq:main`)**: A Redis List holding jobs waiting to be dequeued by workers.
2. **Active Queue (`rq:active`)**: A Redis List holding jobs currently in-flight. Jobs are moved atomically from `rq:main` to `rq:active` via `BLMOVE` (or `BRPOPLPUSH`), ensuring no job is dropped on crash.
3. **Delayed Set (`rq:delayed`)**: A Redis Sorted Set where retried jobs wait until their backoff period elapses.
4. **Dead-Letter Queue (`rq:dlq`)**: A Redis List storing failed jobs that exceeded retry limits, threw a `FatalError`, or repeatedly crashed workers.

---

## Basic Usage

### 1. Initialize the Queue

Create a queue instance by providing connection details. You can pass a Redis URL or an options object:

```javascript
const { ResilientQueue } = require("@mayurbhusare/resilient-queue");

const queue = new ResilientQueue({
  redisUrl: "redis://127.0.0.1:6379",
  maxRetries: 3,
  baseDelay: 500, // 500ms base delay for exponential backoff
  keyPrefix: "rq"  // Prefix for Redis keys
});
```

For advanced connection options (such as TLS, Sentinels, or Redis Cluster), see the [Configuration Guide](./configuration.md).

---

### 2. Enqueue Jobs (Producer)

Enqueue arbitrary JSON-serializable payloads using `queue.enqueue()`. The method returns a Promise resolving to the unique UUID v4 generated for the job:

```javascript
async function addJob() {
  const jobId = await queue.enqueue({
    email: "user@example.com",
    template: "welcome-email",
    userId: 1042
  });

  console.log(`Enqueued job: ${jobId}`);
}

addJob();
```

To prevent duplicate job creation across distributed producers, supply an `idempotencyKey`:

```javascript
await queue.enqueue(
  { invoiceId: "INV-2026-001", amount: 49.99 },
  {
    idempotencyKey: "invoice:INV-2026-001",
    ttlSeconds: 86400 // Key expires after 24 hours
  }
);
```

If a job with the same `idempotencyKey` has already been enqueued, Redis atomically returns the existing job ID without pushing a duplicate to the queue.

---

### 3. Process Jobs (Consumer)

Register an asynchronous worker function using `queue.process(handler, options)`:

```javascript
const {
  ResilientQueue,
  RetryableError,
  FatalError
} = require("@mayurbhusare/resilient-queue");

queue.process(async (job) => {
  console.log(`Processing job ${job.id} (attempt ${job.attempt}):`, job.data);

  if (job.data.shouldFailTemporarily) {
    // Transient failure: triggers exponential retry
    throw new RetryableError("Downstream service timed out");
  }

  if (job.data.isCorrupted) {
    // Unrecoverable error: moves directly to DLQ
    throw new FatalError("Invalid schema in payload");
  }

  // Completing the function successfully acknowledges and finishes the job
  console.log(`Job ${job.id} finished successfully`);
}, { concurrency: 2 });
```

---

### 4. Graceful Shutdown

In production environments, services receive `SIGTERM` or `SIGINT` signals during deploys or restarts. Call `queue.close()` to stop fetching new jobs, await in-flight tasks, and close queue-owned Redis connections cleanly:

```javascript
async function shutdown() {
  console.log("Shutting down worker gracefully...");
  await queue.close();
  console.log("Worker closed.");
  process.exit(0);
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
```

---

## Complete Working Example

Below is a self-contained script demonstrating an end-to-end producer and consumer workflow:

```javascript
const {
  ResilientQueue,
  RetryableError,
  FatalError
} = require("@mayurbhusare/resilient-queue");

async function main() {
  const queue = new ResilientQueue({
    redisUrl: "redis://127.0.0.1:6379",
    maxRetries: 2,
    baseDelay: 200,
    keyPrefix: "demo"
  });

  // Start consumer
  await queue.process(async (job) => {
    console.log(`[Worker] Received job ${job.id}:`, job.data);

    if (job.data.action === "transient-failure" && job.attempt === 0) {
      console.log(`[Worker] Simulating temporary failure for job ${job.id}`);
      throw new RetryableError("Service temporarily unavailable");
    }

    if (job.data.action === "fatal-failure") {
      console.log(`[Worker] Simulating fatal error for job ${job.id}`);
      throw new FatalError("Permanent schema validation failure");
    }

    console.log(`[Worker] Successfully completed job ${job.id}`);
  }, { concurrency: 1 });

  // Enqueue sample jobs
  const job1 = await queue.enqueue({ action: "send-notification", user: "Alice" });
  const job2 = await queue.enqueue({ action: "transient-failure", user: "Bob" });
  const job3 = await queue.enqueue({ action: "fatal-failure", user: "Charlie" });

  console.log("Enqueued sample jobs:", { job1, job2, job3 });

  // Wait for processing to complete, then shut down
  setTimeout(async () => {
    await queue.close();
    console.log("Demo completed and queue closed.");
  }, 3000);
}

main().catch(console.error);
```

---

## Next Steps

- **[Configuration Guide](./configuration.md)**: Explore Redis connections, TLS, Sentinel, Cluster setup, and operational queue parameters.
- **[API Reference](./api-reference.md)**: Detailed reference for all classes, methods, error types, and TypeScript definitions.
- **[Architecture & Internals](./architecture.md)**: In-depth design of the dual-connection model, crash recovery, and Lua scripts.
- **[Error Handling Guide](./error-handling.md)**: Detailed guide on `RetryableError`, `FatalError`, DLQ diagnostics, and lease signals.
- **[Code Examples](./examples.md)**: Practical patterns for idempotency, concurrency, custom connections, and DLQ management.
