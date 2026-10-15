# Code Examples

This document provides copy-pasteable, verified code examples demonstrating practical usage patterns for `@mayurbhusare/resilient-queue`.

---

## Table of Contents

- [1. Basic Producer and Consumer](#1-basic-producer-and-consumer)
- [2. Controlled In-Memory Concurrency](#2-controlled-in-memory-concurrency)
- [3. Idempotent Job Enqueuing with TTL](#3-idempotent-job-enqueuing-with-ttl)
- [4. Handling Transient Failures with RetryableError](#4-handling-transient-failures-with-retryableerror)
- [5. Handling Unrecoverable Errors with FatalError](#5-handling-unrecoverable-errors-with-fatalerror)
- [6. AbortSignal and Lease Loss Protection](#6-abortsignal-and-lease-loss-protection)
- [7. Reusing an Existing Redis Client Instance](#7-reusing-an-existing-redis-client-instance)
- [8. Redis Cluster Setup with Hash Tags](#8-redis-cluster-setup-with-hash-tags)
- [9. Production-Grade Graceful Shutdown](#9-production-grade-graceful-shutdown)
- [10. Inspecting and Reprocessing the DLQ](#10-inspecting-and-reprocessing-the-dlq)

---

## 1. Basic Producer and Consumer

A minimal queue setup enqueuing a job and processing it with a worker.

```javascript
const { ResilientQueue } = require("@mayurbhusare/resilient-queue");

async function main() {
  const queue = new ResilientQueue({
    redisUrl: "redis://127.0.0.1:6379",
    keyPrefix: "basic-app"
  });

  // Start worker
  queue.process(async (job) => {
    console.log(`[Worker] Processing job ${job.id}:`, job.data);
  });

  // Enqueue a job
  const jobId = await queue.enqueue({
    userId: 42,
    action: "send_welcome_email"
  });

  console.log(`[Producer] Enqueued job ${jobId}`);
}

main().catch(console.error);
```

---

## 2. Controlled In-Memory Concurrency

Process multiple jobs simultaneously without opening a separate Redis socket per worker.

```javascript
const { ResilientQueue } = require("@mayurbhusare/resilient-queue");

const queue = new ResilientQueue({
  redisUrl: "redis://127.0.0.1:6379",
  keyPrefix: "concurrent-app"
});

// Process up to 5 jobs concurrently using a single Redis consumer connection
queue.process(async (job) => {
  console.log(`[Start] Processing item ${job.data.index}`);
  await new Promise((resolve) => setTimeout(resolve, 1000));
  console.log(`[Finish] Completed item ${job.data.index}`);
}, { concurrency: 5 });

async function enqueueBatch() {
  for (let i = 1; i <= 10; i++) {
    await queue.enqueue({ index: i });
  }
}

enqueueBatch().catch(console.error);
```

---

## 3. Idempotent Job Enqueuing with TTL

Deduplicate requests so that identical operations (such as processing an invoice or payment) are executed at most once.

```javascript
const { ResilientQueue } = require("@mayurbhusare/resilient-queue");

const queue = new ResilientQueue({
  redisUrl: "redis://127.0.0.1:6379",
  keyPrefix: "billing"
});

async function processPayment(payment) {
  // Enqueue with a deterministic idempotency key and 1-hour TTL
  const jobId = await queue.enqueue(
    { invoiceId: payment.invoiceId, amount: payment.amount },
    {
      idempotencyKey: `invoice:${payment.invoiceId}`,
      ttlSeconds: 3600
    }
  );

  console.log(`Payment registered with job ID: ${jobId}`);
  return jobId;
}

async function testDeduplication() {
  const payment = { invoiceId: "INV-1001", amount: 149.99 };

  // First call creates the job
  const id1 = await processPayment(payment);

  // Second call with same key immediately returns existing job ID without duplicate enqueue
  const id2 = await processPayment(payment);

  console.log("Returned identical job ID:", id1 === id2); // true
}

testDeduplication().catch(console.error);
```

---

## 4. Handling Transient Failures with RetryableError

Use `RetryableError` for intermittent network failures, rate limits, or downstream outages.

```javascript
const {
  ResilientQueue,
  RetryableError
} = require("@mayurbhusare/resilient-queue");

const queue = new ResilientQueue({
  redisUrl: "redis://127.0.0.1:6379",
  maxRetries: 3,
  baseDelay: 500, // Retries at 1s, 2s, 4s
  keyPrefix: "api-sync"
});

queue.process(async (job) => {
  console.log(`Attempt ${job.attempt} for job ${job.id}`);

  const res = await fetch(`https://api.external.com/records/${job.data.recordId}`);

  if (res.status === 429 || res.status >= 500) {
    throw new RetryableError(`API returned status ${res.status}`);
  }

  const data = await res.json();
  console.log("Record synced successfully:", data);
});
```

---

## 5. Handling Unrecoverable Errors with FatalError

Use `FatalError` to bypass retries and route permanently invalid jobs directly to the DLQ.

```javascript
const {
  ResilientQueue,
  FatalError,
  RetryableError
} = require("@mayurbhusare/resilient-queue");

const queue = new ResilientQueue({
  redisUrl: "redis://127.0.0.1:6379",
  maxRetries: 3
});

queue.process(async (job) => {
  // Validate schema
  if (!job.data.email || typeof job.data.email !== "string") {
    // Immediately moves to DLQ without retrying
    throw new FatalError("Invalid or missing email address in payload");
  }

  try {
    await sendEmail(job.data.email, job.data.body);
  } catch (err) {
    // Retry temporary SMTP connection issues
    throw new RetryableError(`SMTP connection error: ${err.message}`);
  }
});
```

---

## 6. AbortSignal and Lease Loss Protection

Pass `job.signal` to abort-aware APIs so in-flight work is halted if another worker takes over the lease.

```javascript
const { ResilientQueue } = require("@mayurbhusare/resilient-queue");

const queue = new ResilientQueue({
  redisUrl: "redis://127.0.0.1:6379",
  lockDuration: 10000 // 10s lease
});

queue.process(async (job) => {
  // Pass job.signal to fetch so the HTTP request cancels if lease is lost
  const response = await fetch("https://slow-service.internal/process", {
    method: "POST",
    body: JSON.stringify(job.data),
    signal: job.signal
  });

  // Verify lease status before performing irreversible external mutations
  if (job.isLeaseLost) {
    console.warn(`Job ${job.id} lease expired during execution. Discarding local result.`);
    return;
  }

  await saveResultToDatabase(await response.json());
});
```

---

## 7. Reusing an Existing Redis Client Instance

Share an existing `ioredis` client with your application. The queue uses your client for producer operations and duplicates it for consumer operations, leaving your client open on shutdown.

```javascript
const Redis = require("ioredis");
const { ResilientQueue } = require("@mayurbhusare/resilient-queue");

const appRedis = new Redis("redis://127.0.0.1:6379");

const queue = new ResilientQueue({
  redis: appRedis,
  keyPrefix: "shared-client"
});

async function main() {
  await queue.enqueue({ task: "send-receipt" });

  // When closing the queue:
  await queue.close();

  // appRedis remains open and functional
  const pong = await appRedis.ping();
  console.log("External client is still active:", pong); // "PONG"

  // Clean up external client when application terminates
  await appRedis.quit();
}

main().catch(console.error);
```

---

## 8. Redis Cluster Setup with Hash Tags

When deploying on Redis Cluster, you must enclose `keyPrefix` in curly braces `{...}` to pin all multi-key operations and Lua scripts to a single hash slot.

```javascript
const { ResilientQueue } = require("@mayurbhusare/resilient-queue");

const queue = new ResilientQueue({
  cluster: [
    { host: "cluster-node-1.internal", port: 7000 },
    { host: "cluster-node-2.internal", port: 7000 },
    { host: "cluster-node-3.internal", port: 7000 }
  ],
  clusterOptions: {
    scaleReads: "slave"
  },
  // Hash tag {payments} ensures all keys map to the same cluster shard
  keyPrefix: "{payments}"
});

queue.process(async (job) => {
  console.log("Cluster worker processing job:", job.id);
});
```

---

## 9. Production-Grade Graceful Shutdown

Handle system signals (`SIGTERM`, `SIGINT`) cleanly during deployments in Kubernetes, PM2, or Docker.

```javascript
const { ResilientQueue } = require("@mayurbhusare/resilient-queue");

const queue = new ResilientQueue({
  redisUrl: process.env.REDIS_URL || "redis://127.0.0.1:6379"
});

queue.process(async (job) => {
  console.log(`[Worker] Started ${job.id}`);
  await new Promise((resolve) => setTimeout(resolve, 2000));
  console.log(`[Worker] Finished ${job.id}`);
}, { concurrency: 4 });

let isShuttingDown = false;

async function handleShutdown(signal) {
  if (isShuttingDown) return;
  isShuttingDown = true;

  console.log(`Received ${signal}. Shutting down gracefully...`);

  try {
    // Stops fetching, waits for active in-flight jobs, and closes connections
    await queue.close();
    console.log("Queue shut down cleanly. Exiting.");
    process.exit(0);
  } catch (error) {
    console.error("Error during graceful shutdown:", error);
    process.exit(1);
  }
}

process.on("SIGTERM", () => handleShutdown("SIGTERM"));
process.on("SIGINT", () => handleShutdown("SIGINT"));
```

---

## 10. Inspecting and Reprocessing the DLQ

View failed jobs in `rq:dlq` and selectively re-enqueue them after resolving an incident.

```javascript
const Redis = require("ioredis");
const { ResilientQueue } = require("@mayurbhusare/resilient-queue");

const redis = new Redis("redis://127.0.0.1:6379");
const queue = new ResilientQueue({ redisUrl: "redis://127.0.0.1:6379" });

async function inspectAndReprocessDLQ() {
  const count = await redis.llen("rq:dlq");
  console.log(`Jobs currently in DLQ: ${count}`);

  // Fetch first 5 failed jobs
  const failedItems = await redis.lrange("rq:dlq", 0, 4);

  for (const item of failedItems) {
    const failedJob = JSON.parse(item);
    console.log(`- Job ${failedJob.id}: failed after ${failedJob.attempt} attempts.`);
    console.log(`  Reason: ${failedJob.failureReason}`);
    console.log(`  Payload:`, failedJob.data);
  }

  // To reprocess: pop a job from DLQ and re-enqueue it
  const popped = await redis.rpop("rq:dlq");
  if (popped) {
    const jobToRetry = JSON.parse(popped);
    const newJobId = await queue.enqueue(jobToRetry.data, {
      idempotencyKey: jobToRetry.idempotencyKey,
      ttlSeconds: jobToRetry.ttlSeconds
    });
    console.log(`Re-enqueued DLQ job as ${newJobId}`);
  }

  await queue.close();
  await redis.quit();
}

inspectAndReprocessDLQ().catch(console.error);
```
