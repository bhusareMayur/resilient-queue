# Error Handling Guide

This guide details the error handling model of `@mayurbhusare/resilient-queue`, explaining how transient failures, fatal errors, worker crashes, and lease expirations are handled and routed.

---

## Table of Contents

- [Error Handling Philosophy](#error-handling-philosophy)
- [Error Classification](#error-classification)
  - [RetryableError (Transient Failures)](#retryableerror-transient-failures)
  - [FatalError (Permanent Failures)](#fatalerror-permanent-failures)
  - [Standard Unhandled Errors](#standard-unhandled-errors)
- [Exponential Backoff Retries](#exponential-backoff-retries)
  - [Delay Calculation](#delay-calculation)
  - [Backoff Progression Table](#backoff-progression-table)
  - [Delayed Retry Durability](#delayed-retry-durability)
- [Crash Durability & Stalled Jobs](#crash-durability-stalled-jobs)
  - [Detection via Visibility Heartbeats](#detection-via-visibility-heartbeats)
  - [Poison Pill Protection (maxStalledCount)](#poison-pill-protection-maxstalledcount)
- [Lease Loss & Fencing Tokens](#lease-loss-fencing-tokens)
  - [Fencing Tokens](#fencing-tokens)
  - [Using job.signal and job.isLeaseLost](#using-jobsignal-and-jobisleaselost)
- [Dead-Letter Queue (DLQ) Management](#dead-letter-queue-dlq-management)
  - [DLQ Entry Schema](#dlq-entry-schema)
  - [Inspecting the DLQ](#inspecting-the-dlq)
  - [Draining or Reprocessing DLQ Jobs](#draining-or-reprocessing-dlq-jobs)

---

## Error Handling Philosophy

In distributed queues, errors fall into two distinct categories:
1. **Transient Failures**: Intermittent network timeouts, HTTP 429 rate limits, and temporary service unavailability. These succeed when retried with backoff.
2. **Permanent Failures**: Malformed payloads, validation errors, 4xx client errors, or corrupted database records. Retrying these wastes compute and clogs queues.

`ResilientQueue` provides explicit error classes to distinguish between these cases.

---

## Error Classification

### RetryableError (Transient Failures)

Throw a `RetryableError` when an operation fails due to a temporary condition:

```javascript
const { RetryableError } = require("@mayurbhusare/resilient-queue");

queue.process(async (job) => {
  try {
    await externalPaymentGateway.charge(job.data);
  } catch (err) {
    if (err.code === "ECONNRESET" || err.status === 429) {
      throw new RetryableError(`Payment gateway temporary error: ${err.message}`);
    }
    throw err;
  }
});
```

When `RetryableError` is caught:
- `job.attempt` increments.
- If `job.attempt <= maxRetries`, the job is scheduled in `rq:delayed` with an exponential backoff delay.
- If `job.attempt > maxRetries`, the job moves to `rq:dlq`.

---

### FatalError (Permanent Failures)

Throw a `FatalError` when a failure is unrecoverable:

```javascript
const { FatalError } = require("@mayurbhusare/resilient-queue");

queue.process(async (job) => {
  if (!job.data.userId || !job.data.amount) {
    throw new FatalError("Missing mandatory userId or amount fields");
  }

  // Work proceeds only if valid
});
```

When `FatalError` is caught:
- All retries are bypassed.
- The job is moved immediately to `rq:dlq` on the current attempt.

---

### Standard Unhandled Errors

> [!IMPORTANT]
> **DLQ by Default**: If your handler throws an unhandled standard JavaScript error (e.g., `throw new Error("Something broke")`) that does **not** inherit from `RetryableError`, `ResilientQueue` treats it as permanent and routes the job directly to the DLQ on its very first attempt. Only errors explicitly inheriting from `RetryableError` undergo exponential retry.

```javascript
queue.process(async (job) => {
  // Any standard error routes directly to DLQ
  JSON.parse(job.data.malformedString);
});
```

---

## Exponential Backoff Retries

### Delay Calculation

When a `RetryableError` is thrown, the delay is calculated as:

$$\text{delay} = \text{baseDelay} \times 2^{\text{attempt}}$$

Where:
- `baseDelay`: Configured in queue options (default: `500` ms).
- `attempt`: Current retry attempt number (1 for first retry, 2 for second, etc.).

### Backoff Progression Table

| Attempt | Formula Multiplier | Delay (Default `baseDelay: 500ms`) | Delay (`baseDelay: 1000ms`) |
| :---: | :---: | :---: | :---: |
| 1 | $500 \times 2^1$ | **1,000 ms** (1.0 s) | **2,000 ms** (2.0 s) |
| 2 | $500 \times 2^2$ | **2,000 ms** (2.0 s) | **4,000 ms** (4.0 s) |
| 3 | $500 \times 2^3$ | **4,000 ms** (4.0 s) | **8,000 ms** (8.0 s) |
| 4 | $500 \times 2^4$ | **8,000 ms** (8.0 s) | **16,000 ms** (16.0 s) |

### Delayed Retry Durability

Retried jobs are not kept in volatile JavaScript memory timers. Instead, they are stored in a Redis Sorted Set (`<keyPrefix>:delayed`) with the score set to the exact millisecond execution timestamp (`Date.now() + delay`).

A promotion scheduler periodically checks `rq:delayed` and atomically transfers due jobs back into `rq:main`. If a worker process reboots or restarts, pending retries remain safe in Redis and resume immediately when workers start up.

---

## Crash Durability & Stalled Jobs

### Detection via Visibility Heartbeats

When a worker picks up a job via `BLMOVE`, the job remains in `rq:active` during execution:
1. The worker registers an active visibility lease in `rq:locks` expiring in `lockDuration` ms (default: `30000` ms).
2. A background heartbeat timer continually renews this lock every `Math.floor(lockDuration / 3)` ms.
3. If the worker process abruptly terminates (e.g., `SIGKILL`, OOM crash, unhandled rejection, power outage), the heartbeat ceases.
4. The stalled job recovery scheduler inspects `rq:locks`, identifies expired leases, and re-queues abandoned jobs back into `rq:main`.

### Poison Pill Protection (maxStalledCount)

A "poison pill" is a job whose payload causes the worker process to crash immediately (e.g. fatal native addon crash or out-of-memory error).

To prevent endless crash loops:
- The queue increments `stalledCount` each time a job is recovered.
- If `stalledCount > maxStalledCount` (default: `1`), the job is routed directly to `rq:dlq` with failure reason:
  `"Job stalled more than maxStalledCount times (1)"`.

---

## Lease Loss & Fencing Tokens

### Fencing Tokens

If a worker is delayed by a prolonged garbage collection pause or network latency, its visibility lease in Redis may expire. The recovery scheduler may reassign the job to another worker.

To prevent the paused worker from later releasing or renewing the new worker's lock, each dequeue assigns a unique `lockToken` (UUID). Renewal and release operations execute Lua scripts that verify the `lockToken` matches the token currently stored in Redis. Mismatched operations are rejected.

### Using job.signal and job.isLeaseLost

When a worker's lease is lost or superseded:
- `job.isLeaseLost` is set to `true`.
- `job.signal` (a standard Node.js `AbortSignal`) is triggered.

You can connect `job.signal` to abort-aware operations:

```javascript
queue.process(async (job) => {
  // Pass job.signal to fetch to abort network calls if lease is revoked
  const response = await fetch("https://api.example.com/process", {
    method: "POST",
    body: JSON.stringify(job.data),
    signal: job.signal
  });

  // Check before committing a side-effect
  if (job.isLeaseLost) {
    console.warn(`Lease was lost for job ${job.id}; aborting external commit`);
    return;
  }

  await database.commitTransaction();
});
```

---

## Dead-Letter Queue (DLQ) Management

### DLQ Entry Schema

Jobs in `rq:dlq` are serialized JSON objects containing the original payload plus diagnostics:

```json
{
  "id": "e0b9b32a-5b12-429a-9e11-cf9787e91d89",
  "data": {
    "orderId": "ORD-9912",
    "customer": "customer@example.com"
  },
  "attempt": 4,
  "idempotencyKey": "order:ORD-9912",
  "ttlSeconds": 86400,
  "createdAt": 1713000000000,
  "lastError": "Gateway timeout",
  "retriedAt": 1713000008000,
  "failedAt": 1713000016000,
  "failureReason": "Gateway timeout"
}
```

### Inspecting the DLQ

Inspect DLQ jobs directly using any Redis client:

```javascript
const Redis = require("ioredis");
const redis = new Redis("redis://127.0.0.1:6379");

async function viewDLQ() {
  const jobs = await redis.lrange("rq:dlq", 0, 9);
  for (const raw of jobs) {
    const job = JSON.parse(raw);
    console.log(`Failed Job ${job.id}:`);
    console.log(`  Reason: ${job.failureReason}`);
    console.log(`  Attempts: ${job.attempt}`);
    console.log(`  Payload:`, job.data);
  }
}
```

### Draining or Reprocessing DLQ Jobs

To reprocess failed jobs after resolving a downstream outage:

```javascript
async function reprocessDLQ(queue, redis) {
  let rawJob;
  while ((rawJob = await redis.rpop("rq:dlq")) !== null) {
    const job = JSON.parse(rawJob);
    console.log(`Re-enqueuing DLQ job: ${job.id}`);
    
    // Re-enqueue with original payload and idempotency key
    await queue.enqueue(job.data, {
      idempotencyKey: job.idempotencyKey,
      ttlSeconds: job.ttlSeconds
    });
  }
}
```
