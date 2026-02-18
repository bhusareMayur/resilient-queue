> ⚠ Maintenance Mode  
> This project is currently in maintenance mode. Contributions are welcome.


# @mayurbhusare/resilient-queue

![npm version](https://img.shields.io/npm/v/@mayurbhusare/resilient-queue)
![license](https://img.shields.io/npm/l/@mayurbhusare/resilient-queue)
![downloads](https://img.shields.io/npm/dt/@mayurbhusare/resilient-queue)
![node](https://img.shields.io/node/v/@mayurbhusare/resilient-queue)


Minimal resilient Redis-backed job queue for Node.js.

Provides exponential retry, dead-letter queue (DLQ) handling, idempotency guarantees, and graceful shutdown — without heavy frameworks.

---

## ✨ Features

- Redis-backed job queue
- Exponential backoff retry
- Dead Letter Queue (DLQ) support
- Idempotency protection
- Graceful shutdown support
- Lightweight and minimal design
- Fully tested with Jest

---

## 📦 Installation

```bash
npm install @mayurbhusare/resilient-queue
```

## 🚀 Quick Example
```bash

const {
  ResilientQueue,
  RetryableError,
  FatalError
} = require("@mayurbhusare/resilient-queue");

const queue = new ResilientQueue({
  redisUrl: "redis://127.0.0.1:6379",
  maxRetries: 2,
  baseDelay: 500
});

queue.process(async (job) => {
  console.log("Processing:", job.data);

  if (job.data.retry) {
    throw new RetryableError("Temporary failure");
  }

  if (job.data.fatal) {
    throw new FatalError("Permanent failure");
  }

  console.log("Completed");
});

queue.enqueue({ message: "Hello World" });

```
## 🧠 How It Works
Main Queue

Jobs are pushed into:
```bash
rq:main
```
Workers consume jobs using blocking Redis BLPOP.

## Retry Strategy

Retryable errors trigger exponential backoff:
```bash
delay = baseDelay * 2^attempt
```

After exceeding maxRetries, the job is moved to the DLQ.

## Dead Letter Queue (DLQ)

Failed jobs are stored in:
```bash
rq:dlq
```

### DLQ entries contain:

- failure reason
- attempt count
- timestamps

## Idempotency

If an `idempotencyKey` is provided:

- First execution succeeds
- Duplicate submissions are ignored
- Retries are not blocked

## Graceful Shutdown
```bash
await queue.close();
```
Safely stops worker and closes Redis connections.

## 🎯 Use Cases
- Email processing
- Webhook consumers
- Payment confirmation retries
- Background jobs
- Distributed microservices tasks