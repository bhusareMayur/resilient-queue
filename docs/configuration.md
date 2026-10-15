# Configuration Guide

This guide covers all configuration options available in `@mayurbhusare/resilient-queue`, including flexible Redis connection topologies, connection ownership semantics, cluster requirements, and queue runtime parameters.

---

## Table of Contents

- [Queue Initialization Options](#queue-initialization-options)
- [Redis Connection Formats](#redis-connection-formats)
  - [1. Redis Connection URL](#1-redis-connection-url)
  - [2. Standard ioredis Options Object](#2-standard-ioredis-options-object)
  - [3. Redis Sentinel (High Availability)](#3-redis-sentinel-high-availability)
  - [4. TLS Encryption](#4-tls-encryption)
  - [5. Pre-existing Redis Client Instance](#5-pre-existing-redis-client-instance)
  - [6. Redis Cluster Configuration](#6-redis-cluster-configuration)
  - [7. Explicit Producer and Consumer Connection Pair](#7-explicit-producer-and-consumer-connection-pair)
  - [8. Direct Connection Options](#8-direct-connection-options)
- [Connection Ownership & Lifecycle](#connection-ownership-lifecycle)
- [Redis Cluster Hash Tag Requirement](#redis-cluster-hash-tag-requirement)
- [Operational Queue Parameters](#operational-queue-parameters)
- [Enqueue Options](#enqueue-options)
- [Worker Processing Options](#worker-processing-options)

---

## Queue Initialization Options

Pass options to the constructor when creating a queue instance:

```javascript
const { ResilientQueue } = require("@mayurbhusare/resilient-queue");

const queue = new ResilientQueue(options);
```

### Options Summary

| Option | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `redis` | `string \| object \| Redis \| Cluster` | *(Optional)* | Flexible connection configuration: URL, options object, existing client, cluster config, or connection pair. |
| `redisUrl` | `string` | *(Optional)* | Redis URL (e.g., `"redis://127.0.0.1:6379"`). Preserved for backward compatibility. |
| `client` | `Redis \| Cluster` | *(Optional)* | Existing ioredis client instance. |
| `cluster` | `Array \| object` | *(Optional)* | Redis Cluster node array `[{ host, port }]` or `{ nodes, options }` configuration object. |
| `clusterNodes` | `Array` | *(Optional)* | Array of Redis Cluster nodes. |
| `clusterOptions` | `object` | *(Optional)* | Configuration options for Redis Cluster. |
| `producer` | `Redis \| Cluster \| object \| string` | *(Optional)* | Explicit client instance or connection options for the producer connection. |
| `consumer` | `Redis \| Cluster \| object \| string` | *(Optional)* | Explicit client instance or connection options for the blocking consumer connection. |
| `maxRetries` | `number` | `3` | Maximum retry attempts before moving a failed job to the DLQ. |
| `baseDelay` | `number` | `500` | Base retry delay in milliseconds for exponential backoff (`baseDelay * 2^attempt`). |
| `keyPrefix` | `string` | `"rq"` | Redis key namespace. On Redis Cluster, must include a hash tag like `"{rq}"`. |
| `stalledInterval` | `number` | `15000` | Interval in milliseconds between stalled job recovery checks. |
| `lockDuration` | `number` | `30000` | Visibility lock lease duration in milliseconds before an unacknowledged job is considered stalled. |
| `maxStalledCount` | `number` | `1` | Maximum times a job can stall (worker crash) before being routed to the DLQ. |

---

## Redis Connection Formats

`ResilientQueue` uses a dual-connection architecture under the hood:
1. **Producer Connection**: For non-blocking commands (`RPUSH`, `SET`, `ZADD`, `eval`, etc.).
2. **Consumer Connection**: For blocking operations (`BLMOVE` / `BRPOPLPUSH`).

The library automatically manages connection duplication according to the format provided.

### 1. Redis Connection URL

Pass a connection URL string via `redis` or `redisUrl`:

```javascript
const queue = new ResilientQueue({
  redis: "redis://username:password@127.0.0.1:6379/0"
});
```

### 2. Standard ioredis Options Object

Pass any options accepted by `ioredis`:

```javascript
const queue = new ResilientQueue({
  redis: {
    host: "redis.internal.example.com",
    port: 6379,
    password: "secretpassword",
    db: 2,
    connectTimeout: 10000
  }
});
```

### 3. Redis Sentinel (High Availability)

Configure Sentinel failover by passing `sentinels` and the master `name`:

```javascript
const queue = new ResilientQueue({
  redis: {
    sentinels: [
      { host: "sentinel-1.internal", port: 26379 },
      { host: "sentinel-2.internal", port: 26379 },
      { host: "sentinel-3.internal", port: 26379 }
    ],
    name: "mymaster",
    password: "sentinel-auth-password"
  }
});
```

### 4. TLS Encryption

Enable TLS for encrypted Redis endpoints (e.g., AWS ElastiCache, Azure Cache for Redis, Upstash):

```javascript
const queue = new ResilientQueue({
  redis: {
    host: "secure-redis.example.com",
    port: 6380,
    password: "auth-token",
    tls: {
      rejectUnauthorized: true
    }
  }
});
```

### 5. Pre-existing Redis Client Instance

If your application already manages an `ioredis` client instance, pass it directly. The library reuses your instance for the producer and calls `.duplicate()` to create a dedicated consumer connection:

```javascript
const Redis = require("ioredis");
const myRedisClient = new Redis("redis://127.0.0.1:6379");

const queue = new ResilientQueue({
  redis: myRedisClient
});
```

### 6. Redis Cluster Configuration

Connect to a Redis Cluster by passing cluster nodes:

```javascript
const queue = new ResilientQueue({
  cluster: [
    { host: "10.0.0.1", port: 7000 },
    { host: "10.0.0.2", port: 7000 },
    { host: "10.0.0.3", port: 7000 }
  ],
  clusterOptions: {
    scaleReads: "slave"
  },
  keyPrefix: "{my-queue}" // REQUIRED for Redis Cluster (see Hash Tag Requirement below)
});
```

Alternatively, supply the configuration under `redis`:

```javascript
const queue = new ResilientQueue({
  redis: {
    nodes: [
      { host: "10.0.0.1", port: 7000 },
      { host: "10.0.0.2", port: 7000 }
    ],
    scaleReads: "all"
  },
  keyPrefix: "{my-queue}"
});
```

### 7. Explicit Producer and Consumer Connection Pair

If you want fine-grained control over both connections, supply distinct producer and consumer configurations:

```javascript
const queue = new ResilientQueue({
  producer: "redis://127.0.0.1:6379/1",
  consumer: "redis://127.0.0.1:6379/1"
});
```

You can also pass pre-existing client instances:

```javascript
const producer = new Redis("redis://127.0.0.1:6379");
const consumer = new Redis("redis://127.0.0.1:6379", { maxRetriesPerRequest: null });

const queue = new ResilientQueue({
  producer,
  consumer
});
```

### 8. Direct Connection Options

Connection options can also be placed directly at the top level of the queue configuration object:

```javascript
const queue = new ResilientQueue({
  host: "127.0.0.1",
  port: 6379,
  password: "mypassword"
});
```

---

## Connection Ownership & Lifecycle

`ResilientQueue` strictly distinguishes between queue-owned and externally owned connections:

- **Queue-Owned Connections**: Connections instantiated by the queue (e.g. from URLs, option objects, cluster node lists, or duplicated consumer clients) are considered queue-owned. Calling `await queue.close()` safely calls `.quit()` (or `.disconnect()`) on these connections.
- **Externally Owned Connections**: When you supply an existing `Redis` or `Cluster` client instance (e.g., `redis: existingClient`), the library **will never close your client**. During `queue.close()`, only the internal duplicated consumer connection is closed, leaving your external client active and responsive for the rest of your application.

---

## Redis Cluster Hash Tag Requirement

In Redis Cluster, multi-key commands and Lua scripts can only operate across keys located on the **exact same cluster shard (hash slot)**.

`ResilientQueue` executes atomic multi-key Lua scripts and commands across:
- `<keyPrefix>:main`
- `<keyPrefix>:active`
- `<keyPrefix>:locks`
- `<keyPrefix>:lock_tokens`
- `<keyPrefix>:stalled_meta`
- `<keyPrefix>:delayed`
- `<keyPrefix>:dlq`
- `<keyPrefix>:idempotency:<idempotencyKey>`

To guarantee that all of these keys map to the same hash slot, **you must wrap your `keyPrefix` in Redis hash tags `{...}`**:

```javascript
const queue = new ResilientQueue({
  cluster: [{ host: "127.0.0.1", port: 7000 }],
  keyPrefix: "{orders-queue}" // Curly braces pin all keys for this queue to one hash slot
});
```

> [!WARNING]
> Without hash tags in `keyPrefix`, Redis Cluster will throw:
> `CROSSSLOT Keys in request don't hash to the same slot`.
>
> **Single-Shard Queue Constraint**: Pinning keys to a single hash slot guarantees transactional atomicity across operations. Consequently, an individual queue instance is served by a single Redis Cluster master node; a single queue cannot be split across multiple shards.

---

## Operational Queue Parameters

### `maxRetries` (Default: `3`)

The maximum number of retry attempts executed for a job that encounters a `RetryableError`. On failure:
- Attempt 1 fails $\to$ retried (attempt 1).
- Attempt 2 fails $\to$ retried (attempt 2).
- Attempt 3 fails $\to$ retried (attempt 3).
- If attempt 4 fails and `maxRetries` is 3, the job is moved to `rq:dlq`.

### `baseDelay` (Default: `500`)

Base delay in milliseconds for exponential backoff. Retries are scheduled at:
$$\text{delay} = \text{baseDelay} \times 2^{\text{attempt}}$$

With `baseDelay: 500`:
- Attempt 1: $500 \times 2^1 = 1{,}000\text{ ms}$ (1 second)
- Attempt 2: $500 \times 2^2 = 2{,}000\text{ ms}$ (2 seconds)
- Attempt 3: $500 \times 2^3 = 4{,}000\text{ ms}$ (4 seconds)

### `keyPrefix` (Default: `"rq"`)

The namespace prefix prepended to all Redis keys. For example, `keyPrefix: "emails"` creates keys like `emails:main`, `emails:active`, `emails:locks`, etc.

### `stalledInterval` (Default: `15000`)

Frequency in milliseconds at which the recovery scheduler checks for stalled jobs in `rq:locks`. A lower interval detects crashed workers faster, at the cost of more frequent Redis queries.

### `lockDuration` (Default: `30000`)

The duration in milliseconds of the worker's visibility lock lease. If a worker process dies without renewing its lease, the recovery scheduler re-queues or dead-letters the job after this duration. While the worker is actively processing, a background heartbeat automatically renews this lease every `Math.floor(lockDuration / 3)` milliseconds.

### `maxStalledCount` (Default: `1`)

The maximum number of times an individual job can stall (due to worker crash, timeout, or OOM) and be recovered before being treated as a poison pill. If a job crashes workers more than `maxStalledCount` times, it is routed directly to `rq:dlq` to prevent infinite crash loops.

---

## Enqueue Options

Options passed to `queue.enqueue(data, options)`:

```javascript
await queue.enqueue(data, {
  idempotencyKey: "unique-order-uuid",
  ttlSeconds: 3600
});
```

| Option | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `idempotencyKey` | `string \| null` | `null` | Unique idempotency key. Duplicate enqueue calls return the existing job ID without creating a duplicate job. |
| `ttlSeconds` | `number \| null` | `null` | Time-to-live in seconds for the idempotency key in Redis. Must be an integer if supplied. If omitted, `0`, or negative, the key persists indefinitely. |

---

## Worker Processing Options

Options passed to `queue.process(handler, options)`:

```javascript
queue.process(handler, {
  concurrency: 5
});
```

| Option | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `concurrency` | `number` | `1` | Maximum number of jobs this worker instance can process concurrently in-memory. Must be a positive integer. |
