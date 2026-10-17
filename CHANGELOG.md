# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [2.0.1] - 2026-10-17

### Documentation
- Reaffirmed active project maintenance status and added production-ready badges in `README.md`.
- Updated `CONTRIBUTING.md` guidelines to reflect an active development roadmap and welcoming contribution posture.

---

## [2.0.0] - 2026-10-17

### Added
- **Real-Time Queue Metrics**: Retrieve waiting, active, delayed, and dead-letter queue job counts in a single network round-trip via `queue.getMetrics()`.
- **Distributed Pause & Resume**: Pause and resume job consumption locally and across distributed worker nodes with atomic Redis coordination (`pause()`, `resume()`, `isPaused()`), active-job draining, and race-safe rollback.
- **Configurable Retry Backoff Engine**: Added configurable backoff strategies (exponential, fixed, linear), jitter algorithms (full, equal, none), configurable delay caps (`maxDelay`), and per-job dynamic backoff overrides.
- **Lifecycle Events Observability**: Native Node.js `EventEmitter` lifecycle hooks (`completed`, `failed`, `retrying`, `dlq`, `stalled`, `paused`, `resumed`) with isolated listener safety and zero queue corruption.
- **Job Execution Timeouts**: Configurable execution timeouts (`jobTimeout`) with cooperative cancellation via `AbortSignal`, `JobTimeoutError`, automatic retry/DLQ routing, and safe lock retention.
- **Scheduled Delayed Jobs**: Direct job scheduling via `delay` and `runAt` backed by Redis Sorted Sets (`rq:delayed`) and atomic Lua promotion.
- **High-Throughput Batch Enqueuing**: Pipeline-backed batch enqueuing via `queue.enqueueMany()` for 50x–100x throughput boost.
- **Native Dead-Letter Queue (DLQ) Management**: Comprehensive DLQ inspection and remediation API (`getDLQ`, `getDLQCount`, `retryDLQJob`, `retryAllDLQ`, `removeDLQJob`, `purgeDLQ`).
- **Flexible Redis Connection Support**: Initialize `ResilientQueue` using connection URLs, `ioredis` options objects, existing client instances, dedicated producer/consumer pairs, or Redis Cluster configurations with hash tag support.
- **Stalled Job Protection & Worker Crash Recovery**: Active visibility heartbeats renew job locks in `rq:locks`; background scheduler recovers abandoned jobs from `rq:active` via atomic Lua script.
- **Per-Dequeue Fencing Tokens**: Unique `lockToken` assigned per dequeue in `rq:lock_tokens` to prevent stale workers from releasing or renewing successor workers' locks.
- **Lease Loss Detection**: Surface revoked or expired leases to handlers via `job.isLeaseLost` and `job.signal` (`AbortSignal`).
- **External Stalled Metadata Tracking**: Store recovery metadata in `rq:stalled_meta` to keep raw JSON job payloads intact and prevent Lua `cjson` array corruption.
- **Poison Pill Routing**: Automatically route jobs that exceed `maxStalledCount` straight to `rq:dlq`.
- **TypeScript Type Definitions**: Complete TypeScript typings exported via `index.d.ts`.
- **Job Metadata API**: Added `queue.getJobMetadata(jobId)` to inspect recovery and failure metadata.

### Changed
- Defense-in-depth dequeue-time idempotency now checks `job.id` against registered candidate ID on initial attempt.
- Worker concurrency now uses an in-memory worker pool with `Promise.race` backpressure over a single consumer socket.

---

## [1.2.3] - 2026-10-14

### Fixed
- Resolved worker concurrency Redis socket bottleneck by replacing multi-connection worker model with an in-memory worker pool using `Promise.race` backpressure over a single consumer connection.

### Documentation
- Overhauled `README.md` to reflect actual codebase architecture, APIs, and Redis key layout.

---

## [1.2.2] - 2026-10-10

### Fixed
- Persisted delayed retries in Redis Sorted Set (`rq:delayed`) so scheduled retries survive queue restarts and server reboots.
- Implemented atomic Lua promotion script for transferring due retries from `rq:delayed` to `rq:main`.

---

## [1.2.1] - 2026-10-10

### Fixed
- Resolved worker loop syntax errors and improved error boundary handling.

### Security
- Updated dependencies (`ioredis`, `uuid`, `jest`) to resolve security vulnerabilities.

---

## [1.2.0] - 2026-02-18

### Changed
- Updated graceful shutdown in `queue.close()` to await active in-flight worker execution before disconnecting.

### Documentation
- Added maintenance mode notice to repository.

---

## [1.1.0] - 2026-02-18

### Added
- Added worker concurrency support via the `concurrency` option in `queue.process(handler, options)`.

---

## [1.0.3] - 2026-02-17

### Fixed
- Corrected repository URL in `package.json`.

---

## [1.0.2] - 2026-02-17

### Fixed
- Cleaned up package metadata.

---

## [1.0.1] - 2026-02-17

### Documentation
- Updated repository and documentation links.

---

## [1.0.0] - 2026-02-15

### Added
- Initial stable release of `@mayurbhusare/resilient-queue`.
- Core `ResilientQueue` class backed by Redis FIFO list.
- Dedicated producer and consumer dual Redis connection architecture.
- Custom `RetryableError` and `FatalError` classes.
- Exponential backoff retry handling with configurable `maxRetries` and `baseDelay`.
- Dead-Letter Queue (`<keyPrefix>:dlq`) routing for unhandled errors and exhausted retries.
- Idempotency manager with Redis `SET NX` support.
- Graceful worker shutdown via `stop()` and `close()`.
- Jest test suite with in-band execution.
