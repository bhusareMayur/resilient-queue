const {
  ResilientQueue,
  RetryableError,
  FatalError
} = require("../src");

const Redis = require("ioredis");

const redisUrl = "redis://127.0.0.1:6379";

jest.setTimeout(20000);

async function waitForCondition(conditionFn, timeoutMs = 4000, intervalMs = 25) {
  const startTime = Date.now();
  while (Date.now() - startTime < timeoutMs) {
    try {
      if (await conditionFn()) {
        return;
      }
    } catch {
      // ignore and retry
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
  throw new Error(`waitForCondition timed out after ${timeoutMs}ms`);
}

describe("ResilientQueue Core Behavior", () => {
  let queue;
  let redis;

  beforeAll(async () => {
    redis = new Redis(redisUrl);
  });

  beforeEach(async () => {
    await redis.flushall();

    queue = new ResilientQueue({
      redisUrl,
      maxRetries: 1,
      baseDelay: 100
    });
  });

afterEach(async () => {
  if (queue) {
    await queue.close();
    queue = null;
  }
});

  afterAll(async () => {
    await redis.quit();
  });

  test("processes successful job", async () => {
    let processed = false;

    queue.process(async () => {
      processed = true;
    });

    await new Promise(r => setTimeout(r, 200));

    await queue.enqueue({ test: "success" });

    await new Promise(r => setTimeout(r, 500));

    expect(processed).toBe(true);
  });

  test("retries once then moves to DLQ", async () => {
    let attempts = 0;

    queue.process(async () => {
      attempts++;
      throw new RetryableError("fail");
    });

    await new Promise(r => setTimeout(r, 200));

    await queue.enqueue({ test: "retry" });

    await new Promise(r => setTimeout(r, 1000));

    const dlqItems = await redis.lrange("rq:dlq", 0, -1);

    expect(attempts).toBe(2);
    expect(dlqItems.length).toBe(1);
  });

  test("fatal error goes directly to DLQ", async () => {
    queue.process(async () => {
      throw new FatalError("fatal");
    });

    await new Promise(r => setTimeout(r, 200));

    await queue.enqueue({ test: "fatal" });

    await new Promise(r => setTimeout(r, 500));

    const dlqItems = await redis.lrange("rq:dlq", 0, -1);

    expect(dlqItems.length).toBe(1);
  });

  test("idempotency blocks duplicate jobs", async () => {
    let count = 0;

    queue.process(async () => {
      count++;
    });

    await new Promise(r => setTimeout(r, 200));

    await queue.enqueue(
      { test: "idempotent" },
      { idempotencyKey: "unique" }
    );

    await queue.enqueue(
      { test: "idempotent" },
      { idempotencyKey: "unique" }
    );

    await new Promise(r => setTimeout(r, 800));

    expect(count).toBe(1);
  });


test("recovers delayed retry after queue restart", async () => {
  const retryBaseDelay = 1000;

  // Close the queue created by beforeEach() to avoid leaking
  // its producer and consumer Redis connections.
  await queue.close();

  // Create a fresh queue with a longer retry delay.
  queue = new ResilientQueue({
    redisUrl,
    maxRetries: 1,
    baseDelay: retryBaseDelay
  });

  const job = {
    id: "restart-retry-test",
    data: { test: "restart-retry" },
    attempt: 0,
    idempotencyKey: null,
    ttlSeconds: null,
    createdAt: Date.now()
  };


  // Persist a delayed retry without starting workers.
  await queue.retryManager.handleFailure(
    job,
    new RetryableError("temporary failure")
  );

  expect(await redis.zcard("rq:delayed")).toBe(1);

  // Stop before the retry becomes due.
  // await new Promise(resolve => setTimeout(resolve, 100));
  // await queue.close();
  // Stop before the retry becomes due.
await new Promise(resolve => setTimeout(resolve, 100));

// console.log("[diagnostic] Closing first queue");
await queue.close();
// console.log("[diagnostic] First queue closed");

// console.log("[diagnostic] Creating replacement queue");
//

  // Restart using the same Redis instance and key prefix.
  queue = new ResilientQueue({
    redisUrl,
    maxRetries: 1,
    baseDelay: retryBaseDelay
  });

  let resolveProcessed;

  const processed = new Promise(resolve => {
    resolveProcessed = resolve;
  });

  queue.process(async recoveredJob => {
    if (recoveredJob.id === job.id) {
      resolveProcessed(recoveredJob);
    }
  });

  let timer;

  try {
    const recoveredJob = await Promise.race([
      processed,
      new Promise((_, reject) => {
        timer = setTimeout(() => {
          reject(
            new Error("Retry was not recovered within 5 seconds")
          );
        }, 5000);
      })
    ]);

    expect(recoveredJob.id).toBe(job.id);
  } finally {
    clearTimeout(timer);
  }

  expect(await redis.zcard("rq:delayed")).toBe(0);
  // console.log("[diagnostic] Restart test assertions completed");
});

  test("processes multiple jobs concurrently according to concurrency option", async () => {
    const concurrency = 3;
    let running = 0;
    let maxRunning = 0;
    const processed = [];

    queue.process(
      async (job) => {
        running++;
        maxRunning = Math.max(maxRunning, running);
        await new Promise((resolve) => setTimeout(resolve, 200));
        running--;
        processed.push(job.data.index);
      },
      { concurrency }
    );

    await new Promise((r) => setTimeout(r, 100));

    await Promise.all([
      queue.enqueue({ index: 1 }),
      queue.enqueue({ index: 2 }),
      queue.enqueue({ index: 3 })
    ]);

    await new Promise((r) => setTimeout(r, 600));

    expect(processed.length).toBe(3);
    expect(maxRunning).toBe(concurrency);
  });

  test("limits concurrency to specified limit", async () => {
    const concurrency = 2;
    let running = 0;
    let maxRunning = 0;
    const processed = [];

    queue.process(
      async (job) => {
        running++;
        maxRunning = Math.max(maxRunning, running);
        await new Promise((resolve) => setTimeout(resolve, 200));
        running--;
        processed.push(job.data.index);
      },
      { concurrency }
    );

    await new Promise((r) => setTimeout(r, 100));

    await Promise.all([
      queue.enqueue({ index: 1 }),
      queue.enqueue({ index: 2 }),
      queue.enqueue({ index: 3 }),
      queue.enqueue({ index: 4 })
    ]);

    await new Promise((r) => setTimeout(r, 800));

    expect(processed.length).toBe(4);
    expect(maxRunning).toBe(2);
  });

  test("graceful shutdown waits for in-flight jobs to complete", async () => {
    let completed = false;

    queue.process(async () => {
      await new Promise((resolve) => setTimeout(resolve, 300));
      completed = true;
    });

    await new Promise((r) => setTimeout(r, 100));
    await queue.enqueue({ test: "in-flight" });

    // Allow worker to pick up the job
    await new Promise((r) => setTimeout(r, 100));

    // Close queue while job is still in progress
    await queue.close();

    expect(completed).toBe(true);
  });

  test("in-flight jobs are tracked in rq:active and rq:locks then cleaned up upon completion", async () => {
    let checkedInFlight = false;
    let inFlightActiveCount = 0;
    let inFlightLocksCount = 0;

    queue.process(async () => {
      inFlightActiveCount = await redis.llen("rq:active");
      inFlightLocksCount = await redis.zcard("rq:locks");
      checkedInFlight = true;
    });

    await new Promise((r) => setTimeout(r, 100));
    await queue.enqueue({ test: "in-flight-tracking" });
    await waitForCondition(async () => checkedInFlight && (await redis.llen("rq:active")) === 0);

    expect(checkedInFlight).toBe(true);
    expect(inFlightActiveCount).toBe(1);
    expect(inFlightLocksCount).toBe(1);

    expect(await redis.llen("rq:active")).toBe(0);
    expect(await redis.zcard("rq:locks")).toBe(0);
  });

  test("automatically recovers stalled job when worker crashes or stops heartbeating", async () => {
    await queue.close();

    queue = new ResilientQueue({
      redisUrl,
      maxRetries: 2,
      baseDelay: 100,
      lockDuration: 400,
      stalledInterval: 100
    });

    const crashedJob = {
      id: "crashed-job-1",
      data: { action: "send-email" },
      attempt: 0,
      stalledCount: 0,
      idempotencyKey: null,
      ttlSeconds: null,
      createdAt: Date.now()
    };

    await redis.rpush("rq:active", JSON.stringify(crashedJob));
    await redis.zadd("rq:locks", Date.now() - 500, crashedJob.id);

    let processedJob = null;
    queue.process(async (job) => {
      processedJob = job;
    });

    await waitForCondition(async () => processedJob !== null && (await redis.llen("rq:active")) === 0);

    expect(processedJob).not.toBeNull();
    expect(processedJob.id).toBe("crashed-job-1");
    expect(processedJob.stalledCount).toBe(1);
    expect(await redis.llen("rq:active")).toBe(0);
    expect(await redis.zcard("rq:locks")).toBe(0);
  });

  test("stalled job with idempotencyKey is not blocked by idempotency upon recovery", async () => {
    await queue.close();

    queue = new ResilientQueue({
      redisUrl,
      maxRetries: 2,
      baseDelay: 100,
      lockDuration: 400,
      stalledInterval: 100
    });

    const idempotencyKey = "order-tx-999";
    await redis.set(`rq:idempotency:${idempotencyKey}`, "1");

    const crashedJob = {
      id: "crashed-job-idempotent",
      data: { orderId: 999 },
      attempt: 0,
      stalledCount: 0,
      idempotencyKey,
      ttlSeconds: 3600,
      createdAt: Date.now()
    };

    await redis.rpush("rq:active", JSON.stringify(crashedJob));
    await redis.zadd("rq:locks", Date.now() - 500, crashedJob.id);

    let processed = false;
    queue.process(async () => {
      processed = true;
    });

    await waitForCondition(async () => processed && (await redis.llen("rq:active")) === 0);

    expect(processed).toBe(true);
    expect(await redis.llen("rq:active")).toBe(0);
  });

  test("routes poison pill job to DLQ when maxStalledCount is exceeded", async () => {
    await queue.close();

    queue = new ResilientQueue({
      redisUrl,
      maxRetries: 3,
      baseDelay: 100,
      lockDuration: 400,
      stalledInterval: 100,
      maxStalledCount: 1
    });

    const poisonJob = {
      id: "poison-job-1",
      data: { bad: "payload" },
      attempt: 1,
      stalledCount: 1,
      idempotencyKey: null,
      ttlSeconds: null,
      createdAt: Date.now()
    };

    await redis.rpush("rq:active", JSON.stringify(poisonJob));
    await redis.zadd("rq:locks", Date.now() - 500, poisonJob.id);

    queue.process(async () => {});

    await waitForCondition(async () => (await redis.llen("rq:dlq")) === 1 && (await redis.llen("rq:active")) === 0);

    const dlqItems = await redis.lrange("rq:dlq", 0, -1);
    expect(dlqItems.length).toBe(1);

    const dlqJob = JSON.parse(dlqItems[0]);
    expect(dlqJob.id).toBe("poison-job-1");
    const meta = await queue.getJobMetadata(dlqJob.id);
    expect(meta.stalledCount).toBe(2);
    expect(meta.failureReason).toContain("Job stalled more than maxStalledCount times");
    expect(await redis.llen("rq:active")).toBe(0);
    expect(await redis.zcard("rq:locks")).toBe(0);
  });

  test("heartbeat prevents active long-running jobs from being treated as stalled", async () => {
    await queue.close();

    queue = new ResilientQueue({
      redisUrl,
      maxRetries: 2,
      baseDelay: 100,
      lockDuration: 500,
      stalledInterval: 150
    });

    let executionCount = 0;
    queue.process(async () => {
      executionCount++;
      await new Promise((r) => setTimeout(r, 900));
    });

    await new Promise((r) => setTimeout(r, 50));
    await queue.enqueue({ test: "long-runner" });

    await waitForCondition(async () => executionCount === 1 && (await redis.llen("rq:active")) === 0, 4000);

    expect(executionCount).toBe(1);
    expect(await redis.llen("rq:active")).toBe(0);
    expect(await redis.zcard("rq:locks")).toBe(0);
  });

  test("fencing token rejects stale release when lease was superseded by another worker", async () => {
    const jobId = "fenced-job-1";
    const tokenA = "token-A";
    const tokenB = "token-B";
    const rawJob = JSON.stringify({ id: jobId, data: { test: "fencing" } });

    // Worker A acquires lock with tokenA
    await queue.stalledJobManager.acquireLock(jobId, tokenA);
    await redis.rpush("rq:active", rawJob);

    // Simulate Worker B taking over with tokenB
    await queue.stalledJobManager.acquireLock(jobId, tokenB);

    // Stale Worker A attempts to release lock with tokenA
    const staleReleaseResult = await queue.stalledJobManager.releaseLock(rawJob, jobId, tokenA);
    expect(staleReleaseResult).toBe(0);

    // Active queue and Worker B's token must remain untouched
    expect(await redis.llen("rq:active")).toBe(1);
    expect(await redis.hget("rq:lock_tokens", jobId)).toBe(tokenB);

    // Worker B releases with valid tokenB
    const validReleaseResult = await queue.stalledJobManager.releaseLock(rawJob, jobId, tokenB);
    expect(validReleaseResult).toBe(1);
    expect(await redis.llen("rq:active")).toBe(0);
    expect(await redis.hget("rq:lock_tokens", jobId)).toBeNull();
  });

  test("surfaces lease loss to running handler via job.signal and job.isLeaseLost", async () => {
    await queue.close();

    queue = new ResilientQueue({
      redisUrl,
      maxRetries: 1,
      baseDelay: 100,
      lockDuration: 200,
      stalledInterval: 100
    });

    let leaseLostObserved = false;
    let signalAbortedObserved = false;

    queue.process(async (job) => {
      // Simulate that the token in Redis is superseded externally by another worker / recovery
      await new Promise((r) => setTimeout(r, 50));
      // Invalidate Worker's token in Redis
      await redis.hset("rq:lock_tokens", job.id, "different-token");

      // Wait for heartbeat tick to detect lease loss
      await new Promise((resolve) => {
        const check = setInterval(() => {
          if (job.isLeaseLost && job.signal.aborted) {
            leaseLostObserved = job.isLeaseLost;
            signalAbortedObserved = job.signal.aborted;
            clearInterval(check);
            resolve();
          }
        }, 20);
      });
    });

    await new Promise((r) => setTimeout(r, 50));
    await queue.enqueue({ test: "lease-loss-signal" });

    await waitForCondition(async () => leaseLostObserved && signalAbortedObserved, 3000);

    expect(leaseLostObserved).toBe(true);
    expect(signalAbortedObserved).toBe(true);
  });

  test("preserves raw job data structures without Lua cjson serialization corruption during recovery", async () => {
    await queue.close();

    queue = new ResilientQueue({
      redisUrl,
      maxRetries: 2,
      baseDelay: 100,
      lockDuration: 400,
      stalledInterval: 100
    });

    const originalData = {
      emptyTags: [],
      emptyList: [],
      largePrecisionId: 9007199254740991,
      nested: { emptyChild: [] }
    };

    const crashedJob = {
      id: "raw-preserve-job",
      data: originalData,
      attempt: 0,
      stalledCount: 0,
      idempotencyKey: null,
      ttlSeconds: null,
      createdAt: Date.now()
    };

    const rawJobString = JSON.stringify(crashedJob);
    await redis.rpush("rq:active", rawJobString);
    await redis.zadd("rq:locks", Date.now() - 500, crashedJob.id);

    let recoveredJobData = null;
    queue.process(async (job) => {
      recoveredJobData = job.data;
    });

    await waitForCondition(async () => recoveredJobData !== null && (await redis.llen("rq:active")) === 0);

    // Verify empty arrays were NOT converted to objects by Lua cjson.decode/encode
    expect(Array.isArray(recoveredJobData.emptyTags)).toBe(true);
    expect(recoveredJobData.emptyTags.length).toBe(0);
    expect(Array.isArray(recoveredJobData.emptyList)).toBe(true);
    expect(Array.isArray(recoveredJobData.nested.emptyChild)).toBe(true);
  });
});

describe("Atomic Enqueue-Time Idempotency", () => {
  let queue;
  let redis;

  beforeAll(async () => {
    redis = new Redis(redisUrl);
  });

  beforeEach(async () => {
    await redis.flushall();

    queue = new ResilientQueue({
      redisUrl,
      maxRetries: 1,
      baseDelay: 100
    });
  });

  afterEach(async () => {
    if (queue) {
      await queue.close();
      queue = null;
    }
  });

  afterAll(async () => {
    await redis.quit();
  });

  test("enqueues job without idempotencyKey normally", async () => {
    const jobId = await queue.enqueue({ test: "no-idempotency" });
    expect(typeof jobId).toBe("string");
    expect(jobId).toMatch(/^[0-9a-f-]{36}$/);

    const mainItems = await redis.lrange("rq:main", 0, -1);
    expect(mainItems.length).toBe(1);
    const parsed = JSON.parse(mainItems[0]);
    expect(parsed.id).toBe(jobId);
    expect(parsed.idempotencyKey).toBeNull();
    expect(parsed.data).toEqual({ test: "no-idempotency" });
  });

  test("enqueues job with null options normally", async () => {
    const jobId = await queue.enqueue({ test: "null-options" }, null);
    expect(typeof jobId).toBe("string");
    expect(jobId).toMatch(/^[0-9a-f-]{36}$/);

    const mainItems = await redis.lrange("rq:main", 0, -1);
    expect(mainItems.length).toBe(1);
    const parsed = JSON.parse(mainItems[0]);
    expect(parsed.id).toBe(jobId);
    expect(parsed.idempotencyKey).toBeNull();
    expect(parsed.data).toEqual({ test: "null-options" });
  });

    test("first enqueue with a new key inserts exactly one job", async () => {
      const jobId = await queue.enqueue(
        { test: "first-enqueue" },
        { idempotencyKey: "new-key-1" }
      );
      expect(typeof jobId).toBe("string");

      const mainItems = await redis.lrange("rq:main", 0, -1);
      expect(mainItems.length).toBe(1);

      const parsed = JSON.parse(mainItems[0]);
      expect(parsed.id).toBe(jobId);
      expect(parsed.idempotencyKey).toBe("new-key-1");

      const storedKeyVal = await redis.get("rq:idempotency:new-key-1");
      expect(storedKeyVal).toBe(jobId);
    });

    test("repeated enqueue calls with the same key return the same job ID", async () => {
      const id1 = await queue.enqueue(
        { test: "repeated" },
        { idempotencyKey: "dup-key" }
      );
      const id2 = await queue.enqueue(
        { test: "repeated-second-call" },
        { idempotencyKey: "dup-key" }
      );
      const id3 = await queue.enqueue(
        { test: "repeated-third-call" },
        { idempotencyKey: "dup-key" }
      );

      expect(id2).toBe(id1);
      expect(id3).toBe(id1);
    });

    test("duplicate enqueue calls do not increase main queue length", async () => {
      await queue.enqueue({ step: 1 }, { idempotencyKey: "single-queue-entry" });
      expect(await redis.llen("rq:main")).toBe(1);

      await queue.enqueue({ step: 2 }, { idempotencyKey: "single-queue-entry" });
      expect(await redis.llen("rq:main")).toBe(1);

      await queue.enqueue({ step: 3 }, { idempotencyKey: "single-queue-entry" });
      expect(await redis.llen("rq:main")).toBe(1);

      const mainItems = await redis.lrange("rq:main", 0, -1);
      expect(mainItems.length).toBe(1);
      expect(JSON.parse(mainItems[0]).data).toEqual({ step: 1 });
    });

    test("concurrent enqueue calls with the same key produce only one queued job and consistent job ID", async () => {
      const concurrentCount = 10;
      const promises = [];

      for (let i = 0; i < concurrentCount; i++) {
        promises.push(
          queue.enqueue({ index: i }, { idempotencyKey: "concurrent-key" })
        );
      }

      const jobIds = await Promise.all(promises);

      // All returned IDs must be identical
      const firstId = jobIds[0];
      for (const id of jobIds) {
        expect(id).toBe(firstId);
      }

      // Only 1 job in Redis main queue
      expect(await redis.llen("rq:main")).toBe(1);
      const mainItems = await redis.lrange("rq:main", 0, -1);
      const queuedJob = JSON.parse(mainItems[0]);
      expect(queuedJob.id).toBe(firstId);
      expect(queuedJob.idempotencyKey).toBe("concurrent-key");
    });

    test("different idempotency keys create independent jobs", async () => {
      const idA = await queue.enqueue({ type: "A" }, { idempotencyKey: "key-A" });
      const idB = await queue.enqueue({ type: "B" }, { idempotencyKey: "key-B" });

      expect(idA).not.toBe(idB);
      expect(await redis.llen("rq:main")).toBe(2);

      expect(await redis.get("rq:idempotency:key-A")).toBe(idA);
      expect(await redis.get("rq:idempotency:key-B")).toBe(idB);
    });

    test("TTL expiration allows a new job to be enqueued with the same key after expiration", async () => {
      const id1 = await queue.enqueue(
        { run: 1 },
        { idempotencyKey: "expiring-key", ttlSeconds: 1 }
      );

      const initialTtl = await redis.ttl("rq:idempotency:expiring-key");
      expect(initialTtl).toBeGreaterThan(0);
      expect(initialTtl).toBeLessThanOrEqual(1);

      // Wait for key to expire in Redis
      await new Promise((resolve) => setTimeout(resolve, 1200));

      expect(await redis.exists("rq:idempotency:expiring-key")).toBe(0);

      // Enqueue again after expiration
      const id2 = await queue.enqueue(
        { run: 2 },
        { idempotencyKey: "expiring-key", ttlSeconds: 1 }
      );

      expect(id2).not.toBe(id1);
      expect(await redis.llen("rq:main")).toBe(2);
    });

    test("omitted, zero, negative, and positive TTL values follow documented behavior", async () => {
      // 1. Positive TTL
      await queue.enqueue(
        { type: "positive" },
        { idempotencyKey: "ttl-positive", ttlSeconds: 60 }
      );
      const ttlPos = await redis.ttl("rq:idempotency:ttl-positive");
      expect(ttlPos).toBeGreaterThan(0);
      expect(ttlPos).toBeLessThanOrEqual(60);

      // 2. Omitted TTL (null/undefined)
      await queue.enqueue(
        { type: "omitted" },
        { idempotencyKey: "ttl-omitted" }
      );
      const ttlOmitted = await redis.ttl("rq:idempotency:ttl-omitted");
      expect(ttlOmitted).toBe(-1); // persistent key in Redis

      // 3. Zero TTL
      await queue.enqueue(
        { type: "zero" },
        { idempotencyKey: "ttl-zero", ttlSeconds: 0 }
      );
      const ttlZero = await redis.ttl("rq:idempotency:ttl-zero");
      expect(ttlZero).toBe(-1); // persistent key

      // 4. Negative TTL
      await queue.enqueue(
        { type: "negative" },
        { idempotencyKey: "ttl-negative", ttlSeconds: -10 }
      );
      const ttlNeg = await redis.ttl("rq:idempotency:ttl-negative");
      expect(ttlNeg).toBe(-1); // persistent key

      // 5. Invalid TTL values reject
      await expect(
        queue.enqueue({ test: "invalid" }, { idempotencyKey: "k", ttlSeconds: "invalid" })
      ).rejects.toThrow(/ttlSeconds must be an integer/);

      await expect(
        queue.enqueue({ test: "invalid" }, { idempotencyKey: "k", ttlSeconds: NaN })
      ).rejects.toThrow(/ttlSeconds must be an integer/);

      await expect(
        queue.enqueue({ test: "invalid" }, { idempotencyKey: "k", ttlSeconds: 1.5 })
      ).rejects.toThrow(/ttlSeconds must be an integer/);
    });

    test("Redis or Lua error does not result in a falsely successful enqueue", async () => {
      // Corrupt the main queue key by setting it to a string instead of a list
      await redis.set("rq:main", "corrupted-string-key");

      await expect(
        queue.enqueue({ test: "lua-err" }, { idempotencyKey: "corrupt-test-key" })
      ).rejects.toThrow(/Main queue key must be a Redis list/);

      // Ensure idempotency key was NOT registered in Redis
      expect(await redis.exists("rq:idempotency:corrupt-test-key")).toBe(0);
    });

    test("original job can be processed successfully after enqueue-time idempotency registration", async () => {
      let processedJob = null;

      queue.process(async (job) => {
        processedJob = job;
      });

      const enqueuedId = await queue.enqueue(
        { orderId: "ORDER-42" },
        { idempotencyKey: "process-test-key", ttlSeconds: 300 }
      );

      await waitForCondition(async () => processedJob !== null && (await redis.llen("rq:active")) === 0);

      expect(processedJob).not.toBeNull();
      expect(processedJob.id).toBe(enqueuedId);
      expect(processedJob.data).toEqual({ orderId: "ORDER-42" });
      expect(processedJob.idempotencyKey).toBe("process-test-key");
    });

    test("retries and DLQ behavior continue to work with idempotency enabled", async () => {
      let executionAttempts = 0;

      queue.process(async () => {
        executionAttempts++;
        throw new RetryableError("transient network issue");
      });

      const jobId = await queue.enqueue(
        { chargeId: "ch_999" },
        { idempotencyKey: "retry-test-key", ttlSeconds: 300 }
      );

      // Wait for the retry attempt and eventual move to DLQ
      await waitForCondition(
        async () => (await redis.llen("rq:dlq")) === 1 && (await redis.llen("rq:active")) === 0,
        5000
      );

      expect(executionAttempts).toBe(2); // initial attempt + 1 retry (maxRetries = 1)
      const dlqItems = await redis.lrange("rq:dlq", 0, -1);
      expect(dlqItems.length).toBe(1);
      const dlqJob = JSON.parse(dlqItems[0]);
      expect(dlqJob.id).toBe(jobId);
      expect(dlqJob.idempotencyKey).toBe("retry-test-key");

      // Idempotency key remains registered and returns the original job ID
      const duplicateId = await queue.enqueue(
        { chargeId: "ch_999" },
        { idempotencyKey: "retry-test-key" }
      );
      expect(duplicateId).toBe(jobId);
      // Queue length for main should still be 0 (no new job pushed)
      expect(await redis.llen("rq:main")).toBe(0);
    });

    test("stalled job enqueued with idempotencyKey recovers and executes without being blocked", async () => {
      await queue.close();

      queue = new ResilientQueue({
        redisUrl,
        maxRetries: 2,
        baseDelay: 100,
        lockDuration: 400,
        stalledInterval: 100
      });

      const enqueuedId = await queue.enqueue(
        { task: "stalled-idem" },
        { idempotencyKey: "stalled-idem-key", ttlSeconds: 300 }
      );

      // Simulate worker dequeue and crash by moving to active with expired lock
      const rawJob = await redis.rpoplpush("rq:main", "rq:active");
      await redis.zadd("rq:locks", Date.now() - 500, enqueuedId);

      let processedJob = null;
      queue.process(async (job) => {
        processedJob = job;
      });

      await waitForCondition(async () => processedJob !== null && (await redis.llen("rq:active")) === 0);

      expect(processedJob).not.toBeNull();
      expect(processedJob.id).toBe(enqueuedId);
      expect(processedJob.stalledCount).toBe(1);
    });

    test("dequeue-time idempotency acts as defense in depth for duplicate jobs in queue", async () => {
      let executionCount = 0;
      const executedIds = [];

      queue.process(async (job) => {
        executionCount++;
        executedIds.push(job.id);
      });

      // Simulate two jobs directly in rq:main with different IDs but the same idempotencyKey
      const idempotencyKey = "defense-in-depth-key";
      const job1 = {
        id: "manual-job-1",
        data: { test: 1 },
        attempt: 0,
        stalledCount: 0,
        idempotencyKey,
        ttlSeconds: 60,
        createdAt: Date.now()
      };
      const job2 = {
        id: "manual-job-2",
        data: { test: 2 },
        attempt: 0,
        stalledCount: 0,
        idempotencyKey,
        ttlSeconds: 60,
        createdAt: Date.now()
      };

      await redis.rpush("rq:main", JSON.stringify(job1), JSON.stringify(job2));

      await waitForCondition(
        async () => executionCount === 1 && (await redis.llen("rq:main")) === 0 && (await redis.llen("rq:active")) === 0,
        4000
      );

      expect(executionCount).toBe(1);
      expect(executedIds).toEqual(["manual-job-1"]);
      expect(await redis.get(`rq:idempotency:${idempotencyKey}`)).toBe("manual-job-1");
    });
  });

  describe("Redis Connection Flexibility & Client Ownership", () => {
    let testQueue;
    let externalRedis;
    let redis;

    beforeAll(async () => {
      redis = new Redis(redisUrl);
    });

    beforeEach(async () => {
      await redis.flushall();
    });

    afterEach(async () => {
      if (testQueue) {
        await testQueue.close();
        testQueue = null;
      }
      if (externalRedis) {
        if (externalRedis.status !== "end") {
          await externalRedis.quit();
        }
        externalRedis = null;
      }
    });

    afterAll(async () => {
      await redis.quit();
    });

    test("initializes with redis URL string under redis option and processes jobs", async () => {
      testQueue = new ResilientQueue({
        redis: redisUrl,
        keyPrefix: "test-url"
      });

      let processed = false;
      testQueue.process(async (job) => {
        processed = true;
      });

      await testQueue.enqueue({ task: "via-redis-url" });
      await waitForCondition(async () => processed, 4000);

      expect(processed).toBe(true);
      expect(testQueue.producer !== testQueue.consumer).toBe(true);
    });

    test("initializes with standard ioredis options object and processes jobs", async () => {
      testQueue = new ResilientQueue({
        redis: { host: "127.0.0.1", port: 6379 },
        keyPrefix: "test-opts"
      });

      let processedJob = null;
      testQueue.process(async (job) => {
        processedJob = job.data;
      });

      await testQueue.enqueue({ message: "ioredis-options" });
      await waitForCondition(async () => processedJob !== null, 4000);

      expect(processedJob).toEqual({ message: "ioredis-options" });
      expect(testQueue.producer !== testQueue.consumer).toBe(true);
    });

    test("initializes with direct options on queue options object", async () => {
      testQueue = new ResilientQueue({
        host: "127.0.0.1",
        port: 6379,
        keyPrefix: "test-direct"
      });

      let processed = false;
      testQueue.process(async () => {
        processed = true;
      });

      await testQueue.enqueue({ task: "direct-options" });
      await waitForCondition(async () => processed, 4000);

      expect(processed).toBe(true);
    });

    test("supports Sentinel configuration options", async () => {
      testQueue = new ResilientQueue({
        redis: {
          sentinels: [{ host: "127.0.0.1", port: 26379 }],
          name: "mymaster",
          lazyConnect: true
        }
      });

      expect(testQueue.producer.options.name).toBe("mymaster");
      expect(testQueue.consumer.options.name).toBe("mymaster");
      expect(testQueue.producer !== testQueue.consumer).toBe(true);
    });

    test("supports TLS configuration options", async () => {
      testQueue = new ResilientQueue({
        redis: {
          host: "127.0.0.1",
          port: 6380,
          tls: { rejectUnauthorized: false },
          lazyConnect: true
        }
      });

      expect(testQueue.producer.options.tls).toBeDefined();
      expect(testQueue.consumer.options.tls).toBeDefined();
      expect(testQueue.producer !== testQueue.consumer).toBe(true);
    });

    test("maintains separate producer and consumer connections when initialized with an existing client", async () => {
      externalRedis = new Redis(redisUrl);
      await externalRedis.ping();

      testQueue = new ResilientQueue({
        redis: externalRedis,
        keyPrefix: "test-ext"
      });

      expect(testQueue.producer).toBe(externalRedis);
      expect(testQueue.consumer).not.toBe(externalRedis);

      let processed = false;
      testQueue.process(async () => {
        processed = true;
      });

      await testQueue.enqueue({ task: "existing-client" });
      await waitForCondition(async () => processed, 4000);

      expect(processed).toBe(true);
    });

    test("respects connection ownership: closes queue-owned consumer but never closes an externally owned client", async () => {
      externalRedis = new Redis(redisUrl);
      expect(await externalRedis.ping()).toBe("PONG");

      testQueue = new ResilientQueue({
        redis: externalRedis,
        keyPrefix: "test-own"
      });

      const consumer = testQueue.consumer;

      await testQueue.close();
      testQueue = null;

      // External client must remain connected and responsive
      expect(await externalRedis.ping()).toBe("PONG");

      // Queue-owned consumer must be closed
      expect(consumer.status).toBe("end");
    });

    test("preserves externally owned clients when explicit producer and consumer pair is passed", async () => {
      const extProducer = new Redis(redisUrl);
      const extConsumer = new Redis(redisUrl, { maxRetriesPerRequest: null });

      testQueue = new ResilientQueue({
        producer: extProducer,
        consumer: extConsumer,
        keyPrefix: "test-pair"
      });

      expect(testQueue.producer).toBe(extProducer);
      expect(testQueue.consumer).toBe(extConsumer);

      await testQueue.close();
      testQueue = null;

      // Both external connections must still be alive
      expect(await extProducer.ping()).toBe("PONG");
      expect(await extConsumer.ping()).toBe("PONG");

      await Promise.all([extProducer.quit(), extConsumer.quit()]);
    });

    test("creates separate consumer if same external client is passed for both producer and consumer", async () => {
      externalRedis = new Redis(redisUrl);

      testQueue = new ResilientQueue({
        producer: externalRedis,
        consumer: externalRedis,
        keyPrefix: "test-same"
      });

      expect(testQueue.producer).toBe(externalRedis);
      expect(testQueue.consumer).not.toBe(externalRedis);

      await testQueue.close();
      testQueue = null;

      expect(await externalRedis.ping()).toBe("PONG");
    });

    test("supports Redis Cluster configuration with cluster nodes array", async () => {
      testQueue = new ResilientQueue({
        cluster: [{ host: "127.0.0.1", port: 7000 }],
        clusterOptions: { lazyConnect: true }
      });

      expect(testQueue.producer).toBeInstanceOf(Redis.Cluster);
      expect(testQueue.consumer).toBeInstanceOf(Redis.Cluster);
      expect(testQueue.producer !== testQueue.consumer).toBe(true);
    });

    test("supports Redis Cluster configuration under redis.nodes", async () => {
      testQueue = new ResilientQueue({
        redis: {
          nodes: [{ host: "127.0.0.1", port: 7000 }],
          lazyConnect: true
        }
      });

      expect(testQueue.producer).toBeInstanceOf(Redis.Cluster);
      expect(testQueue.consumer).toBeInstanceOf(Redis.Cluster);
      expect(testQueue.producer !== testQueue.consumer).toBe(true);
    });

    test("handles existing Redis Cluster client instance and preserves ownership", async () => {
      const externalCluster = new Redis.Cluster([{ host: "127.0.0.1", port: 7000 }], { lazyConnect: true });

      let extClusterQuitCalled = false;
      externalCluster.quit = async () => {
        extClusterQuitCalled = true;
      };

      testQueue = new ResilientQueue({
        redis: externalCluster,
        keyPrefix: "{test-ext-cluster}"
      });

      expect(testQueue.producer).toBe(externalCluster);
      expect(testQueue.consumer).not.toBe(externalCluster);
      expect(testQueue.consumer).toBeInstanceOf(Redis.Cluster);

      let consumerQuitCalled = false;
      testQueue.consumer.quit = async () => {
        consumerQuitCalled = true;
      };

      await testQueue.close();
      testQueue = null;

      // External cluster instance must NOT be closed
      expect(extClusterQuitCalled).toBe(false);
      // Queue-owned duplicated consumer MUST be closed
      expect(consumerQuitCalled).toBe(true);
    });

    test("throws an informative error when no Redis configuration is provided", () => {
      expect(() => new ResilientQueue({})).toThrow(/redisUrl or redis options are required/);
      expect(() => new ResilientQueue()).toThrow(/redisUrl or redis options are required/);
    });

    test("verifies Redis Cluster hash slot requirement for Lua scripts and multi-key commands", () => {
      const calculateSlot = require("cluster-key-slot");

      // Without hash tags: keys map to DIFFERENT slots, causing CROSSSLOT error in cluster
      const unhashedMain = "rq:main";
      const unhashedActive = "rq:active";
      const unhashedDelayed = "rq:delayed";
      expect(calculateSlot(unhashedMain)).not.toBe(calculateSlot(unhashedActive));
      expect(calculateSlot(unhashedMain)).not.toBe(calculateSlot(unhashedDelayed));

      // With hash tags: all queue keys map to the EXACT SAME slot, enabling multi-key scripts & BLMOVE
      const hashedMain = "{rq}:main";
      const hashedActive = "{rq}:active";
      const hashedDelayed = "{rq}:delayed";
      const hashedLocks = "{rq}:locks";
      const hashedTokens = "{rq}:lock_tokens";
      const hashedDLQ = "{rq}:dlq";
      const hashedIdempotency = "{rq}:idempotency:order-123";

      const targetSlot = calculateSlot(hashedMain);
      expect(calculateSlot(hashedActive)).toBe(targetSlot);
      expect(calculateSlot(hashedDelayed)).toBe(targetSlot);
      expect(calculateSlot(hashedLocks)).toBe(targetSlot);
      expect(calculateSlot(hashedTokens)).toBe(targetSlot);
      expect(calculateSlot(hashedDLQ)).toBe(targetSlot);
      expect(calculateSlot(hashedIdempotency)).toBe(targetSlot);
    });
  });

describe("Native Dead-Letter Queue (DLQ) Management API", () => {
  let queue;
  let redis;

  beforeAll(async () => {
    redis = new Redis(redisUrl);
  });

  beforeEach(async () => {
    await redis.flushall();

    queue = new ResilientQueue({
      redisUrl,
      maxRetries: 1,
      baseDelay: 100
    });
  });

  afterEach(async () => {
    if (queue) {
      await queue.close();
      queue = null;
    }
  });

  afterAll(async () => {
    await redis.quit();
  });

  test("getDLQCount returns exact count including 0 on empty DLQ", async () => {
    expect(await queue.getDLQCount()).toBe(0);

    await redis.rpush("rq:dlq", JSON.stringify({ id: "j1", data: 1 }));
    await redis.rpush("rq:dlq", JSON.stringify({ id: "j2", data: 2 }));
    expect(await queue.getDLQCount()).toBe(2);

    await redis.rpush("rq:dlq", "corrupted-json");
    expect(await queue.getDLQCount()).toBe(3);
  });

  test("getDLQ pagination with start, limit, and bounds", async () => {
    for (let i = 0; i < 5; i++) {
      await redis.rpush("rq:dlq", JSON.stringify({ id: `job-${i}`, data: { index: i } }));
    }

    const page1 = await queue.getDLQ({ start: 0, limit: 2 });
    expect(page1.total).toBe(5);
    expect(page1.jobs.length).toBe(2);
    expect(page1.jobs[0].id).toBe("job-0");
    expect(page1.jobs[1].id).toBe("job-1");

    const page2 = await queue.getDLQ({ start: 2, limit: 2 });
    expect(page2.total).toBe(5);
    expect(page2.jobs.length).toBe(2);
    expect(page2.jobs[0].id).toBe("job-2");
    expect(page2.jobs[1].id).toBe("job-3");

    const page3 = await queue.getDLQ({ start: 4, limit: 2 });
    expect(page3.total).toBe(5);
    expect(page3.jobs.length).toBe(1);
    expect(page3.jobs[0].id).toBe("job-4");

    const emptyPage = await queue.getDLQ({ start: 5, limit: 2 });
    expect(emptyPage.total).toBe(5);
    expect(emptyPage.jobs).toEqual([]);

    const zeroLimit = await queue.getDLQ({ start: 0, limit: 0 });
    expect(zeroLimit.total).toBe(5);
    expect(zeroLimit.jobs).toEqual([]);
  });

  test("getDLQ supports ascending and descending order case-insensitively", async () => {
    for (let i = 0; i < 4; i++) {
      await redis.rpush("rq:dlq", JSON.stringify({ id: `job-${i}`, data: i }));
    }

    const asc = await queue.getDLQ({ start: 0, limit: 4, order: "asc" });
    expect(asc.jobs.map(j => j.id)).toEqual(["job-0", "job-1", "job-2", "job-3"]);

    const descUpper = await queue.getDLQ({ start: 0, limit: 4, order: "DESC" });
    expect(descUpper.jobs.map(j => j.id)).toEqual(["job-3", "job-2", "job-1", "job-0"]);

    const descPaged = await queue.getDLQ({ start: 1, limit: 2, order: "desc" });
    expect(descPaged.jobs.map(j => j.id)).toEqual(["job-2", "job-1"]);
  });

  test("getDLQ safely handles corrupted JSON entries without crashing", async () => {
    await redis.rpush("rq:dlq", JSON.stringify({ id: "valid-1", data: { ok: true } }));
    await redis.rpush("rq:dlq", "{invalid-json-content-bad");
    await redis.rpush("rq:dlq", "not-even-an-object");
    await redis.rpush("rq:dlq", JSON.stringify({ id: "valid-2", data: { ok: true } }));

    const res = await queue.getDLQ({ start: 0, limit: 10 });
    expect(res.total).toBe(4);
    expect(res.jobs.length).toBe(4);

    expect(res.jobs[0].id).toBe("valid-1");
    expect(res.jobs[0].corrupted).toBeUndefined();

    expect(res.jobs[1].id).toBeNull();
    expect(res.jobs[1].corrupted).toBe(true);
    expect(res.jobs[1].raw).toBe("{invalid-json-content-bad");
    expect(res.jobs[1].error).toBeDefined();

    expect(res.jobs[2].id).toBeNull();
    expect(res.jobs[2].corrupted).toBe(true);
    expect(res.jobs[2].raw).toBe("not-even-an-object");

    expect(res.jobs[3].id).toBe("valid-2");
  });

  test("getDLQ merges metadata from stalled_meta when available", async () => {
    const rawJob = { id: "stalled-in-dlq", data: { test: 1 } };
    await redis.rpush("rq:dlq", JSON.stringify(rawJob));
    await redis.hset("rq:stalled_meta", "stalled-in-dlq", JSON.stringify({
      stalledCount: 2,
      lastStalledAt: 12345,
      failedAt: 67890,
      failureReason: "Stalled more than maxStalledCount"
    }));

    const res = await queue.getDLQ();
    expect(res.jobs.length).toBe(1);
    expect(res.jobs[0].id).toBe("stalled-in-dlq");
    expect(res.jobs[0].stalledCount).toBe(2);
    expect(res.jobs[0].lastStalledAt).toBe(12345);
    expect(res.jobs[0].failedAt).toBe(67890);
    expect(res.jobs[0].failureReason).toBe("Stalled more than maxStalledCount");
  });

  test("getDLQ validates arguments strictly", async () => {
    await expect(queue.getDLQ({ start: -1 })).rejects.toThrow(/start must be a non-negative integer/);
    await expect(queue.getDLQ({ start: 1.5 })).rejects.toThrow(/start must be a non-negative integer/);
    await expect(queue.getDLQ({ limit: -5 })).rejects.toThrow(/limit must be a non-negative integer/);
    await expect(queue.getDLQ({ limit: "invalid" })).rejects.toThrow(/limit must be a non-negative integer/);
    await expect(queue.getDLQ({ order: "invalid" })).rejects.toThrow(/order must be "asc" or "desc"/);
  });

  test("retryDLQJob atomically moves one matching job to main queue and resets failure metadata", async () => {
    const failedJob = {
      id: "retry-target",
      data: {
        emptyArr: [],
        largeNum: 9007199254740991,
        nested: { innerArr: [] }
      },
      attempt: 3,
      stalledCount: 2,
      createdAt: 1000,
      failedAt: 2000,
      failureReason: "Network timeout",
      lastError: "Connection refused",
      retriedAt: 1500,
      lastStalledAt: 1800,
      lockToken: "token-abc"
    };

    await redis.rpush("rq:dlq", JSON.stringify(failedJob));
    await redis.hset("rq:stalled_meta", "retry-target", JSON.stringify({ stalledCount: 2 }));

    const retried = await queue.retryDLQJob("retry-target");
    expect(retried).toBe(true);

    // DLQ should be empty
    expect(await queue.getDLQCount()).toBe(0);

    // Stalled meta should be cleaned
    expect(await redis.hget("rq:stalled_meta", "retry-target")).toBeNull();

    // Main queue should have the job
    const mainJobs = await redis.lrange("rq:main", 0, -1);
    expect(mainJobs.length).toBe(1);
    const requeuedJob = JSON.parse(mainJobs[0]);

    expect(requeuedJob.id).toBe("retry-target");
    expect(requeuedJob.attempt).toBe(0);
    expect(requeuedJob.stalledCount).toBe(0);
    expect(requeuedJob.failedAt).toBeUndefined();
    expect(requeuedJob.failureReason).toBeUndefined();
    expect(requeuedJob.lastError).toBeUndefined();
    expect(requeuedJob.retriedAt).toBeUndefined();
    expect(requeuedJob.lastStalledAt).toBeUndefined();
    expect(requeuedJob.lockToken).toBeUndefined();
    expect(requeuedJob.createdAt).toBe(1000);

    // Verify preservation of empty arrays and large integers (no Lua cjson corruption)
    expect(Array.isArray(requeuedJob.data.emptyArr)).toBe(true);
    expect(requeuedJob.data.emptyArr.length).toBe(0);
    expect(Array.isArray(requeuedJob.data.nested.innerArr)).toBe(true);
    expect(requeuedJob.data.largeNum).toBe(9007199254740991);
  });

  test("retryDLQJob returns false for missing or non-existent job ID", async () => {
    await redis.rpush("rq:dlq", JSON.stringify({ id: "existing-job", data: 1 }));

    expect(await queue.retryDLQJob("non-existent")).toBe(false);
    expect(await queue.retryDLQJob(null)).toBe(false);
    expect(await queue.retryDLQJob(undefined)).toBe(false);
    expect(await queue.retryDLQJob("")).toBe(false);

    // DLQ was not modified
    expect(await queue.getDLQCount()).toBe(1);
  });

  test("requeued job can be processed successfully by worker", async () => {
    let processedJob = null;
    queue.process(async (job) => {
      processedJob = job;
    });

    const failedJob = {
      id: "worker-reprocess-job",
      data: { action: "send-email" },
      attempt: 2,
      stalledCount: 1,
      createdAt: Date.now(),
      failedAt: Date.now(),
      failureReason: "Worker crashed"
    };

    await redis.rpush("rq:dlq", JSON.stringify(failedJob));
    const retried = await queue.retryDLQJob("worker-reprocess-job");
    expect(retried).toBe(true);

    await waitForCondition(async () => processedJob !== null);
    expect(processedJob.id).toBe("worker-reprocess-job");
    expect(processedJob.data).toEqual({ action: "send-email" });
    expect(processedJob.attempt).toBe(0);
  });

  test("retryAllDLQ requeues jobs in controlled batches, resets failure metadata, and cleans stalled_meta", async () => {
    for (let i = 0; i < 5; i++) {
      await redis.rpush("rq:dlq", JSON.stringify({
        id: `batch-job-${i}`,
        data: { num: i },
        attempt: 2,
        failedAt: Date.now(),
        failureReason: "Some error"
      }));
      await redis.hset("rq:stalled_meta", `batch-job-${i}`, JSON.stringify({ stalledCount: 1 }));
    }
    // Also add a corrupted item in DLQ
    await redis.rpush("rq:dlq", "corrupted-dlq-entry");

    // Requeue batch of 2
    const firstBatchCount = await queue.retryAllDLQ({ limit: 2 });
    expect(firstBatchCount).toBe(2);
    expect(await redis.llen("rq:main")).toBe(2);
    expect(await queue.getDLQCount()).toBe(4); // 3 valid + 1 corrupted remaining

    // Verify first 2 metadata cleaned
    expect(await redis.hget("rq:stalled_meta", "batch-job-0")).toBeNull();
    expect(await redis.hget("rq:stalled_meta", "batch-job-1")).toBeNull();
    expect(await redis.hget("rq:stalled_meta", "batch-job-2")).not.toBeNull();

    // Requeue remaining (limit omitted -> all eligible)
    const secondBatchCount = await queue.retryAllDLQ();
    expect(secondBatchCount).toBe(3); // 3 valid jobs requeued, corrupted skipped!
    expect(await redis.llen("rq:main")).toBe(5);
    expect(await queue.getDLQCount()).toBe(1); // corrupted entry remains in DLQ

    const dlqItems = await redis.lrange("rq:dlq", 0, -1);
    expect(dlqItems[0]).toBe("corrupted-dlq-entry");

    // Check main queue jobs have reset metadata
    const mainItems = await redis.lrange("rq:main", 0, -1);
    for (const item of mainItems) {
      const parsed = JSON.parse(item);
      expect(parsed.attempt).toBe(0);
      expect(parsed.stalledCount).toBe(0);
      expect(parsed.failedAt).toBeUndefined();
      expect(parsed.failureReason).toBeUndefined();
    }
  });

  test("retryAllDLQ validates limit argument", async () => {
    expect(await queue.retryAllDLQ({ limit: 0 })).toBe(0);
    await expect(queue.retryAllDLQ({ limit: -1 })).rejects.toThrow(/limit must be a non-negative integer/);
    await expect(queue.retryAllDLQ({ limit: 1.5 })).rejects.toThrow(/limit must be a non-negative integer/);
  });

  test("removeDLQJob removes a specific failed job and cleans associated metadata", async () => {
    await redis.rpush("rq:dlq", JSON.stringify({ id: "job-remove-1", data: 1 }));
    await redis.rpush("rq:dlq", JSON.stringify({ id: "job-remove-2", data: 2 }));
    await redis.hset("rq:stalled_meta", "job-remove-1", JSON.stringify({ stalledCount: 1 }));
    await redis.hset("rq:stalled_meta", "job-remove-2", JSON.stringify({ stalledCount: 1 }));

    const removed = await queue.removeDLQJob("job-remove-1");
    expect(removed).toBe(true);

    expect(await queue.getDLQCount()).toBe(1);
    expect(await redis.hget("rq:stalled_meta", "job-remove-1")).toBeNull();
    expect(await redis.hget("rq:stalled_meta", "job-remove-2")).not.toBeNull();

    const notFound = await queue.removeDLQJob("non-existent-id");
    expect(notFound).toBe(false);

    const emptyId = await queue.removeDLQJob("");
    expect(emptyId).toBe(false);
  });

  test("purgeDLQ clears the DLQ, cleans associated metadata, and returns count purged", async () => {
    expect(await queue.purgeDLQ()).toBe(0);

    for (let i = 0; i < 3; i++) {
      await redis.rpush("rq:dlq", JSON.stringify({ id: `purge-${i}`, data: i }));
      await redis.hset("rq:stalled_meta", `purge-${i}`, JSON.stringify({ stalledCount: 1 }));
    }
    await redis.rpush("rq:dlq", "malformed-json-entry");

    // Also set an active/delayed job metadata in stalled_meta which should NOT be touched
    await redis.hset("rq:stalled_meta", "unrelated-active-job", JSON.stringify({ stalledCount: 1 }));

    const count = await queue.purgeDLQ();
    expect(count).toBe(4); // 3 valid + 1 malformed = 4
    expect(await queue.getDLQCount()).toBe(0);

    // DLQ jobs metadata cleaned
    expect(await redis.hget("rq:stalled_meta", "purge-0")).toBeNull();
    expect(await redis.hget("rq:stalled_meta", "purge-1")).toBeNull();
    expect(await redis.hget("rq:stalled_meta", "purge-2")).toBeNull();

    // Unrelated job metadata preserved
    expect(await redis.hget("rq:stalled_meta", "unrelated-active-job")).not.toBeNull();
  });

  test("concurrent operations handle race conditions safely and atomically", async () => {
    const job = { id: "concurrent-target", data: { test: "race" }, attempt: 2 };
    await redis.rpush("rq:dlq", JSON.stringify(job));

    // Two workers attempt to retry the same job concurrently
    const [retry1, retry2] = await Promise.all([
      queue.retryDLQJob("concurrent-target"),
      queue.retryDLQJob("concurrent-target")
    ]);

    // Exactly one should succeed, the other should fail
    expect([retry1, retry2].sort()).toEqual([false, true]);

    // Only 1 job in main queue
    expect(await redis.llen("rq:main")).toBe(1);
    expect(await queue.getDLQCount()).toBe(0);

    // Now test concurrent remove and retry
    await redis.rpush("rq:dlq", JSON.stringify({ id: "race-job-2", data: 2 }));
    const [remRes, retRes] = await Promise.all([
      queue.removeDLQJob("race-job-2"),
      queue.retryDLQJob("race-job-2")
    ]);
    expect([remRes, retRes].sort()).toEqual([false, true]);
    expect(await queue.getDLQCount()).toBe(0);
  });
});