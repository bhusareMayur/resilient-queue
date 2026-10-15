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


