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
    expect(recoveredJobData.largePrecisionId).toBe(9007199254740991);
  });
});

