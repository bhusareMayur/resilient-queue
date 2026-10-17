const {
  ResilientQueue,
  FatalError,
  RetryableError
} = require("../src");

const Redis = require("ioredis");

const redisUrl = "redis://127.0.0.1:6379";

jest.setTimeout(25000);

async function waitForCondition(conditionFn, timeoutMs = 6000, intervalMs = 25) {
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

describe("Queue Metrics (getMetrics)", () => {
  let queue;
  let redis;
  const keyPrefix = "rq:test:metrics";

  beforeAll(async () => {
    redis = new Redis(redisUrl);
  });

  beforeEach(async () => {
    await redis.flushall();
    queue = new ResilientQueue({
      redisUrl,
      keyPrefix,
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
    await redis.flushall();
    try {
      await redis.quit();
    } catch {}
    if (typeof redis.disconnect === "function") {
      redis.disconnect();
    }
  });

  test("returns all zero counts on an empty queue", async () => {
    const metrics = await queue.getMetrics();

    expect(metrics).toEqual({
      waiting: 0,
      active: 0,
      delayed: 0,
      dlq: 0
    });
    expect(typeof metrics.waiting).toBe("number");
    expect(typeof metrics.active).toBe("number");
    expect(typeof metrics.delayed).toBe("number");
    expect(typeof metrics.dlq).toBe("number");
  });

  test("accurately counts waiting jobs from enqueue and enqueueMany", async () => {
    await queue.enqueue({ item: 1 });
    await queue.enqueue({ item: 2 });
    await queue.enqueueMany([{ data: { item: 3 } }, { data: { item: 4 } }]);

    const metrics = await queue.getMetrics();
    expect(metrics.waiting).toBe(4);
    expect(metrics.active).toBe(0);
    expect(metrics.delayed).toBe(0);
    expect(metrics.dlq).toBe(0);
  });

  test("accurately counts active jobs while processing", async () => {
    let unblockJob;
    const blocker = new Promise((resolve) => {
      unblockJob = resolve;
    });

    let jobStarted = false;
    queue.process(
      async () => {
        jobStarted = true;
        await blocker;
      },
      { concurrency: 2 }
    );

    await queue.enqueue({ task: "active-1" });
    await waitForCondition(() => jobStarted);

    const metrics = await queue.getMetrics();
    expect(metrics.active).toBe(1);
    expect(metrics.waiting).toBe(0);

    unblockJob();
    await waitForCondition(async () => (await queue.getMetrics()).active === 0);

    const finalMetrics = await queue.getMetrics();
    expect(finalMetrics.active).toBe(0);
    expect(finalMetrics.waiting).toBe(0);
  });

  test("accurately counts delayed jobs and retries", async () => {
    // 1. Direct delayed jobs
    await queue.enqueue({ task: "delayed-1" }, { delay: 10000 });
    await queue.enqueue({ task: "delayed-2" }, { delay: 20000 });

    let metrics = await queue.getMetrics();
    expect(metrics.delayed).toBe(2);
    expect(metrics.waiting).toBe(0);

    // 2. Retryable failure scheduled in delayed set
    const retryJob = {
      id: "retry-metric-test",
      data: { test: "retry" },
      attempt: 0,
      idempotencyKey: null,
      ttlSeconds: null,
      createdAt: Date.now()
    };

    await queue.retryManager.handleFailure(
      retryJob,
      new RetryableError("transient network hiccup")
    );

    metrics = await queue.getMetrics();
    expect(metrics.delayed).toBe(3);
  });

  test("accurately counts DLQ jobs from fatal errors and exhausted retries", async () => {
    queue.process(async () => {
      throw new FatalError("invalid schema");
    });

    await queue.enqueue({ test: "fatal" });
    await waitForCondition(async () => (await queue.getDLQCount()) === 1);

    const metrics = await queue.getMetrics();
    expect(metrics.dlq).toBe(1);
    expect(metrics.waiting).toBe(0);
    expect(metrics.active).toBe(0);
  });

  test("accurately reports combined multi-state metrics simultaneously", async () => {
    // 1. Waiting jobs
    await queue.enqueue({ type: "waiting-1" });
    await queue.enqueue({ type: "waiting-2" });

    // 2. Delayed jobs
    await queue.enqueue({ type: "delayed-1" }, { delay: 30000 });

    // 3. DLQ job
    await queue.dlqManager.moveToDLQ(
      { id: "dlq-job-1", data: { bad: true } },
      new FatalError("manual dlq")
    );

    // 4. Active job
    await redis.rpush(`${keyPrefix}:active`, JSON.stringify({ id: "active-manual" }));

    const metrics = await queue.getMetrics();
    expect(metrics).toEqual({
      waiting: 2,
      active: 1,
      delayed: 1,
      dlq: 1
    });
  });

  test("uses an ioredis pipeline to query all metrics in a single round-trip", async () => {
    let pipelineCalled = false;
    const origPipeline = queue.producer.pipeline.bind(queue.producer);

    queue.producer.pipeline = function () {
      pipelineCalled = true;
      return origPipeline();
    };

    const metrics = await queue.getMetrics();
    expect(pipelineCalled).toBe(true);
    expect(metrics).toHaveProperty("waiting");
    expect(metrics).toHaveProperty("active");
    expect(metrics).toHaveProperty("delayed");
    expect(metrics).toHaveProperty("dlq");
  });

  test("validates pipeline errors and throws if pipeline execution fails", async () => {
    const origPipeline = queue.producer.pipeline.bind(queue.producer);

    queue.producer.pipeline = function () {
      const p = origPipeline();
      const origExec = p.exec.bind(p);
      p.exec = async function () {
        const results = await origExec();
        // Simulate error on the second command (active list)
        results[1] = [new Error("Simulated Redis pipeline command failure"), null];
        return results;
      };
      return p;
    };

    await expect(queue.getMetrics()).rejects.toThrow(
      "Simulated Redis pipeline command failure"
    );
  });

  test("throws if pipeline returns null or undefined results", async () => {
    const origPipeline = queue.producer.pipeline.bind(queue.producer);

    queue.producer.pipeline = function () {
      const p = origPipeline();
      p.exec = async function () {
        return null;
      };
      return p;
    };

    await expect(queue.getMetrics()).rejects.toThrow(/Pipeline execution failed/);
  });

  test("throws clear error when getMetrics is called on a closed queue", async () => {
    await queue.close();
    await expect(queue.getMetrics()).rejects.toThrow("Queue is closed");
  });
});

describe("Distributed Pause and Resume", () => {
  let queue;
  let redis;
  const keyPrefix = "rq:test:pause";

  beforeAll(async () => {
    redis = new Redis(redisUrl);
  });

  beforeEach(async () => {
    await redis.flushall();
    queue = new ResilientQueue({
      redisUrl,
      keyPrefix,
      maxRetries: 1,
      baseDelay: 100,
      pausePollInterval: 25
    });
  });

  afterEach(async () => {
    if (queue) {
      await queue.close();
      queue = null;
    }
  });

  afterAll(async () => {
    await redis.flushall();
    try {
      await redis.quit();
    } catch {}
    if (typeof redis.disconnect === "function") {
      redis.disconnect();
    }
  });

  test("pause() sets local flag and Redis key, isPaused() returns true", async () => {
    expect(await queue.isPaused()).toBe(false);

    await queue.pause();

    expect(await queue.isPaused()).toBe(true);
    expect(await queue.isPaused({ localOnly: true })).toBe(true);
    expect(await redis.get(`${keyPrefix}:paused`)).toBe("1");
  });

  test("resume() clears local flag and deletes Redis key, isPaused() returns false", async () => {
    await queue.pause();
    expect(await queue.isPaused()).toBe(true);

    await queue.resume();

    expect(await queue.isPaused()).toBe(false);
    expect(await queue.isPaused({ localOnly: true })).toBe(false);
    expect(await redis.get(`${keyPrefix}:paused`)).toBeNull();
  });

  test("pause() and resume() are idempotent and emit events only on state transitions", async () => {
    const pausedEvents = [];
    const resumedEvents = [];

    queue.on("paused", () => pausedEvents.push(Date.now()));
    queue.on("resumed", () => resumedEvents.push(Date.now()));

    // Repeated pause calls
    await queue.pause();
    await queue.pause();
    await queue.pause();

    expect(pausedEvents.length).toBe(1);
    expect(await queue.isPaused()).toBe(true);

    // Repeated resume calls
    await queue.resume();
    await queue.resume();
    await queue.resume();

    expect(resumedEvents.length).toBe(1);
    expect(await queue.isPaused()).toBe(false);
  });

  test("safely emits paused and resumed events without crashing on listener errors", async () => {
    let errorEventReceived = null;
    queue.on("error", (err) => {
      errorEventReceived = err;
    });

    queue.on("paused", () => {
      throw new Error("listener boom on paused");
    });

    queue.on("resumed", async () => {
      throw new Error("async listener rejection on resumed");
    });

    await queue.pause();
    expect(errorEventReceived).not.toBeNull();
    expect(errorEventReceived.message).toBe("listener boom on paused");

    errorEventReceived = null;
    await queue.resume();

    await waitForCondition(() => errorEventReceived !== null);
    expect(errorEventReceived.message).toBe("async listener rejection on resumed");
  });

  test("stop dequeuing new jobs while paused, but allow active jobs to finish normally", async () => {
    let activeStarted = false;
    let finishActiveJob;
    const activeGate = new Promise((resolve) => {
      finishActiveJob = resolve;
    });

    const processedOrder = [];

    try {
      queue.process(
        async (job) => {
          if (job.data.id === "first-in-flight") {
            activeStarted = true;
            await activeGate;
          }
          processedOrder.push(job.data.id);
        },
        { concurrency: 2 }
      );

      await queue.enqueue({ id: "first-in-flight" });
      await waitForCondition(() => activeStarted);

      // Queue has active job in flight. Now pause!
      await queue.pause();

      // Enqueue second job while paused
      await queue.enqueue({ id: "queued-while-paused" });

      // Verify metrics while paused: 1 active, 1 waiting
      await waitForCondition(async () => {
        const m = await queue.getMetrics();
        return m.active === 1 && m.waiting === 1;
      });

      let metrics = await queue.getMetrics();
      expect(metrics.active).toBe(1);
      expect(metrics.waiting).toBe(1);

      // Allow in-flight active job to complete
      finishActiveJob();
      await waitForCondition(() => processedOrder.length === 1);

      // Active job has drained normally!
      expect(processedOrder).toEqual(["first-in-flight"]);

      // Wait a bit to verify second job is NOT dequeued while paused
      await new Promise((resolve) => setTimeout(resolve, 300));
      metrics = await queue.getMetrics();
      expect(metrics.active).toBe(0);
      expect(metrics.waiting).toBe(1);
      expect(processedOrder.length).toBe(1);

      // Now resume: the waiting job should immediately be dequeued and processed
      await queue.resume();
      await waitForCondition(() => processedOrder.length === 2);

      expect(processedOrder).toEqual(["first-in-flight", "queued-while-paused"]);
      metrics = await queue.getMetrics();
      expect(metrics.active).toBe(0);
      expect(metrics.waiting).toBe(0);
    } finally {
      if (typeof finishActiveJob === "function") {
        finishActiveJob();
      }
    }
  });

  test("enqueue() and enqueueMany() remain fully available while paused", async () => {
    await queue.pause();

    const id1 = await queue.enqueue({ task: "paused-enqueue-1" });
    const id2 = await queue.enqueue({ task: "paused-enqueue-2" });
    const batchIds = await queue.enqueueMany([
      { data: { task: "batch-1" } },
      { data: { task: "batch-2" } }
    ]);

    expect(typeof id1).toBe("string");
    expect(typeof id2).toBe("string");
    expect(batchIds.length).toBe(2);

    const metrics = await queue.getMetrics();
    expect(metrics.waiting).toBe(4);
    expect(metrics.active).toBe(0);
  });

  test("cross-worker coordination: Worker 1 pauses, Worker 2 stops dequeuing and resumes promptly", async () => {
    // Worker 1 and Worker 2 share the same keyPrefix
    const queue1 = new ResilientQueue({
      redisUrl,
      keyPrefix,
      pausePollInterval: 25
    });

    const queue2 = new ResilientQueue({
      redisUrl,
      keyPrefix,
      pausePollInterval: 25
    });

    const queue2Processed = [];
    const queue2PausedEvents = [];
    const queue2ResumedEvents = [];

    queue2.on("paused", () => queue2PausedEvents.push(Date.now()));
    queue2.on("resumed", () => queue2ResumedEvents.push(Date.now()));

    queue2.process(async (job) => {
      queue2Processed.push(job.data.num);
    });

    // 1. Process one job normally on queue2
    await queue1.enqueue({ num: 1 });
    await waitForCondition(() => queue2Processed.length === 1);
    expect(queue2Processed).toEqual([1]);

    // 2. Queue 1 pauses the distributed queue
    await queue1.pause();

    // 3. Enqueue jobs while paused
    await queue1.enqueue({ num: 2 });
    await queue1.enqueue({ num: 3 });

    // Wait and verify Queue 2 does NOT process them
    await new Promise((resolve) => setTimeout(resolve, 300));
    expect(queue2Processed.length).toBe(1);

    const metricsWhilePaused = await queue2.getMetrics();
    expect(metricsWhilePaused.waiting).toBe(2);
    expect(metricsWhilePaused.active).toBe(0);

    // Verify Queue 2 noticed the pause
    expect(await queue2.isPaused()).toBe(true);

    // 4. Queue 1 resumes the distributed queue
    await queue1.resume();

    // 5. Queue 2 should promptly resume and process both waiting jobs
    await waitForCondition(() => queue2Processed.length === 3, 3000);
    expect(queue2Processed.sort()).toEqual([1, 2, 3]);

    const metricsAfterResume = await queue2.getMetrics();
    expect(metricsAfterResume.waiting).toBe(0);
    expect(metricsAfterResume.active).toBe(0);

    await queue1.close();
    await queue2.close();
  });

  test("rolls back job popped during pause race condition without dropping or processing it", async () => {
    // Start worker
    const processed = [];
    queue.process(async (job) => {
      processed.push(job.data.id);
    });

    // Pause queue before any jobs are enqueued
    await queue.pause();

    // Enqueue jobs while paused
    await queue.enqueue({ id: "race-job-1" });

    // Wait to confirm it remains untouched
    await new Promise((resolve) => setTimeout(resolve, 200));
    expect(processed.length).toBe(0);
    expect((await queue.getMetrics()).waiting).toBe(1);

    // Resume queue
    await queue.resume();

    // Job processes now
    await waitForCondition(() => processed.length === 1);
    expect(processed).toEqual(["race-job-1"]);
  });

  test("handles Redis errors safely during pause and resume without leaving inconsistent state", async () => {
    const origSet = queue.producer.set.bind(queue.producer);
    queue.producer.set = async () => {
      throw new Error("Redis SET connection failure");
    };

    // pause() should throw the Redis error and revert local pause flag
    await expect(queue.pause()).rejects.toThrow("Redis SET connection failure");
    expect(queue._isLocallyPaused).toBe(false);

    queue.producer.set = origSet;

    // Normal pause succeeds
    await queue.pause();
    expect(queue._isLocallyPaused).toBe(true);

    // Now test resume error
    const origDel = queue.producer.del.bind(queue.producer);
    queue.producer.del = async () => {
      throw new Error("Redis DEL connection failure");
    };

    await expect(queue.resume()).rejects.toThrow("Redis DEL connection failure");
    expect(queue._isLocallyPaused).toBe(true);

    queue.producer.del = origDel;
    await queue.resume();
    expect(queue._isLocallyPaused).toBe(false);
  });

  test("isPaused() safely falls back to local state on Redis error", async () => {
    const origGet = queue.producer.get.bind(queue.producer);
    queue.producer.get = async () => {
      throw new Error("Redis GET connection failure");
    };

    // By default not paused locally
    expect(await queue.isPaused()).toBe(false);

    // Set local flag
    queue._isLocallyPaused = true;
    expect(await queue.isPaused()).toBe(true);

    queue.producer.get = origGet;
    queue._isLocallyPaused = false;
  });

  test("pause() and resume() throw clear error when queue is closed", async () => {
    await queue.close();

    await expect(queue.pause()).rejects.toThrow("Queue is closed");
    await expect(queue.resume()).rejects.toThrow("Queue is closed");
  });

  test("close() cleans up pause waiters, timers, and local state cleanly", async () => {
    queue.process(async () => {});

    await queue.pause();

    // Verify waiters exist while paused
    await new Promise((resolve) => setTimeout(resolve, 50));

    // Close queue while in paused wait state
    await queue.close();

    expect(queue.isClosed).toBe(true);
    expect(queue._resumeWaiters.size).toBe(0);
    expect(queue._isLocallyPaused).toBe(false);
    expect(await queue.isPaused()).toBe(false);
  });

  test("works on producer-only instances without process()", async () => {
    const producerOnlyQueue = new ResilientQueue({
      redisUrl,
      keyPrefix: "rq:test:producer-only"
    });

    expect(await producerOnlyQueue.isPaused()).toBe(false);

    await producerOnlyQueue.pause();
    expect(await producerOnlyQueue.isPaused()).toBe(true);

    await producerOnlyQueue.enqueue({ data: 123 });
    const metrics = await producerOnlyQueue.getMetrics();
    expect(metrics.waiting).toBe(1);

    await producerOnlyQueue.resume();
    expect(await producerOnlyQueue.isPaused()).toBe(false);

    await producerOnlyQueue.close();
  });

  test("concurrent pause() and resume() calls emit events exactly once", async () => {
    const pausedEvents = [];
    const resumedEvents = [];

    queue.on("paused", () => pausedEvents.push(Date.now()));
    queue.on("resumed", () => resumedEvents.push(Date.now()));

    // Concurrent pause calls
    await Promise.all([
      queue.pause(),
      queue.pause(),
      queue.pause()
    ]);

    expect(pausedEvents.length).toBe(1);
    expect(await queue.isPaused()).toBe(true);

    // Concurrent resume calls
    await Promise.all([
      queue.resume(),
      queue.resume(),
      queue.resume()
    ]);

    expect(resumedEvents.length).toBe(1);
    expect(await queue.isPaused()).toBe(false);
  });

  test("Redis cluster hash slot parity for all queue keys when using hash tags", async () => {
    const calculateSlot = require("cluster-key-slot");
    const clusterPrefix = "{my-cluster-queue}";
    const clusterQueue = new ResilientQueue({
      redisUrl,
      keyPrefix: clusterPrefix
    });

    try {
      const mainSlot = calculateSlot(clusterQueue.mainQueueKey);
      const activeSlot = calculateSlot(clusterQueue.activeQueueKey);
      const delayedSlot = calculateSlot(clusterQueue.delayedQueueKey);
      const dlqSlot = calculateSlot(clusterQueue.dlqKey);
      const pausedSlot = calculateSlot(clusterQueue.pausedKey);

      expect(mainSlot).toBe(activeSlot);
      expect(activeSlot).toBe(delayedSlot);
      expect(delayedSlot).toBe(dlqSlot);
      expect(dlqSlot).toBe(pausedSlot);
    } finally {
      await clusterQueue.close();
    }
  });

  test("sanitizes pausePollInterval and uses default when invalid", async () => {
    const defaultQueue = new ResilientQueue({
      redisUrl,
      keyPrefix: "rq:test:default-poll",
      pausePollInterval: -10
    });

    const nanQueue = new ResilientQueue({
      redisUrl,
      keyPrefix: "rq:test:nan-poll",
      pausePollInterval: "not-a-number"
    });

    const customQueue = new ResilientQueue({
      redisUrl,
      keyPrefix: "rq:test:custom-poll",
      pausePollInterval: 123
    });

    expect(defaultQueue._pausePollInterval).toBe(50);
    expect(nanQueue._pausePollInterval).toBe(50);
    expect(customQueue._pausePollInterval).toBe(123);

    await defaultQueue.close();
    await nanQueue.close();
    await customQueue.close();
  });
});

