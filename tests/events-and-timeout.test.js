const {
  ResilientQueue,
  RetryableError,
  FatalError,
  JobTimeoutError
} = require("../src");

const Redis = require("ioredis");
const { EventEmitter } = require("events");

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

describe("Event Emitter Observability Hooks", () => {
  let queue;
  let redis;

  beforeAll(async () => {
    redis = new Redis(redisUrl);
  });

  beforeEach(async () => {
    await redis.flushall();
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

  test("ResilientQueue extends EventEmitter", () => {
    queue = new ResilientQueue({ redisUrl });
    expect(queue).toBeInstanceOf(EventEmitter);
    expect(typeof queue.on).toBe("function");
    expect(typeof queue.emit).toBe("function");
  });

  test("emits completed(job, result) upon successful job execution", async () => {
    queue = new ResilientQueue({ redisUrl, baseDelay: 50 });

    const completedEvents = [];
    queue.on("completed", (job, result) => {
      completedEvents.push({ job, result });
    });

    queue.process(async (job) => {
      return { processedOrderId: job.data.orderId, status: "ok" };
    });

    await queue.enqueue({ orderId: "ORD-999" });

    await waitForCondition(() => completedEvents.length === 1);

    expect(completedEvents.length).toBe(1);
    expect(completedEvents[0].job.data.orderId).toBe("ORD-999");
    expect(completedEvents[0].result).toEqual({ processedOrderId: "ORD-999", status: "ok" });
  });

  test("emits failed(job, error) and retrying(job, error, attempt, delayMs) on retryable failures", async () => {
    queue = new ResilientQueue({
      redisUrl,
      maxRetries: 2,
      baseDelay: 100
    });

    const failedEvents = [];
    const retryingEvents = [];

    queue.on("failed", (job, error) => {
      failedEvents.push({ job, error });
    });

    queue.on("retrying", (job, error, attempt, delayMs) => {
      retryingEvents.push({ job, error, attempt, delayMs });
    });

    let attempts = 0;
    queue.process(async (job) => {
      attempts++;
      throw new RetryableError("transient database glitch");
    });

    await queue.enqueue({ task: "retry-test" });

    await waitForCondition(() => retryingEvents.length >= 1);

    expect(failedEvents.length).toBeGreaterThanOrEqual(1);
    expect(failedEvents[0].job.data.task).toBe("retry-test");
    expect(failedEvents[0].error.message).toBe("transient database glitch");

    expect(retryingEvents.length).toBeGreaterThanOrEqual(1);
    expect(retryingEvents[0].attempt).toBe(1);
    expect(retryingEvents[0].delayMs).toBe(200); // 100 * 2^1
    expect(retryingEvents[0].error.message).toBe("transient database glitch");
  });

  test("emits dlq(job, reason) when a job fails with FatalError", async () => {
    queue = new ResilientQueue({ redisUrl, maxRetries: 3 });

    const dlqEvents = [];
    const failedEvents = [];

    queue.on("dlq", (job, reason) => {
      dlqEvents.push({ job, reason });
    });
    queue.on("failed", (job, error) => {
      failedEvents.push({ job, error });
    });

    queue.process(async () => {
      throw new FatalError("invalid schema payload");
    });

    await queue.enqueue({ task: "fatal-dlq-test" });

    await waitForCondition(() => dlqEvents.length === 1);

    expect(failedEvents.length).toBe(1);
    expect(failedEvents[0].error).toBeInstanceOf(FatalError);

    expect(dlqEvents.length).toBe(1);
    expect(dlqEvents[0].job.data.task).toBe("fatal-dlq-test");
    expect(dlqEvents[0].reason).toBeInstanceOf(FatalError);
    expect(dlqEvents[0].reason.message).toBe("invalid schema payload");
  });

  test("emits dlq(job, reason) when retries are exhausted", async () => {
    queue = new ResilientQueue({
      redisUrl,
      maxRetries: 1,
      baseDelay: 50
    });

    const dlqEvents = [];
    const retryingEvents = [];

    queue.on("dlq", (job, reason) => {
      dlqEvents.push({ job, reason });
    });
    queue.on("retrying", (job, error, attempt, delayMs) => {
      retryingEvents.push({ attempt, delayMs });
    });

    queue.process(async () => {
      throw new RetryableError("persistent retryable failure");
    });

    await queue.enqueue({ task: "exhaust-retries" });

    await waitForCondition(() => dlqEvents.length === 1);

    expect(retryingEvents.length).toBe(1);
    expect(retryingEvents[0].attempt).toBe(1);

    expect(dlqEvents.length).toBe(1);
    expect(dlqEvents[0].job.attempt).toBe(2);
    expect(dlqEvents[0].reason.message).toBe("persistent retryable failure");
  });

  test("emits stalled(job) when a stalled job is recovered to main queue", async () => {
    queue = new ResilientQueue({
      redisUrl,
      maxRetries: 2,
      baseDelay: 50,
      lockDuration: 300,
      stalledInterval: 100
    });

    const crashedJob = {
      id: "stalled-job-recovery-test",
      data: { invoiceId: "INV-123" },
      attempt: 0,
      stalledCount: 0,
      idempotencyKey: null,
      ttlSeconds: null,
      createdAt: Date.now()
    };

    await redis.rpush("rq:active", JSON.stringify(crashedJob));
    await redis.zadd("rq:locks", Date.now() - 500, crashedJob.id);

    const stalledEvents = [];
    queue.on("stalled", (job) => {
      stalledEvents.push(job);
    });

    let processedJob = null;
    queue.process(async (job) => {
      processedJob = job;
    });

    await waitForCondition(() => stalledEvents.length === 1 && processedJob !== null);

    expect(stalledEvents.length).toBe(1);
    expect(stalledEvents[0].id).toBe("stalled-job-recovery-test");
    expect(stalledEvents[0].stalledCount).toBe(1);
    expect(stalledEvents[0].data.invoiceId).toBe("INV-123");
  });

  test("emits dlq(job, reason) when a stalled poison pill exceeds maxStalledCount", async () => {
    queue = new ResilientQueue({
      redisUrl,
      maxRetries: 2,
      baseDelay: 50,
      lockDuration: 300,
      stalledInterval: 100,
      maxStalledCount: 1
    });

    const poisonPill = {
      id: "poison-pill-test",
      data: { bad: true },
      attempt: 0,
      stalledCount: 1,
      idempotencyKey: null,
      ttlSeconds: null,
      createdAt: Date.now()
    };

    await redis.rpush("rq:active", JSON.stringify(poisonPill));
    await redis.zadd("rq:locks", Date.now() - 500, poisonPill.id);

    const dlqEvents = [];
    queue.on("dlq", (job, reason) => {
      dlqEvents.push({ job, reason });
    });

    queue.process(async () => {});

    await waitForCondition(() => dlqEvents.length === 1);

    expect(dlqEvents.length).toBe(1);
    expect(dlqEvents[0].job.id).toBe("poison-pill-test");
    expect(String(dlqEvents[0].reason)).toContain("Job stalled more than maxStalledCount times");
  });

  test("listener synchronous errors do not corrupt processing or fail the queue", async () => {
    queue = new ResilientQueue({ redisUrl, baseDelay: 50 });

    let processedCount = 0;
    queue.on("completed", () => {
      throw new Error("unruly synchronous listener error");
    });

    queue.process(async () => {
      processedCount++;
    });

    await queue.enqueue({ item: 1 });
    await queue.enqueue({ item: 2 });

    await waitForCondition(async () => {
      return processedCount === 2 && (await redis.llen("rq:active")) === 0;
    });

    expect(processedCount).toBe(2);
    expect(await redis.llen("rq:active")).toBe(0);
    expect(await redis.llen("rq:main")).toBe(0);
  });

  test("listener async rejections are safely caught and do not cause unhandled rejections", async () => {
    queue = new ResilientQueue({ redisUrl, baseDelay: 50 });

    let errorEventReceived = null;
    queue.on("error", (err) => {
      errorEventReceived = err;
    });

    queue.on("completed", async () => {
      throw new Error("async listener failure");
    });

    let processed = false;
    queue.process(async () => {
      processed = true;
    });

    await queue.enqueue({ item: 1 });

    await waitForCondition(() => processed && errorEventReceived !== null);

    expect(processed).toBe(true);
    expect(errorEventReceived.message).toBe("async listener failure");
  });

  test("async 'error' listeners that reject do not trigger unhandled promise rejections", async () => {
    queue = new ResilientQueue({ redisUrl, baseDelay: 50 });

    let unhandledRejectionDetected = null;
    const onUnhandledRejection = (reason) => {
      unhandledRejectionDetected = reason;
    };
    process.on("unhandledRejection", onUnhandledRejection);

    let errorListenerRan = false;
    queue.on("error", async () => {
      errorListenerRan = true;
      throw new Error("async error listener rejection");
    });

    queue.on("completed", () => {
      throw new Error("synchronous listener failure");
    });

    let processed = false;
    queue.process(async () => {
      processed = true;
    });

    try {
      await queue.enqueue({ item: "async-error-listener-test" });
      await waitForCondition(() => processed && errorListenerRan);
      await new Promise((resolve) => setTimeout(resolve, 200));

      expect(processed).toBe(true);
      expect(errorListenerRan).toBe(true);
      expect(unhandledRejectionDetected).toBeNull();
    } finally {
      process.removeListener("unhandledRejection", onUnhandledRejection);
    }
  });

  test("prevents duplicate event emissions for the same execution", async () => {
    queue = new ResilientQueue({ redisUrl });

    let completedCount = 0;
    queue.on("completed", () => {
      completedCount++;
    });

    queue.process(async () => {
      return "done";
    });

    await queue.enqueue({ item: "single" });

    await waitForCondition(() => completedCount === 1);
    await new Promise((r) => setTimeout(r, 200));

    expect(completedCount).toBe(1);
  });
});

describe("Job Execution Timeout", () => {
  let queue;
  let redis;

  beforeAll(async () => {
    redis = new Redis(redisUrl);
  });

  beforeEach(async () => {
    await redis.flushall();
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

  test("JobTimeoutError class hierarchy and properties", () => {
    const err = new JobTimeoutError("Execution exceeded 250ms limit", 250);

    expect(err).toBeInstanceOf(JobTimeoutError);
    expect(err).toBeInstanceOf(RetryableError);
    expect(err).toBeInstanceOf(Error);
    expect(err.name).toBe("JobTimeoutError");
    expect(err.message).toBe("Execution exceeded 250ms limit");
    expect(err.timeoutMs).toBe(250);
  });

  test("validates jobTimeout option strictly across queue, process, and enqueue", async () => {
    expect(() => new ResilientQueue({ redisUrl, jobTimeout: -50 })).toThrow(
      "jobTimeout must be a positive number"
    );
    expect(() => new ResilientQueue({ redisUrl, jobTimeout: 0 })).toThrow(
      "jobTimeout must be a positive number"
    );
    expect(() => new ResilientQueue({ redisUrl, jobTimeout: "invalid" })).toThrow(
      "jobTimeout must be a positive number"
    );

    queue = new ResilientQueue({ redisUrl });

    await expect(queue.process(async () => {}, { jobTimeout: -100 })).rejects.toThrow(
      "jobTimeout must be a positive number"
    );

    await expect(queue.enqueue({ test: 1 }, { jobTimeout: 0 })).rejects.toThrow(
      "jobTimeout must be a positive number"
    );
  });

  test("times out slow job using queue-level jobTimeout and routes through retry pipeline", async () => {
    queue = new ResilientQueue({
      redisUrl,
      jobTimeout: 100,
      maxRetries: 1,
      baseDelay: 100
    });

    const failedEvents = [];
    const retryingEvents = [];
    const dlqEvents = [];

    queue.on("failed", (job, error) => {
      failedEvents.push({ job, error });
    });
    queue.on("retrying", (job, error, attempt, delayMs) => {
      retryingEvents.push({ job, error, attempt, delayMs });
    });
    queue.on("dlq", (job, reason) => {
      dlqEvents.push({ job, reason });
    });

    let runCount = 0;
    queue.process(async (job) => {
      runCount++;
      // Sleep longer than the 100ms timeout
      await new Promise((resolve) => setTimeout(resolve, 400));
    });

    await queue.enqueue({ item: "timeout-retry" });

    // Should timeout first attempt -> schedule retry -> timeout second attempt -> route to DLQ
    await waitForCondition(() => dlqEvents.length === 1, 8000);

    expect(failedEvents.length).toBe(2);
    expect(failedEvents[0].error).toBeInstanceOf(JobTimeoutError);
    expect(failedEvents[0].error.message).toContain("100ms");

    expect(retryingEvents.length).toBe(1);
    expect(retryingEvents[0].attempt).toBe(1);

    expect(dlqEvents.length).toBe(1);
    expect(dlqEvents[0].reason).toBeInstanceOf(JobTimeoutError);
    expect(await queue.getDLQCount()).toBe(1);
  });

  test("worker-level jobTimeout overrides queue-level default", async () => {
    queue = new ResilientQueue({
      redisUrl,
      jobTimeout: 2000, // queue default = 2s
      maxRetries: 0
    });

    const failedEvents = [];
    queue.on("failed", (job, error) => {
      failedEvents.push(error);
    });

    queue.process(
      async () => {
        // Sleep 300ms, should timeout because worker jobTimeout is 100ms
        await new Promise((resolve) => setTimeout(resolve, 300));
      },
      { jobTimeout: 100 }
    );

    const startTime = Date.now();
    await queue.enqueue({ test: "worker-override" });

    await waitForCondition(() => failedEvents.length === 1);
    const elapsed = Date.now() - startTime;

    expect(failedEvents[0]).toBeInstanceOf(JobTimeoutError);
    expect(failedEvents[0].message).toContain("100ms");
    expect(elapsed).toBeLessThan(1500); // Definitely completed around 100ms, not 2000ms
  });

  test("per-job jobTimeout overrides worker and queue defaults", async () => {
    queue = new ResilientQueue({
      redisUrl,
      jobTimeout: 2000,
      maxRetries: 0
    });

    const failedEvents = [];
    queue.on("failed", (job, error) => {
      failedEvents.push(error);
    });

    queue.process(
      async () => {
        await new Promise((resolve) => setTimeout(resolve, 300));
      },
      { jobTimeout: 1500 }
    );

    await queue.enqueue({ test: "per-job-override" }, { jobTimeout: 100 });

    await waitForCondition(() => failedEvents.length === 1);

    expect(failedEvents[0]).toBeInstanceOf(JobTimeoutError);
    expect(failedEvents[0].message).toContain("100ms");
  });

  test("supports AbortSignal so handlers can cancel ongoing operations on timeout", async () => {
    queue = new ResilientQueue({
      redisUrl,
      jobTimeout: 100,
      maxRetries: 0
    });

    let abortedOnTimeout = false;
    let abortReason = null;

    queue.process(async (job) => {
      await new Promise((resolve, reject) => {
        const timer = setTimeout(resolve, 1000);

        job.signal.addEventListener("abort", () => {
          clearTimeout(timer);
          abortedOnTimeout = true;
          abortReason = job.signal.reason;
          reject(job.signal.reason);
        });
      });
    });

    await queue.enqueue({ test: "cancellation" });

    await waitForCondition(() => abortedOnTimeout);

    expect(abortedOnTimeout).toBe(true);
    expect(abortReason).toBeInstanceOf(JobTimeoutError);
    expect(abortReason.name).toBe("JobTimeoutError");
    expect(abortReason.timeoutMs).toBe(100);
    expect(abortReason.message).toContain("100ms");
  });

  test("clears timers when job finishes before timeout expires", async () => {
    queue = new ResilientQueue({
      redisUrl,
      jobTimeout: 500
    });

    let completed = false;
    queue.on("completed", () => {
      completed = true;
    });

    queue.process(async () => {
      // Completes well within 500ms
      await new Promise((resolve) => setTimeout(resolve, 50));
      return "fast-success";
    });

    await queue.enqueue({ test: "fast-job" });

    await waitForCondition(() => completed);

    // Wait extra to confirm no late timeout fires
    await new Promise((resolve) => setTimeout(resolve, 600));

    expect(completed).toBe(true);
    expect(await queue.getDLQCount()).toBe(0);
  });

  test("safe handling of late handler resolution: timed-out job is not marked completed", async () => {
    queue = new ResilientQueue({
      redisUrl,
      jobTimeout: 80,
      maxRetries: 0
    });

    let completedFired = false;
    const dlqEvents = [];

    queue.on("completed", () => {
      completedFired = true;
    });

    queue.on("dlq", (job, reason) => {
      dlqEvents.push({ job, reason });
    });

    let handlerFinished = false;
    queue.process(async () => {
      // Non-abortable operation takes 250ms
      await new Promise((resolve) => setTimeout(resolve, 250));
      handlerFinished = true;
      return "late-result";
    });

    await queue.enqueue({ test: "late-resolution" });

    await waitForCondition(() => dlqEvents.length === 1);

    // Wait for the late handler to finish its internal execution
    await waitForCondition(() => handlerFinished);
    await new Promise((resolve) => setTimeout(resolve, 100));

    // Must NOT have emitted completed
    expect(completedFired).toBe(false);
    expect(dlqEvents.length).toBe(1);
    expect(dlqEvents[0].reason).toBeInstanceOf(JobTimeoutError);
  });

  test("safe handling of late handler rejection: no unhandled rejection or duplicate failed event", async () => {
    queue = new ResilientQueue({
      redisUrl,
      jobTimeout: 80,
      maxRetries: 0
    });

    const failedEvents = [];
    queue.on("failed", (job, err) => {
      failedEvents.push(err);
    });

    let handlerThrew = false;
    queue.process(async () => {
      await new Promise((resolve) => setTimeout(resolve, 250));
      handlerThrew = true;
      throw new Error("late unhandled error from slow query");
    });

    await queue.enqueue({ test: "late-rejection" });

    await waitForCondition(() => failedEvents.length === 1);
    await waitForCondition(() => handlerThrew);
    await new Promise((resolve) => setTimeout(resolve, 100));

    // Only the timeout failure was emitted, not the late secondary error
    expect(failedEvents.length).toBe(1);
    expect(failedEvents[0]).toBeInstanceOf(JobTimeoutError);
  });

  test("prevents timed-out jobs from unsafe lock release while handler is executing", async () => {
    queue = new ResilientQueue({
      redisUrl,
      jobTimeout: 80,
      maxRetries: 0
    });

    let handlerStarted = false;
    let handlerFinished = false;

    queue.process(async () => {
      handlerStarted = true;
      await new Promise((resolve) => setTimeout(resolve, 300));
      handlerFinished = true;
    });

    await queue.enqueue({ test: "unsafe-release-test" });

    await waitForCondition(() => handlerStarted);

    // Give timeout time to fire (at ~80ms)
    await new Promise((resolve) => setTimeout(resolve, 120));

    // While handler is still sleeping (before 300ms), lock should NOT be prematurely released
    // active queue should still track the in-flight job until the handler promise settles
    expect(handlerFinished).toBe(false);
    expect(await redis.llen("rq:active")).toBe(1);

    // Wait for handler to completely finish
    await waitForCondition(() => handlerFinished);
    await new Promise((resolve) => setTimeout(resolve, 100));

    // Now cleanly released
    expect(await redis.llen("rq:active")).toBe(0);
  });

  test("preserves exact legacy behavior when timeouts are disabled", async () => {
    queue = new ResilientQueue({
      redisUrl
      // jobTimeout omitted / disabled
    });

    let processedJob = null;
    queue.process(async (job) => {
      // Long running operation
      await new Promise((resolve) => setTimeout(resolve, 300));
      processedJob = job;
      return "success";
    });

    await queue.enqueue({ task: "no-timeout" });

    await waitForCondition(() => processedJob !== null);

    expect(processedJob).not.toBeNull();
    expect(processedJob.data.task).toBe("no-timeout");
    expect(processedJob.isTimedOut).toBeUndefined();
    expect(await queue.getDLQCount()).toBe(0);
  });

  test("retried jobs are never executed concurrently while a timed-out handler is still finishing", async () => {
    queue = new ResilientQueue({
      redisUrl,
      jobTimeout: 100,
      maxRetries: 1,
      baseDelay: 50
    });

    let activeExecutions = 0;
    let maxConcurrentObserved = 0;
    const executionLogs = [];
    let completedRuns = 0;

    queue.process(
      async (job) => {
        activeExecutions++;
        if (activeExecutions > maxConcurrentObserved) {
          maxConcurrentObserved = activeExecutions;
        }

        const runIndex = executionLogs.length;
        executionLogs.push({
          runIndex,
          attempt: job.attempt,
          startedAt: Date.now()
        });

        if (runIndex === 0) {
          // Slow first attempt that runs longer than timeout and does not abort early
          await new Promise((resolve) => setTimeout(resolve, 350));
          // Verify that job.signal was aborted with JobTimeoutError
          expect(job.signal.aborted).toBe(true);
          expect(job.signal.reason).toBeInstanceOf(JobTimeoutError);
          expect(job.signal.reason.timeoutMs).toBe(100);
        } else {
          // Fast second attempt
          await new Promise((resolve) => setTimeout(resolve, 30));
        }

        activeExecutions--;
        completedRuns++;
      },
      { concurrency: 2 }
    );

    await queue.enqueue({ item: "timeout-concurrency-safety" });

    // Wait until both executions complete
    await waitForCondition(() => completedRuns >= 2, 8000);

    expect(maxConcurrentObserved).toBe(1);
    expect(activeExecutions).toBe(0);
    expect(executionLogs.length).toBe(2);
    expect(executionLogs[0].attempt).toBe(0);
    expect(executionLogs[1].attempt).toBe(1);
  });

  test("heartbeat remains active during timeout settlement preventing false stalled recovery", async () => {
    queue = new ResilientQueue({
      redisUrl,
      jobTimeout: 80,
      lockDuration: 300,
      stalledInterval: 60,
      maxRetries: 0
    });

    const stalledEvents = [];
    const dlqEvents = [];

    queue.on("stalled", (job) => {
      stalledEvents.push(job);
    });
    queue.on("dlq", (job, reason) => {
      dlqEvents.push({ job, reason });
    });

    let handlerFinished = false;
    queue.process(async () => {
      // Handler runs for 320ms, exceeding both jobTimeout (80ms) and initial lockDuration (300ms)
      // while settling within maxWaitMs (300ms from timeout = 380ms total)
      await new Promise((resolve) => setTimeout(resolve, 320));
      handlerFinished = true;
    });

    await queue.enqueue({ item: "stalled-timeout-heartbeat" });

    await waitForCondition(() => dlqEvents.length === 1, 8000);
    expect(handlerFinished).toBe(true);

    // Heartbeat was kept alive during the 320ms execution, so no stalled job recovery was falsely triggered
    expect(stalledEvents.length).toBe(0);
    expect(dlqEvents.length).toBe(1);
    expect(dlqEvents[0].reason).toBeInstanceOf(JobTimeoutError);
  });
});
