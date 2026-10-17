const Redis = require("ioredis");
const {
  ResilientQueue,
  RetryableError,
  FatalError
} = require("../src");
const RetryManager = require("../src/managers/RetryManager");
const DLQManager = require("../src/managers/DLQManager");

const redisUrl = "redis://127.0.0.1:6379";

jest.setTimeout(30000);

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

describe("Configurable Retry Backoff Engine", () => {
  let redis;
  let queue;

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

  describe("Strategy Delay Calculations", () => {
    let mockDlqManager;
    let retryManager;

    beforeEach(() => {
      mockDlqManager = new DLQManager(redis, "test-rq");
      retryManager = new RetryManager(redis, mockDlqManager, {
        baseDelay: 100,
        maxDelay: 60000
      });
    });

    test("exponential strategy (default) calculates baseDelay * 2^attempt", async () => {
      const job = { id: "job-1", attempt: 0 };
      const err = new RetryableError("transient");

      const delay1 = await retryManager.calculateDelay(1, err, job);
      const delay2 = await retryManager.calculateDelay(2, err, job);
      const delay3 = await retryManager.calculateDelay(3, err, job);

      expect(delay1).toBe(200); // 100 * 2^1
      expect(delay2).toBe(400); // 100 * 2^2
      expect(delay3).toBe(800); // 100 * 2^3
    });

    test("fixed strategy returns constant baseDelay regardless of attempt", async () => {
      const fixedManager = new RetryManager(redis, mockDlqManager, {
        strategy: "fixed",
        baseDelay: 350
      });

      const job = { id: "job-fixed", attempt: 0 };
      const err = new RetryableError("transient");

      expect(await fixedManager.calculateDelay(1, err, job)).toBe(350);
      expect(await fixedManager.calculateDelay(2, err, job)).toBe(350);
      expect(await fixedManager.calculateDelay(3, err, job)).toBe(350);
      expect(await fixedManager.calculateDelay(5, err, job)).toBe(350);
    });

    test("custom strategy invokes fn(attempt, error, job) and uses returned delay", async () => {
      const mockFn = jest.fn((attempt, error, job) => attempt * 300);

      const customManager = new RetryManager(redis, mockDlqManager, {
        strategy: "custom",
        fn: mockFn
      });

      const testJob = { id: "job-custom", data: { foo: "bar" } };
      const testErr = new RetryableError("rate limited");

      const delay = await customManager.calculateDelay(2, testErr, testJob);

      expect(delay).toBe(600);
      expect(mockFn).toHaveBeenCalledWith(2, testErr, testJob);
    });

    test("custom strategy supports async function returning a promise", async () => {
      const customManager = new RetryManager(redis, mockDlqManager, {
        strategy: "custom",
        fn: async (attempt) => {
          await new Promise((resolve) => setTimeout(resolve, 10));
          return attempt * 500;
        }
      });

      const delay = await customManager.calculateDelay(3, new RetryableError(), { id: "async-job" });
      expect(delay).toBe(1500);
    });

    test("custom strategy supports dynamic retry policies like HTTP Retry-After headers", async () => {
      const customManager = new RetryManager(redis, mockDlqManager, {
        strategy: "custom",
        fn: (attempt, error) => {
          if (error.response?.headers?.["retry-after"]) {
            return parseInt(error.response.headers["retry-after"], 10) * 1000;
          }
          return 500;
        }
      });

      const http429Error = new RetryableError("Too Many Requests");
      http429Error.response = { headers: { "retry-after": "5" } };

      const delay = await customManager.calculateDelay(1, http429Error, { id: "job-http" });
      expect(delay).toBe(5000);
    });
  });

  describe("Jitter Bounds & Delay Caps", () => {
    let mockDlqManager;

    beforeEach(() => {
      mockDlqManager = new DLQManager(redis, "test-rq");
    });

    test("exponential strategy with Equal Jitter (jitter: true) stays within [capped/2, capped]", async () => {
      const manager = new RetryManager(redis, mockDlqManager, {
        strategy: "exponential",
        baseDelay: 100,
        maxDelay: 60000,
        jitter: true
      });

      const job = { id: "job-jitter" };
      const err = new RetryableError();

      // For attempt 2, uncapped is 100 * 2^2 = 400.
      // Equal Jitter bounds: [400 / 2, 400] = [200, 400].
      const samples = [];
      for (let i = 0; i < 100; i++) {
        const delay = await manager.calculateDelay(2, err, job);
        samples.push(delay);
        expect(delay).toBeGreaterThanOrEqual(200);
        expect(delay).toBeLessThanOrEqual(400);
      }

      // Verify that values are non-deterministic (variance > 0)
      const uniqueValues = new Set(samples);
      expect(uniqueValues.size).toBeGreaterThan(10);
    });

    test("fixed strategy with jitter stays within ±20% [baseDelay * 0.8, baseDelay * 1.2]", async () => {
      const manager = new RetryManager(redis, mockDlqManager, {
        strategy: "fixed",
        baseDelay: 1000,
        jitter: true
      });

      const job = { id: "job-fixed-jitter" };
      const err = new RetryableError();

      const samples = [];
      for (let i = 0; i < 100; i++) {
        const delay = await manager.calculateDelay(1, err, job);
        samples.push(delay);
        expect(delay).toBeGreaterThanOrEqual(800);
        expect(delay).toBeLessThanOrEqual(1200);
      }

      // Verify randomness across the distribution
      const uniqueValues = new Set(samples);
      expect(uniqueValues.size).toBeGreaterThan(10);
    });

    test("exponential backoff respects default maxDelay of 60000ms", async () => {
      const manager = new RetryManager(redis, mockDlqManager, {
        baseDelay: 10000
        // default maxDelay: 60000
      });

      const job = { id: "job-cap" };
      const err = new RetryableError();

      // attempt 4: 10000 * 2^4 = 160000 -> capped at 60000
      const delay4 = await manager.calculateDelay(4, err, job);
      expect(delay4).toBe(60000);

      // attempt 10: 10000 * 2^10 = 10240000 -> capped at 60000
      const delay10 = await manager.calculateDelay(10, err, job);
      expect(delay10).toBe(60000);
    });

    test("exponential backoff respects custom maxDelay cap", async () => {
      const manager = new RetryManager(redis, mockDlqManager, {
        baseDelay: 500,
        maxDelay: 2500
      });

      const job = { id: "job-custom-cap" };
      const err = new RetryableError();

      // attempt 1: 500 * 2^1 = 1000
      expect(await manager.calculateDelay(1, err, job)).toBe(1000);

      // attempt 2: 500 * 2^2 = 2000
      expect(await manager.calculateDelay(2, err, job)).toBe(2000);

      // attempt 3: 500 * 2^3 = 4000 -> capped at 2500
      expect(await manager.calculateDelay(3, err, job)).toBe(2500);

      // attempt 5: 500 * 2^5 = 16000 -> capped at 2500
      expect(await manager.calculateDelay(5, err, job)).toBe(2500);
    });

    test("Equal Jitter on capped delays stays within [maxDelay/2, maxDelay]", async () => {
      const manager = new RetryManager(redis, mockDlqManager, {
        baseDelay: 1000,
        maxDelay: 4000,
        jitter: true
      });

      const job = { id: "job-cap-jitter" };
      const err = new RetryableError();

      // attempt 5: 1000 * 2^5 = 32000 -> capped at 4000.
      // Equal jitter bounds: [2000, 4000].
      for (let i = 0; i < 50; i++) {
        const delay = await manager.calculateDelay(5, err, job);
        expect(delay).toBeGreaterThanOrEqual(2000);
        expect(delay).toBeLessThanOrEqual(4000);
      }
    });
  });

  describe("Configuration Precedence (per-job > worker-level > queue-level)", () => {
    test("queue-level defaults are used when worker and job omit backoff configuration", async () => {
      queue = new ResilientQueue({
        redisUrl,
        backoff: {
          strategy: "fixed",
          baseDelay: 400
        }
      });

      const retryingEvents = [];
      queue.on("retrying", (job, error, attempt, delayMs) => {
        retryingEvents.push({ attempt, delayMs });
      });

      queue.process(async () => {
        throw new RetryableError("fail");
      });

      await queue.enqueue({ test: "queue-defaults" });
      await waitForCondition(() => retryingEvents.length >= 1);

      expect(retryingEvents[0].attempt).toBe(1);
      expect(retryingEvents[0].delayMs).toBe(400);
    });

    test("worker-level backoff overrides queue-level default", async () => {
      queue = new ResilientQueue({
        redisUrl,
        backoff: {
          strategy: "exponential",
          baseDelay: 100
        }
      });

      const retryingEvents = [];
      queue.on("retrying", (job, error, attempt, delayMs) => {
        retryingEvents.push({ attempt, delayMs });
      });

      // Worker specifies fixed strategy with 750ms delay
      queue.process(async () => {
        throw new RetryableError("worker override test");
      }, {
        backoff: {
          strategy: "fixed",
          baseDelay: 750
        }
      });

      await queue.enqueue({ test: "worker-override" });
      await waitForCondition(() => retryingEvents.length >= 1);

      expect(retryingEvents[0].attempt).toBe(1);
      expect(retryingEvents[0].delayMs).toBe(750);
    });

    test("per-job backoff overrides worker-level and queue-level defaults", async () => {
      queue = new ResilientQueue({
        redisUrl,
        backoff: {
          strategy: "exponential",
          baseDelay: 100
        }
      });

      const retryingEvents = [];
      queue.on("retrying", (job, error, attempt, delayMs) => {
        retryingEvents.push({ attempt, delayMs, id: job.id });
      });

      queue.process(async () => {
        throw new RetryableError("per job override test");
      }, {
        backoff: {
          strategy: "fixed",
          baseDelay: 500
        }
      });

      // Job specifies custom strategy
      const jobId = await queue.enqueue(
        { test: "job-override" },
        {
          backoff: {
            strategy: "custom",
            fn: (attempt) => attempt * 1111
          }
        }
      );

      await waitForCondition(() => retryingEvents.length >= 1);

      expect(retryingEvents[0].attempt).toBe(1);
      expect(retryingEvents[0].delayMs).toBe(1111);
    });

    test("partial backoff overrides properly inherit unset options from lower levels", async () => {
      queue = new ResilientQueue({
        redisUrl,
        backoff: {
          strategy: "fixed",
          baseDelay: 500
        }
      });

      const retryingEvents = [];
      queue.on("retrying", (job, error, attempt, delayMs) => {
        retryingEvents.push({ attempt, delayMs });
      });

      // Worker only overrides baseDelay; inherits strategy: "fixed" from queue
      queue.process(async () => {
        throw new RetryableError("partial override");
      }, {
        backoff: {
          baseDelay: 1500
        }
      });

      await queue.enqueue({ test: "partial" });
      await waitForCondition(() => retryingEvents.length >= 1);

      expect(retryingEvents[0].attempt).toBe(1);
      expect(retryingEvents[0].delayMs).toBe(1500); // Fixed strategy with worker's baseDelay
    });

    test("per-job maxRetries overrides worker and queue defaults", async () => {
      queue = new ResilientQueue({
        redisUrl,
        maxRetries: 5,
        baseDelay: 50
      });

      const dlqEvents = [];
      queue.on("dlq", (job, reason) => {
        dlqEvents.push({ job, reason });
      });

      queue.process(async () => {
        throw new RetryableError("per job max retries");
      }, {
        maxRetries: 4
      });

      // Per-job overrides maxRetries to 1
      await queue.enqueue(
        { test: "job-max-retries" },
        { maxRetries: 1 }
      );

      await waitForCondition(() => dlqEvents.length === 1);

      // Exhausted on attempt 2 (nextAttempt > 1)
      expect(dlqEvents[0].job.attempt).toBe(2);
    });
  });

  describe("Legacy Backward Compatibility", () => {
    test("legacy queue instantiation with baseDelay and maxRetries retains exact deterministic timing", async () => {
      queue = new ResilientQueue({
        redisUrl,
        baseDelay: 100,
        maxRetries: 2
      });

      const retryingEvents = [];
      queue.on("retrying", (job, error, attempt, delayMs) => {
        retryingEvents.push({ attempt, delayMs });
      });

      queue.process(async () => {
        throw new RetryableError("legacy failure");
      });

      await queue.enqueue({ test: "legacy-timing" });

      await waitForCondition(() => retryingEvents.length >= 1);

      expect(retryingEvents[0].attempt).toBe(1);
      expect(retryingEvents[0].delayMs).toBe(200); // 100 * 2^1 deterministic
    });

    test("legacy enqueue options with direct baseDelay work seamlessly", async () => {
      queue = new ResilientQueue({
        redisUrl,
        baseDelay: 100
      });

      const retryingEvents = [];
      queue.on("retrying", (job, error, attempt, delayMs) => {
        retryingEvents.push({ attempt, delayMs });
      });

      queue.process(async () => {
        throw new RetryableError("direct baseDelay");
      });

      await queue.enqueue({ test: "direct-delay" }, { baseDelay: 300 });

      await waitForCondition(() => retryingEvents.length >= 1);

      expect(retryingEvents[0].attempt).toBe(1);
      expect(retryingEvents[0].delayMs).toBe(600); // 300 * 2^1
    });
  });

  describe("Configuration & Runtime Validation", () => {
    test("validates strategy strictly in constructor, process, and enqueue", async () => {
      expect(() => new ResilientQueue({ redisUrl, strategy: "unknown" })).toThrow(
        'Invalid backoff strategy: "unknown". Must be "exponential", "fixed", or "custom"'
      );

      expect(() => new ResilientQueue({ redisUrl, backoff: { strategy: "invalid" } })).toThrow(
        'Invalid backoff strategy: "invalid". Must be "exponential", "fixed", or "custom"'
      );

      queue = new ResilientQueue({ redisUrl });

      await expect(
        queue.process(async () => {}, { strategy: "bogus" })
      ).rejects.toThrow('Invalid backoff strategy: "bogus". Must be "exponential", "fixed", or "custom"');

      await expect(
        queue.enqueue({ a: 1 }, { backoff: "nonexistent" })
      ).rejects.toThrow('Invalid backoff strategy: "nonexistent". Must be "exponential", "fixed", or "custom"');
    });

    test("validates custom strategy requires fn callback", () => {
      expect(() => new ResilientQueue({ redisUrl, strategy: "custom" })).toThrow(
        "Custom backoff strategy requires a fn callback function"
      );

      expect(() => new ResilientQueue({ redisUrl, backoff: { strategy: "custom" } })).toThrow(
        "Custom backoff strategy requires a fn callback function"
      );
    });

    test("validates numeric options strictly", () => {
      expect(() => new ResilientQueue({ redisUrl, baseDelay: -10 })).toThrow(
        "baseDelay must be a non-negative number"
      );

      expect(() => new ResilientQueue({ redisUrl, maxDelay: -5 })).toThrow(
        "maxDelay must be a non-negative number"
      );

      expect(() => new ResilientQueue({ redisUrl, jitter: "yes" })).toThrow(
        "jitter must be a boolean"
      );

      expect(() => new ResilientQueue({ redisUrl, maxRetries: -1 })).toThrow(
        "maxRetries must be a non-negative integer"
      );

      expect(() => new ResilientQueue({ redisUrl, maxRetries: 1.5 })).toThrow(
        "maxRetries must be a non-negative integer"
      );
    });

    test("safely handles custom fn throwing runtime error without scheduling invalid delay and routes to DLQ", async () => {
      queue = new ResilientQueue({
        redisUrl,
        baseDelay: 50,
        backoff: {
          strategy: "custom",
          fn: () => {
            throw new Error("Fatal bug in custom backoff calculation");
          }
        }
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
        throw new RetryableError("transient failure");
      });

      await queue.enqueue({ test: "custom-throw" });

      await waitForCondition(() => dlqEvents.length === 1);

      // Must NOT schedule a retry
      expect(retryingEvents.length).toBe(0);

      // Must route to DLQ with the error from the custom callback
      expect(dlqEvents.length).toBe(1);
      expect(dlqEvents[0].reason.message).toBe("Fatal bug in custom backoff calculation");

      // Verify DLQ in Redis
      const dlqCount = await queue.getDLQCount();
      expect(dlqCount).toBe(1);
    });

    test("safely handles custom fn returning rejected promise and routes to DLQ", async () => {
      queue = new ResilientQueue({
        redisUrl,
        backoff: {
          strategy: "custom",
          fn: async () => {
            throw new Error("Rejected promise from backoff fn");
          }
        }
      });

      const dlqEvents = [];
      queue.on("dlq", (job, reason) => {
        dlqEvents.push({ job, reason });
      });

      queue.process(async () => {
        throw new RetryableError("transient");
      });

      await queue.enqueue({ test: "rejected-custom" });

      await waitForCondition(() => dlqEvents.length === 1);
      expect(dlqEvents[0].reason.message).toBe("Rejected promise from backoff fn");
    });

    test("safely handles custom fn returning NaN, negative number, or non-number without scheduling invalid delay", async () => {
      queue = new ResilientQueue({
        redisUrl,
        backoff: {
          strategy: "custom",
          fn: () => NaN
        }
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
        throw new RetryableError("transient");
      });

      await queue.enqueue({ test: "nan-custom" });

      await waitForCondition(() => dlqEvents.length === 1);

      expect(retryingEvents.length).toBe(0);
      expect(dlqEvents[0].reason.message).toMatch(/Invalid retry delay/);
    });

    test("safely handles custom fn returning negative number", async () => {
      queue = new ResilientQueue({
        redisUrl,
        backoff: {
          strategy: "custom",
          fn: () => -500
        }
      });

      const dlqEvents = [];
      queue.on("dlq", (job, reason) => {
        dlqEvents.push({ job, reason });
      });

      queue.process(async () => {
        throw new RetryableError("transient");
      });

      await queue.enqueue({ test: "negative-custom" });

      await waitForCondition(() => dlqEvents.length === 1);
      expect(dlqEvents[0].reason.message).toMatch(/Invalid retry delay: -500/);
    });

    test("safely handles custom fn returning null or string", async () => {
      queue = new ResilientQueue({
        redisUrl,
        backoff: {
          strategy: "custom",
          fn: () => "invalid-delay"
        }
      });

      const dlqEvents = [];
      queue.on("dlq", (job, reason) => {
        dlqEvents.push({ job, reason });
      });

      queue.process(async () => {
        throw new RetryableError("transient");
      });

      await queue.enqueue({ test: "string-custom" });

      await waitForCondition(() => dlqEvents.length === 1);
      expect(dlqEvents[0].reason.message).toMatch(/Invalid retry delay/);
    });
  });

  describe("Integration with Delayed Jobs, Retry Promotion, and DLQ", () => {
    test("failed job is stored in Redis delayed sorted set with executeAt score matching delay", async () => {
      queue = new ResilientQueue({
        redisUrl,
        keyPrefix: "rq_int_test",
        backoff: {
          strategy: "fixed",
          baseDelay: 2000
        }
      });

      const retryingEvents = [];
      queue.on("retrying", (job, error, attempt, delayMs) => {
        retryingEvents.push({ job, delayMs });
      });

      queue.process(async () => {
        throw new RetryableError("delayed-zset-check");
      });

      const beforeEnqueue = Date.now();
      await queue.enqueue({ order: "check-score" });

      await waitForCondition(() => retryingEvents.length === 1);

      expect(retryingEvents[0].delayMs).toBe(2000);

      // Verify the delayed sorted set in Redis directly
      const delayedKey = "rq_int_test:delayed";
      const delayedJobs = await redis.zrangebyscore(delayedKey, "-inf", "+inf", "WITHSCORES");

      expect(delayedJobs.length).toBeGreaterThanOrEqual(2); // member and score
      const member = JSON.parse(delayedJobs[0]);
      const score = parseFloat(delayedJobs[1]);

      expect(member.data.order).toBe("check-score");
      expect(member.attempt).toBe(1);
      expect(score).toBeGreaterThanOrEqual(beforeEnqueue + 2000);
      expect(score).toBeLessThanOrEqual(Date.now() + 2000);
    });

    test("due retries are promoted to main queue and succeed on subsequent attempt", async () => {
      queue = new ResilientQueue({
        redisUrl,
        backoff: {
          strategy: "fixed",
          baseDelay: 100 // fast 100ms delay
        }
      });

      let attempts = 0;
      const completedEvents = [];

      queue.on("completed", (job, result) => {
        completedEvents.push({ job, result });
      });

      queue.process(async (job) => {
        attempts++;
        if (attempts === 1) {
          throw new RetryableError("first attempt fails");
        }
        return { success: true, attempts };
      });

      await queue.enqueue({ task: "succeed-on-retry" });

      await waitForCondition(() => completedEvents.length === 1, 6000);

      expect(attempts).toBe(2);
      expect(completedEvents[0].result).toEqual({ success: true, attempts: 2 });
    });

    test("jobs that exhaust retries under custom/fixed backoff route to DLQ and can be recovered", async () => {
      queue = new ResilientQueue({
        redisUrl,
        maxRetries: 2,
        backoff: {
          strategy: "fixed",
          baseDelay: 50
        }
      });

      const dlqEvents = [];
      queue.on("dlq", (job, reason) => {
        dlqEvents.push({ job, reason });
      });

      let executionCount = 0;
      let shouldFail = true;

      queue.process(async (job) => {
        executionCount++;
        if (shouldFail) {
          throw new RetryableError("always fails initially");
        }
        return { recovered: true };
      });

      const jobId = await queue.enqueue({ task: "dlq-recovery-test" });

      await waitForCondition(() => dlqEvents.length === 1);

      expect(dlqEvents[0].job.id).toBe(jobId);
      expect(dlqEvents[0].job.attempt).toBe(3); // 0 -> 1 -> 2 -> 3 (exhausted)

      const dlqCount = await queue.getDLQCount();
      expect(dlqCount).toBe(1);

      // Now recover the job from DLQ
      shouldFail = false;
      const retried = await queue.retryDLQJob(jobId);
      expect(retried).toBe(true);

      const completedEvents = [];
      queue.on("completed", (job, result) => {
        completedEvents.push({ job, result });
      });

      await waitForCondition(() => completedEvents.length === 1);
      expect(completedEvents[0].result).toEqual({ recovered: true });

      const finalDLQCount = await queue.getDLQCount();
      expect(finalDLQCount).toBe(0);
    });
  });

  describe("Boundary Cases, Memory Management & Distributed Fallbacks", () => {
    let mockDlqManager;

    beforeEach(() => {
      mockDlqManager = new DLQManager(redis, "test-rq");
    });

    test("handles baseDelay = 0 cleanly and guards against NaN on extreme attempt numbers", async () => {
      const manager = new RetryManager(redis, mockDlqManager, {
        strategy: "exponential",
        baseDelay: 0,
        maxDelay: 60000,
        jitter: false
      });

      const job = { id: "zero-delay-job" };
      const err = new RetryableError("zero delay");

      expect(await manager.calculateDelay(1, err, job)).toBe(0);
      // Math.pow(2, 1050) is Infinity; baseDelay === 0 must safely return 0 without NaN
      expect(await manager.calculateDelay(1050, err, job)).toBe(0);

      // With Equal Jitter, 0 delay remains 0
      const jitterManager = new RetryManager(redis, mockDlqManager, {
        strategy: "exponential",
        baseDelay: 0,
        maxDelay: 60000,
        jitter: true
      });
      expect(await jitterManager.calculateDelay(1, err, job)).toBe(0);
    });

    test("strictly caps delay when maxDelay is smaller than baseDelay", async () => {
      const manager = new RetryManager(redis, mockDlqManager, {
        strategy: "exponential",
        baseDelay: 5000,
        maxDelay: 200,
        jitter: false
      });

      const delay = await manager.calculateDelay(1, new RetryableError(), { id: "capped-low" });
      expect(delay).toBe(200);
    });

    test("handles very large retry attempt numbers without numerical overflow", async () => {
      const manager = new RetryManager(redis, mockDlqManager, {
        strategy: "exponential",
        baseDelay: 500,
        maxDelay: 60000,
        jitter: false
      });

      const delay = await manager.calculateDelay(1050, new RetryableError(), { id: "extreme-attempt" });
      expect(delay).toBe(60000);
    });

    test("supports custom backoff fn returning 0 for immediate retry", async () => {
      const manager = new RetryManager(redis, mockDlqManager, {
        strategy: "custom",
        fn: () => 0
      });

      const delay = await manager.calculateDelay(1, new RetryableError(), { id: "immediate-retry" });
      expect(delay).toBe(0);
    });

    test("in-memory custom callback Map is completely cleaned up after job completion, DLQ routing, and queue.close()", async () => {
      queue = new ResilientQueue({ redisUrl });

      // 1. Success cleanup
      const successJobId = await queue.enqueue(
        { task: "cleanup-success" },
        { backoff: { strategy: "custom", fn: () => 100 } }
      );
      expect(queue.customJobBackoffFns.has(successJobId)).toBe(true);

      const completedEvents = [];
      queue.on("completed", (job) => completedEvents.push(job.id));

      queue.process(async (job) => {
        if (job.data.task === "cleanup-success") {
          return "ok";
        }
        if (job.data.task === "cleanup-dlq") {
          throw new FatalError("dlq cleanup fatal");
        }
      });

      await waitForCondition(() => completedEvents.includes(successJobId));
      expect(queue.customJobBackoffFns.has(successJobId)).toBe(false);

      // 2. DLQ cleanup
      const dlqEvents = [];
      queue.on("dlq", (job) => dlqEvents.push(job.id));

      const dlqJobId = await queue.enqueue(
        { task: "cleanup-dlq" },
        { backoff: { strategy: "custom", fn: () => 100 } }
      );
      expect(queue.customJobBackoffFns.has(dlqJobId)).toBe(true);

      await waitForCondition(() => dlqEvents.includes(dlqJobId));
      expect(queue.customJobBackoffFns.has(dlqJobId)).toBe(false);

      // 3. Close cleanup
      const pendingJobId = await queue.enqueue(
        { task: "pending-close" },
        { delay: 100000, backoff: { strategy: "custom", fn: () => 100 } }
      );
      expect(queue.customJobBackoffFns.has(pendingJobId)).toBe(true);

      await queue.close();
      expect(queue.customJobBackoffFns.size).toBe(0);
      queue = null;
    });

    test("distributed worker fallback: worker-level fn is used when job specifies strategy custom without in-memory fn", async () => {
      queue = new ResilientQueue({ redisUrl });

      const retryingEvents = [];
      queue.on("retrying", (job, error, attempt, delayMs) => {
        retryingEvents.push({ attempt, delayMs });
      });

      // Worker configures custom backoff with fn
      queue.process(async () => {
        throw new RetryableError("custom fallback error");
      }, {
        backoff: {
          strategy: "custom",
          fn: (attempt) => attempt * 777
        }
      });

      // Push serialized job to Redis (simulating a job enqueued by another Node.js process)
      await redis.rpush("rq:main", JSON.stringify({
        id: "distributed-fallback-id",
        data: { test: "distributed-fallback" },
        attempt: 0,
        createdAt: Date.now(),
        backoff: { strategy: "custom" }
      }));

      await waitForCondition(() => retryingEvents.length >= 1);

      expect(retryingEvents[0].attempt).toBe(1);
      expect(retryingEvents[0].delayMs).toBe(777);
    });

    test("distributed worker without fn routes job with strategy custom to DLQ safely", async () => {
      queue = new ResilientQueue({ redisUrl });

      const dlqEvents = [];
      queue.on("dlq", (job, reason) => {
        dlqEvents.push({ job, reason });
      });

      // Worker has no custom backoff fn configured
      queue.process(async () => {
        throw new RetryableError("needs custom fn");
      });

      // Job in Redis has strategy: "custom" but worker has no fn
      await redis.rpush("rq:main", JSON.stringify({
        id: "no-fn-distributed-id",
        data: { test: "no-fn" },
        attempt: 0,
        createdAt: Date.now(),
        backoff: { strategy: "custom" }
      }));

      await waitForCondition(() => dlqEvents.length === 1);

      expect(dlqEvents[0].job.id).toBe("no-fn-distributed-id");
      expect(dlqEvents[0].reason.message).toContain("Custom backoff strategy requires a fn callback function");
      expect(await queue.getDLQCount()).toBe(1);
    });

    test("maxRetries: 0 causes immediate DLQ routing on initial failure without scheduling retries", async () => {
      queue = new ResilientQueue({ redisUrl });

      const retryingEvents = [];
      const dlqEvents = [];

      queue.on("retrying", (job) => retryingEvents.push(job.id));
      queue.on("dlq", (job, reason) => dlqEvents.push({ id: job.id, reason }));

      queue.process(async () => {
        throw new RetryableError("fail immediately");
      });

      const jobId = await queue.enqueue(
        { test: "zero-retries" },
        { maxRetries: 0 }
      );

      await waitForCondition(() => dlqEvents.length === 1);

      expect(dlqEvents[0].id).toBe(jobId);
      expect(retryingEvents.length).toBe(0);
      expect(await redis.zcard("rq:delayed")).toBe(0);
    });

    test("batch enqueuing (enqueueMany) supports per-item backoff configurations", async () => {
      queue = new ResilientQueue({ redisUrl });

      const retryingEvents = [];
      queue.on("retrying", (job, error, attempt, delayMs) => {
        retryingEvents.push({ id: job.id, delayMs, tag: job.data.tag });
      });

      queue.process(async () => {
        throw new RetryableError("batch failure");
      });

      const jobIds = await queue.enqueueMany([
        {
          data: { tag: "fixed-item" },
          backoff: { strategy: "fixed", baseDelay: 123 }
        },
        {
          data: { tag: "custom-item" },
          backoff: { strategy: "custom", fn: () => 789 }
        }
      ]);

      expect(jobIds.length).toBe(2);

      await waitForCondition(() => retryingEvents.length === 2);

      const fixedEvent = retryingEvents.find((e) => e.tag === "fixed-item");
      const customEvent = retryingEvents.find((e) => e.tag === "custom-item");

      expect(fixedEvent).toBeDefined();
      expect(fixedEvent.delayMs).toBe(123);

      expect(customEvent).toBeDefined();
      expect(customEvent.delayMs).toBe(789);
    });
  });
});
