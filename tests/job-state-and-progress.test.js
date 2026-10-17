const {
  ResilientQueue,
  RetryableError,
  FatalError
} = require("../src");

const Redis = require("ioredis");

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

describe("Job State & Progress Tracking", () => {
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

  describe("Job State Transitions & Lifecycle", () => {
    test("sets initial state to 'waiting' when enqueued normally", async () => {
      queue = new ResilientQueue({ redisUrl, keyPrefix: "test_rq" });
      const jobId = await queue.enqueue({ task: "send_email" });

      const job = await queue.getJob(jobId);
      expect(job).not.toBeNull();
      expect(job.id).toBe(jobId);
      expect(job.state).toBe("waiting");
      expect(job.data).toEqual({ task: "send_email" });
      expect(job.attempt).toBe(0);
      expect(job.progress).toBe(0);
      expect(typeof job.createdAt).toBe("number");
    });

    test("sets initial state to 'delayed' when enqueued with delay", async () => {
      queue = new ResilientQueue({ redisUrl, keyPrefix: "test_rq" });
      const jobId = await queue.enqueue({ task: "delayed_task" }, { delay: 5000 });

      const job = await queue.getJob(jobId);
      expect(job).not.toBeNull();
      expect(job.state).toBe("delayed");
      expect(job.delay).toBe(5000);
      expect(job.attempt).toBe(0);
    });

    test("transitions state to 'active' and records startedAt when processing starts", async () => {
      queue = new ResilientQueue({ redisUrl, keyPrefix: "test_rq" });
      const jobId = await queue.enqueue({ task: "compute" });

      let executionStarted = false;
      let stateWhileActive = null;

      await queue.process(async (job) => {
        executionStarted = true;
        expect(job.state).toBe("active");
        expect(typeof job.startedAt).toBe("number");
        const stored = await queue.getJob(job.id);
        stateWhileActive = stored.state;
      });

      await waitForCondition(() => executionStarted);
      expect(stateWhileActive).toBe("active");
    });

    test("transitions state to 'completed', records completedAt and result, and clears failure fields", async () => {
      queue = new ResilientQueue({ redisUrl, keyPrefix: "test_rq" });
      const jobId = await queue.enqueue({ n: 42 });

      let completedJob = null;
      let completedResult = null;
      queue.on("completed", (job, result) => {
        completedJob = job;
        completedResult = result;
      });

      await queue.process(async (job) => {
        return { square: job.data.n * job.data.n };
      });

      await waitForCondition(() => completedJob !== null);
      expect(completedJob.state).toBe("completed");
      expect(completedResult).toEqual({ square: 1764 });

      const stored = await queue.getJob(jobId);
      expect(stored.state).toBe("completed");
      expect(stored.result).toEqual({ square: 1764 });
      expect(stored.returnValue).toEqual({ square: 1764 });
      expect(typeof stored.completedAt).toBe("number");
      expect(stored.failedAt).toBeUndefined();
      expect(stored.failureReason).toBeUndefined();
      expect(stored.lastError).toBeUndefined();
    });

    test("clears previous failure metadata when a job retries and succeeds", async () => {
      queue = new ResilientQueue({
        redisUrl,
        keyPrefix: "test_rq",
        maxRetries: 2,
        baseDelay: 50
      });

      let attempts = 0;
      const jobId = await queue.enqueue({ attemptTest: true });

      await queue.process(async () => {
        attempts++;
        if (attempts === 1) {
          throw new RetryableError("First attempt transient failure");
        }
        return "success-on-retry";
      });

      await waitForCondition(() => attempts === 2);
      // Wait for completion
      await waitForCondition(async () => {
        const j = await queue.getJob(jobId);
        return j && j.state === "completed";
      });

      const stored = await queue.getJob(jobId);
      expect(stored.state).toBe("completed");
      expect(stored.result).toBe("success-on-retry");
      expect(stored.failedAt).toBeUndefined();
      expect(stored.failureReason).toBeUndefined();
      expect(stored.lastError).toBeUndefined();
    });

    test("transitions state to 'failed' on fatal failure, records failedAt and failureReason", async () => {
      queue = new ResilientQueue({ redisUrl, keyPrefix: "test_rq" });
      const jobId = await queue.enqueue({ fail: true });

      let failedEventJob = null;
      queue.on("failed", (job) => {
        failedEventJob = job;
      });

      await queue.process(async () => {
        throw new FatalError("Permanent database corruption");
      });

      await waitForCondition(async () => {
        const stored = await queue.getJob(jobId);
        return failedEventJob !== null && stored && stored.state === "failed";
      });
      expect(failedEventJob.state).toBe("failed");

      const stored = await queue.getJob(jobId);
      expect(stored.state).toBe("failed");
      expect(stored.failureReason).toBe("Permanent database corruption");
      expect(typeof stored.failedAt).toBe("number");
    });

    test("transitions state to 'delayed' during retry backoff, then to 'waiting' upon promotion", async () => {
      queue = new ResilientQueue({
        redisUrl,
        keyPrefix: "test_rq",
        maxRetries: 2,
        baseDelay: 2000 // Long delay so we can inspect 'delayed'
      });

      let failedOnce = false;
      const jobId = await queue.enqueue({ retryMe: true });

      await queue.process(async (job) => {
        if (!failedOnce) {
          failedOnce = true;
          throw new RetryableError("Temporary timeout");
        }
      });

      await waitForCondition(() => failedOnce);

      // Check state while delayed
      const delayedJob = await queue.getJob(jobId);
      expect(delayedJob.state).toBe("delayed");
      expect(delayedJob.attempt).toBe(1);
      expect(delayedJob.lastError).toBe("Temporary timeout");
      expect(typeof delayedJob.retriedAt).toBe("number");

      // Manually promote delayed job early
      await redis.zadd(`${queue.keyPrefix}:delayed`, Date.now() - 1000, JSON.stringify({ id: jobId }));
      await queue.promoteDelayedJobs();

      const promotedJob = await queue.getJob(jobId);
      expect(promotedJob.state).toBe("waiting");
    });

    test("recovering stalled job transitions back to 'waiting' and persists key", async () => {
      queue = new ResilientQueue({
        redisUrl,
        keyPrefix: "test_rq",
        lockDuration: 200,
        stalledInterval: 100,
        maxStalledCount: 2
      });

      const jobId = await queue.enqueue({ task: "stall_test" });
      // Transition to active
      await queue.jobStateManager.transitionToActive(jobId, 0);

      // Push raw job into active queue and set an expired lock
      const rawJob = JSON.stringify({ id: jobId, data: { task: "stall_test" } });
      await redis.rpush(`${queue.keyPrefix}:active`, rawJob);
      await redis.zadd(`${queue.keyPrefix}:locks`, Date.now() - 1000, jobId);

      // Trigger stalled recovery
      const recoveredCount = await queue.stalledJobManager.recoverStalledJobs();
      expect(recoveredCount).toBe(1);

      const job = await queue.getJob(jobId);
      expect(job.state).toBe("waiting");
      expect(job.stalledCount).toBe(1);
      expect(typeof job.lastStalledAt).toBe("number");
    });

    test("retrying a DLQ job resets attempt to 0, progress to 0, clears error fields, and persists key", async () => {
      queue = new ResilientQueue({ redisUrl, keyPrefix: "test_rq", removeOnFail: { age: 3600 } });
      const jobId = await queue.enqueue({ task: "dlq_retry" });

      // Move to DLQ
      await queue.dlqManager.moveToDLQ({ id: jobId, data: { task: "dlq_retry" }, progress: 75, attempt: 3 }, new FatalError("Broken"));

      const failedJob = await queue.getJob(jobId);
      expect(failedJob.state).toBe("failed");
      expect(failedJob.failureReason).toBe("Broken");

      // Retry from DLQ
      const retried = await queue.retryDLQJob(jobId);
      expect(retried).toBe(true);

      const requeuedJob = await queue.getJob(jobId);
      expect(requeuedJob.state).toBe("waiting");
      expect(requeuedJob.attempt).toBe(0);
      expect(requeuedJob.progress).toBe(0);
      expect(requeuedJob.failedAt).toBeUndefined();
      expect(requeuedJob.failureReason).toBeUndefined();
      expect(requeuedJob.lastError).toBeUndefined();

      // Ensure TTL is cleared (persisted)
      const ttl = await redis.ttl(`${queue.keyPrefix}:job:${jobId}`);
      expect(ttl).toBe(-1); // -1 means key exists without expiration
    });

    test("removing or purging DLQ job cleans up Redis job hash", async () => {
      queue = new ResilientQueue({ redisUrl, keyPrefix: "test_rq" });
      const jobId = await queue.enqueue({ task: "dlq_remove" });
      await queue.dlqManager.moveToDLQ({ id: jobId, data: { task: "dlq_remove" } }, "Error");

      expect(await queue.getJob(jobId)).not.toBeNull();

      const removed = await queue.removeDLQJob(jobId);
      expect(removed).toBe(true);

      const afterRemove = await queue.getJob(jobId);
      expect(afterRemove).toBeNull();
    });
  });

  describe("getJob() Deserialization & Edge Cases", () => {
    test("returns null for non-existent IDs, empty string, null, or undefined", async () => {
      queue = new ResilientQueue({ redisUrl, keyPrefix: "test_rq" });
      expect(await queue.getJob("non-existent-id")).toBeNull();
      expect(await queue.getJob("")).toBeNull();
      expect(await queue.getJob(null)).toBeNull();
      expect(await queue.getJob(undefined)).toBeNull();
    });

    test("throws an error if getJob() is called after queue is closed", async () => {
      queue = new ResilientQueue({ redisUrl, keyPrefix: "test_rq" });
      await queue.close();
      await expect(queue.getJob("some-id")).rejects.toThrow("Queue is closed");
      queue = null;
    });

    test("faithfully preserves primitive and complex job data payloads", async () => {
      queue = new ResilientQueue({ redisUrl, keyPrefix: "test_rq" });

      const testCases = [
        { name: "numeric string", data: "12345" },
        { name: "boolean string true", data: "true" },
        { name: "boolean string false", data: "false" },
        { name: "number", data: 42 },
        { name: "boolean true", data: true },
        { name: "boolean false", data: false },
        { name: "null", data: null },
        { name: "empty string", data: "" },
        { name: "array", data: [1, "two", { three: 3 }] },
        { name: "nested object", data: { a: { b: { c: "deep" } } } }
      ];

      for (const tc of testCases) {
        const jobId = await queue.enqueue(tc.data);
        const job = await queue.getJob(jobId);
        expect(job.data).toEqual(tc.data);
      }
    });

    test("faithfully preserves results returned by job handlers", async () => {
      queue = new ResilientQueue({ redisUrl, keyPrefix: "test_rq" });

      const resultsToTest = [
        { name: "string", val: "ok" },
        { name: "numeric string", val: "999" },
        { name: "number 0", val: 0 },
        { name: "number 42", val: 42 },
        { name: "boolean false", val: false },
        { name: "boolean true", val: true },
        { name: "null", val: null },
        { name: "empty string", val: "" },
        { name: "object", val: { status: "processed", items: [1, 2] } }
      ];

      for (const item of resultsToTest) {
        const jobId = await queue.enqueue(item.val);
        await queue.jobStateManager.transitionToCompleted(jobId, item.val);

        const job = await queue.getJob(jobId);
        expect(job.result).toEqual(item.val);
        expect(job.returnValue).toEqual(item.val);
      }
    });

    test("returns undefined for result if handler returns undefined", async () => {
      queue = new ResilientQueue({ redisUrl, keyPrefix: "test_rq" });
      const jobId = await queue.enqueue("task");
      await queue.jobStateManager.transitionToCompleted(jobId, undefined);

      const job = await queue.getJob(jobId);
      expect(job.result).toBeUndefined();
      expect(job.returnValue).toBeUndefined();
    });
  });

  describe("Progress Updates & Event Emission", () => {
    test("updates progress inside worker handler and emits 'progress' event", async () => {
      queue = new ResilientQueue({ redisUrl, keyPrefix: "test_rq" });
      const jobId = await queue.enqueue({ file: "video.mp4" });

      const progressEvents = [];
      queue.on("progress", (job, progress) => {
        progressEvents.push({ id: job.id, progress });
      });

      await queue.process(async (job) => {
        await job.updateProgress(25);
        expect(job.progress).toBe(25);
        await job.updateProgress(50);
        expect(job.progress).toBe(50);
        await job.updateProgress(100);
      });

      await waitForCondition(() => progressEvents.length === 3);

      expect(progressEvents).toEqual([
        { id: jobId, progress: 25 },
        { id: jobId, progress: 50 },
        { id: jobId, progress: 100 }
      ]);

      const stored = await queue.getJob(jobId);
      expect(stored.progress).toBe(100);
    });

    test("allows updateProgress on job retrieved via getJob() and emits queue event", async () => {
      queue = new ResilientQueue({ redisUrl, keyPrefix: "test_rq" });
      const jobId = await queue.enqueue({ initial: true });

      const emitted = [];
      queue.on("progress", (job, progress) => {
        emitted.push({ id: job.id, progress });
      });

      const job = await queue.getJob(jobId);
      await job.updateProgress(60);

      expect(job.progress).toBe(60);
      expect(emitted).toEqual([{ id: jobId, progress: 60 }]);

      const refreshed = await queue.getJob(jobId);
      expect(refreshed.progress).toBe(60);
    });

    test("validates progress input and rejects out-of-range or non-numeric values", async () => {
      queue = new ResilientQueue({ redisUrl, keyPrefix: "test_rq" });
      const jobId = await queue.enqueue({ test: true });
      const job = await queue.getJob(jobId);

      const invalidValues = [-1, 101, NaN, Infinity, -Infinity, "50", null, undefined, {}, []];

      for (const val of invalidValues) {
        await expect(job.updateProgress(val)).rejects.toThrow(
          "Progress must be a number between 0 and 100"
        );
      }

      // Valid boundary values
      await expect(job.updateProgress(0)).resolves.not.toThrow();
      await expect(job.updateProgress(100)).resolves.not.toThrow();
      await expect(job.updateProgress(33.33)).resolves.not.toThrow();
    });

    test("throws an error and does NOT create a zombie hash when updateProgress is called on non-existent job", async () => {
      queue = new ResilientQueue({ redisUrl, keyPrefix: "test_rq" });

      await expect(queue.jobStateManager.updateProgress("ghost-id", 50)).rejects.toThrow(
        "Job ghost-id not found"
      );

      // Verify no orphaned key was created in Redis
      const exists = await redis.exists(`${queue.keyPrefix}:job:ghost-id`);
      expect(exists).toBe(0);
    });

    test("emits 'error' event on queue when updateProgress encounters Redis or validation error", async () => {
      queue = new ResilientQueue({ redisUrl, keyPrefix: "test_rq" });
      const jobId = await queue.enqueue({ errorTest: true });
      const job = await queue.getJob(jobId);

      let errorEmitted = null;
      queue.on("error", (err) => {
        errorEmitted = err;
      });

      await expect(job.updateProgress(-5)).rejects.toThrow();
      expect(errorEmitted).not.toBeNull();
      expect(errorEmitted.message).toBe("Progress must be a number between 0 and 100");
    });
  });

  describe("Retention Policies", () => {
    test("removeOnComplete: true removes job immediately upon completion", async () => {
      queue = new ResilientQueue({
        redisUrl,
        keyPrefix: "test_rq",
        removeOnComplete: true
      });

      const jobId = await queue.enqueue({ ephemeral: true });

      let completed = false;
      await queue.process(async () => {
        completed = true;
        return "done";
      });

      await waitForCondition(() => completed);

      // Give Redis a moment to execute retention lua
      await new Promise((r) => setTimeout(r, 50));

      const stored = await queue.getJob(jobId);
      expect(stored).toBeNull();

      // Ensure key is deleted from Redis
      const exists = await redis.exists(`${queue.keyPrefix}:job:${jobId}`);
      expect(exists).toBe(0);
    });

    test("removeOnComplete: 0 removes job immediately upon completion", async () => {
      queue = new ResilientQueue({
        redisUrl,
        keyPrefix: "test_rq",
        removeOnComplete: 0
      });

      const jobId = await queue.enqueue({ ephemeral: true });

      let completed = false;
      await queue.process(async () => {
        completed = true;
      });

      await waitForCondition(() => completed);
      await new Promise((r) => setTimeout(r, 50));

      expect(await queue.getJob(jobId)).toBeNull();
    });

    test("removeOnComplete: false keeps job hash indefinitely", async () => {
      queue = new ResilientQueue({
        redisUrl,
        keyPrefix: "test_rq",
        removeOnComplete: false
      });

      const jobId = await queue.enqueue({ keep: true });

      let completed = false;
      await queue.process(async () => {
        completed = true;
      });

      await waitForCondition(() => completed);
      await new Promise((r) => setTimeout(r, 50));

      const stored = await queue.getJob(jobId);
      expect(stored).not.toBeNull();
      expect(stored.state).toBe("completed");
    });

    test("count-based retention trims oldest completed jobs without deleting active/waiting jobs", async () => {
      queue = new ResilientQueue({
        redisUrl,
        keyPrefix: "test_rq",
        removeOnComplete: 2 // Retain at most 2 completed jobs
      });

      // Enqueue job 1 and complete it
      const id1 = await queue.enqueue({ n: 1 });
      await queue.jobStateManager.transitionToCompleted(id1, "res1");

      // Enqueue job 2 and complete it
      const id2 = await queue.enqueue({ n: 2 });
      await queue.jobStateManager.transitionToCompleted(id2, "res2");

      expect(await queue.getJob(id1)).not.toBeNull();
      expect(await queue.getJob(id2)).not.toBeNull();

      // Enqueue waiting job 3 (must not be deleted)
      const id3Waiting = await queue.enqueue({ n: 3 });

      // Enqueue and complete job 4
      const id4 = await queue.enqueue({ n: 4 });
      await queue.jobStateManager.transitionToCompleted(id4, "res4");

      // id1 should now be trimmed (excess over 2)
      expect(await queue.getJob(id1)).toBeNull();
      expect(await queue.getJob(id2)).not.toBeNull();
      expect(await queue.getJob(id4)).not.toBeNull();
      // Waiting job 3 must remain intact!
      expect(await queue.getJob(id3Waiting)).not.toBeNull();
      expect((await queue.getJob(id3Waiting)).state).toBe("waiting");
    });

    test("age-based retention sets TTL and lazily expires on getJob()", async () => {
      queue = new ResilientQueue({
        redisUrl,
        keyPrefix: "test_rq",
        removeOnComplete: { age: 1 } // 1 second
      });

      const jobId = await queue.enqueue({ shortLived: true });
      await queue.jobStateManager.transitionToCompleted(jobId, "done");

      // Initially present
      expect(await queue.getJob(jobId)).not.toBeNull();

      // Wait 1.1s for expiration
      await new Promise((r) => setTimeout(r, 1100));

      // getJob() should detect expiration and return null
      const expiredJob = await queue.getJob(jobId);
      expect(expiredJob).toBeNull();
    });

    test("per-job retention override overrides queue default", async () => {
      queue = new ResilientQueue({
        redisUrl,
        keyPrefix: "test_rq",
        removeOnComplete: false // Queue default: keep
      });

      // Override to true for this specific job
      const id1 = await queue.enqueue({ ephemeral: true }, { removeOnComplete: true });
      // Keep this one
      const id2 = await queue.enqueue({ keep: true });

      await queue.jobStateManager.transitionToCompleted(id1, "res1", true);
      await queue.jobStateManager.transitionToCompleted(id2, "res2", false);

      expect(await queue.getJob(id1)).toBeNull();
      expect(await queue.getJob(id2)).not.toBeNull();
    });

    test("removeOnFail: true deletes failed job hash on DLQ transition", async () => {
      queue = new ResilientQueue({
        redisUrl,
        keyPrefix: "test_rq",
        removeOnFail: true
      });

      const jobId = await queue.enqueue({ failEphemeral: true });
      await queue.dlqManager.moveToDLQ({ id: jobId, data: { failEphemeral: true } }, "Fatal");

      // Hash deleted due to removeOnFail: true
      expect(await queue.getJob(jobId)).toBeNull();

      // But job is still in DLQ list
      const dlqCount = await queue.getDLQCount();
      expect(dlqCount).toBe(1);
    });

    test("normalizes retention options and throws on invalid inputs", async () => {
      expect(() => new ResilientQueue({ redisUrl, removeOnComplete: -1 })).toThrow(
        "Retention count must be a non-negative number"
      );
      expect(() => new ResilientQueue({ redisUrl, removeOnComplete: "invalid" })).toThrow(
        "Retention policy must be a boolean, number, or object with age and/or count"
      );
      expect(() => new ResilientQueue({ redisUrl, removeOnComplete: {} })).toThrow(
        "Retention policy object must specify 'age' and/or 'count'"
      );
      expect(() => new ResilientQueue({ redisUrl, removeOnComplete: { age: -5 } })).toThrow(
        "Retention age must be a non-negative number of seconds"
      );
      expect(() => new ResilientQueue({ redisUrl, removeOnComplete: { count: -10 } })).toThrow(
        "Retention count must be a non-negative number"
      );
    });
  });

  describe("Concurrency & Multi-Worker Scenarios", () => {
    test("handles multiple workers executing and updating progress concurrently", async () => {
      queue = new ResilientQueue({ redisUrl, keyPrefix: "test_rq" });

      const count = 10;
      const jobIds = [];
      for (let i = 0; i < count; i++) {
        jobIds.push(await queue.enqueue({ index: i }));
      }

      const completed = new Set();

      await queue.process(
        async (job) => {
          await job.updateProgress(50);
          await new Promise((r) => setTimeout(r, 20));
          await job.updateProgress(100);
          completed.add(job.id);
          return { doneIndex: job.data.index };
        },
        { concurrency: 4 }
      );

      await waitForCondition(() => completed.size === count);

      for (const id of jobIds) {
        const j = await queue.getJob(id);
        expect(j.state).toBe("completed");
        expect(j.progress).toBe(100);
        expect(j.result.doneIndex).toBeDefined();
      }
    });

    test("batch enqueueMany properly initializes job states and retention", async () => {
      queue = new ResilientQueue({ redisUrl, keyPrefix: "test_rq" });

      const items = [
        { data: "first", delay: 1000 },
        { data: "second" },
        { data: { key: "third" } }
      ];

      const ids = await queue.enqueueMany(items);
      expect(ids).toHaveLength(3);

      const job0 = await queue.getJob(ids[0]);
      expect(job0.state).toBe("delayed");
      expect(job0.data).toBe("first");

      const job1 = await queue.getJob(ids[1]);
      expect(job1.state).toBe("waiting");
      expect(job1.data).toBe("second");

      const job2 = await queue.getJob(ids[2]);
      expect(job2.state).toBe("waiting");
      expect(job2.data).toEqual({ key: "third" });
    });
  });
});
