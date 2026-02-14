const {
  ResilientQueue,
  RetryableError,
  FatalError
} = require("../src");

const Redis = require("ioredis");

const redisUrl = "redis://127.0.0.1:6379";

jest.setTimeout(20000);

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
    await queue.close();
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
});
