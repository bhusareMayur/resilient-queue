const {
  ResilientQueue,
  RetryableError,
  FatalError
} = require("./src");

async function main() {
  const queue = new ResilientQueue({
    redisUrl: "redis://127.0.0.1:6379",
    maxRetries: 2,
    baseDelay: 500
  });

  queue.process(async (job) => {
    console.log("Processing:", job.id, job.data);

    if (job.data.failType === "retry") {
      throw new RetryableError("Temporary failure");
    }

    if (job.data.failType === "fatal") {
      throw new FatalError("Permanent failure");
    }

    console.log("Completed:", job.id);
  });

  await new Promise(res => setTimeout(res, 1000));

  console.log("Enqueuing jobs...");

  await queue.enqueue({ message: "Success case" });

  await queue.enqueue(
    { message: "Retry case", failType: "retry" },
    { idempotencyKey: "retry-1" }
  );

  await queue.enqueue(
    { message: "Fatal case", failType: "fatal" },
    { idempotencyKey: "fatal-1" }
  );

  console.log("Jobs enqueued.");

  // Let process run long enough for retries naturally
  setTimeout(() => {
    console.log("Test completed.");
  }, 10000);
}

main();
