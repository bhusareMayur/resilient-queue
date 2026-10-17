const Redis = require("ioredis");

/**
 * Checks whether an object is an existing ioredis client or cluster instance.
 */
function isClientInstance(obj) {
  if (!obj || typeof obj !== "object") {
    return false;
  }
  if (obj instanceof Redis || (Redis.Cluster && obj instanceof Redis.Cluster)) {
    return true;
  }
  return (
    typeof obj.duplicate === "function" &&
    (typeof obj.quit === "function" || typeof obj.disconnect === "function")
  );
}

/**
 * Checks whether an object is a Redis Cluster instance.
 */
function isClusterInstance(obj) {
  if (!obj || typeof obj !== "object") {
    return false;
  }
  if (Redis.Cluster && obj instanceof Redis.Cluster) {
    return true;
  }
  return obj.isCluster === true;
}

/**
 * Creates a duplicate connection from an existing client instance.
 * Ensures maxRetriesPerRequest is null for blocking commands.
 */
function duplicateClient(client, defaultOptions = {}) {
  if (!client || typeof client.duplicate !== "function") {
    throw new Error("Redis client instance must provide a duplicate() method");
  }
  if (isClusterInstance(client)) {
    const opts = { ...defaultOptions };
    opts.redisOptions = { maxRetriesPerRequest: null, ...opts.redisOptions };
    return client.duplicate([], opts);
  }
  const opts = { maxRetriesPerRequest: null, ...defaultOptions };
  return client.duplicate(opts);
}

/**
 * Resolves connection configuration from various accepted formats.
 */
function resolveConfig(input) {
  if (!input) {
    return null;
  }

  // 1. Redis URL string
  if (typeof input === "string") {
    return { type: "url", url: input };
  }

  // 2. Existing client instance
  if (isClientInstance(input)) {
    return { type: "instance", instance: input };
  }

  // 3. Array of cluster nodes
  if (Array.isArray(input)) {
    return { type: "cluster", nodes: input, options: {} };
  }

  if (typeof input !== "object") {
    return null;
  }

  // 4. Custom producer/consumer pair
  if (input.producer || input.consumer) {
    return {
      type: "custom",
      producer: input.producer,
      consumer: input.consumer
    };
  }

  // 5. Existing client under .client
  if (input.client) {
    if (isClientInstance(input.client)) {
      return { type: "instance", instance: input.client };
    }
    return resolveConfig(input.client);
  }

  // 6. Config under .redis
  if (input.redis !== undefined && input.redis !== null) {
    if (typeof input.redis === "string") {
      return { type: "url", url: input.redis };
    }
    if (isClientInstance(input.redis)) {
      return { type: "instance", instance: input.redis };
    }
    if (Array.isArray(input.redis)) {
      return {
        type: "cluster",
        nodes: input.redis,
        options: input.clusterOptions || {}
      };
    }
    if (typeof input.redis === "object") {
      if (input.redis.producer || input.redis.consumer) {
        return {
          type: "custom",
          producer: input.redis.producer,
          consumer: input.redis.consumer
        };
      }
      if (input.redis.nodes && Array.isArray(input.redis.nodes)) {
        const { nodes, ...opts } = input.redis;
        return { type: "cluster", nodes, options: opts };
      }
      return { type: "options", options: input.redis };
    }
  }

  // 7. Backward compatibility with redisUrl property
  if (typeof input.redisUrl === "string") {
    return { type: "url", url: input.redisUrl, options: input.options || {} };
  }

  // 8. Cluster configurations
  if (input.clusterNodes && Array.isArray(input.clusterNodes)) {
    return {
      type: "cluster",
      nodes: input.clusterNodes,
      options: input.clusterOptions || {}
    };
  }

  if (input.cluster) {
    if (Array.isArray(input.cluster)) {
      return {
        type: "cluster",
        nodes: input.cluster,
        options: input.clusterOptions || {}
      };
    }
    if (typeof input.cluster === "object") {
      const nodes = input.cluster.nodes || [];
      const opts = input.cluster.options || {};
      return { type: "cluster", nodes, options: opts };
    }
  }

  // 9. Config under .connection
  if (input.connection !== undefined && input.connection !== null) {
    return resolveConfig(input.connection);
  }

  // 10. Direct options on object (host, port, sentinels, tls, etc.)
  const queueKeys = [
    "redisUrl",
    "redis",
    "cluster",
    "clusterNodes",
    "clusterOptions",
    "client",
    "connection",
    "producer",
    "consumer",
    "maxRetries",
    "baseDelay",
    "keyPrefix",
    "stalledInterval",
    "lockDuration",
    "maxStalledCount",
    "jobTimeout",
    "backoff",
    "maxDelay",
    "strategy",
    "jitter",
    "fn"
  ];
  const keys = Object.keys(input);
  const onlyQueueKeys = keys.length > 0 && keys.every(k => queueKeys.includes(k));

  if (!onlyQueueKeys) {
    return { type: "options", options: input };
  }

  return null;
}

/**
 * RedisClient
 *
 * Creates separate Redis connections for:
 *  - Producer (writes: RPUSH, SET, etc.)
 *  - Consumer (blocking reads: BLMOVE / BRPOPLPUSH)
 *
 * This separation is critical because a connection
 * blocked by BLMOVE cannot execute other commands.
 *
 * Supports Redis URL, ioredis options (including TLS and Sentinel),
 * Redis Cluster, and existing client instances.
 * Correctly manages connection ownership so externally owned clients
 * are never closed by the queue.
 */
class RedisClient {
  constructor(config, extraOptions = {}) {
    const resolved = resolveConfig(config);

    if (!resolved) {
      throw new Error("redisUrl or redis options are required to initialize RedisClient");
    }

    this.ownsProducer = false;
    this.ownsConsumer = false;

    switch (resolved.type) {
      case "url": {
        const opts = { maxRetriesPerRequest: null, ...resolved.options, ...extraOptions };
        this.producer = new Redis(resolved.url, opts);
        this.consumer = new Redis(resolved.url, opts);
        this.ownsProducer = true;
        this.ownsConsumer = true;
        break;
      }

      case "options": {
        const opts = { maxRetriesPerRequest: null, ...resolved.options, ...extraOptions };
        this.producer = new Redis(opts);
        this.consumer = new Redis(opts);
        this.ownsProducer = true;
        this.ownsConsumer = true;
        break;
      }

      case "cluster": {
        const clusterOpts = { ...resolved.options, ...extraOptions };
        clusterOpts.redisOptions = {
          maxRetriesPerRequest: null,
          ...clusterOpts.redisOptions
        };
        this.producer = new Redis.Cluster(resolved.nodes, clusterOpts);
        this.consumer = new Redis.Cluster(resolved.nodes, clusterOpts);
        this.ownsProducer = true;
        this.ownsConsumer = true;
        break;
      }

      case "instance": {
        this.producer = resolved.instance;
        this.ownsProducer = false;
        this.consumer = duplicateClient(resolved.instance, extraOptions);
        this.ownsConsumer = true;
        break;
      }

      case "custom": {
        if (resolved.producer && resolved.consumer) {
          if (resolved.producer === resolved.consumer) {
            this.producer = resolved.producer;
            this.ownsProducer = !isClientInstance(resolved.producer);
            this.consumer = duplicateClient(resolved.producer, extraOptions);
            this.ownsConsumer = true;
          } else {
            if (isClientInstance(resolved.producer)) {
              this.producer = resolved.producer;
              this.ownsProducer = false;
            } else if (typeof resolved.producer === "string") {
              this.producer = new Redis(resolved.producer, {
                maxRetriesPerRequest: null,
                ...extraOptions
              });
              this.ownsProducer = true;
            } else {
              this.producer = new Redis({
                maxRetriesPerRequest: null,
                ...resolved.producer,
                ...extraOptions
              });
              this.ownsProducer = true;
            }

            if (isClientInstance(resolved.consumer)) {
              this.consumer = resolved.consumer;
              this.ownsConsumer = false;
            } else if (typeof resolved.consumer === "string") {
              this.consumer = new Redis(resolved.consumer, {
                maxRetriesPerRequest: null,
                ...extraOptions
              });
              this.ownsConsumer = true;
            } else {
              this.consumer = new Redis({
                maxRetriesPerRequest: null,
                ...resolved.consumer,
                ...extraOptions
              });
              this.ownsConsumer = true;
            }
          }
        } else if (resolved.producer) {
          if (isClientInstance(resolved.producer)) {
            this.producer = resolved.producer;
            this.ownsProducer = false;
            this.consumer = duplicateClient(resolved.producer, extraOptions);
            this.ownsConsumer = true;
          } else if (typeof resolved.producer === "string") {
            const opts = { maxRetriesPerRequest: null, ...extraOptions };
            this.producer = new Redis(resolved.producer, opts);
            this.consumer = new Redis(resolved.producer, opts);
            this.ownsProducer = true;
            this.ownsConsumer = true;
          } else {
            const opts = { maxRetriesPerRequest: null, ...resolved.producer, ...extraOptions };
            this.producer = new Redis(opts);
            this.consumer = new Redis(opts);
            this.ownsProducer = true;
            this.ownsConsumer = true;
          }
        } else if (resolved.consumer) {
          if (isClientInstance(resolved.consumer)) {
            this.consumer = resolved.consumer;
            this.ownsConsumer = false;
            this.producer = duplicateClient(resolved.consumer, extraOptions);
            this.ownsProducer = true;
          } else if (typeof resolved.consumer === "string") {
            const opts = { maxRetriesPerRequest: null, ...extraOptions };
            this.producer = new Redis(resolved.consumer, opts);
            this.consumer = new Redis(resolved.consumer, opts);
            this.ownsProducer = true;
            this.ownsConsumer = true;
          } else {
            const opts = { maxRetriesPerRequest: null, ...resolved.consumer, ...extraOptions };
            this.producer = new Redis(opts);
            this.consumer = new Redis(opts);
            this.ownsProducer = true;
            this.ownsConsumer = true;
          }
        }
        break;
      }
    }

    this._attachEventHandlers();
  }

  /**
   * Attach safe error handlers.
   * Library should not log by default.
   * Host application can manage Redis errors.
   */
  _attachEventHandlers() {
    if (this.producer && typeof this.producer.on === "function") {
      this.producer.on("error", () => {});
    }
    if (this.consumer && typeof this.consumer.on === "function") {
      this.consumer.on("error", () => {});
    }
  }

  /**
   * Returns producer connection.
   */
  getProducer() {
    return this.producer;
  }

  /**
   * Returns consumer connection.
   */
  getConsumer() {
    return this.consumer;
  }

  /**
   * Gracefully close connections.
   * Only closes queue-owned connections.
   * Never closes externally owned client instances.
   */
  async disconnect() {
    const promises = [];

    const closeConnection = async (client) => {
      if (!client) return;
      try {
        if (typeof client.quit === "function") {
          await client.quit();
        } else if (typeof client.disconnect === "function") {
          client.disconnect();
        }
      } catch {
        // Safe fallback if quit fails
        if (typeof client.disconnect === "function") {
          client.disconnect();
        }
      }
    };

    if (this.ownsProducer && this.producer) {
      promises.push(closeConnection(this.producer));
    }

    if (this.ownsConsumer && this.consumer) {
      promises.push(closeConnection(this.consumer));
    }

    await Promise.all(promises);
  }
}

module.exports = RedisClient;
