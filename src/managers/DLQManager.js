const RESET_JOB_LUA = `
  local function resetJob(raw)
    local ok, obj = pcall(cjson.decode, raw)
    if not ok or type(obj) ~= 'table' then
      return nil
    end

    local len = #raw
    local i = 1
    while i <= len and raw:sub(i, i) ~= '{' do i = i + 1 end
    if i > len then return nil end
    i = i + 1

    local fields = {}
    local skipKeys = {
      failedAt = true,
      failureReason = true,
      lastError = true,
      retriedAt = true,
      lastStalledAt = true,
      lockToken = true
    }

    local hasAttempt = false
    local hasStalledCount = false

    while i <= len do
      while i <= len and (raw:sub(i, i):match('%s') or raw:sub(i, i) == ',') do i = i + 1 end
      if i > len or raw:sub(i, i) == '}' then break end

      if raw:sub(i, i) ~= '"' then break end
      i = i + 1
      local keyStart = i
      while i <= len and raw:sub(i, i) ~= '"' do
        if raw:sub(i, i) == '\\\\' then i = i + 1 end
        i = i + 1
      end
      local key = raw:sub(keyStart, i - 1)
      i = i + 1

      while i <= len and raw:sub(i, i) ~= ':' do i = i + 1 end
      i = i + 1
      while i <= len and raw:sub(i, i):match('%s') do i = i + 1 end

      local valStart = i
      local c = raw:sub(i, i)
      if c == '"' then
        i = i + 1
        while i <= len and raw:sub(i, i) ~= '"' do
          if raw:sub(i, i) == '\\\\' then i = i + 1 end
          i = i + 1
        end
        i = i + 1
      elseif c == '{' or c == '[' then
        local depth = 1
        local inString = false
        i = i + 1
        while i <= len and depth > 0 do
          local ch = raw:sub(i, i)
          if inString then
            if ch == '\\\\' then
              i = i + 1
            elseif ch == '"' then
              inString = false
            end
          else
            if ch == '"' then
              inString = true
            elseif ch == '{' or ch == '[' then
              depth = depth + 1
            elseif ch == '}' or ch == ']' then
              depth = depth - 1
            end
          end
          i = i + 1
        end
      else
        while i <= len and raw:sub(i, i) ~= ',' and raw:sub(i, i) ~= '}' and not raw:sub(i, i):match('%s') do
          i = i + 1
        end
      end
      local val = raw:sub(valStart, i - 1)

      if not skipKeys[key] then
        if key == 'attempt' then
          hasAttempt = true
          table.insert(fields, '"attempt":0')
        elseif key == 'stalledCount' then
          hasStalledCount = true
          table.insert(fields, '"stalledCount":0')
        else
          table.insert(fields, '"' .. key .. '":' .. val)
        end
      end
    end

    if not hasAttempt then
      table.insert(fields, '"attempt":0')
    end
    if not hasStalledCount then
      table.insert(fields, '"stalledCount":0')
    end

    return '{' .. table.concat(fields, ',') .. '}'
  end
`;

const RETRY_SINGLE_SCRIPT = `
  ${RESET_JOB_LUA}

  local destTypeTable = redis.call('TYPE', KEYS[2])
  local destinationType = type(destTypeTable) == 'table' and destTypeTable.ok or destTypeTable
  if destinationType ~= 'none' and destinationType ~= 'list' then
    return redis.error_reply('Main queue key must be a Redis list')
  end

  local dlqKey = KEYS[1]
  local mainKey = KEYS[2]
  local metaKey = KEYS[3]
  local targetId = ARGV[1]

  if not targetId or targetId == '' then
    return 0
  end

  local items = redis.call('LRANGE', dlqKey, 0, -1)
  for _, rawJob in ipairs(items) do
    local ok, obj = pcall(cjson.decode, rawJob)
    if ok and obj and type(obj) == 'table' and tostring(obj.id) == targetId then
      local removed = redis.call('LREM', dlqKey, 1, rawJob)
      if removed > 0 then
        local resetRaw = resetJob(rawJob)
        if resetRaw then
          redis.call('RPUSH', mainKey, resetRaw)
          redis.call('HDEL', metaKey, targetId)
          return 1
        else
          redis.call('RPUSH', dlqKey, rawJob)
          return 0
        end
      end
    end
  end

  return 0
`;

const RETRY_ALL_SCRIPT = `
  ${RESET_JOB_LUA}

  local destTypeTable = redis.call('TYPE', KEYS[2])
  local destinationType = type(destTypeTable) == 'table' and destTypeTable.ok or destTypeTable
  if destinationType ~= 'none' and destinationType ~= 'list' then
    return redis.error_reply('Main queue key must be a Redis list')
  end

  local dlqKey = KEYS[1]
  local mainKey = KEYS[2]
  local metaKey = KEYS[3]
  local limit = tonumber(ARGV[1])
  local count = 0

  local items = redis.call('LRANGE', dlqKey, 0, -1)
  for _, rawJob in ipairs(items) do
    if limit and limit > 0 and count >= limit then
      break
    end

    local ok, obj = pcall(cjson.decode, rawJob)
    if ok and obj and type(obj) == 'table' and obj.id then
      local removed = redis.call('LREM', dlqKey, 1, rawJob)
      if removed > 0 then
        local resetRaw = resetJob(rawJob)
        if resetRaw then
          redis.call('RPUSH', mainKey, resetRaw)
          redis.call('HDEL', metaKey, tostring(obj.id))
          count = count + 1
        else
          redis.call('RPUSH', dlqKey, rawJob)
        end
      end
    end
  end

  return count
`;

const REMOVE_JOB_SCRIPT = `
  local dlqKey = KEYS[1]
  local metaKey = KEYS[2]
  local targetId = ARGV[1]

  if not targetId or targetId == '' then
    return 0
  end

  local items = redis.call('LRANGE', dlqKey, 0, -1)
  for _, rawJob in ipairs(items) do
    local ok, obj = pcall(cjson.decode, rawJob)
    if ok and obj and type(obj) == 'table' and tostring(obj.id) == targetId then
      local removed = redis.call('LREM', dlqKey, 1, rawJob)
      if removed > 0 then
        redis.call('HDEL', metaKey, targetId)
        return 1
      end
    end
  end

  return 0
`;

const PURGE_SCRIPT = `
  local dlqKey = KEYS[1]
  local metaKey = KEYS[2]

  local total = redis.call('LLEN', dlqKey)
  if total == 0 then
    return 0
  end

  local items = redis.call('LRANGE', dlqKey, 0, -1)
  for _, rawJob in ipairs(items) do
    local ok, obj = pcall(cjson.decode, rawJob)
    if ok and obj and type(obj) == 'table' and obj.id then
      redis.call('HDEL', metaKey, tostring(obj.id))
    end
  end

  redis.call('DEL', dlqKey)
  return total
`;

/**
 * Handles Dead Letter Queue (DLQ) operations.
 */
class DLQManager {
  constructor(redisClient, keyPrefix = "rq", options = {}) {
    if (!redisClient) {
      throw new Error("Redis client is required for DLQManager");
    }

    let prefix = keyPrefix;
    let opts = options;
    if (typeof keyPrefix === "object" && keyPrefix !== null) {
      opts = keyPrefix;
      prefix = opts.keyPrefix || "rq";
    }

    this.redis = redisClient;
    this.keyPrefix = prefix;
    this.onDLQ = opts?.onDLQ || null;
    this.dlqKey = `${this.keyPrefix}:dlq`;
    this.mainQueueKey = `${this.keyPrefix}:main`;
    this.stalledMetaKey = `${this.keyPrefix}:stalled_meta`;
  }

  /**
   * Moves a failed job to DLQ.
   */
  async moveToDLQ(job, reason) {
    const failedJob = {
      ...job,
      failedAt: Date.now(),
      failureReason: reason?.message || (typeof reason === "string" ? reason : "Unknown error")
    };

    await this.redis.rpush(this.dlqKey, JSON.stringify(failedJob));

    if (typeof this.onDLQ === "function") {
      this.onDLQ(failedJob, reason);
    }
  }

  /**
   * Returns exact count of DLQ jobs.
   */
  async getDLQCount() {
    const count = await this.redis.llen(this.dlqKey);
    return Number(count);
  }

  /**
   * Retrieves paginated DLQ jobs.
   */
  async getDLQ(options = {}) {
    const { start = 0, limit = 50, order = "asc" } = options || {};

    if (typeof start !== "number" || !Number.isInteger(start) || start < 0) {
      throw new Error("start must be a non-negative integer");
    }

    if (typeof limit !== "number" || !Number.isInteger(limit) || limit < 0) {
      throw new Error("limit must be a non-negative integer");
    }

    const normalizedOrder = typeof order === "string" ? order.toLowerCase() : "asc";
    if (normalizedOrder !== "asc" && normalizedOrder !== "desc") {
      throw new Error('order must be "asc" or "desc"');
    }

    const total = await this.redis.llen(this.dlqKey);
    const totalCount = Number(total);

    if (totalCount === 0 || start >= totalCount || limit === 0) {
      return { total: totalCount, jobs: [] };
    }

    let rawJobs = [];
    if (normalizedOrder === "desc") {
      const rangeEnd = totalCount - 1 - start;
      const rangeStart = Math.max(0, totalCount - start - limit);
      rawJobs = await this.redis.lrange(this.dlqKey, rangeStart, rangeEnd);
      rawJobs.reverse();
    } else {
      const rangeStart = start;
      const rangeEnd = start + limit - 1;
      rawJobs = await this.redis.lrange(this.dlqKey, rangeStart, rangeEnd);
    }

    const jobs = [];
    for (const raw of rawJobs) {
      try {
        const parsed = JSON.parse(raw);
        if (parsed && typeof parsed === "object") {
          jobs.push(parsed);
        } else {
          jobs.push({
            id: null,
            raw,
            corrupted: true,
            error: "Invalid JSON payload"
          });
        }
      } catch (err) {
        jobs.push({
          id: null,
          raw,
          corrupted: true,
          error: err.message || "Invalid JSON"
        });
      }
    }

    const idsToFetch = jobs
      .filter((j) => j && j.id && !j.corrupted && (j.failedAt === undefined || j.failureReason === undefined))
      .map((j) => String(j.id));

    if (idsToFetch.length > 0) {
      const pipeline = this.redis.pipeline();
      for (const id of idsToFetch) {
        pipeline.hget(this.stalledMetaKey, id);
      }
      const results = await pipeline.exec();
      const metaMap = new Map();
      idsToFetch.forEach((id, idx) => {
        const [err, res] = results[idx] || [];
        if (!err && res) {
          try {
            metaMap.set(id, JSON.parse(res));
          } catch {
            // ignore malformed metadata
          }
        }
      });

      for (const job of jobs) {
        if (job && job.id && metaMap.has(String(job.id))) {
          const meta = metaMap.get(String(job.id));
          if (job.failedAt === undefined && meta.failedAt !== undefined) {
            job.failedAt = meta.failedAt;
          }
          if (job.failureReason === undefined && meta.failureReason !== undefined) {
            job.failureReason = meta.failureReason;
          }
          if (meta.stalledCount !== undefined) {
            job.stalledCount = meta.stalledCount;
          }
          if (meta.lastStalledAt !== undefined) {
            job.lastStalledAt = meta.lastStalledAt;
          }
          if (meta.attempt !== undefined && job.attempt === undefined) {
            job.attempt = meta.attempt;
          }
        }
      }
    }

    return { total: totalCount, jobs };
  }

  /**
   * Atomically retries a specific DLQ job by moving it back to the main queue.
   */
  async retryDLQJob(jobId) {
    if (!jobId || (typeof jobId !== "string" && typeof jobId !== "number")) {
      return false;
    }

    const targetId = String(jobId);
    const result = await this.redis.eval(
      RETRY_SINGLE_SCRIPT,
      3,
      this.dlqKey,
      this.mainQueueKey,
      this.stalledMetaKey,
      targetId
    );

    return result === 1;
  }

  /**
   * Atomically retries DLQ jobs in batches by moving them back to the main queue.
   */
  async retryAllDLQ(options = {}) {
    const { limit } = options || {};

    if (limit !== undefined && limit !== null) {
      if (typeof limit !== "number" || !Number.isInteger(limit) || limit < 0) {
        throw new Error("limit must be a non-negative integer");
      }
      if (limit === 0) {
        return 0;
      }
    }

    const limitArg = typeof limit === "number" && limit > 0 ? limit : 0;
    const result = await this.redis.eval(
      RETRY_ALL_SCRIPT,
      3,
      this.dlqKey,
      this.mainQueueKey,
      this.stalledMetaKey,
      limitArg
    );

    return Number(result);
  }

  /**
   * Removes a specific failed job from DLQ.
   */
  async removeDLQJob(jobId) {
    if (!jobId || (typeof jobId !== "string" && typeof jobId !== "number")) {
      return false;
    }

    const targetId = String(jobId);
    const result = await this.redis.eval(
      REMOVE_JOB_SCRIPT,
      2,
      this.dlqKey,
      this.stalledMetaKey,
      targetId
    );

    return result === 1;
  }

  /**
   * Purges all jobs from DLQ and cleans associated metadata.
   */
  async purgeDLQ() {
    const result = await this.redis.eval(
      PURGE_SCRIPT,
      2,
      this.dlqKey,
      this.stalledMetaKey
    );

    return Number(result);
  }
}

module.exports = DLQManager;
