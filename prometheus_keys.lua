-- Storage to keep track of used keys. Allows to atomically create, delete
-- and list keys. The keys are synchronized between nginx workers
-- using ngx.shared.dict. The whole purpose of this module is to avoid
-- using ngx.shared.dict:get_keys (see https://github.com/openresty/lua-nginx-module#ngxshareddictget_keys),
-- which blocks all workers and therefore it shouldn't be used with large
-- amounts of keys.
local KeyIndex = {}
KeyIndex.__index = KeyIndex

-- Upper bound on how far a single add() call may advance key_count while it
-- repairs a counter that has fallen behind occupied slots (see the comment in
-- add()). It bounds the worst-case latency of one call; the advanced counter
-- is shared through the dict, so later calls resume where this one stopped
-- and the index converges even when far more slots need repairing.
local MAX_KEY_COUNT_REPAIRS = 1000

-- Entries a single flush_expired() call may reclaim.
--
-- flush_expired(n) walks the LRU queue from its tail and only stops early once
-- n entries have been freed, so one call costs
--
--   walk_cost * entries in the dict  +  free_cost * entries freed
--
-- and only the second term is bounded by n: every call pays the walk again
-- (measured on OpenResty 1.29.2.4: ~7.5ns and ~90-148ns per entry). A small
-- batch therefore barely shortens a single hold -- it multiplies the walk, and
-- with it the total time the lock is held and the number of requests that wait
-- on it. Reclaiming a 1.5M backlog out of 3M entries took 151 calls and 1771ms
-- of lock time at n=10000 and pushed the request p99 to 1.53s, against a single
-- 151ms hold and a p99 of 2.19ms unbounded.
--
-- So n is the largest value whose own freeing cost still keeps one hold short
-- (~5ms here). A normal round -- the expiry rate times the reclaim interval --
-- is drained in one call, and the batching only comes into play for a backlog
-- that is not.
local FLUSH_EXPIRED_BATCH = 50000

-- Upper bound on the batches one round may run, so a backlog that is not normal
-- -- inherited across an upgrade, or left by a dict that filled up with expired
-- entries -- is drained over several rounds instead of one long stretch. What is
-- left over is picked up by the next round.
local FLUSH_EXPIRED_MAX_BATCHES = 10

-- Pause between batches, so the lock is not taken back to back. It also bounds
-- how long one round can occupy its timer: FLUSH_EXPIRED_MAX_BATCHES times this.
local FLUSH_EXPIRED_BATCH_DELAY = 1

-- Slot numbers the local scan of remove_expired_keys() may walk in one round.
--
-- That scan asks the dict for the ttl of every slot the worker knows has one,
-- which is a lock acquisition per slot, and it is what a round actually costs:
-- with 150,000 such slots across 8 workers and a steady 20k rps it showed up as
-- a ~480ms stall at p99.9, of which the reclamation itself was 5%. A tenth of
-- the slots made it a ~40ms stall, so the cost is linear in them and has to be
-- bounded the way the reclamation is.
--
-- The scan therefore walks a slice of the slot range per round and resumes
-- where the previous round stopped. A full sweep takes key_count divided by
-- this many rounds, which only delays dropping the worker-local reference to a
-- slot whose node is already gone -- nothing reads it in the meantime, because
-- every dict API reports that slot as missing anyway.
local REMOVE_EXPIRED_KEYS_SCAN_SLOTS = 10000


-- Exposed so a caller that schedules the reclamation itself can reason about it:
-- one round drains at most FLUSH_EXPIRED_BATCH * FLUSH_EXPIRED_MAX_BATCHES
-- entries and may occupy its timer for FLUSH_EXPIRED_MAX_BATCHES *
-- FLUSH_EXPIRED_BATCH_DELAY seconds.
KeyIndex.FLUSH_EXPIRED_BATCH = FLUSH_EXPIRED_BATCH
KeyIndex.FLUSH_EXPIRED_MAX_BATCHES = FLUSH_EXPIRED_MAX_BATCHES
KeyIndex.FLUSH_EXPIRED_BATCH_DELAY = FLUSH_EXPIRED_BATCH_DELAY
KeyIndex.REMOVE_EXPIRED_KEYS_SCAN_SLOTS = REMOVE_EXPIRED_KEYS_SCAN_SLOTS


-- check and remove expired keys
local function remove_expired_keys(_, self)
  self:remove_expired_keys()
end


-- Every worker runs init_worker at the same moment, so a plain ngx.timer.every
-- would have all of them reclaim at the same moment too and their stalls would
-- add up. Spread the first round evenly over the interval instead, by worker
-- id, and only then settle into the interval.
local function start_remove_expired_keys_timer(self, interval)
  local id = tonumber(ngx.worker.id()) or 0
  local count = ngx.worker.count and tonumber(ngx.worker.count()) or 1
  if id < 0 or not count or count < 1 then
    id, count = 0, 1
  end

  local offset = interval * id / count
  if offset <= 0 then
    ngx.timer.every(interval, remove_expired_keys, self)
    return
  end

  ngx.timer.at(offset, function(premature, s)
    if premature then
      return
    end
    ngx.timer.every(interval, remove_expired_keys, s)
    s:remove_expired_keys()
  end, self)
end


function KeyIndex.new(shared_dict, prefix, remove_expired_keys_interval,
                      auto_flush_expired)
  local self = setmetatable({}, KeyIndex)
  self.dict = shared_dict
  self.auto_flush_expired = auto_flush_expired ~= false
  self.key_prefix = prefix .. "key_"
  self.delete_count = prefix .. "delete_count"
  self.key_count = prefix .. "key_count"
  self.last = 0
  self.deleted = 0
  self.not_expired_index = 1
  self.keys = {}
  self.index = {}
  self.expire_keys = {}

  self.scan_cursor = 0

  start_remove_expired_keys_timer(self, remove_expired_keys_interval or 600)
  return self
end

-- check and remove expired keys
function KeyIndex:remove_expired_keys()
  -- A slice of the slot range per round, resuming where the last round stopped,
  -- so a round costs the same whatever the index has grown to. The range is
  -- bounded by key_count rather than by self.last: a worker's own newest slots
  -- are above the range it has synced, and they have to be scanned too.
  local last = self.dict:get(self.key_count) or 0
  local from = self.scan_cursor
  if from > last then
    from = 0
  end
  local to = from + REMOVE_EXPIRED_KEYS_SCAN_SLOTS - 1
  if to > last then
    to = last
  end
  self.scan_cursor = to + 1

  for i = from, to do
    if self.expire_keys[i] then
      -- Read i-th key. If it is nil or ttl is < 0, it means it was expired
      local ttl, err = self.dict:ttl(self.key_prefix .. i)
      if not (ttl and ttl >= 0 or err and err ~= "not found") then
        if self.keys[i] then
          self.index[self.keys[i]] = nil
          self.keys[i] = nil
        end
        self.expire_keys[i] = nil
      end
    end
  end

  -- The loop above only drops worker-local references, so the expired entries
  -- still have to be reclaimed from the dict itself. Callers that schedule
  -- flush_expired() themselves -- in a single process rather than in every
  -- worker -- turn this off with auto_flush_expired.
  if self.auto_flush_expired then
    self:flush_expired()
  end
end


-- Reclaims the expired entries of the shared dict, in batches.
--
-- The expired entries (both the __ngx_prom__key_N index slots and the metric
-- value keys, which live in the same dict) are only *logically* dead: every
-- dict API treats them as missing, but their slab pages stay allocated. The
-- passive per-write expiry scan cannot reclaim them either, because it stops
-- at the first non-expired entry at the LRU tail, and a permanent entry (the
-- error metric, or any metric registered without an exptime) inevitably ends
-- up sitting there. Without this the dict grows without bound under label
-- churn: index slots are never reused, so free_space steps down on every new
-- series and never recovers (apache/apisix#13658). Since expired entries are
-- indistinguishable from absent ones through every dict API, reclaiming them
-- cannot change any observable behaviour.
--
-- flush_expired() holds the dict lock for its whole scan of the LRU queue, so
-- it is called with a batch size: a bounded number of entries is reclaimed per
-- call, and the workers get the lock back in between.
--
-- Returns the number of entries reclaimed.
--
-- The batch loop itself, so that the public entry point below can guard it.
local function flush_expired_batches(self)
  local total = 0
  for _ = 1, FLUSH_EXPIRED_MAX_BATCHES do
    local freed = self.dict:flush_expired(FLUSH_EXPIRED_BATCH)
    total = total + freed
    -- freeing less than a full batch means this call has already walked the
    -- whole queue, so there is nothing left to reclaim
    if freed < FLUSH_EXPIRED_BATCH then
      break
    end

    ngx.sleep(FLUSH_EXPIRED_BATCH_DELAY)
  end

  return total
end


function KeyIndex:flush_expired()
  -- A round yields between batches, so the timer can fire again before the
  -- previous run finished (ngx.timer.every does not wait for it), and a caller
  -- scheduling its own round can overlap with it too. Two rounds racing for the
  -- dict lock double the stall and reclaim nothing extra, so the second one
  -- steps aside.
  if self.flushing then
    return 0
  end

  self.flushing = true
  -- pcall so that a throwing ngx.sleep -- a caller reaching this from a phase
  -- that does not allow yielding -- cannot leave the guard set and stop the
  -- reclamation for the life of the worker.
  local ok, total = pcall(flush_expired_batches, self)
  self.flushing = false

  if not ok then
    ngx.log(ngx.ERR, "failed to reclaim expired entries: ", tostring(total))
    return 0
  end

  return total
end

-- Loads new keys that might have been added by other workers since last sync.
function KeyIndex:sync()
  local delete_count = self.dict:get(self.delete_count) or 0
  local N = self.dict:get(self.key_count) or 0
  if self.deleted ~= delete_count then
    -- Some other worker deleted something, lets do a full sync.
    self:sync_range(0, N)
    self.deleted = delete_count
  elseif N ~= self.last then
    -- Sync only new keys, if there are any.
    self:sync_range(self.last, N)
  end
  return N
end

-- Iterates keys from first to last, adds new items and removes deleted items.
function KeyIndex:sync_range(first, last)
  for i = first, last do
    -- Read i-th key. If it is nil, it means it was deleted by some other thread.
    local key = self.dict:get(self.key_prefix .. i)
    if key then
      self.keys[i] = key
      self.index[key] = i

      -- if it is nil and ttl not is 0, set expire_keys map
      if not self.expire_keys[i] then
        local ttl, _ = self.dict:ttl(self.key_prefix .. i)
        if ttl and ttl ~= 0 then
          self.expire_keys[i] = true
        end
      end
    elseif self.keys[i] then
      self.index[self.keys[i]] = nil
      self.keys[i] = nil
      self.expire_keys[i] = nil
    end
  end
  self.last = last
end

-- Returns array of all keys.
function KeyIndex:list()
  self:sync()
  local copy = {}
  local i = 1
  -- Emit a key only from the slot the index currently points at
  -- (self.index[key] == idx). self.keys can transiently hold the same key value
  -- in two different slots (e.g. when an expired metric is re-added at a new
  -- slot before the old slot is reclaimed); listing the raw self.keys values
  -- would emit duplicate metrics. Consulting the index guarantees each key is
  -- listed exactly once, at its canonical slot. Iterating self.keys (not
  -- 0..self.last) keeps this O(live keys): self.last grows monotonically with
  -- every add and is never reclaimed, so a slot range scan would walk every
  -- dead slot ever created on long-lived, high-churn workers.
  for idx, key in pairs(self.keys) do
    if self.index[key] == idx then
      copy[i] = key
      i = i + 1
    end
  end
  return copy
end

-- Atomically adds one or more keys to the index.
--
-- Args:
-- key_or_keys: Single string or a list of strings containing keys to add.
--
-- Returns:
-- nil on success, string with error message otherwise
function KeyIndex:add(key_or_keys, err_msg_lru_eviction, exptime)
  local keys = key_or_keys
  if type(key_or_keys) == "string" then
    keys = { key_or_keys }
  end

  for _, key in pairs(keys) do
    local retried = false
    local repairs = 0
    local repair_forcible = false
    while true do
      local N = self:sync()
      if self.index[key] ~= nil then
        -- key already exists, if has exptime, set expire
        local expired = false
        if exptime then
          local ok, err = self.dict:expire(self.key_prefix .. self.index[key], exptime)
          if not ok then
            if err == "not found" then
              -- The slot already expired in the shared dict. Drop the stale
              -- local state and bump delete_count so other workers do a full
              -- sync and reclaim the slot; without this the old slot lingers in
              -- their local self.keys while the metric is re-added at a new slot,
              -- desynchronizing the index and causing duplicate metric emission.
              -- The dict slot is already gone (expire returned "not found"), so
              -- there is no slot to clear here.
              local idx = self.index[key]
              self.index[key] = nil
              self.keys[idx] = nil
              self.expire_keys[idx] = nil
              self.deleted = self.deleted + 1
              local _, incr_err, forcible = self.dict:incr(self.delete_count, 1, 0)
              if incr_err or forcible then
                return incr_err or err_msg_lru_eviction
              end
              expired = true
            else
              -- Unexpected expire error: the slot may still be live, so leave it
              -- as-is rather than re-adding it, which would create a duplicate.
              ngx.log(ngx.ERR, "failed to renew expire for key '", key, "': ",
                      tostring(err))
            end
          end
        end
        if not expired then
          if repair_forcible then
            -- the key was adopted from an occupied slot after repair
            -- increments that forcibly displaced other entries; report the
            -- eviction just like the new-slot success path does.
            return (err_msg_lru_eviction .. "; key index: adopted key after " ..
              "key_count repair: idx=" .. self.key_prefix .. self.index[key] ..
              ", key=" .. key)
          end
          break
        end
      end
      N = N+1
      local ok, err, forcible = self.dict:add(self.key_prefix .. N, key, exptime)
      if ok then
        local _, _, forcible2 = self.dict:incr(self.key_count, 1, 0)
        self.keys[N] = key
        self.index[key] = N
        if exptime and exptime > 0 then
          self.expire_keys[N] = true
        end
        if forcible or forcible2 or repair_forcible then
          return (err_msg_lru_eviction .. "; key index: add key: idx=" ..
                  self.key_prefix .. N .. ", key=" .. key)
        end
        break
      elseif err ~= "exists" then
        return "Unexpected error adding a key: " .. err
      end

      -- "exists": slot N is already occupied although key_count reported N-1.
      -- Once per key this can be a benign race with another worker that has
      -- created slot N but not incremented key_count yet, so retry and let
      -- sync() pick the new slot up. If it repeats, key_count has fallen
      -- behind the occupied slots: it is an ordinary shared-dict node, so on
      -- a full dict it can be LRU-evicted (it is only refreshed when new keys
      -- are registered, so it goes cold under steady traffic) and incr() then
      -- re-creates it at 1, far below the surviving slots. Retrying the same
      -- slot forever would spin the worker at 100% CPU with the shared-dict
      -- lock held hot (apache/apisix#12275). Advance the counter past the
      -- occupied slot instead: the next sync() adopts that slot's occupant
      -- and progress resumes.
      if retried then
        local _, incr_err, forcible3 = self.dict:incr(self.key_count, 1, 0)
        if incr_err then
          -- hard failure (e.g. "no memory"): give up immediately, mirroring
          -- the add() error path above, instead of burning the repair budget
          -- on retries that cannot succeed.
          return "Unexpected error advancing key_count: " .. incr_err
        end
        if forcible3 then
          -- re-creating an evicted key_count displaced another entry; surface
          -- it through the LRU-eviction warning on the success path, like
          -- forcible/forcible2.
          repair_forcible = true
        end
        -- The cap counts attempts, not successes: it exists to guarantee the
        -- loop terminates.
        repairs = repairs + 1
        if repairs >= MAX_KEY_COUNT_REPAIRS then
          return (err_msg_lru_eviction .. "; key index: key_count fell " ..
            "behind occupied slots; advanced it by " .. repairs ..
            " without finding a free slot, dropping key: " .. key)
        end
      end
      retried = true
    end
  end
end

-- Removes a key based on its value.
--
-- Args:
-- key: String value of the key, must exists in this index.
function KeyIndex:remove(key, err_msg_lru_eviction)
  local i = self.index[key]
  if i then
    self.index[key] = nil
    self.keys[i] = nil
    self.expire_keys[i] = nil
    self.dict:set(self.key_prefix .. i, nil)
    self.deleted = self.deleted + 1

    -- increment delete_count to signalize other workers that they should do a full sync
    local _, err, forcible = self.dict:incr(self.delete_count, 1, 0)
    if err or forcible then
      return err or err_msg_lru_eviction
    end
  else
    ngx.log(ngx.ERR, "Trying to remove non-existent key: ", key)
  end
end

return KeyIndex