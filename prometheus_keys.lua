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

-- Slots the local scan of remove_expired_keys() asks the dict about between two
-- yields.
--
-- Asking the dict for a slot's ttl is a lock acquisition, and doing it for
-- every slot the worker knows has one is what a round used to cost: with
-- 150,000 such slots across 8 workers and a steady 20k rps it showed up as a
-- ~480ms stall at p99.9, of which the reclamation itself was 5%. With 200,000
-- slots and 10 workers whose rounds fire together it was a ~1s stall of every
-- worker at once, all of them spinning on the dict lock.
--
-- Two things bound it now. The scan skips any slot whose recorded deadline has
-- not passed, without touching the dict, so the lock acquisitions are
-- proportional to what has actually lapsed rather than to the index. And it
-- yields after every this many of them, so a round where everything has lapsed
-- still keeps the worker's event loop running: ~0.2ms of work between two
-- yields. Each round still covers every lapsed slot, so the worker-local index
-- cannot fall behind the rate the metrics expire at.
local REMOVE_EXPIRED_KEYS_YIELD_EVERY = 500


-- Exposed so a caller that schedules the reclamation itself can reason about it:
-- one round drains at most FLUSH_EXPIRED_BATCH * FLUSH_EXPIRED_MAX_BATCHES
-- entries and may occupy its timer for FLUSH_EXPIRED_MAX_BATCHES *
-- FLUSH_EXPIRED_BATCH_DELAY seconds.
KeyIndex.FLUSH_EXPIRED_BATCH = FLUSH_EXPIRED_BATCH
KeyIndex.FLUSH_EXPIRED_MAX_BATCHES = FLUSH_EXPIRED_MAX_BATCHES
KeyIndex.FLUSH_EXPIRED_BATCH_DELAY = FLUSH_EXPIRED_BATCH_DELAY
KeyIndex.REMOVE_EXPIRED_KEYS_YIELD_EVERY = REMOVE_EXPIRED_KEYS_YIELD_EVERY


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

  start_remove_expired_keys_timer(self, remove_expired_keys_interval or 600)
  return self
end

-- Drops the worker-local references to the slots whose node is gone.
local function remove_expired_slots(self)
  -- self.expire_keys[i] is when this worker last saw slot i expire. While
  -- that is still in the future the slot was there and nothing but a forced
  -- eviction can have taken it, so there is nothing to ask the dict -- which
  -- is what makes a round cost nothing when nothing has lapsed. Should a
  -- forced eviction have taken it anyway, the only consequence is a local
  -- reference that outlives its slot, and add() drops it on the next
  -- observation of that key.
  --
  -- The lapsed slots are collected first, in plain Lua: the scan below yields,
  -- and a pairs() traversal must not span a yield, during which add() may
  -- insert into self.expire_keys.
  local now = ngx.now()
  local due, n = {}, 0
  for i, deadline in pairs(self.expire_keys) do
    if deadline <= now then
      n = n + 1
      due[n] = i
    end
  end

  for j = 1, n do
    if j > 1 and (j - 1) % REMOVE_EXPIRED_KEYS_YIELD_EVERY == 0 then
      ngx.sleep(0)
      -- do not hold up a reload: what is left is gone with the worker anyway
      if ngx.worker.exiting() then
        return
      end
      now = ngx.now()
    end

    -- re-read: while this round yielded the slot may have been renewed, or
    -- dropped by add()
    local i = due[j]
    local deadline = self.expire_keys[i]
    if deadline and deadline <= now then
      -- A slot is in one of three states, and only ttl() tells them apart --
      -- get() reports the last two alike, as nil:
      --
      --   (1) live:                                 ttl > 0
      --   (2) past its ttl, node still in the dict:  ttl < 0
      --   (3) node physically reclaimed:             "not found"
      --
      -- In state (2) the node is still there and expire() resurrects it with
      -- its value intact, so the slot is left exactly as it is: the key keeps
      -- its number and comes back on it. Listing it in the meantime costs
      -- nothing -- metric_data() skips a key whose value has expired. Only
      -- state (3) gives the number up.
      local ttl, err = self.dict:ttl(self.key_prefix .. i)
      if err == "not found" then
        self:forget_slot(i)
      elseif ttl and ttl > 0 then
        -- another worker renewed it, so wait out the new ttl instead of
        -- asking again every round
        self.expire_keys[i] = now + ttl
      elseif ttl == 0 then
        -- made permanent: it will never lapse
        self.expire_keys[i] = nil
      end
    end
  end
end


-- check and remove expired keys
function KeyIndex:remove_expired_keys()
  -- Reclaiming first means the scan below sees the final state of every slot,
  -- so a number whose node has just been reclaimed is given up in this round
  -- rather than the next. Callers that schedule flush_expired() themselves --
  -- in a single process rather than in every worker -- turn this off with
  -- auto_flush_expired.
  if self.auto_flush_expired then
    self:flush_expired()
  end

  -- A round yields, so the timer can fire again before the previous run
  -- finished (ngx.timer.every does not wait for it). Two rounds would only ask
  -- the dict about the same slots twice, so the second one steps aside.
  if self.removing then
    return
  end

  self.removing = true
  -- pcall so that a throwing ngx.sleep -- a caller reaching this from a phase
  -- that does not allow yielding -- cannot leave the guard set and stop the
  -- local scan for the life of the worker.
  local ok, err = pcall(remove_expired_slots, self)
  self.removing = false

  if not ok then
    ngx.log(ngx.ERR, "failed to remove expired keys: ", tostring(err))
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
  for batch = 1, FLUSH_EXPIRED_MAX_BATCHES do
    local freed = self.dict:flush_expired(FLUSH_EXPIRED_BATCH)
    total = total + freed
    -- freeing less than a full batch means this call has already walked the
    -- whole queue, so there is nothing left to reclaim. The pause belongs
    -- between two batches, so there is none after the last one either -- it
    -- would just hold up the timer for a round that is already over.
    if freed < FLUSH_EXPIRED_BATCH or batch == FLUSH_EXPIRED_MAX_BATCHES then
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

      -- if it is nil and ttl not is 0, record when the slot expires
      if not self.expire_keys[i] then
        local ttl, _ = self.dict:ttl(self.key_prefix .. i)
        if ttl and ttl ~= 0 then
          self.expire_keys[i] = ngx.now() + ttl
        end
      end
    elseif self.keys[i] then
      -- get() cannot tell a slot that is merely past its ttl from one whose
      -- node is gone, and the two must not be treated alike: the first keeps
      -- its number (see remove_expired_keys), only the second gives it up.
      local _, err = self.dict:ttl(self.key_prefix .. i)
      if err == "not found" then
        self:forget_slot(i)
      end
    end
  end
  self.last = last
end


-- Drops every local reference to a slot whose node is gone.
--
-- self.keys (slot -> key) and self.index (key -> slot) are two views of one
-- mapping, and the index entry is only this slot's to drop while it still
-- points here. A key that has since moved to another slot keeps a live entry
-- there, and dropping it would hide that key from list() although its value is
-- in the dict -- which is what the delete_count broadcast used to paper over.
function KeyIndex:forget_slot(i)
  local key = self.keys[i]
  if key and self.index[key] == i then
    self.index[key] = nil
  end
  self.keys[i] = nil
  self.expire_keys[i] = nil
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

  -- when the slot's expiry is renewed or set below, remember when it lands, so
  -- that remove_expired_keys() does not have to ask the dict about it
  local now = exptime and exptime > 0 and ngx.now()

  for _, key in pairs(keys) do
    local retried = false
    local repairs = 0
    local repair_forcible = false

    -- The common case by far: this key has a slot and the slot still holds it,
    -- so only its ttl has to be pushed out. Reading the slot costs one dict
    -- read where sync() costs two, and none of the shared counters are touched
    -- -- so nothing another worker does can turn this into a full sync.
    local mine = self.index[key]
    if mine and exptime and self.dict:get(self.key_prefix .. mine) == key then
      local ok, err = self.dict:expire(self.key_prefix .. mine, exptime)
      if ok then
        goto renewed
      end
      if err ~= "not found" then
        ngx.log(ngx.ERR, "failed to renew expire for key '", key, "': ",
                tostring(err))
        goto renewed
      end
    end

    while true do
      local N = self:sync()
      if self.index[key] ~= nil then
        -- key already exists, if has exptime, set expire
        local expired = false
        if exptime then
          local ok, err = self.dict:expire(self.key_prefix .. self.index[key], exptime)
          if ok and now then
            self.expire_keys[self.index[key]] = now + exptime
          end
          if not ok then
            if err == "not found" then
              -- The node is gone, so this number is no longer ours to renew.
              -- The key takes a fresh slot below, which is above every worker's
              -- self.last and is therefore picked up by their incremental sync
              -- -- nothing has to be broadcast. Bumping delete_count here, as
              -- this used to, sent every worker through a full sync of
              -- key_count slots on its request path, and made each of them
              -- forget every slot that was merely past its ttl, so those
              -- metrics took new numbers too (apache/apisix#13658).
              self:forget_slot(self.index[key])
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
        if now then
          self.expire_keys[N] = now + exptime
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
    ::renewed::
  end
end

-- Removes a key based on its value.
--
-- Args:
-- key: String value of the key, must exists in this index.
function KeyIndex:remove(key, err_msg_lru_eviction)
  local i = self.index[key]
  if i then
    self:forget_slot(i)
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