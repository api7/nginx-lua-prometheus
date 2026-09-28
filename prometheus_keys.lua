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

-- Entries a single flush_expired() call may reclaim. That call holds the shared
-- dict lock until it returns, so this is what bounds how long the other workers
-- can be kept waiting on the lock: measured at ~1ms per 10000 entries reclaimed
-- on OpenResty 1.29.2.4.
local FLUSH_EXPIRED_BATCH = 10000

-- Upper bound on the batches one call may run, so a large backlog is drained
-- over several ticks instead of in a single long stretch. What is left over is
-- picked up by the next tick.
local FLUSH_EXPIRED_MAX_BATCHES = 30

-- Pause between batches, so the lock is not taken back to back.
local FLUSH_EXPIRED_BATCH_DELAY = 1


-- check and remove expired keys
local function remove_expired_keys(_, self)
  self:remove_expired_keys()
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

  ngx.timer.every(remove_expired_keys_interval or 600, remove_expired_keys, self)
  return self
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

  for i, _ in pairs(self.expire_keys) do
    -- A slot is in one of three states, and only ttl() tells them apart --
    -- get() reports the last two alike, as nil:
    --
    --   (1) live:                                 ttl > 0
    --   (2) past its ttl, node still in the dict:  ttl < 0
    --   (3) node physically reclaimed:             "not found"
    --
    -- In state (2) the node is still there and expire() resurrects it with its
    -- value intact, so the slot is left exactly as it is: the key keeps its
    -- number and comes back on it. Listing it in the meantime costs nothing --
    -- metric_data() skips a key whose value has expired. Only state (3) gives
    -- the number up.
    local _, err = self.dict:ttl(self.key_prefix .. i)
    if err == "not found" then
      self:forget_slot(i)
    end
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
function KeyIndex:flush_expired()
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
      -- safe_add, so that a full dict costs a metric that cannot be registered
      -- rather than one that is evicted to make room: a plain add() would drop
      -- the LRU tail, which is a cold permanent entry -- the error metric, a
      -- metric configured without an expire, or key_count itself, whose loss
      -- makes every slot above the rebuilt counter invisible. "no memory" is
      -- returned to the caller, which accounts for it on the error metric.
      local ok, err, forcible = self.dict:safe_add(self.key_prefix .. N, key, exptime)
      if err == "no memory" then
        return err_msg_lru_eviction .. "; key index: no memory for idx=" ..
               self.key_prefix .. N .. ", key=" .. key
      end
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