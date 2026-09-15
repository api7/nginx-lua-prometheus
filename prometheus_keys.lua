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

-- Upper bound on how many generation switches a single add() call follows
-- for one key before it gives up on that key.
local MAX_GENERATION_RETRIES = 3

-- Slots are never reused: every key that expires or is removed leaves a dead
-- slot behind, and every full sync (a worker starting, or any delete) walks
-- all of them. Once key_count reaches COMPACT_MIN_SLOTS and is at least
-- COMPACT_RATIO times the live keys, remove_expired_keys() copies the live
-- keys into a fresh generation of slots and drops the old one, so key_count
-- stays proportional to the live keys instead of to the keys ever created.
local COMPACT_MIN_SLOTS = 10000
local COMPACT_RATIO = 2
-- Slots handled between yields while compacting.
local COMPACT_BATCH = 1000
-- The compaction lock and the old generation's key_count expire after this
-- many seconds; the lock is refreshed after every batch.
local COMPACT_LOCK_TTL = 60
-- An add() in another worker writes its slot before it increments key_count,
-- so up to one slot per concurrent writer may sit past key_count. Scans that
-- must not miss such slots look this far past key_count.
local COMPACT_TAIL_SLACK = 64
local COMPACT_ERR_PREFIX = "key index compaction: "


-- check and remove expired keys
local function remove_expired_keys(premature, self)
  if premature then
    return
  end
  self:remove_expired_keys()
end

-- Generation 0 keeps the historical key names, so workers running a version
-- without compaction still share the same index while nginx reloads.
local function generation_names(prefix, gen)
  if gen == 0 then
    return prefix .. "key_", prefix .. "key_count"
  end
  return prefix .. gen .. "_key_", prefix .. gen .. "_key_count"
end

-- ngx.sleep(0) is a posted event, which does not keep an exiting worker alive:
-- the worker would exit with the compaction suspended and its lock held. A
-- sleep timer does, and an exiting worker finishes without yielding.
local function yield()
  if ngx.get_phase() == "timer" and not ngx.worker.exiting() then
    ngx.sleep(0.001)
  end
end


function KeyIndex.new(shared_dict, prefix, remove_expired_keys_interval)
  local self = setmetatable({}, KeyIndex)
  self.dict = shared_dict
  self.prefix = prefix
  self.gen_key = prefix .. "gen"
  self.lock_key = prefix .. "compact_lock"
  self.delete_count = prefix .. "delete_count"
  self.deleted = 0
  self.not_expired_index = 1
  self.compact_min_slots = COMPACT_MIN_SLOTS
  self:use_generation(0)

  ngx.timer.every(remove_expired_keys_interval or 600, remove_expired_keys, self)
  return self
end

-- Points this worker at generation `gen` with an empty local view; the next
-- sync() loads it.
function KeyIndex:use_generation(gen)
  self.gen = gen
  self.key_prefix, self.key_count = generation_names(self.prefix, gen)
  self.last = 0
  self.keys = {}
  self.index = {}
  self.expire_keys = {}
end

-- check and remove expired keys
function KeyIndex:remove_expired_keys()
  self:sync()
  for i, _ in pairs(self.expire_keys) do
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

  -- The loop above only drops worker-local references. The expired shared-dict
  -- entries themselves (both the __ngx_prom__key_N index slots and the metric
  -- value keys, which live in the same dict) are only *logically* dead: every
  -- dict API treats them as missing, but their slab pages stay allocated. The
  -- passive per-write expiry scan cannot reclaim them either, because it stops
  -- at the first non-expired entry at the LRU tail, and a permanent entry (the
  -- error metric, or any metric registered without an exptime) inevitably ends
  -- up sitting there. Without this call the dict grows without bound under
  -- label churn: index slots are never reused, so free_space steps down on
  -- every new series and never recovers (apache/apisix#13658). Since expired
  -- entries are indistinguishable from absent ones through every dict API,
  -- reclaiming them here cannot change any observable behaviour.
  self.dict:flush_expired()

  self:compact()
end

-- Loads new keys that might have been added by other workers since last sync.
function KeyIndex:sync()
  local gen = self.dict:get(self.gen_key) or 0
  if gen > self.gen then
    -- Another worker compacted the index; the slots tracked locally are
    -- being deleted.
    self:use_generation(gen)
  elseif gen < self.gen then
    -- The generation only ever grows, so a lower value means its node was
    -- evicted from a full dict.
    self.dict:set(self.gen_key, self.gen)
  end

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
    local slot_forcible = false
    local switches = 0
    while true do
      if switches > MAX_GENERATION_RETRIES then
        return ("key index: generation switched " .. switches ..
          " times while adding key, dropping key: " .. key)
      end
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
          if exptime and (self.dict:get(self.gen_key) or 0) > self.gen then
            -- A compaction switched generations after this worker synced and
            -- may have reconciled the copy before this renewal of the old
            -- slot, so renew the key again in the current generation. A lower
            -- generation only means its node was evicted (see sync()).
            switches = switches + 1
            retried = false
            goto continue
          end
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
      local slot = self.key_prefix .. N
      local ok, err, forcible = self.dict:add(slot, key, exptime)
      if ok then
        local _, _, forcible2 = self.dict:incr(self.key_count, 1, 0)
        slot_forcible = slot_forcible or forcible or forcible2
        if (self.dict:get(self.gen_key) or 0) <= self.gen then
          self.keys[N] = key
          self.index[key] = N
          if exptime and exptime > 0 then
            self.expire_keys[N] = true
          end
          if slot_forcible or repair_forcible then
            return (err_msg_lru_eviction .. "; key index: add key: idx=" ..
                    slot .. ", key=" .. key)
          end
          break
        end
        -- A compaction switched generations after this worker synced, so the
        -- slot may land in the generation being dropped after its keys were
        -- copied. Delete it and register the key in the current generation.
        self.dict:delete(slot)
        switches = switches + 1
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
      if not ok and retried then
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
      -- after a generation switch the slots start over, so an earlier
      -- "exists" in the old generation says nothing about the new one
      retried = not ok
      ::continue::
    end
  end
end

-- Removes a key based on its value.
--
-- Args:
-- key: String value of the key, must exists in this index.
function KeyIndex:remove(key, err_msg_lru_eviction)
  self:sync()
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

-- Yields, then extends the compaction lock. Returns why the compaction has to
-- stop, or nil to go on.
function KeyIndex:refresh_compact_lock(token)
  yield()
  if ngx.worker.exiting() then
    return "worker is exiting"
  end
  if self.dict:get(self.lock_key) ~= token then
    return "lost lock"
  end
  self.dict:expire(self.lock_key, COMPACT_LOCK_TTL)
end

function KeyIndex:release_compact_lock(token)
  if self.dict:get(self.lock_key) == token then
    self.dict:delete(self.lock_key)
  end
end

-- Deletes slots first..last of the generation named by `slot_prefix`,
-- yielding between batches.
function KeyIndex:delete_slots(slot_prefix, first, last)
  for i = first, last do
    self.dict:delete(slot_prefix .. i)
    if i % COMPACT_BATCH == 0 then
      yield()
    end
  end
end

-- Deletes the leftovers of a compaction into generation `gen` that stopped
-- before switching to it; those slots are written densely from 1.
function KeyIndex:clear_generation(gen)
  local slot_prefix, count_key = generation_names(self.prefix, gen)
  local misses, i = 0, 1
  while misses < COMPACT_TAIL_SLACK do
    if self.dict:get(slot_prefix .. i) == nil then
      misses = misses + 1
    else
      misses = 0
      self.dict:delete(slot_prefix .. i)
    end
    i = i + 1
  end
  self.dict:delete(count_key)
end

-- Copies the live keys into generation self.gen + 1 and switches every worker
-- to it once most slots of the current generation are dead. Runs in at most
-- one worker at a time.
--
-- The copy is written before the generation switch, so other workers never
-- list a partial index. Changes other workers make to the old generation
-- while it is copied are applied afterwards: keys removed in the meantime are
-- dropped from the copy, and slots written past the copied range are added.
-- add() re-registers keys whose slot it wrote into the old generation after
-- the switch.
function KeyIndex:compact()
  local N = self:sync()
  if N < self.compact_min_slots or ngx.worker.exiting() then
    return
  end
  local live = 0
  for _ in pairs(self.index) do
    live = live + 1
  end
  if N < live * COMPACT_RATIO then
    return
  end

  local token = ngx.worker.pid() .. ":" .. N
  local locked, lock_err = self.dict:add(self.lock_key, token, COMPACT_LOCK_TTL)
  if not locked then
    if lock_err ~= "exists" then
      ngx.log(ngx.ERR, COMPACT_ERR_PREFIX, "failed to take lock: ", lock_err)
    end
    return
  end

  local from_prefix, from_count = self.key_prefix, self.key_count
  local to_gen = self.gen + 1
  local to_prefix, to_count = generation_names(self.prefix, to_gen)
  self:clear_generation(to_gen)

  local keys, index, expire_keys, origin, copied_ttl = {}, {}, {}, {}, {}
  local M = 0
  local function abort(msg)
    self:delete_slots(to_prefix, 1, M)
    self.dict:delete(to_count)
    self:release_compact_lock(token)
    if not ngx.worker.exiting() then
      ngx.log(ngx.ERR, COMPACT_ERR_PREFIX, msg)
    end
  end

  for i = 1, N do
    local slot = from_prefix .. i
    local key = self.dict:get(slot)
    local ttl = key and self.dict:ttl(slot)
    if ttl and not index[key] then
      local exptime = ttl > 0 and ttl or nil
      M = M + 1
      local ok, err, forcible = self.dict:set(to_prefix .. M, key, exptime)
      if not ok or forcible then
        return abort(err or "copying a slot evicted other entries")
      end
      keys[M], index[key], origin[M], copied_ttl[M] = key, M, i, ttl
      if exptime then
        expire_keys[M] = true
      end
    end
    if i % COMPACT_BATCH == 0 then
      local stop = self:refresh_compact_lock(token)
      if stop then
        return abort(stop .. " while copying slots")
      end
    end
  end

  local ok, err, forcible = self.dict:set(to_count, M)
  if not ok or forcible then
    return abort(err or "writing key_count evicted other entries")
  end
  local stop = self:refresh_compact_lock(token)
  if stop then
    return abort(stop .. " before switching generation")
  end
  ok, err = self.dict:set(self.gen_key, to_gen)
  if not ok then
    return abort(err)
  end

  self:use_generation(to_gen)
  self.keys, self.index, self.expire_keys, self.last = keys, index, expire_keys, M

  -- Workers renew and remove keys in the old generation until they switch,
  -- and in the new one afterwards. A ttl above the one read while copying
  -- (0 for permanent keys) means the slot was renewed: keep the key while
  -- either slot holds it unrenewed or renewed, never shorten the new slot,
  -- and drop keys whose unrenewed slot is gone from either generation.
  local dropped = false
  for j, i in pairs(origin) do
    local key, copied = keys[j], copied_ttl[j]
    local old_slot, new_slot = from_prefix .. i, to_prefix .. j
    local old_ttl = self.dict:get(old_slot) == key and self.dict:ttl(old_slot)
    local new_ttl = self.dict:get(new_slot) == key and self.dict:ttl(new_slot)
    local old_renewed = old_ttl and old_ttl > copied
    local keep
    if new_ttl then
      keep = old_ttl or new_ttl > copied
      if old_renewed and new_ttl > 0 and new_ttl < old_ttl then
        self.dict:expire(new_slot, old_ttl)
      end
    elseif old_renewed then
      keep = self.dict:add(new_slot, key, old_ttl)
    end
    if not keep then
      if new_ttl then
        self.dict:delete(new_slot)
      end
      if self.index[key] == j then
        self.index[key] = nil
      end
      self.keys[j] = nil
      self.expire_keys[j] = nil
      dropped = true
    end
    if j % COMPACT_BATCH == 0 then
      yield()
    end
  end
  if dropped then
    self.deleted = self.deleted + 1
    self.dict:incr(self.delete_count, 1, 0)
  end

  local last = N
  repeat
    local first = last + 1
    last = (self.dict:get(from_count) or 0) + COMPACT_TAIL_SLACK
    for i = first, last do
      local slot = from_prefix .. i
      local key = self.dict:get(slot)
      local ttl = key and self.dict:ttl(slot)
      if ttl then
        local add_err = self:add(key, COMPACT_ERR_PREFIX .. "evicted entries",
                                 ttl > 0 and ttl or nil)
        if add_err then
          ngx.log(ngx.ERR, COMPACT_ERR_PREFIX, add_err)
        end
      end
      if i % COMPACT_BATCH == 0 then
        yield()
      end
    end
  until (self.dict:get(from_count) or 0) + COMPACT_TAIL_SLACK <= last

  self:delete_slots(from_prefix, 1, last)
  -- Not deleted: a late add() in another worker would re-create it without
  -- an expiry.
  self.dict:expire(from_count, COMPACT_LOCK_TTL)
  self:release_compact_lock(token)
end

return KeyIndex
