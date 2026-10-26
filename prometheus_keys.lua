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

local DEFAULT_REMOVE_EXPIRED_KEYS_INTERVAL = 600

-- Yield between groups of shared-dictionary TTL checks.
local REMOVE_EXPIRED_KEYS_YIELD_EVERY = 500

KeyIndex.REMOVE_EXPIRED_KEYS_YIELD_EVERY = REMOVE_EXPIRED_KEYS_YIELD_EVERY


-- check and remove expired keys
local function remove_expired_keys(premature, self)
  if not premature then
    self:remove_expired_keys()
  end
end


local function run_remove_expired_keys_every(self, interval)
  local ok, err = ngx.timer.every(interval, remove_expired_keys, self)
  if not ok then
    ngx.log(ngx.ERR, "failed to start the remove expired keys timer: ", err)
  end
end


-- Stagger worker cleanup timers to avoid starting every scan together.
local function start_remove_expired_keys_timer(self, interval)
  local id = ngx.worker.id() or 0
  local count = ngx.worker.count()

  local offset = interval * id / count
  if offset <= 0 then
    run_remove_expired_keys_every(self, interval)
    return
  end

  local ok, err = ngx.timer.at(offset, function(premature, s)
    if premature then
      return
    end
    run_remove_expired_keys_every(s, interval)
    s:remove_expired_keys()
  end, self)
  if not ok then
    ngx.log(ngx.ERR, "failed to start the remove expired keys timer: ", err)
  end
end


-- remove_expired_keys_interval: seconds between cleanup rounds; 0 or less
-- starts no cleanup timer. auto_flush_expired: false leaves
-- dict:flush_expired() to the caller.
function KeyIndex.new(shared_dict, prefix, remove_expired_keys_interval,
                      auto_flush_expired)
  local self = setmetatable({}, KeyIndex)
  self.dict = shared_dict
  self.key_prefix = prefix .. "key_"
  self.delete_count = prefix .. "delete_count"
  self.key_count = prefix .. "key_count"
  self.generation = prefix .. "generation"
  self.flush_marker = prefix .. "flush_expired"
  self.last = 0
  self.deleted = 0
  self.keys = {}
  self.index = {}
  self.aliases = {}
  self.expire_keys = {}

  local interval = remove_expired_keys_interval or
    DEFAULT_REMOVE_EXPIRED_KEYS_INTERVAL
  self.remove_expired_keys_interval = interval
  -- The dict truncates exptime to milliseconds, and 0 would never expire.
  self.flush_marker_ttl = math.max(interval, 0.001)
  -- the flush marker expires after the interval, so it needs a positive one
  self.auto_flush_expired = auto_flush_expired ~= false and interval > 0
  if interval > 0 then
    start_remove_expired_keys_timer(self, interval)
  end
  return self
end

-- ttl() reports a node that expired exactly 5ms ago as "not found" too: the
-- C function returns expires - now in milliseconds, and -5 is NGX_DECLINED.
-- Forgetting that node would hide it once expire() revives it in place.
local function slot_gone(self, i, err)
  return err == "not found" and self.dict:get_stale(self.key_prefix .. i) == nil
end

-- Drops the worker-local references to the slots that are gone.
local function remove_expired_slots(self)
  -- Snapshot due slots before yielding: add() may mutate expire_keys while
  -- this coroutine is suspended. Future deadlines need no dictionary lookup.
  local now = ngx.now()
  local due = {}
  for i, deadline in pairs(self.expire_keys) do
    if deadline <= now then
      due[#due + 1] = i
    end
  end

  for j, i in ipairs(due) do
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
    local deadline = self.expire_keys[i]
    if deadline and deadline <= now then
      -- Keep expired nodes until reclamation: expire() can renew them in place.
      local ttl, err = self.dict:ttl(self.key_prefix .. i)
      if ttl and ttl > 0 then
        self.expire_keys[i] = now + ttl
      elseif slot_gone(self, i, err) then
        self:forget_slot(i)
      end
    end
  end
end

-- check and remove expired keys
function KeyIndex:remove_expired_keys()
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

  -- The loop above only drops worker-local references. The expired shared-dict
  -- entries themselves (both the __ngx_prom__key_N index slots and the metric
  -- value keys, which live in the same dict) are only *logically* dead: every
  -- dict API treats them as missing, but their slab pages stay allocated. The
  -- passive per-write expiry scan cannot reclaim them either, because it stops
  -- at the first non-expired entry at the LRU tail, and a permanent entry (the
  -- error metric, or any metric registered without an exptime) inevitably ends
  -- up sitting there. Without this call the dict grows without bound under
  -- label churn: index slots are never reused, so free_space steps down on
  -- every new series and never recovers (apache/apisix#13658).
  --
  -- flush_expired() walks the whole dict under its lock, and one call reclaims
  -- for every worker, so two reclamations are at least an interval apart: the
  -- first process to finish its local cleanup after the marker has expired
  -- runs it. The marker is never deleted. A round that finds it still set
  -- skips the reclamation, which delays it until a later round.
  --
  -- A worker that has never seen an entry with an exptime has nothing to
  -- reclaim, and must not take the interval from one that has. The flag is
  -- never cleared: forgetting the last expiring slot does not mean its metric
  -- values are gone, as counters renew them up to sync_interval later.
  if not self.auto_flush_expired or not self.has_expiring_entries
      or ngx.worker.exiting() then
    return
  end
  -- safe_add: add() would force-evict a live entry to store the marker. A dict
  -- too full to hold it is the one that most needs reclaiming, so flush anyway.
  local added, add_err = self.dict:safe_add(self.flush_marker, true,
                                            self.flush_marker_ttl)
  if added or add_err == "no memory" then
    self.dict:flush_expired()
  end
end

-- Tokens for the generation key (see advance_key_count()), unique per
-- process: a respawned worker keeps its id, but not its pid.
local tokens = 0
local function new_token()
  tokens = tokens + 1
  local process = ngx.worker.pid and ngx.worker.pid() or ngx.worker.id()
  return ngx.now() .. ":" .. tostring(process) .. ":" .. tokens
end

-- Loads new keys that might have been added by other workers since last sync.
function KeyIndex:sync()
  -- The token is read before key_count: a re-creation in between leaves the
  -- old token recorded, so the next sync still sees it change. The other way
  -- round, an old key_count paired with the new token hides the re-creation.
  local generation = self.dict:get(self.generation)
  local delete_count = self.dict:get(self.delete_count) or 0
  local N = self.dict:get(self.key_count) or 0
  if self.deleted ~= delete_count or generation == nil
     or generation ~= self.synced_generation or N < self.last then
    -- Some other worker deleted something, or key_count was re-created (see
    -- advance_key_count()), lets do a full sync. A missing token may have
    -- hidden a re-creation, so publish a new one and read key_count again
    -- after it: a re-creation after the publish replaces the token, one
    -- before it shows in the new count. If that fails, the next sync is a
    -- full one too.
    if generation == nil then
      generation = new_token()
      local ok, err, forcible = self.dict:add(self.generation, generation)
      if ok then
        N = self.dict:get(self.key_count) or 0
      else
        generation = nil
      end
      if err and err ~= "exists" or forcible then
        ngx.log(ngx.ERR, "key index: publishing the key_count token: ",
                tostring(err), ", forcible: ", tostring(forcible))
      end
    end
    self:sync_range(0, N)
    self.deleted = delete_count
    self.synced_generation = generation
  elseif N ~= self.last then
    -- Sync only new keys, if there are any.
    self:sync_range(self.last, N)
  end
  return N
end

-- Iterates keys from first to last, adds new items and removes deleted items.
function KeyIndex:sync_range(first, last)
  for i = first, last do
    -- A writer can revive an expired node without advancing key_count.
    local key = self.dict:get_stale(self.key_prefix .. i)
    if key then
      self:remember_slot(i, key)
      if not self.expire_keys[i] then
        local ttl, err = self.dict:ttl(self.key_prefix .. i)
        if ttl then
          if ttl ~= 0 then
            self.expire_keys[i] = ngx.now() + ttl
            self.has_expiring_entries = true
          end
        elseif slot_gone(self, i, err) then
          self:forget_slot(i)
        else
          -- Kept at the ttl() sentinel; check it again on the next round.
          self.expire_keys[i] = ngx.now()
          self.has_expiring_entries = true
        end
      end
    elseif self.keys[i] then
      self:forget_slot(i)
    end
  end
  self.last = last
end


-- Allocate a reverse set only when concurrent registrations create aliases.
function KeyIndex:remember_slot(i, key)
  if self.keys[i] == key then
    return
  end
  if self.keys[i] then
    self:forget_slot(i)
  end
  local previous = self.index[key]
  if previous then
    local aliases = self.aliases[key] or {[previous] = true}
    aliases[i] = true
    self.aliases[key] = aliases
  end
  self.keys[i] = key
  self.index[key] = i
end

-- Keep another alias discoverable without scanning the entire local index.
function KeyIndex:forget_slot(i)
  local key = self.keys[i]
  if key then
    local aliases = self.aliases[key]
    if aliases then
      aliases[i] = nil
      local remaining = next(aliases)
      if self.index[key] == i then
        self.index[key] = remaining
      end
      if not remaining or next(aliases, remaining) == nil then
        self.aliases[key] = nil
      end
    elseif self.index[key] == i then
      self.index[key] = nil
    end
  end
  self.keys[i] = nil
  self.expire_keys[i] = nil
end

-- Returns array of all keys.
function KeyIndex:list()
  self:sync()
  local copy = {}
  local i = 1
  -- Emit one slot per name, including retained expired nodes. metric_data()
  -- filters missing values. Iterating local slots avoids scanning the entire
  -- monotonically growing key_count range on every scrape.
  for idx, key in pairs(self.keys) do
    if self.index[key] == idx then
      copy[i] = key
      i = i + 1
    end
  end
  return copy
end

-- key_count is an ordinary dict node: when it is evicted, the next
-- registration re-creates it at 1 and the numbers below every worker's
-- self.last are handed out again, where incremental syncs never look. No
-- observer can be relied on to notice -- renewals do not sync, and by a
-- reader's next sync the count may be past its self.last again -- so the
-- writer that re-creates it tells every worker to do a full sync. Only incr()
-- with an init creates it, and that is the one call that returns 1. The very
-- first registration looks the same and costs one full sync of a single slot.
--
-- The signal is a token rather than a counter: should it be evicted too, a
-- re-created counter could land on the very value a worker last saw.
local function advance_key_count(self)
  local n, err, forcible = self.dict:incr(self.key_count, 1, 0)
  if n == 1 then
    local ok, set_err, set_forcible = self.dict:set(self.generation,
                                                    new_token())
    if not ok then
      ngx.log(ngx.ERR, "failed to broadcast a re-created key_count: ",
              set_err)
      -- the old token would hide the re-creation, a missing one cannot
      self.dict:delete(self.generation)
    end
    forcible = forcible or set_forcible
  end
  return n, err, forcible
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
  if now then
    self.has_expiring_entries = true
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
        if now then
          self.expire_keys[mine] = now + exptime
        end
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
              -- -- nothing has to be broadcast, unless key_count was lost,
              -- which advance_key_count() broadcasts. Bumping
              -- delete_count here, as this used to, sent every worker through
              -- a full sync of key_count slots on its request path, and made
              -- each of them forget every slot that was merely past its ttl,
              -- so those metrics took new numbers too (apache/apisix#13658).
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
        local _, _, forcible2 = advance_key_count(self)
        self:remember_slot(N, key)
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
        local _, incr_err, forcible3 = advance_key_count(self)
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
    repeat
      self:forget_slot(i)
      local slot = self.key_prefix .. i
      -- Eviction and a counter reset may have reassigned an old alias.
      if self.dict:get_stale(slot) == key then
        self.dict:set(slot, nil)
      end
      i = self.index[key]
    until not i
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