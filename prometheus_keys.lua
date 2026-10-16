-- Storage to keep track of used keys. Allows to atomically register and list
-- keys. The keys are synchronized between nginx workers
-- using ngx.shared.dict. The whole purpose of this module is to avoid
-- using ngx.shared.dict:get_keys (see https://github.com/openresty/lua-nginx-module#ngxshareddictget_keys),
-- which blocks all workers and therefore it shouldn't be used with large
-- amounts of keys.
local KeyIndex = {}
KeyIndex.__index = KeyIndex

-- Upper bound on how far a single register() call may advance key_count while
-- it repairs a counter that has fallen behind occupied slots (see the comment
-- in register()). It bounds the worst-case latency of one call; the advanced counter
-- is shared through the dict, so later calls resume where this one stopped
-- and the index converges even when far more slots need repairing.
local MAX_KEY_COUNT_REPAIRS = 1000
local MAX_PENDING_KEYS = 1000
local RETRY_BATCH = 10
local MAX_CHECKED_KEYS = 4096
local CHECK_INTERVAL = 1


-- Yield between bounded batches of slot checks so a reader with a large
-- index gives the event loop back regularly.
local SWEEP_YIELD_EVERY = 500
KeyIndex.SWEEP_YIELD_EVERY = SWEEP_YIELD_EVERY


local function remove_expired_keys(premature, self)
  if premature then
    return
  end
  self:remove_expired_keys()
end


-- A slot names one metric and never expires on its own. A shared owner hint
-- lets a writer find its slot without loading the reader's complete index.
--
-- Readers and designated collectors keep a local copy of the index and give
-- slots back once their values are gone. The
-- other workers keep no index copy. They validate a shared owner hint when
-- updating values, and retain only a bounded queue of failed registrations.
-- Worker 0 collects even before the first scrape unless configured otherwise.
function KeyIndex.new(shared_dict, prefix, remove_expired_keys_interval, collector)
  local self = setmetatable({}, KeyIndex)
  self.dict = shared_dict
  self.key_prefix = prefix .. "key_"
  self.key_count = prefix .. "key_count"
  self.owner_prefix = prefix .. "owner_"
  self.pending = {}
  self.pending_count = 0
  self.checked = {}
  self.checked_keys = {}
  self.next_checked = 1
  if collector == nil then
    collector = ngx.worker.id() == 0
  end
  self.collector = collector
  self.last = 0
  self.keys = {}
  self.index = {}

  ngx.timer.every(remove_expired_keys_interval or 600, remove_expired_keys, self)
  ngx.timer.every(1, function(premature)
    if not premature then
      self:retry_pending()
    end
  end)
  return self
end


-- Gives back the slots that no longer name a live metric. Run by the readers
-- only: a slot is checked against the value it names, and the only processes
-- that know which slots exist are the ones that have read them.
local function sweep(self)
  self:sync()

  -- collected first: the loop below yields, and a pairs() traversal must not
  -- span a yield, during which sync() may insert into self.keys
  local slots, n = {}, 0
  for i in pairs(self.keys) do
    n = n + 1
    slots[n] = i
  end

  for j = 1, n do
    if j > 1 and (j - 1) % SWEEP_YIELD_EVERY == 0 then
      ngx.sleep(0)
      -- do not hold up a reload: what is left is gone with the worker anyway
      if ngx.worker.exiting() then
        return
      end
    end

    local i = slots[j]
    local key = self.keys[i]
    if key then
      if self.index[key] ~= i then
        -- an older slot of a metric that has been registered again
        if self.dict:get(self.key_prefix .. i) == key then
          self.dict:delete(self.key_prefix .. i)
        end
        if self.dict:get(self.owner_prefix .. key) == i then
          self.dict:delete(self.owner_prefix .. key)
        end
        self.keys[i] = nil
      else
        local value, err = self.dict:get(key)
        if value == nil and not err then
          -- The value is gone (expired or deleted). Should it be created
          -- again, the worker creating it registers it in a new slot.
          if self.dict:get(self.key_prefix .. i) == key then
            self.dict:delete(self.key_prefix .. i)
          end
          if self.dict:get(self.owner_prefix .. key) == i then
            self.dict:delete(self.owner_prefix .. key)
          end
          self.keys[i] = nil
          self.index[key] = nil
        end
      end
    end
  end
end


function KeyIndex:remove_expired_keys()
  if not self.reading and not self.collector then
    return
  end

  -- A run yields, so the timer can fire again before the previous one has
  -- finished; two would only check the same slots twice.
  if self.sweeping then
    return
  end

  self.sweeping = true
  local ok, err = pcall(sweep, self)
  self.sweeping = false
  if not ok then
    ngx.log(ngx.ERR, "failed to remove expired keys: ", tostring(err))
  end

  -- The expired values themselves are only *logically* dead: every dict API
  -- treats them as missing, but their slab pages stay allocated. The passive
  -- per-write expiry scan cannot reclaim them either, because it stops at the
  -- first non-expired entry at the LRU tail, and a permanent entry inevitably
  -- ends up sitting there (apache/apisix#13658). Since expired entries are
  -- indistinguishable from absent ones through every dict API, reclaiming them
  -- cannot change any observable behaviour.
  self.dict:flush_expired()
end

-- Loads the slots written since the last sync.
function KeyIndex:sync()
  local N = self.dict:get(self.key_count) or 0
  if N < self.last then
    self:sync_range(0, N)
  elseif N ~= self.last then
    self:sync_range(self.last, N)
  end
  return N
end

-- Reads the slots from first to last.
function KeyIndex:sync_range(first, last)
  for i = first, last do
    local slot = self.key_prefix .. i
    local key, _, stale = self.dict:get_stale(slot)
    if key and (not stale or self.dict:get(key) ~= nil) then
      -- Adopt legacy TTL slots before their nodes are reclaimed. Writers also
      -- register existing values, so a lost legacy slot can be repaired.
      self.dict:expire(slot, 0)
      self.keys[i] = key
      -- A metric created again after its value expired is registered in a new
      -- slot. Every reader keeps the newest one, and remove_expired_keys()
      -- gives the older ones back.
      local prev = self.index[key]
      if not prev or prev < i then
        self.index[key] = i
      end
    end
  end
  self.last = last
end

-- Returns array of all keys.
function KeyIndex:list()
  self.reading = true
  self:sync()
  local copy = {}
  local i = 1
  -- Emit a key only from the slot the index currently points at
  -- (self.index[key] == idx): a metric created again after its value expired
  -- holds a second slot until remove_expired_keys() gives the older one back.
  -- Iterating self.keys (not 0..self.last) keeps this O(live keys), as
  -- self.last grows with every registration and is never reclaimed.
  for idx, key in pairs(self.keys) do
    if self.index[key] == idx then
      copy[i] = key
      i = i + 1
    end
  end
  return copy
end

-- Metadata repair must not evict the values it is trying to expose.
local function ensure_counter(self, minimum)
  local n = self.dict:get(self.key_count)
  if not n then
    local ok, err = self.dict:safe_add(self.key_count, minimum)
    if not ok and err ~= "exists" then
      return nil, err
    end
    n = self.dict:get(self.key_count)
    if not n then
      return nil, "not found"
    end
  end
  if n < minimum then
    return self.dict:incr(self.key_count, minimum - n)
  end
  return n
end

-- Allocate a slot and publish its owner hint. Concurrent repairs can create
-- duplicate slots; readers retain the newest and reclaim the older ones.
--
-- Returns:
-- nil on success, string with error message otherwise
function KeyIndex:register(key, err_msg_lru_eviction, exptime)
  local retried = false
  local repairs = 0
  local repair_forcible = false
  while true do
    local count, count_err = ensure_counter(self, 0)
    if not count then
      return "Unexpected error restoring key_count: " .. count_err
    end
    local N = count + 1
    local ok, err, forcible = self.dict:safe_add(self.key_prefix .. N, key)
    if ok then
      local _, publish_err, forcible2 = self.dict:incr(self.key_count, 1)
      if publish_err then
        return "Unexpected error advancing key_count: " .. publish_err
      end
      local stored, store_err = self.dict:safe_set(self.owner_prefix .. key, N, exptime)
      if not stored then
        return "Unexpected error recording key ownership: " .. store_err
      end
      if forcible or forcible2 or repair_forcible then
        return (err_msg_lru_eviction .. "; key index: add key: idx=" ..
                self.key_prefix .. N .. ", key=" .. key)
      end
      return nil
    elseif err ~= "exists" then
      return "Unexpected error adding a key: " .. err
    end

    -- "exists": slot N is already occupied although key_count reported N-1.
    -- Once per key this can be a benign race with another worker that has
    -- created slot N but not incremented key_count yet, so retry and read
    -- key_count again. If it repeats, key_count has fallen behind the occupied
    -- slots: it is an ordinary shared-dict node, so on a full dict it can be
    -- LRU-evicted and incr() then re-creates it at 1, far below the surviving
    -- slots. Retrying the same slot forever would spin the worker at 100% CPU
    -- with the shared-dict lock held hot (apache/apisix#12275). Advance the
    -- counter past the occupied slot instead.
    if retried then
      local _, incr_err, forcible3 = self.dict:incr(self.key_count, 1)
      if incr_err then
        return "Unexpected error advancing key_count: " .. incr_err
      end
      if forcible3 then
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

-- Validate only this value's registration, never the full index. The owner
-- entry is a hint: its slot must still name the same metric after eviction.
function KeyIndex:ensure(key, err_msg_lru_eviction, exptime, created)
  created = created or (self.pending[key] and self.pending[key].created)
  local now = ngx.now()
  if not created and not self.pending[key]
      and self.checked[key] and self.checked[key] > now then
    return nil
  end
  local i = self.dict:get(self.owner_prefix .. key)
  local err
  if not created and i and self.dict:get(self.key_prefix .. i) == key then
    local count, forcible
    count, err, forcible = ensure_counter(self, i)
    if forcible then
      err = err_msg_lru_eviction
    elseif not count and not err then
      err = "failed to restore key_count"
    end
    if not err and exptime then
      local ok
      ok, err = self.dict:expire(self.owner_prefix .. key, exptime)
      if not ok and err == "not found" then
        err = self:register(key, err_msg_lru_eviction, exptime)
      end
    end
  else
    err = self:register(key, err_msg_lru_eviction, exptime)
  end

  if err then
    if self.checked[key] then
      self.checked[key] = 0
    end
    if self.pending[key] or self.pending_count < MAX_PENDING_KEYS then
      if not self.pending[key] then
        self.pending_count = self.pending_count + 1
      end
      self.pending[key] = {
        message = err_msg_lru_eviction, exptime = exptime, created = created,
      }
    else
      return err .. "; key index: registration retry queue is full"
    end
  else
    if self.pending[key] then
      self.pending[key] = nil
      self.pending_count = self.pending_count - 1
    end
    if self.checked[key] == nil then
      local old = self.checked_keys[self.next_checked]
      if old then
        self.checked[old] = nil
      end
      self.checked_keys[self.next_checked] = key
      self.next_checked = self.next_checked % MAX_CHECKED_KEYS + 1
    end
    local interval = CHECK_INTERVAL
    if exptime and exptime > 0 then
      interval = math.min(interval, exptime / 2)
    end
    self.checked[key] = now + interval
  end
  return err
end

function KeyIndex:retry_pending()
  local n = 0
  for key, entry in pairs(self.pending) do
    if n >= RETRY_BATCH then
      break
    end
    n = n + 1
    if self.dict:get(key) == nil then
      self.pending[key] = nil
      self.pending_count = self.pending_count - 1
    else
      self:ensure(key, entry.message, entry.exptime, entry.created)
    end
  end
end

return KeyIndex