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


-- How many slots remove_expired_keys() checks between two yields. Each check
-- is one dict read, so a run of them stays well under a millisecond, and a
-- reader with a large index still gives the event loop back regularly.
local SWEEP_YIELD_EVERY = 500
KeyIndex.SWEEP_YIELD_EVERY = SWEEP_YIELD_EVERY


local function remove_expired_keys(premature, self)
  if premature then
    return
  end
  self:remove_expired_keys()
end


-- A slot (prefix .. "key_" .. N) names one metric. It is written once, by the
-- worker that creates the metric's value in the dict (see register()), and it
-- never expires on its own: nothing has to renew it while the metric is in use.
--
-- Only the processes that read the index -- the ones calling list(), which is
-- usually a single one serving /metrics -- keep a local copy of it, and they
-- are the ones that give a slot back once the value it names is gone. The
-- other workers only ever write slots, so they keep no local state and do no
-- dict reads on the request path.
function KeyIndex.new(shared_dict, prefix, remove_expired_keys_interval)
  local self = setmetatable({}, KeyIndex)
  self.dict = shared_dict
  self.key_prefix = prefix .. "key_"
  self.key_count = prefix .. "key_count"
  self.last = 0
  self.keys = {}
  self.index = {}

  ngx.timer.every(remove_expired_keys_interval or 600, remove_expired_keys, self)
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
        self.dict:delete(self.key_prefix .. i)
        self.keys[i] = nil
      else
        local value, err = self.dict:get(key)
        if value == nil and not err then
          -- The value is gone (expired or deleted). Should it be created
          -- again, the worker creating it registers it in a new slot.
          self.dict:delete(self.key_prefix .. i)
          self.keys[i] = nil
          self.index[key] = nil
        end
      end
    end
  end
end


function KeyIndex:remove_expired_keys()
  if not self.reading then
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
  if N ~= self.last then
    self:sync_range(self.last, N)
  end
  return N
end

-- Reads the slots from first to last.
function KeyIndex:sync_range(first, last)
  for i = first, last do
    local key = self.dict:get(self.key_prefix .. i)
    if key then
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

-- Puts a key in a new slot. Called by the worker that has just created the
-- key's value in the dict, which is what makes it the only one doing so: the
-- value is created by add(), which only one worker can win.
--
-- Returns:
-- nil on success, string with error message otherwise
function KeyIndex:register(key, err_msg_lru_eviction)
  local retried = false
  local repairs = 0
  local repair_forcible = false
  while true do
    local N = (self.dict:get(self.key_count) or 0) + 1
    local ok, err, forcible = self.dict:add(self.key_prefix .. N, key)
    if ok then
      local _, _, forcible2 = self.dict:incr(self.key_count, 1, 0)
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
      local _, incr_err, forcible3 = self.dict:incr(self.key_count, 1, 0)
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

return KeyIndex