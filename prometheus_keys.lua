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


-- check and remove expired keys
local function remove_expired_keys(_, self)
  self:remove_expired_keys()
end


function KeyIndex.new(shared_dict, prefix, remove_expired_keys_interval)
  local self = setmetatable({}, KeyIndex)
  self.dict = shared_dict
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
        if forcible or forcible2 then
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
        self.dict:incr(self.key_count, 1, 0)
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