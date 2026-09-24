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

-- Slot numbers this worker keeps for reuse after seeing them reclaimed. They
-- go to the next keys that need one, so a registration costs the same single
-- write it always did: a number taken in the meantime just fails that write.
local FREE_SLOTS_KEEP = 8192

-- Slots examined per reclaim round for numbers that died while this worker was
-- not looking: before it started, or in a slot it never held. A tenth of the
-- range per round covers all of it in ten, and each slot is one read.
local RECLAIM_SCAN_DIVISOR = 10
local RECLAIM_SCAN_MIN = 1000
local RECLAIM_SCAN_MAX = 50000

-- Reclaimed numbers tried before falling back to a fresh slot past key_count.
local FREE_SLOT_ATTEMPTS = 4

-- Reused slot numbers kept in the shared dict for a scrape to pick up. A
-- scrape further behind than this walks every slot instead. The entries are
-- created up front and always rewritten with a value of the same length, so
-- publishing one is an in-place write that cannot fail for want of memory --
-- on a full dict a write that had to allocate would either evict a metric or
-- be dropped, and a dropped one costs the scrape a walk over every slot.
-- One entry per 16 KiB of the dict, so the trail costs about 0.9% of it and
-- covers a few thousand reuses between two scrapes -- past that the scrape
-- walks every slot, which is correct but is what the trail exists to avoid.
local REUSE_RING_MAX = 8192
local REUSE_RING_MIN = 1024
local REUSE_RING_BYTES_PER_ENTRY = 16384
local REUSE_ENTRY_FORMAT = "%012d:%09d"

-- Attempts at raising key_count above a slot number. Concurrent raises can
-- only overshoot, so this is about retrying a lost race, not about progress.
local KEY_COUNT_RAISE_ATTEMPTS = 3


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
  self.reuse_count = prefix .. "reuse_count"
  self.reuse_slot = prefix .. "reuse_slot_"
  self.reuse_ready = prefix .. "reuse_ring"
  self.seen_reuse = 0
  self.free_slots = {}
  self.free_n = 0
  self.scan_cursor = 1
  self.last = 0
  self.deleted = 0
  self.not_expired_index = 1
  self.keys = {}
  self.index = {}
  self.hidden = {}
  self.expire_keys = {}

  local capacity = self.dict.capacity and self.dict:capacity() or 0
  self.ring_size = math.max(REUSE_RING_MIN,
    math.min(REUSE_RING_MAX, math.floor(capacity / REUSE_RING_BYTES_PER_ENTRY)))
  self:init_reuse_ring()

  ngx.timer.every(remove_expired_keys_interval or 600, remove_expired_keys, self)
  return self
end


-- Creates the trail entries once, so that publishing one later never has to
-- allocate. Whichever process gets here first does it; the others see the
-- marker and skip the writes.
function KeyIndex:init_reuse_ring()
  local ok = self.dict:add(self.reuse_ready, 1)
  if not ok then
    return
  end

  local blank = string.format(REUSE_ENTRY_FORMAT, 0, 0)
  for i = 0, self.ring_size - 1 do
    self.dict:add(self.reuse_slot .. i, blank)
  end
end

-- check and remove expired keys
function KeyIndex:remove_expired_keys()
  -- Reclaiming first means the scan below sees the final state of every slot
  -- and can give up the numbers that are really gone in the same round.
  -- Callers that schedule flush_expired() themselves -- in a single process
  -- rather than in every worker -- turn this off with auto_flush_expired.
  if self.auto_flush_expired then
    self:flush_expired()
  end

  for i, _ in pairs(self.expire_keys) do
    -- A slot is in one of three states, and only ttl() tells them apart --
    -- get() reports the last two alike, as nil:
    --
    --   (1) live:                                  ttl > 0
    --   (2) past its ttl, node still in the dict:   ttl < 0
    --   (3) node physically reclaimed:              "not found"
    --
    -- In state (2) the slot can still be taken back in place by its own key,
    -- so its number is kept; only the key stops being listed, because its
    -- value has expired. In state (3) the number is gone and may be reused by
    -- another key, so every reference to it has to go.
    local ttl, err = self.dict:ttl(self.key_prefix .. i)
    if err == "not found" then
      self:clear_slot(i)
    elseif ttl and ttl < 0 then
      self:hide_slot(i)
    end
  end

  self:scan_for_free_slots()
end


-- The loop above only reaches the slots this worker registered itself. A number
-- that died before it started -- what a reload or a restart leaves behind -- or
-- one that died in a slot it never held is in nobody's expire_keys here, and
-- without this pass it would never be reused: key_count would keep the high
-- water mark of the previous generation for ever.
function KeyIndex:scan_for_free_slots()
  local last = self.dict:get(self.key_count) or 0
  if last < 1 or self.free_n >= FREE_SLOTS_KEEP then
    return
  end

  local window = math.max(RECLAIM_SCAN_MIN,
    math.min(RECLAIM_SCAN_MAX, math.floor(last / RECLAIM_SCAN_DIVISOR)))
  local i = self.scan_cursor
  for _ = 1, window do
    if i > last then
      i = 1
    end

    if not self.keys[i] and self.dict:get(self.key_prefix .. i) == nil then
      -- only a node that is gone frees its number; one that is merely past its
      -- ttl still belongs to the key that can take it back
      local _, err = self.dict:ttl(self.key_prefix .. i)
      if err == "not found" then
        self.free_n = self.free_n + 1
        self.free_slots[self.free_n] = i
        if self.free_n >= FREE_SLOTS_KEEP then
          i = i + 1
          break
        end
      end
    end

    i = i + 1
  end

  self.scan_cursor = i
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
  -- A walk of the whole range is also the only chance a worker gets to notice
  -- the slots that died before it started -- after a reload, or a restart, the
  -- numbers a previous generation gave up are in nobody's expire_keys, and
  -- without this they would never be reused.
  local whole_range = first == 0

  for i = first, last do
    -- Read i-th key. If it is nil, it means it was deleted by some other thread.
    local key = self.dict:get(self.key_prefix .. i)
    if key then
      self:set_slot(i, key)

      -- if it is nil and ttl not is 0, set expire_keys map
      if not self.expire_keys[i] then
        local ttl, _ = self.dict:ttl(self.key_prefix .. i)
        if ttl and ttl ~= 0 then
          self.expire_keys[i] = true
        end
      end
    elseif whole_range and i > 0 and not self.keys[i]
           and self.free_n < FREE_SLOTS_KEEP then
      -- a slot this worker has no record of: only its node being gone makes the
      -- number reusable, and only ttl() can tell that from a node that is
      -- merely past its ttl and still belongs to its own key
      local _, err = self.dict:ttl(self.key_prefix .. i)
      if err == "not found" then
        self.free_n = self.free_n + 1
        self.free_slots[self.free_n] = i
      end
    elseif self.keys[i] then
      -- The slot holds no live key, which is all a scrape needs to know, so it
      -- is only hidden here -- one read per slot. Telling "past its ttl" from
      -- "node reclaimed" costs a second read and decides whether the number
      -- can be given up, so that belongs to remove_expired_keys(), which runs
      -- on a timer rather than on every scrape.
      self:hide_slot(i)
    end
  end
  self.last = last
end


-- self.keys (slot -> key) and self.index (key -> slot) are two views of one
-- mapping, and every local change goes through the three helpers below so the
-- views cannot drift apart. Leaving the previous occupant's index entry behind
-- would point a later add() at a slot that is no longer its own, and dropping
-- the index entry of a key that has since moved would hide a live key from
-- list().
function KeyIndex:set_slot(i, key)
  local old = self.keys[i]
  if old and old ~= key and self.index[old] == i then
    self.index[old] = nil
  end
  self.keys[i] = key
  self.index[key] = i
  self.hidden[i] = nil
end


-- State (2), past its ttl with the node still in the dict: the key stops being
-- listed, because its value has expired, but the slot number is kept so that
-- add() can take it back in place.
function KeyIndex:hide_slot(i)
  if self.keys[i] then
    self.hidden[i] = true
  end
end


-- State (3), node physically reclaimed: the number may be handed to another
-- key from now on, so every reference to it goes.
function KeyIndex:clear_slot(i)
  -- Its number can go to another key now. Keeping it here is what makes reuse
  -- free to look for: the next key that needs a slot takes one of these and
  -- writes it, exactly as it would write a fresh one.
  if self.free_n < FREE_SLOTS_KEEP then
    self.free_n = self.free_n + 1
    self.free_slots[self.free_n] = i
  end

  local key = self.keys[i]
  if key and self.index[key] == i then
    self.index[key] = nil
  end
  self.keys[i] = nil
  self.hidden[i] = nil
  self.expire_keys[i] = nil
end


-- Raises key_count above a slot number. A slot above key_count is invisible to
-- list(), which walks 0..key_count; that happens when key_count -- an ordinary
-- dict entry -- is LRU-evicted and incr() re-creates it below the slots that
-- are already in use.
function KeyIndex:ensure_key_count(idx)
  for _ = 1, KEY_COUNT_RAISE_ATTEMPTS do
    local n = self.dict:get(self.key_count) or 0
    if n >= idx then
      return
    end

    -- Concurrent raises can only overshoot, which costs a few empty slots in
    -- the next scan. Undershooting would hide the slot, so the result is
    -- checked rather than assumed.
    local new = self.dict:incr(self.key_count, idx - n, 0)
    if not new or new >= idx then
      return
    end
  end
end


-- Re-reads one slot. Unlike sync_range it leaves self.last alone: a reused
-- slot can be anywhere below it.
function KeyIndex:sync_slot(i)
  local key = self.dict:get(self.key_prefix .. i)
  if key then
    self:set_slot(i, key)
    if not self.expire_keys[i] then
      local ttl = self.dict:ttl(self.key_prefix .. i)
      if ttl and ttl ~= 0 then
        self.expire_keys[i] = true
      end
    end
  elseif self.keys[i] then
    self:hide_slot(i)
  end
end


-- Re-reads the slots written below self.last since the last scrape. The ring
-- holds "<seq>:<slot>", so an entry that has been overwritten since -- or that
-- a writer had not finished publishing -- is recognised, and the caller walks
-- every slot instead. Re-reading a slot twice is harmless; missing one is not.
function KeyIndex:follow_reuse(reuse)
  for seq = self.seen_reuse + 1, reuse do
    local entry = self.dict:get(self.reuse_slot .. seq % self.ring_size)
    if not entry then
      return false
    end

    local at, idx = entry:match("^(%d+):(%d+)$")
    if tonumber(at) ~= seq then
      return false
    end

    self:sync_slot(tonumber(idx))
  end

  return true
end


-- Returns array of all keys.
function KeyIndex:list()
  -- The scrape is the only caller that needs the whole set of keys, and the
  -- only one that must not miss any. Slot numbers are taken back in place and
  -- reused between keys, so a slot below self.last can change without
  -- key_count or delete_count moving, and an incremental sync would never read
  -- it again. Every such write publishes its slot number, so the usual case is
  -- re-reading just those; only a scrape that has fallen too far behind, or
  -- one that finds the trail incomplete, walks every slot.
  local reuse = self.dict:get(self.reuse_count) or 0
  if reuse == self.seen_reuse then
    self:sync()
  elseif reuse - self.seen_reuse <= self.ring_size and self:follow_reuse(reuse) then
    self:sync()
  else
    self.deleted = self.dict:get(self.delete_count) or 0
    self:sync_range(0, self.dict:get(self.key_count) or 0)
  end
  self.seen_reuse = reuse

  local copy = {}
  local i = 1
  -- Emit a key only from the slot the index currently points at
  -- (self.index[key] == idx). self.keys can transiently hold the same key value
  -- in two different slots (e.g. when an expired metric is re-added at a new
  -- slot before the old slot is reclaimed); listing the raw self.keys values
  -- would emit duplicate metrics. Consulting the index guarantees each key is
  -- listed exactly once, at its canonical slot.
  for idx, key in pairs(self.keys) do
    if not self.hidden[idx] and self.index[key] == idx then
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
    local err = self:add_key(key, err_msg_lru_eviction, exptime)
    if err then
      return err
    end
  end
end


-- Registers a single key. Returns nil on success, an error message otherwise.
function KeyIndex:add_key(key, err_msg_lru_eviction, exptime)
  local idx = self.index[key]
  if idx then
    local occupant = self.dict:get(self.key_prefix .. idx)

    -- By far the common case: the slot is ours and live, so only its ttl has
    -- to be pushed out, and none of the shared counters are even read.
    -- Reading the slot first is also what makes reusing slot numbers safe: an
    -- index entry left over from a slot that has since been handed to another
    -- key does not match here, where renewing it blindly would extend that
    -- other key's ttl while this key stays unregistered.
    if occupant == key then
      if not exptime then
        return
      end

      local ok, err = self.dict:expire(self.key_prefix .. idx, exptime)
      if ok then
        if exptime > 0 then
          self.expire_keys[idx] = true
        end
        return
      end

      if err ~= "not found" then
        -- Unexpected expire error: the slot may still be live, so leave it
        -- as-is rather than re-adding it, which would create a duplicate.
        ngx.log(ngx.ERR, "failed to renew expire for key '", key, "': ",
                tostring(err))
        return
      end
      -- "not found": the node was reclaimed between the two calls, so fall
      -- through and take the slot number back below
      occupant = nil
    end

    if occupant == nil then
      -- The slot holds no visible key: it is either past its ttl or already
      -- reclaimed. Taking it back in place keeps this key on the slot number
      -- the other workers already know, so nothing has to be broadcast, and
      -- key_count does not grow every time a metric expires and comes back.
      local ok, err, forcible = self.dict:add(self.key_prefix .. idx, key, exptime)
      if ok or (err == "exists" and
                self.dict:get(self.key_prefix .. idx) == key) then
        -- either we took it back, or a peer took it back for us
        self:claim_slot(idx, key, exptime)
        if forcible then
          return (err_msg_lru_eviction .. "; key index: re-claimed slot: idx=" ..
                  self.key_prefix .. idx .. ", key=" .. key)
        end
        return
      end
    end

    -- The slot belongs to another key now. Only this key's own reference is
    -- dropped: self.keys[idx] describes the new occupant and stays.
    if self.index[key] == idx then
      self.index[key] = nil
    end
  end

  return self:alloc_slot(key, err_msg_lru_eviction, exptime)
end


-- Records a slot this worker has just written below the other workers'
-- self.last, and makes sure a full scan can reach it.
function KeyIndex:claim_slot(idx, key, exptime)
  self:set_slot(idx, key)
  if exptime and exptime > 0 then
    self.expire_keys[idx] = true
  end
  self:ensure_key_count(idx)

  -- An incremental sync would never read this slot again, so the number is
  -- published for the scrape to re-read. The workers never read this, so
  -- nothing is forced to re-scan on a request. safe_set, so that publishing
  -- never evicts a metric to make room for itself: on a full dict the entry is
  -- simply not written, and the scrape falls back to walking every slot.
  local seq = self.dict:incr(self.reuse_count, 1, 0)
  if seq then
    self.dict:safe_set(self.reuse_slot .. seq % self.ring_size,
                       string.format(REUSE_ENTRY_FORMAT, seq, idx))
  end
end


-- Gives a key a slot it does not have yet: one this worker has seen reclaimed,
-- if it has one, a fresh one past key_count otherwise.
--
-- Without reuse key_count only ever grows: a metric whose labels are never
-- seen again leaves its number behind for good, and under label churn the
-- range a scrape walks grows without bound (apache/apisix#13658).
function KeyIndex:alloc_slot(key, err_msg_lru_eviction, exptime)
  for _ = 1, FREE_SLOT_ATTEMPTS do
    if self.free_n == 0 then
      break
    end

    local idx = self.free_slots[self.free_n]
    self.free_slots[self.free_n] = nil
    self.free_n = self.free_n - 1

    -- a number taken since it was put aside simply fails here
    local ok, _, forcible = self.dict:add(self.key_prefix .. idx, key, exptime)
    if ok then
      self:claim_slot(idx, key, exptime)
      if forcible then
        return (err_msg_lru_eviction .. "; key index: reused slot: idx=" ..
                self.key_prefix .. idx .. ", key=" .. key)
      end
      return
    end
  end

  local retried = false
  local repairs = 0
  local repair_forcible = false
  while true do
    local N = self:sync()

    -- another worker may have registered this key while we were looking
    local existing = self.index[key]
    if existing and self.dict:get(self.key_prefix .. existing) == key then
      if exptime then
        self.dict:expire(self.key_prefix .. existing, exptime)
        if exptime > 0 then
          self.expire_keys[existing] = true
        end
      end
      if repair_forcible then
        return (err_msg_lru_eviction .. "; key index: adopted key after " ..
          "key_count repair: idx=" .. self.key_prefix .. existing ..
          ", key=" .. key)
      end
      return
    end

    N = N + 1
    local ok, err, forcible = self.dict:add(self.key_prefix .. N, key, exptime)
    if ok then
      local _, _, forcible2 = self.dict:incr(self.key_count, 1, 0)
      self:set_slot(N, key)
      if exptime and exptime > 0 then
        self.expire_keys[N] = true
      end
      if forcible or forcible2 or repair_forcible then
        return (err_msg_lru_eviction .. "; key index: add key: idx=" ..
                self.key_prefix .. N .. ", key=" .. key)
      end
      return
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


-- Removes a key based on its value.
--
-- Args:
-- key: String value of the key, must exists in this index.
function KeyIndex:remove(key, err_msg_lru_eviction)
  local i = self.index[key]
  if i then
    self:clear_slot(i)
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