local KeyIndex = require('prometheus_keys')
local d = ngx.shared.index_metrics

local function new(prefix)
  return KeyIndex.new(d, prefix, 3600)
end

-- A fresh reader must remember a stale node before a writer revives it.
local writer = new("reader_")
assert(not writer:add("target", "eviction", 0.01))
assert(not writer:add("tail", "eviction"))
ngx.sleep(0.02)
local reader = new("reader_")
reader:sync()
assert(not writer:add("target", "eviction", 60))
assert(reader.index.target == writer.index.target, "revived slot was not discovered")
assert(d:get("reader_key_count") == 2, "renewal allocated an unnecessary slot")

-- Expiration can occur between the fast-path read and the renewal.
writer = new("boundary_")
assert(not writer:add("target", "eviction", 60))
assert(not writer:add("tail", "eviction"))
reader = new("boundary_")
local injected = false
writer.dict = setmetatable({get = function(_, slot)
  local value = d:get(slot)
  if not injected and slot == "boundary_key_1" then
    injected = true
    assert(d:expire(slot, 0.001))
    ngx.sleep(0.01)
    reader:sync()
    assert(d:get(slot) == nil, "slot did not expire")
  end
  return value
end}, {__index = function(_, method)
  return function(_, ...) return d[method](d, ...) end
end})
assert(not writer:add("target", "eviction", 60))
assert(injected and reader.index.target == 1, "renewal race hid the metric")

-- Concurrent registrations can leave a live low slot and a stale high alias.
assert(d:set("alias_key_1", "target", 60))
assert(d:set("alias_key_2", "target", 0.01))
assert(d:set("alias_key_count", 2))
ngx.sleep(0.02)
reader = new("alias_")
reader:sync()
d:flush_expired()
reader:remove_expired_keys()
assert(reader.index.target == 1, "reclaimed alias hid the live slot")
assert(#reader:list() == 1, "duplicate or missing metric")
assert(not reader:add("target", "eviction", 60))
assert(d:get("alias_key_count") == 2, "alias recovery allocated another slot")
reader:remove("target", "eviction")
assert(#reader:list() == 0, "deleted metric retained an alias")

-- A reused alias belongs to its new writer, not to the deleting reader.
assert(d:set("owner_key_1", "target", 60))
assert(d:set("owner_key_2", "target", 60))
assert(d:set("owner_key_count", 2))
reader = new("owner_")
reader:sync()
d:delete("owner_key_1")
d:delete("owner_key_count")
local replacement = new("owner_")
assert(not replacement:add("replacement", "eviction", 60))
assert(replacement.index.replacement == 1)
reader:remove("target", "eviction")
assert(d:get("owner_key_1") == "replacement", "deleted another slot owner")
assert(d:get_stale("owner_key_2") == nil, "target alias remained")

-- Exercise the public metric API and the explicit-deletion broadcast.
local p = index_writer
local peer = index_reader
local target = p:gauge("target", "Returning metric", nil, 0.01)
local removable = p:gauge("removable", "Deleted metric")
local tail = p:gauge("tail", "Permanent metric")
target:set(1)
removable:set(1)
tail:set(1)
ngx.sleep(0.02)
peer:gauge("target", "Returning metric", nil, 0.01)
local peer_removable = peer:gauge("removable", "Deleted metric")
peer:gauge("tail", "Permanent metric")
assert(not table.concat(peer:metric_data()):find("\ntarget 1\n", 1, true))
peer_removable:del()
target:set(42)
assert(table.concat(peer:metric_data()):find("\ntarget 42\n", 1, true),
       "public API omitted the returning metric")
ngx.say("ok")
