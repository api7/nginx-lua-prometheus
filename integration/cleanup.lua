local p = cleanup_prometheus
local d = ngx.shared.cleanup_metrics
local index = p.key_index
local boundary = p:gauge("boundary", "Expiration boundary", {"attempt"}, 0.005)
local observed_boundary = false

for attempt = 1, 100 do
  boundary:set(42, {tostring(attempt)})
  local key = 'boundary{attempt="' .. attempt .. '"}'
  local i = assert(index.index[key])
  local slot = index.key_prefix .. i
  index.expire_keys[i] = 0
  local ttl
  repeat
    ngx.update_time()
    ttl = d:ttl(slot)
  until not ttl or ttl <= 0

  if ttl == 0 then
    local observed
    index.dict = setmetatable({ttl = function(_, name)
      local value, err = d:ttl(name)
      if name == slot then observed = value end
      return value, err
    end}, {__index = function(_, method)
      return function(_, ...) return d[method](d, ...) end
    end})
    index:remove_expired_keys()
    index.dict = d
    if observed == 0 then
      assert(index.expire_keys[i] ~= nil, "zero-TTL slot lost its tracking")
      p:flush_expired()
      index:remove_expired_keys()
      assert(index.keys[i] == nil and index.index[key] == nil,
             "reclaimed slot retained its local index")
      d:delete(key)
      observed_boundary = true
      break
    end
  end
end
assert(observed_boundary, "did not observe the expiration boundary")

-- A reader that first imports a slot at its expiration boundary.
local KeyIndex = require("prometheus_keys")
local imported_boundary = false
for attempt = 1, 100 do
  boundary:set(42, {"import" .. attempt})
  local key = 'boundary{attempt="import' .. attempt .. '"}'
  local slot = index.key_prefix .. assert(index.index[key])
  local ttl
  repeat
    ngx.update_time()
    ttl = d:ttl(slot)
  until not ttl or ttl <= 0

  if ttl == 0 then
    local observed
    local reader = KeyIndex.new(setmetatable({ttl = function(_, name)
      local value, err = d:ttl(name)
      if name == slot then observed = value end
      return value, err
    end}, {__index = function(_, method)
      return function(_, ...) return d[method](d, ...) end
    end}), index.key_prefix:sub(1, -5), 0)
    reader:sync()
    local i = reader.index[key]
    if observed == 0 and i then
      assert(reader.expire_keys[i] ~= nil, "zero-TTL import was not tracked")
      reader.dict = d
      p:flush_expired()
      reader:remove_expired_keys()
      assert(reader.keys[i] == nil and reader.index[key] == nil,
             "reclaimed import retained its local index")
      imported_boundary = true
      break
    end
  end
end
assert(imported_boundary, "did not import a slot at the expiration boundary")

p:flush_expired()
d:set("permanent", 42)
for i = 1, 10 do
  assert(d:set("expired_" .. i, i, 0.01))
end
ngx.sleep(0.02)
assert(p:flush_expired() == 10, "did not reclaim the expired entries")
assert(d:get("permanent") == 42, "permanent entry was reclaimed")
ngx.say("ok")
