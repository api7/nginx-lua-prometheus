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
    index.dict = {ttl = function(_, name)
      local value, err = d:ttl(name)
      if name == slot then observed = value end
      return value, err
    end}
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

p:flush_expired()
d:set("permanent", 42)
for i = 1, 50010 do
  assert(d:set("expired_" .. i, i, 0.01))
end
ngx.sleep(0.02)

local sleep = ngx.sleep
ngx.sleep = function() error("injected cleanup pause failure") end
local total, err = p:flush_expired()
ngx.sleep = sleep
assert(total == 50000, "partial reclamation count was lost")
assert(err and err:find("injected cleanup pause failure", 1, true))
assert(not index.flushing, "reclamation guard remained set")
local remaining, retry_err = p:flush_expired()
assert(remaining == 10 and retry_err == nil, "could not reclaim the remaining entries")
assert(d:get("permanent") == 42, "permanent entry was reclaimed")
ngx.say("ok")
