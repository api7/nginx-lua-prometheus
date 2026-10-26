local p = cleanup_prometheus
local d = ngx.shared.cleanup_metrics
local marker = "__ngx_prom__flush_expired"

local function set_expired()
  for i = 1, 10 do
    assert(d:set("expired_" .. i, i, 0.01))
  end
  ngx.sleep(0.02)
end

-- no metric has an exptime yet, so the dict lock is not taken to reclaim
d:delete(marker)
d:set("permanent", 42)
set_expired()
p.key_index:remove_expired_keys()
assert(d:get_stale("expired_1") ~= nil, "reclaimed without an expiring metric")
assert(d:get(marker) == nil, "took the interval without an expiring metric")

-- start from an interval nobody has reclaimed in yet
p:gauge("expiring", "Gauge with an exptime", nil, 3600):set(1)
p.key_index:remove_expired_keys()
for i = 1, 10 do
  assert(d:get_stale("expired_" .. i) == nil, "did not reclaim the expired entries")
end
assert(d:get("permanent") == 42, "permanent entry was reclaimed")
assert(d:get(marker), "did not take the interval")

-- the interval is taken: another round leaves the reclamation alone
set_expired()
p.key_index:remove_expired_keys()
assert(d:get_stale("expired_1") ~= nil, "reclaimed twice in one interval")

-- once it has passed, the next round reclaims again
d:delete(marker)
p.key_index:remove_expired_keys()
assert(d:get_stale("expired_1") == nil, "did not reclaim in the next interval")

-- auto_flush_expired = false leaves the reclamation to the caller
local nd = ngx.shared.cleanup_noflush_metrics
cleanup_noflush:gauge("expiring", "Gauge with an exptime", nil, 3600):set(1)
assert(nd:set("expired", 1, 0.01))
ngx.sleep(0.02)
cleanup_noflush.key_index:remove_expired_keys()
assert(nd:get_stale("expired") ~= nil, "reclaimed with auto_flush_expired off")
assert(nd:get(marker) == nil, "took the interval with auto_flush_expired off")

-- the cleanup timer reclaims on its own, unless the interval turns it off
local td = ngx.shared.cleanup_timer_metrics
local od = ngx.shared.cleanup_off_metrics
for _, p in ipairs({cleanup_timer, cleanup_off}) do
  p:gauge("expiring", "Gauge with an exptime", nil, 3600):set(1)
end
assert(td:set("expired", 1, 0.01))
assert(od:set("expired", 1, 0.01))
ngx.sleep(2.5)
assert(td:get_stale("expired") == nil, "the cleanup timer did not reclaim")
assert(od:get_stale("expired") ~= nil, "reclaimed with the cleanup turned off")

-- the dict truncates exptime to milliseconds; a marker TTL below 1ms would
-- never expire, and stop reclamation after the first round. The rounds are
-- run by hand, so the key index starts no timer.
local sd = ngx.shared.cleanup_tiny_metrics
local at, every = ngx.timer.at, ngx.timer.every
ngx.timer.at = function() return true end
ngx.timer.every = ngx.timer.at
local tiny = require("prometheus_keys").new(sd, "_tiny_", 0.0005)
ngx.timer.at, ngx.timer.every = at, every
assert(tiny:add("expiring", "eviction", 3600) == nil)
tiny:remove_expired_keys()
assert(sd:set("expired", 1, 0.001))
ngx.sleep(0.01)
tiny:remove_expired_keys()
assert(sd:get_stale("expired") == nil, "the flush marker did not expire")
ngx.say("ok")
