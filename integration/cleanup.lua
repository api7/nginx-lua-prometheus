local p = cleanup_prometheus
local d = ngx.shared.cleanup_metrics
local marker = "__ngx_prom__flush_expired"

local function set_expired()
  for i = 1, 10 do
    assert(d:set("expired_" .. i, i, 0.01))
  end
  ngx.sleep(0.02)
end

-- start from an interval nobody has reclaimed in yet
d:delete(marker)
d:set("permanent", 42)
set_expired()
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
ngx.say("ok")
