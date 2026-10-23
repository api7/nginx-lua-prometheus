local p = cleanup_prometheus
local d = ngx.shared.cleanup_metrics

p:flush_expired()
d:set("permanent", 42)
for i = 1, 10 do
  assert(d:set("expired_" .. i, i, 0.01))
end
ngx.sleep(0.02)
assert(p:flush_expired() == 10, "did not reclaim the expired entries")
assert(d:get("permanent") == 42, "permanent entry was reclaimed")
ngx.say("ok")
