# Changelog

This file only calls out major changes. Please see [the list of Git commits](
https://github.com/api7/nginx-lua-prometheus/commits/main) for the full list
of changes.

## Unreleased

- **Changed the default `remove_expired_keys_interval` from 3600 to 1800
  seconds**, and removed its upper bound. At 3600s an expiry rate of 17-58
  series/s, depending on how much of the dict is live, fills a default-sized
  dict with dead entries, after which creating a new series force-evicts live
  metrics (apache/apisix#13658). Halving the interval doubles the rate that is
  tolerated while keeping the reclamation rare enough that a returning metric
  usually still finds its entry and keeps its slot number (#23).
- Bound the work one round of `remove_expired_keys()` does, and spread the
  per-worker timers over the interval instead of firing them all at
  `init_worker`. Asking the dict for a slot's ttl is a lock acquisition, and
  the scan did it for every slot the worker knew had one: with 150,000 of them
  across 8 workers that was a ~480ms stall at p99.9 on a steady 20k rps. It now
  skips the slots whose recorded deadline has not passed and yields after every
  500 of the rest, so one round covers every lapsed slot without stalling the
  worker. With 200,000 expiring series across 10 workers at 20k rps this keeps
  p99 at 2.16ms, against 746ms when every worker scans in one go (#23).
- Reclaim expired shared-dict entries in batches, so no single
  `flush_expired()` call holds the dict lock for a whole backlog, refuse
  overlapping reclamation rounds, require `remove_expired_keys_interval` to be
  at least twice the longest a round can take, and let callers take the
  reclamation over with the `auto_flush_expired` option and
  `prometheus:flush_expired()` (#23).

## 1.0.0

Starting with this release, this fork (`nginx-lua-prometheus-api7`) uses
semantic versioning. Earlier releases used the upstream date-based scheme
(`0.YYYYMMDD`); as upstream is dormant and this fork is now maintained
independently, versions no longer track upstream snapshots. Date-based tags
before `1.0.0` are kept as-is.

- Reclaim expired shared-dict entries in `remove_expired_keys()` so per-worker
  metric key dictionaries no longer step down until the dict fills up (#18).

## 0.20230607

Improved checking of label values.

## 0.20221218

- Added escaping of newline characters in label values (#145).
- Improved detection of LRU evictions (#147, #148).
- Per-worker metric name lookup tables now have a bounded size aimed at preventing
  memory leaking in environments with high metric churn (#151).

## 0.20220527

Performance optimization aimed at decreasing impact that metric collection has
on other requests (#139).

## 0.20220127

Performance optimization of metric collection (#131).

## 0.20210206

Bucket label values no longer have leading and trailing zeroes (#119).

## 0.20201218

Histogram metrics can now be reset (#112).

## 0.20201118

Allow utf8 label values (#110).

## 0.20200523

- Scalability improvements that allow tracking a larger number of metrics
  without impacting nginx performance (#82).
- Simplified library initialization, moving all of it to `init_worker_by_lua_block`.
- Error metric name is now configurable (#91).

## 0.20200420

This is a significant release that includes counter performance improvements.

**BREAKING CHANGE**: this release requires additional per-worker initialization
in the `init_worker_by_lua_block` section of nginx configuration.

- Added support for incrementing and decrementing gauges (#52).
- Added del and reset for gauge and counter metrics (#56).
- Added per-worker lua counters that allow incrementing counter metrics
  without locking the dictionary (#75).

## 0.20181120

Added stream module support (#42).

## 0.20171117

Improved performance of metric collection (#25).

## 0.1-20170610

Initial version of the library.
