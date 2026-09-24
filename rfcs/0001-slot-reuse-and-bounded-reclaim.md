# RFC 0001: bounded reclamation and slot reuse in KeyIndex

Status: proposed
Tracking: apache/apisix#13658, apache/apisix#11934, apache/apisix#12275

## 1. Problem

`KeyIndex` gives every metric name a numbered slot in the shared dict
(`__ngx_prom__key_N`) so that a scrape can enumerate the metrics without
`get_keys()`. `key_count` is the highest number handed out, and a full sync
walks `0..key_count`.

Three problems come out of that, all of them only when metrics are registered
with an `exptime`.

### 1.1 One low-frequency series coming back pegs every worker

When a metric comes back after its slot is gone, `add()` bumps `delete_count`
(added in #14 so that peers stop listing a stale slot). Every worker's next
`sync()` then walks `0..key_count` -- and `sync()` runs on the request path,
once per observation of a metric with an `exptime`.

Measured on the shape of a 3.9.x gateway pod (§6.1): with 140k slots and 200
req/s, a single such return doubles the CPU of the worker set for the next
window, and 50 of them over 10s put three workers at 98%, 93% and 88% of a
core. That is the `top` picture behind the reports.

### 1.2 Slot numbers are never reused

A metric whose labels are never seen again leaves its number behind for good,
and a metric that expires and comes back gets a *new* one. `key_count` only
grows, and everything proportional to it grows with it. The dump in
apache/apisix#13658 had 747,970 dead index entries against 28 live series.

### 1.3 The reclamation holds the dict lock for a whole backlog

`remove_expired_keys()` calls `flush_expired()` with no bound, in every worker.
That call holds the dict mutex until it returns and walks the whole LRU queue,
so an hour's worth of expired entries is reclaimed in one uninterrupted hold,
and every worker repeats the walk (#23 in this repo).

## 2. What the shared dict actually offers

A slot is in one of three states, and `get()` reports the last two alike:

| state | `get()` | `ttl()` | `expire()` | `add()` |
|---|---|---|---|---|
| (1) live | the key | `> 0` | renews | `"exists"` |
| (2) past its ttl, node still in the dict | `nil` | `< 0` | resurrects it, value intact | replaces in place |
| (3) node physically reclaimed | `nil` | `nil, "not found"` | `"not found"` | creates it |

Verified on OpenResty 1.29.2.4 (`resty --shdict`) and in the source: `ttl()`
goes through `ngx_http_lua_shdict_peek()`, which neither checks the expiry nor
touches the LRU position, while `get()` goes through
`ngx_http_lua_shdict_lookup()`, which returns `NGX_DONE` for an expired node.

This is the basis of the design: **`get()` answers "is this key visible",
`ttl()` answers "does this node still exist"**. The second question is what
decides whether a slot number still belongs to its key.

## 3. Design

### 3.1 The renewal path checks ownership instead of syncing

`add()` used to start with `sync()` -- two reads of the shared counters -- and
then renew the slot `self.index[key]` points at. It now reads the slot itself:

```lua
if self.dict:get(self.key_prefix .. idx) == key then
  self.dict:expire(self.key_prefix .. idx, exptime)   -- done
end
```

One read instead of two, and none of the shared counters are touched, so
nothing another worker does can force this path into a full sync. It is also
what makes slot reuse safe: an index entry left over from a slot that has since
been handed to another key does not match here, where renewing it blindly would
extend a foreign key's ttl and leave this key unregistered.

`delete_count` is no longer bumped when a metric comes back (1.1), and
`remove()` -- which APISIX never calls on prometheus metrics -- remains the only
writer of it.

### 3.2 A key takes its own slot back in place

If the slot holds no visible key -- state (2) or (3) -- the key takes the number
back with `add()`, verifying the occupant if that comes back `"exists"` (a peer
may have taken it back first). The key keeps the number every worker already
knows, so nothing has to be broadcast and `key_count` does not grow when a
metric expires and comes back.

### 3.3 Reclaimed numbers are reused by other keys

`clear_slot()` runs when a worker sees a slot in state (3); it puts the number
in a bounded per-worker list. A key that needs a slot takes one from there and
writes it exactly as it would write a fresh one -- a number taken in the
meantime simply fails that write and is dropped. Reuse therefore costs no
search: no shared cursor, no scan, no extra read.

### 3.4 `key_count` is raised above a slot taken in place

A slot above `key_count` is invisible to a full scan. That happens when
`key_count` -- an ordinary dict entry -- is LRU-evicted and `incr()` re-creates
it below the slots in use. `ensure_key_count(idx)` raises it, re-checking the
result because a lost race can only overshoot.

### 3.5 The scrape follows a trail of reused slots

Slots are written below the other workers' `self.last`, where an incremental
sync would never read them again. Every such write publishes `"<seq>:<slot>"`
into a ring in the dict and bumps `reuse_count`; `list()` is the only reader.
When the counter has moved, the scrape re-reads just those slots; only a scrape
that has fallen further behind than the ring, or that finds the trail
incomplete, walks every slot.

The ring entries are created up front and always rewritten with a value of the
same length, so publishing is an in-place write that cannot fail for want of
memory. This matters: while publishing used `safe_set` on a not-yet-existing
entry, a full dict dropped the write, every scrape fell back to walking 400k
slots, and the tail latency of the whole gateway went with it (§6.3). The ring
holds one entry per 64 KiB of dict capacity, between 256 and 8192.

### 3.6 The scan is one read per slot

A slot that holds no live key is *hidden*: dropped from the listing, slot number
kept. Telling state (2) from state (3) costs a second read and decides whether
the number can be given up, so it is done by `remove_expired_keys()`, on a
timer, not on every scrape.

### 3.7 The local views cannot drift apart

`self.keys` (slot -> key) and `self.index` (key -> slot) are maintained only
through `set_slot` / `hide_slot` / `clear_slot`. Leaving a previous occupant's
index entry behind would point a later `add()` at a slot that is no longer its
own; dropping the index entry of a key that has since moved would hide a live
key from `list()`.

## 4. Invariants

1. **Ownership.** A worker only renews a slot whose current value is its own key
   (3.1). Nothing else can extend a foreign key's ttl.
2. **Identity.** A number is reused by another key only after its node is gone
   (3.3), and only through `add()`, which fails on a live node.
3. **Visibility.** Every live slot is within `0..key_count` (3.4), and a slot
   written below `self.last` is either on the trail the scrape follows or causes
   a full walk (3.5).
4. **Uniqueness.** `list()` emits a key only from the slot its own index points
   at, so a key that transiently sits on two slots is listed once.
5. **Locality.** `self.keys` and `self.index` are mutual inverses (3.7).

## 5. Scenario tests

### 5.1 Unit (`prometheus_test.lua`)

The `SimpleDict` mock now models the three states of §2: `get()` no longer
prunes an expired node, `ttl()` returns a negative number for state (2) and
`"not found"` for state (3), `expire()` resurrects a state-(2) node, and `add()`
replaces one in place. Tests that assumed the old semantics were rewritten
rather than patched.

| test | what it pins down |
|---|---|
| `testExpiredReAddReclaimsSameSlot` | a metric that comes back takes its own slot; `key_count` and `delete_count` do not move; a second worker that never re-synced still lists it once |
| `testReclaimedSlotIsTakenBackInPlace` | same, after the node itself was reclaimed; a second worker sees it although the write was below its `self.last` |
| `testReclaimedSlotsAreReused` | three retired numbers are reused by three new metrics; `key_count` stays at 3 |
| `testSlotsOnlyPastTheirTtlAreNotReused` | a state-(2) slot is not given to another key, and its own key still gets it back |
| `testReusedSlotIsNotHijackedByAStaleIndex` | a worker holding a stale index does not renew the new occupant, registers its own key elsewhere, and a scrape lists both exactly once |
| `testKeyCountStaysBoundedUnderChurn` | five rounds of retire-and-register reuse one slot |
| `testReclaimedSlotAboveKeyCountIsMadeVisible` | with `key_count` evicted, a slot taken back above it is still listed by a fresh worker |
| `testRemoveExpiredKeysKeepsLiveSlots` | a reclaim round leaves live slots alone |
| `testFlushExpiredRunsInBatches` | a 20,000-entry backlog is reclaimed in bounded batches (each call is asserted to ask for 10,000) |
| `testAutoFlushExpiredDisabled` | with the option off, a reclaim round only drops local references |

53 of 54 tests pass; `TestPrometheus.testPrintfTable` fails on `main` as well,
under LuaJIT, and is unrelated.

### 5.2 Multi-worker correctness

OpenResty 1.29.2.4, `worker_processes 10` plus the privileged agent, which
scrapes and (on this branch) reclaims. Every check compares what the scraping
process lists against ground truth taken from the dict itself -- a walk of
`1..key_count` -- inside that same process, so nothing is lost in transport.

| dict / series | variant | live slots | listed | duplicates | missing | two slots, same key |
|---|---|---|---|---|---|---|
| 100m / 300k | v1.0.0 | 300,001 | 300,001 | 0 | 0 | 0 |
| 100m / 300k | this branch | 300,001 | 300,001 | 0 | 0 | 0 |
| 100m / 300k, after 40s of churn | v1.0.0 | 402,778 | 405,399 | 0 | 0 | 0 |
| 100m / 300k, after 40s of churn | this branch | 402,418 | 402,592 | 0 | 0 | 76 |
| 500m / 1.5M, after 60s of churn | v1.0.0 | 1,500,001 | 1,500,001 | 0 | 0 | 0 |
| 500m / 1.5M, after 60s of churn | this branch | 1,500,001 | 1,500,001 | 0 | 0 | 0 |

The 76 slots holding the same key are the transient this design allows: two
workers can each end up with a slot for the same key when one of them had
already dropped its reference. `list()` emits the key once (invariant 4), and
the extra slot is reclaimed when the metric next expires. It is 0.02% of the
slots in that run, and the earlier row shows the counterpart: on v1.0.0 it is
the *expired* keys that linger in the listing (2,621 against 250 here).

Churn also shows what the reuse is for. Twelve rounds of 50 fresh series, each
round expiring before the next, on 10 workers:

| | v1.0.0 | this branch |
|---|---|---|
| `key_count` after 12 rounds | 383 -> 934 (+50 per round) | 254, flat |
| consistency check each round | pass | pass |

### 5.3 What the slot-level check cannot see

Comparing what the scrape lists against a walk of `1..key_count` only proves
the two agree about the *slots*. It cannot catch the failure this design has to
rule out: a key whose slot was taken by someone else stays unregistered, its
value is still in the dict, and it is missing from the exposition while every
slot-level count still matches.

So two further checks run inside the scraping process, on 10 workers:

- **every live value is rendered exactly once** -- the ground truth is the set
  of dict keys that are not index bookkeeping and still read non-nil, compared
  against the series in the rendered exposition;
- **counter values survive slot reuse** -- a known number of increments is
  driven through all the workers, and each series' value must be exactly that.

To make the reuse actually happen for the counted series, the run retires a
batch of slots first (20s of churn, then the expiry and one reclaim round), and
only then registers them, so they take the numbers just given up:

| | v1.0.0 | this branch |
|---|---|---|
| entries reclaimed by the round | 0 (hourly timer) | 65,396 |
| slots added by the 20 counted series | 20 | **5** (15 landed on recycled numbers) |
| counter values exactly as driven | yes | **yes** |
| live values in the dict / series rendered | 200,020 / 200,021 | 200,020 / 200,021 |
| rendered twice | 0 | **0** |
| live value missing from the output | 0 | **0** |
| slot-level: listed / live slots / duplicates / missing | 200,021 / 200,021 / 0 / 0 | 200,021 / 200,021 / 0 / 0 |

The one series rendered without a value is the same on both variants: it expired
between the enumeration and the render, which is a race in the check, not in the
library. The same two checks also pass at 300k series with churn running
throughout (1,500 new series/s).

### 5.4 Scenario coverage

Every row is a scenario this design has to survive, what it must guarantee, and
where the evidence is. "unit" is `prometheus_test.lua`; the rest run on
OpenResty with 10 workers and the privileged agent.

| scenario | must hold | evidence |
|---|---|---|
| steady state, 300k and 1.5M series | output = live set, no duplicates | multi-worker, §5.2 |
| a series expires | it leaves the output | unit + multi-worker |
| it comes back, node still there (state 2) | same slot, nothing broadcast | unit |
| it comes back, node reclaimed (state 3) | same slot, peers do not miss it | unit |
| a dead label's number goes to a new series | correct value, listed once | §5.3, 15 of 20 landed on recycled numbers |
| a slot only past its ttl | not given to another key | unit |
| a stale index entry after reuse | must not renew the new occupant | unit |
| two workers racing for one reclaimed number | one wins, the other takes another | unit |
| `remove()` frees a number that is then reused | keys do not get mixed up | unit |
| no `exptime` at all (the APISIX default) | none of the reuse machinery engages | unit + §6.2 |
| `key_count` LRU-evicted | a slot above it is still listed | unit |
| the trail is shorter than the scrape's lag | fall back to walking every slot | unit |
| a trail entry is evicted | same fallback | unit |
| slots inherited from an earlier generation | reused, not stranded | unit + §5.5 |
| in-place upgrade from 1.0.0, shm kept | correct from the first scrape | §5.5 |
| rollback to 1.0.0 over a dict this code wrote | 1.0.0 stays correct | §5.5 |
| a dict kept at 0 free space | *neither* version is correct here -- see §5.6 | §5.6 |
| counter values across all of the above | exactly what was driven | §5.3 |

### 5.5 Upgrade and rollback

A reload keeps the shm zone, so swapping the library under a running instance is
what an in-place upgrade looks like: the new code inherits a dict that 1.0.0
wrote -- 39k dead slots, `key_count` well above the live count, no trail -- and
then 1.0.0 inherits one this code wrote, with reused slots and a trail it knows
nothing about.

| stage | `key_count` | live slots | duplicates | live values missing | first scrape |
|---|---|---|---|---|---|
| on 1.0.0, before | 239,366 | 200,001 | 0 | 0 | -- |
| after the upgrade | 239,366 | 200,001 | 0 | 0 | 272ms |
| after 20s of churn on the inherited state | 246,568 | 200,001 | 0 | 0 | -- |
| after rolling back to 1.0.0 | 246,568 | 200,001 | 0 | 0 | 257ms |

The first run of this exposed a gap rather than a bug: the numbers a previous
generation of workers gave up are in nobody's `expire_keys`, so they were never
reused and `key_count` kept the old high-water mark. A reclaim round now also
walks a slice of the range looking for numbers whose node is gone (a tenth per
round, one read per slot), which brought the growth over the same 20s of churn
from +14.6k to +7.2k. The residual is the working set of simultaneously live
churn series, which has to have numbers.

### 5.6 A dict with no free space

Kept at 0 free space by churn beyond its capacity, with `key_count` evicted as
well, both versions produce a broken exposition: of ~405k live values, 381k
(1.0.0) and 301k (this branch) were missing from the output, and `key_count`
itself was evicted and restarted. Slot reuse does not fix this and does not make
it worse -- it is what the dict does when it is too small for the cardinality,
and it is the state that apache/apisix#13658's bloat drives a gateway into.

Under a churn rate the dict *can* absorb (3,000 new series/s for 60s on 100m,
starting from 150k live series), both stay correct, all 500 continuously-hit
series keep their values, and the difference is the numbering:

| | v1.0.0 | this branch |
|---|---|---|
| `key_count` after 60s | 150,501 -> 298,420 | 150,501 -> **260,468** |
| growth in the last 15s | +12.6k/5s, flat | **+4.5k/5s, falling** |
| live values missing from the output | 0 | 0 |

## 6. Performance report

20-core x86-64, OpenResty 1.29.2.4, load generator on the same host. Each
microbenchmark figure is the median of 5 runs issued over one keepalive
connection, so they land on the same worker, with the scrape and the reclamation
paused for the duration.

### 6.1 The CPU spike of §1.1

10 workers, a 512m dict, the three metrics of `apisix/plugins/prometheus/exporter.lua`
with their label sets and the default latency buckets, 140k label combinations
accumulated, then 200 req/s of exactly what `exporter.http_log()` does
(`status:inc` + three `latency:observe` + two `bandwidth:inc`). CPU is the sum
over the workers, from `/proc/<pid>/stat`, over 10s windows.

| window | v1.0.0 | this branch |
|---|---|---|
| steady 200 req/s | 14.9% | **7.8%** |
| one low-frequency series comes back | **30.2%** (`delete_count` +1) | **2.2%** (+0) |
| 50 of them over 10s | **287.7%**, busiest workers 98.6 / 93.3 / 87.9% | **4.5%**, busiest 4.0% |

The low-frequency series is registered before the 140k are filled in, so its
slot is an old one: on v1.0.0 an incremental sync happens to clear a stale index
entry for a *recent* slot, and only an old slot reaches the branch that bumps
`delete_count`. That matches a pod that has been running for days.

The third row is externally triggered (`delete_count` is bumped directly), and
this branch does not react at all: its request path never reads the shared
counters, so a bump cannot make it scan.

### 6.2 Microbenchmarks

Metric shapes, at 200k live series on 100m, with the scrape and the reclamation
paused:

| path | v1.0.0 | this branch |
|---|---|---|
| counter renewal (1 dict write per request) | 0.91 us/op | **0.60 us/op** |
| histogram renewal (18 keys per observation) | 7.05 us/op | **6.65 us/op** |
| counter registration | 5.0 us/op | 6.3 us/op |
| histogram registration | 43 us/op | 49 us/op |

Registration is the one path that pays for reuse: about one extra dict
operation per key, for `ensure_key_count` and for publishing the number on the
trail. It happens once per series per worker, against the renewal path that runs
on every request.


| | 100m / 300k | | 500m / 1.5M | |
|---|---|---|---|---|
| | v1.0.0 | branch | v1.0.0 | branch |
| renewing a known series | 0.42-0.56 us/op | 0.54-0.70 us/op | 2.04 us/op | 2.14 us/op |
| registering a new series | 3.45-3.75 us/op | 3.50-4.50 us/op | 3.60 us/op | 3.50 us/op |
| `list()` | 9-13 ms | 11-16 ms | 38 ms | 49-55 ms |
| `flush_expired(10000)` | 13-15 ms | 13-16 ms | 30-31 ms | 56-61 ms |

Both variants vary by about a factor of two between runs at these dict sizes, so
the ranges overlap and none of these differences are established. The §6.1
measurement, where the two differ by 60x, is the one that is.

### 6.3 Request latency

`wrk2`, 40s, plus new series arriving as ordinary requests, spread over the
workers.

| | v1.0.0 | this branch |
|---|---|---|
| 100m / 300k, 5k req/s + 100 new series/s: p50 | 0.97ms | 0.96ms |
| p90 | 504ms | 470ms |
| p99 | 1.62s | 1.58s |
| 100m / 300k, 20k req/s + 400 new series/s: p90 | 739ms | 756ms |
| p99 | 1.96s | 2.11s |
| 500m / 1.5M, 2k req/s + 40 new series/s: p50 | 3.93s | 3.41s |
| achieved rps | 75 | 76 |

The tails are large in both variants and for the same reason: rendering the
exposition for 300k series takes ~440ms and for 1.5M series ~3.6s, every
`refresh_interval`, and that runs against the same dict the workers write to.
At 1.5M series neither variant can serve 2k req/s on this box. What matters
here is that the two are indistinguishable.

An earlier revision was *not* indistinguishable: it walked every slot on every
scrape and, on a full dict, could not publish its trail, so the scrape fell back
to that walk permanently. It measured p90 500ms against 123ms for v1.0.0 in the
20k req/s run. Both causes were fixed (3.5, 3.6).

### 6.4 Reclamation lock hold (`resty --shdict`, single process)

| dict contents | call | lock hold |
|---|---|---|
| 750k entries, 749.7k expired | `flush_expired()` | 70ms |
| same | `flush_expired(10000)` | ~1ms per call, 26 calls |
| 301k entries, 1k expired | either | ~3ms (walks the whole queue) |

~0.09us per entry reclaimed, ~0.01us per live node walked. The walk is the
floor: in the steady state, where the backlog is smaller than a batch, every
call still walks the queue once.

## 7. Risks and follow-ups

- **A scrape that loses the trail walks every slot**, which is 0.4s at 400k
  slots and seconds at 1.5M. The ring is pre-created so that publishing cannot
  fail, and its size scales with the dict, but a scrape that falls more than
  `ring_size` reuses behind still pays for one walk.
- **Two workers can hold a slot each for the same key.** Bounded by the number
  of workers, listed once, reclaimed on the next expiry; 0.02% of slots in the
  churn run above.
- **Reuse depends on reclamation.** A number becomes reusable only once its node
  is gone, so the reclamation interval sets how quickly numbers come back. 10s
  to 60s is a reasonable range.
- **The scrape itself is the bottleneck at 1.5M series** in both variants, and
  this RFC does not address it.
