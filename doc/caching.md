# Feed Storage: Hot/Cold Tiering

The source design doc's own data-store table lists the feed timeline
inbox as **"RocksDB LSM on NVMe / Redis on Flash"** in production, with
just `redis:7.2-alpine` as the local stand-in (§2). This document covers
actually implementing that two-tier design instead of flattening it to
"just Redis," and the cost/latency trade-off that motivates it.

## Why two tiers at all

Every active user's feed inbox (`feed:user:<id>`, a Redis ZSET, up to 800
entries) lives entirely in RAM. RAM is the fastest storage tier and the
most expensive per GB, by roughly two orders of magnitude versus SSD.
At 500M DAU, keeping *every* user's inbox in Redis regardless of whether
they've opened the app in the last hour or the last year means paying
RAM prices for data that, for the dormant majority, is read approximately
never. The fan-out worker already recognizes this asymmetry on the write
side (`ACTIVE_WITHIN_DAYS` -- it only pushes into a follower's inbox if
they've been active recently); this tiering extends the same idea to
where the data actually lives, not just whether it gets written to.

## The design

- **Hot tier -- Redis.** Unchanged from before: `feed:user:<id>` ZSETs,
  written by fanout-worker for followers active within
  `ACTIVE_WITHIN_DAYS` (default 7), read first on every feed request.
- **Cold tier -- an embedded LSM-tree store**, owned by
  feed-aggregation-service. When fanout-worker encounters a follower who
  is *not* active, it no longer simply drops that fan-out write (the
  original design's behavior) -- it writes the same `(post_id, score)`
  entry to the cold tier instead, via a gRPC call
  (`ColdTierService.Append`, see [wire-protocols.md](wire-protocols.md))
  on feed-aggregation-service. The entry isn't lost, it's just stored
  somewhere an order of magnitude cheaper per GB, at the cost of being
  slower to read and not being pre-merged into a ready-to-rank inbox.
- **Promotion on read.** When a feed request for user X finds nothing in
  Redis for `feed:user:X`, that's a signal this user has been dormant.
  feed-aggregation-service falls back to the cold tier, and if it finds
  entries there, copies them into a fresh Redis ZSET (bounded to the same
  800-item cap) before proceeding -- the user is reading *now*, so by
  definition they're active again, and the hot path should own their data
  going forward until they go dormant once more.

This means a returning-after-months user's first request after a long
absence pays one extra (disk-backed, but still local) read instead of
either (a) losing their accumulated fan-out history entirely, as the
pre-tiering version of this repo did, or (b) the system having paid RAM
cost to keep their empty inbox warm the whole time they weren't looking
at it.

## RocksDB → BadgerDB: an honest substitution, not a shortcut

The doc says RocksDB. This repo uses **BadgerDB**
(`github.com/dgraph-io/badger`) instead, for a concrete, checkable reason:
RocksDB is a C++ library. Using it from Go requires CGO bindings
(`grocksdb` or similar) *and* a native `librocksdb` install on the host --
in this environment that means `apt install librocksdb-dev`, which needs
`sudo`, which isn't available here (the same wall hit earlier trying to
install `python3-venv`). BadgerDB is a pure-Go LSM-tree key-value store
from the same design lineage (its own authors cite LevelDB/RocksDB as
direct ancestors, and its SSTable/compaction/WAL model is architecturally
the same shape) with zero native dependencies -- `go get` and it works
anywhere Go itself does.

This isn't a hand-wavy substitution: **CockroachDB made the identical
call for the identical reason.** CockroachDB shipped on RocksDB for years,
then built **Pebble** -- their own pure-Go, API-compatible, RocksDB-derived
LSM engine -- specifically to drop the CGO/native-toolchain dependency
from their build and deployment story. BadgerDB here is playing exactly
the role Pebble plays there: same architectural properties (LSM tree,
leveled compaction, WAL for crash recovery, tunable for read vs. write
amplification), no C++ toolchain requirement.

## What's genuinely different from real RocksDB-on-NVMe

Worth naming precisely, not just gesturing at "it's basically the same":
- **Single embedded instance, not a distributed/replicated store.** The
  cold tier lives on one feed-aggregation-service instance's local disk.
  Losing that disk loses cold-tier data for every dormant user it held --
  there's no replication here, unlike Redis's AOF persistence which at
  least survives a process restart on the same disk. A production system
  would need this replicated (or accept that cold-tier data is a rebuild-
  able cache, not a system of record -- which it should be treated as
  regardless: the real system of record for "what should be in someone's
  feed" is the sharded `posts`/graph data, not this cache).
- **No compaction tuning.** BadgerDB ships with defaults; a production
  deployment sizing this for real cold-tier volume would tune LSM level
  sizes, compaction concurrency, and value-log GC thresholds against
  actual write/read ratios -- not attempted here.
- **The hot/cold boundary is a fixed day count, not adaptive.** Real
  systems commonly make this a sliding, load-aware threshold (evict to
  cold more aggressively under memory pressure) rather than a static
  `ACTIVE_WITHIN_DAYS` constant. Static is simpler and transparent, which
  is the right choice for something meant to be read and understood, at
  the cost of not reacting to actual memory pressure.
