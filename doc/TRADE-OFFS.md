# Trade-offs: This Repo vs. Real Production Scale

Written for system-design interview prep. Every row is a real decision made
building this repo, next to what a team running this at real scale would
do instead, and why.

| # | Decision | This repo | Production-grade | Why production wins |
|---|---|---|---|---|
| 1 | Shard count | 4 fixed shards, no resharding tool | Dozens-to-hundreds of shards, with tooling to split/move them | At real scale you need enough shards that losing one barely matters, and a way to add more without downtime. The ID scheme here makes growing shard count *possible* (shard ID is 8 bits, independent of instance count) — it just doesn't include the tool that would execute it. |
| 2 | Social graph | One Neo4j instance, no replication | A replicated graph store (e.g. Meta TAO) | One instance is a single point of failure for the entire social graph — lose it, and no one can follow, unfollow, or fan out a post. |
| 3 | Cold feed tier | One embedded BadgerDB, no replication | A replicated disk-based store | Losing that one disk loses cold-tier history for every dormant user on it. |
| 4 | Cross-store sync | Two spots can silently drift: the Neo4j user node vs. its Postgres row, and the username directory vs. source of truth | An outbox pattern (write once, propagate with retry) | Without it, a Postgres write can succeed while a Neo4j write fails, and nothing notices. |
| 5 | Like/comment counts | Redis only — never written to Postgres at all | Redis, flushed to Postgres periodically for durability | If Redis loses its data, counts reset to zero with nothing to recover from. A deliberate, more aggressive choice than half-building a flush pipeline. |
| 6 | Seen-post tracking | A Redis list per user, 48h expiry | A Bloom filter | A Bloom filter uses a fixed amount of memory per user no matter how long they've been active; a list grows with history. |
| 7 | Ranking | A formula: recency × engagement | A trained model (GBDT + deep learning) | A formula can't learn interaction effects or improve with more data. That said, every real ranking system *starts* as a formula — you need logged traffic before you can train anything. |
| 8 | Content discovery | Caption-text similarity search | A model trained on real engagement/click data | Text similarity finds topically similar posts, not posts this specific user is likely to engage with. |
| 9 | Kafka | One broker, no replication | A multi-broker cluster, replicated | One broker is a single point of failure — lose it, lose unconsumed messages. |
| 10 | Ranking call | One synchronous call per feed request, no fallback | Batched calls, a short cache of recent scores, a fallback if the ranker is slow/down | Right now, if `ranking-service` is down, the whole feed request fails instead of degrading gracefully. |
| 11 | Consumer errors | Logged and skipped, no retry queue | Retry with backoff, then a dead-letter queue with alerting | Without this, one bad message either gets silently dropped or can wedge the whole consumer. |
| 12 | Observability | Logs only | Logs + metrics (consumer lag, per-shard latency, cache hit rate) + tracing | You can't safely operate a system at real scale by reading logs. Consumer lag alone is the single most important signal for "is fan-out keeping up." |
| 13 | Auth | None — anyone can post as any user | Authenticated sessions | Obviously required before this touches real data. Left out because it's a separate concern from the feed architecture itself, not because it's hard. |
| 14 | Language choice | 6 languages, for the learning exercise | Most teams standardize on 1–3 | Every extra language is its own on-call runbook, patching cadence, and hiring pool. |
| 15 | Schema changes | One script, run by hand | A versioned migration tool (Flyway, golang-migrate) | A hand-run script breaks the moment two people change the schema at once, or it partially fails across shards. |
| 16 | Seeing your own post | Doesn't appear in your own feed (you don't follow yourself) | Pinned/injected client-side | Worth noticing unprompted in an interview — it's a real product gap, not just a throughput number. |
| 17 | Internal traffic | No encryption, no unified retry policy between services | A service mesh (mTLS, consistent retries) | At 2–3 internal call sites, a mesh is overhead; at real scale, hand-rolled retry logic everywhere becomes the bigger risk. |
| 18 | HTTP/3 | Only on the feed read path, terminated at the edge | Same — this is already the standard real-world pattern | The internal hop to each service has no packet loss or latency worth solving with QUIC. |

## What to build first, if this were going to production

In order, because each one either prevents data loss or is needed before
anything else can change safely:

1. **Observability** — can't safely change anything without it.
2. **Dead-letter handling** — stops one bad message from taking down a consumer.
3. **Kafka replication** — cheapest fix for the biggest data-loss risk today.
4. **Neo4j replication** — the social graph has zero redundancy right now.
5. **An outbox** for the two places data can silently drift.
6. **Resharding tooling** — only once a shard is an actual bottleneck.
7. **A trained ranking model** — needs (1) in place to know if it's even better.
8. **TLS everywhere, and more than one gateway instance.**

Auth isn't "last" on any real list — it's a prerequisite for launching to
real users at all. It's ranked separately here because it's orthogonal to
the sharding/feed architecture this doc is about.
