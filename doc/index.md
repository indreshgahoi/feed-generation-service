# Documentation Index

Deep-dive docs supporting the top-level [README.md](../README.md). Start there
for "how do I run this"; come here for "how does it actually work" and "what
would an interviewer ask about this."

| Doc | What's in it |
|---|---|
| [architecture.md](architecture.md) | Every service, what it owns, the sharded/graph/hot-cold data model, and how the 6 services + infra pieces fit together |
| [sharding.md](sharding.md) | The application-level sharding design record: self-routing IDs, where consistent hashing is actually used, why the social graph lives in Neo4j instead of sharded Postgres, and what's explicitly still unsolved |
| [caching.md](caching.md) | The hot (Redis) / cold (BadgerDB) feed storage tier: why two tiers, promotion-on-read, and the honest RocksDB→BadgerDB substitution reasoning |
| [engagement-at-scale.md](engagement-at-scale.md) | Likes/comments reconciled against a hyperscale reference design: sharded Redis counters, read-your-own-writes, rate limiting, moderation, cache-stampede protection -- what's real vs. deliberately deferred |
| [flow.md](flow.md) | Step-by-step walkthrough of one post's journey through the sharded write path, and one feed request's journey through the read path -- with the actual code paths and Redis/Kafka/Postgres/Neo4j keys involved |
| [trade-offs.md](trade-offs.md) | What's still simplified vs. real production scale, decision by decision, written for system-design-interview prep -- what breaks at scale, what an interviewer wants to hear, and likely follow-up questions |
| [learning-notes.md](learning-notes.md) | What building the same kind of service in Go/Java/Python/Rust/Node back-to-back actually teaches you, plus the real bugs hit while building this repo -- including a process-crashing nil-pointer panic from a malformed shard ID -- and what each one is a general instance of |

## Reading order

- **Just want to run it?** [README.md](../README.md).
- **Want the mental model before reading code?** [architecture.md](architecture.md) then [flow.md](flow.md).
- **Want the sharding/graph/storage design reasoning specifically?** [sharding.md](sharding.md), then [caching.md](caching.md) and [engagement-at-scale.md](engagement-at-scale.md).
- **Prepping for a system design interview?** [trade-offs.md](trade-offs.md) -- it's written to be read standalone.
- **Curious what's actually transferable knowledge from this exercise?** [learning-notes.md](learning-notes.md).
