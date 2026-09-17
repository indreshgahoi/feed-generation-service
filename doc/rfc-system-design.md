# RFC: Sharded Polyglot Feed Generation Service

| | |
|---|---|
| **Status** | Accepted — Implemented |
| **Author** | Indresh Gahoi |
| **Reviewers** | Self-reviewed against the source design doc's own acceptance criteria (see §2) |
| **Related docs** | [architecture.md](architecture.md) · [sharding.md](sharding.md) · [caching.md](caching.md) · [engagement-at-scale.md](engagement-at-scale.md) · [wire-protocols.md](wire-protocols.md) · [gateway.md](gateway.md) · [flow.md](flow.md) · [trade-offs.md](trade-offs.md) · [learning-notes.md](learning-notes.md) |

This RFC is written in the voice of a design proposal — the document I'd
circulate before building this, not a retrospective. Everything it
proposes is implemented; where the build diverged from the proposal or
left something deliberately unfinished, that's called out explicitly
rather than smoothed over. The nine documents linked above are the
detailed design record for each subsystem; this one is the single
entry point that argues for the shape of the whole system and links out
for depth.

## 1. Summary

Build a feed generation and ranking system against the shape of a
public [Instagram-style feed architecture design doc](instagram_feed_design_doc.pdf)
— 500M DAU, ~80k read QPS, p99 < 200ms — as a runnable, **actually
sharded** implementation rather than a single-database demo that
documents sharding as a hypothetical trade-off. Every entity ID embeds
its own shard number; every repository routes through a shard-aware
connection pool; a script proves two independently-implemented routers
(Go and Java) agree, byte-for-byte, on where a new entity lands. Each
service is written in a different language chosen to match the source
doc's own explicit or implied technology choices, turning the codebase
into a working comparison of how the same problem shapes (HTTP write
path, Kafka consumer, ANN search, stateless scoring, shard routing)
look in six different ecosystems.

## 2. Problem Statement

A system-design document is only a claim until something forces its
assumptions to actually hold. "Shard by `user_id`" is one sentence in a
design doc and a completely different amount of engineering once a
follow edge, a like counter, or an `@mention` needs to work correctly
when the two entities involved live on different physical databases.
The goal of this project is to take a well-specified feed architecture
and build enough of it — for real, not schematically — that those
cross-shard edge cases have to be found and solved, not gestured at.

A secondary goal, stated up front rather than discovered afterward: use
this as a structured way to compare six languages against the *same*
underlying problem, so the comparison is about the languages and
ecosystems, not about six different problems dressed up as one.

## 3. Goals

- Implement application-level sharding across multiple independent
  Postgres instances, with self-routing IDs and no directory-service
  lookup for existing entities.
- Implement the source doc's hybrid fan-out (push for normal users,
  pull for celebrities) against a real social graph store, not a mocked
  follower list.
- Implement genuine hot/cold feed-storage tiering, not a single Redis
  instance standing in for both.
- Implement the full read-path funnel: multi-source candidate
  generation, seen-state filtering, cross-shard hydration, ranking,
  diversity rules, ad insertion, pagination.
- Replace hand-rolled JSON on every internal call with typed wire
  formats (gRPC/Protobuf, FlatBuffers) where the call pattern justifies
  it, and be able to say *why*, per call, not just "faster serialization."
- Prove cross-language contracts (shard routing, schema codegen) rather
  than asserting they match.
- Make every simplification relative to true production scale a named,
  reasoned decision — not a silent omission.

## 4. Non-Goals

- **Not** building a production-ready system: no auth, no observability
  stack, no replicated stateful services, no resharding tooling. These
  are named explicitly in [trade-offs.md](trade-offs.md), prioritized in
  §11 below, and are non-goals for this RFC's scope, not gaps I missed.
- **Not** training real ranking or recall models. `P(Like)`, `P(Comment)`,
  etc. are transparent heuristics (§8); a two-tower recall model and a
  GBDT/deep ranking model are out of scope for a project that has no
  production traffic to train them on (see [trade-offs.md](trade-offs.md)
  row 7-8 for why that's not a cop-out — it's an accurate model of stage
  one of a real ranking system's life cycle).
- **Not** validating this at the source doc's actual scale (500M DAU).
  This runs on one laptop, with 4 shards instead of hundreds and one
  instance of every stateful store instead of a replicated fleet.

## 5. Proposed Architecture

```
                                   Client
                                     │
                        ┌─────────────────────────────┐
                        │        Envoy gateway           │
                        │  :8080 (HTTP/1.1, h2, all paths) │
                        │  :8443 (HTTP/3/QUIC, feed only)    │
                        └──┬──────────────┬──────────────┬──┘
                /api/ingest│    /api/feed  │           /* │
                           ▼               ▼               ▼
   ┌────────────────────────────┐ ┌──────────────────────────────┐ ┌──────────────┐
   │ post-ingestion-service (Go) │ │ feed-aggregation-service (Go) │ │ web-ui        │
   │           :4001              │ │            :4002               │ │ (nginx)       │
   └──┬─────────┬─────────┬──────┘ └──┬───────┬───────┬───────┬───┘ └──────────────┘
      ▼         ▼         ▼           ▼       ▼       ▼       ▼
  Postgres    Neo4j     Redis     Redis    Redis   BadgerDB  Qdrant
  x4 shards  (graph)   (counters, hot     celeb    (cold     (ANN /
                        rate limit) inbox   outbox   tier)    taste vec)
                          │
                        Kafka "post-created" (FlatBuffers, key=author_id)
                          │
        ┌─────────────────┼──────────────────┐
        ▼                 ▼                  ▼
 fanout-worker      vector-pipeline    notification-service
 (Java)             (Python)           (Node/TS)
                                              
                        ranking-service (Rust) -- gRPC, stateless,
                        called directly by feed-aggregation-service
```

One gateway (Envoy) is the sole door in for client traffic. Two
synchronous services own the write and read HTTP surfaces respectively.
Three independent Kafka consumer groups react to every post, each
without knowledge of the others. One stateless scoring service is
called directly, off the gateway, because it's an internal hop with no
exposure to real client network conditions. Full component/service
table and per-service responsibilities: [architecture.md](architecture.md).

### Why one language per service, not one language for the whole system

Six languages is a real operational cost (see §10, row 15) taken on
deliberately: each service's language matches the source doc's own
explicit or implied choice for that role (Go for a fast write path,
Java for Kafka's native consumer ecosystem, Python for
Sentence-Transformers, Node for a lightweight I/O-bound consumer, Rust
as the doc's named scoring-service alternative). The interesting
consequence, not just the interesting premise: any cross-language
contract — the shard-routing algorithm, the wire schemas — now has to
be actively kept honest across implementations instead of trivially
guaranteed by being the same code. §9 covers how.

## 6. Data Model & Storage Choices

| Concern | Store | Why not the "obvious" choice |
|---|---|---|
| Users, posts, comments, notifications | 4 independent Postgres shards, identical schema | A single Postgres instance doesn't force any cross-shard edge case to actually surface |
| Social graph (follows, follower count, celebrity flag) | Neo4j | A follow edge connects two users on any two of four shards — the design tried first (dual-write the edge to both endpoints' shards) turns every follow into a hand-rolled distributed transaction with permanent drift risk. A graph store gives both traversal directions natively, with one transaction, no drift. Full record: [sharding.md](sharding.md#the-social-graph-lives-in-neo4j-not-sharded-postgres) |
| Like/comment counts | Redis (sharded 16-way sub-counters) | The liking user and the post author are frequently on different Postgres shards; incrementing a `like_count` column transactionally would itself be a cross-shard write, for data that changes far more often than a follow edge. Moved out of the relational tier entirely rather than half-solved. [engagement-at-scale.md](engagement-at-scale.md) |
| Hot feed timelines | Redis ZSETs, capped at 800 items | Sub-millisecond reads for active users, the case that dominates request volume |
| Cold feed timelines (dormant users) | BadgerDB, embedded LSM tree | The source doc names RocksDB; BadgerDB is a pure-Go architectural equivalent (same lineage CockroachDB's own Pebble comes from) with no CGO/native-toolchain dependency. Reasoning and honest limitations: [caching.md](caching.md) |
| Caption embeddings | Qdrant, 384-dim cosine | Real ANN search against genuine sentence embeddings, not a mocked recall step |
| Post-created event | Kafka, 3 partitions, key = `author_id` | One producer, three independent consumer groups, no coordination between them needed |

IDs are never database-native auto-increment — every primary key is an
application-generated 64-bit Snowflake variant (`41 bits timestamp | 8
bits shard ID | 14 bits sequence`), so that routing an *existing* row to
its shard is a bit-shift, never a lookup, and IDs stay globally unique
across four otherwise-independent databases. This is the single design
choice everything else in this RFC leans on; full reasoning and the
distribution bug the parity tests actually caught:
[sharding.md](sharding.md#ids-are-self-routing).

## 7. Write Path

`POST /v1/posts` → synchronous, single-shard Postgres commit (the post
inherits its author's shard bits, no re-hashing) → Kafka publish
(FlatBuffers, non-blocking) → `201` returned immediately. From there,
three independent consumers react without blocking the client or each
other:

- **fanout-worker (Java):** celebrity author → one outbox append, no
  per-follower work. Normal author → query the full follower list from
  Neo4j, split into active/dormant via a concurrent cross-shard Postgres
  scatter-gather, push active followers into their Redis hot inbox and
  dormant followers into the BadgerDB cold tier via gRPC (not dropped).
- **vector-pipeline (Python):** embed the caption, upsert into Qdrant.
- **notification-service (Node/TS):** resolve `@mentions` via a Redis
  username directory, mint a shard-aware notification ID, write to the
  recipient's shard.

Full step-by-step with file references: [flow.md](flow.md#write-path-user_2-posts-a-photo-mentioning-user_3).

## 8. Read Path

`GET /v1/feed?userId=` runs an eight-stage funnel in
`feed-aggregation-service`:

1. **Fan-in**, three sources concurrently: hot Redis inbox (falling back
   to and promoting from the BadgerDB cold tier if empty), followed
   celebrities' outboxes, and Qdrant ANN search against an on-the-fly
   "taste vector" (averaged embeddings of the user's own/followed
   recent posts).
2. **Merge + dedupe** across sources.
3. **Seen-state filter** — drop anything shown in the last 48h.
4. **Hydration**, grouped by shard (one batch query per shard, not one
   per candidate) — up to ~750 candidates, at most 4 concurrent
   Postgres round-trips, not up to 750 of them.
5. **Ranking** — a gRPC call to `ranking-service` (Rust), which scores
   `w1·P(Like) + w2·P(Comment) + w3·P(Share) + w4·P(Dwell>5s) − w5·P(Hide)`
   from recency decay × normalized engagement counts. This call
   deliberately layers FlatBuffers *inside* the gRPC envelope rather
   than using gRPC's own Protobuf codec — reasoning in §9.
6. **Diversity** — cap at 2 posts per author.
7. **Ad insertion** — synthetic sponsored items at fixed slots.
8. **Respond** — `likedByMe` from a read-your-own-writes Redis set,
   mark returned posts as seen, return an AES-256-GCM encrypted cursor.

Full walkthrough with file references, and *why* the stages are
ordered the way they are (seen-state before ranking, diversity after):
[flow.md](flow.md#read-path-user_7-requests-their-feed).

## 9. Key Design Decisions

**Consistent hashing, scoped to exactly one decision.** Self-routing
IDs mean an existing entity never needs a lookup to find its shard —
consistent hashing (CRC32, 150 virtual nodes/shard) is only needed to
decide which shard a *brand-new* entity lands on. This is a narrower
use of consistent hashing than most write-ups show, and it's narrower
on purpose: the ring only ever has to reason about the one thing that's
actually mutable (where new signups land), not about re-routing
entities that already exist. [sharding.md](sharding.md#where-consistent-hashing-actually-gets-used)

**Cross-language parity is tested, not assumed.** Go and Java implement
the identical shard-routing algorithm against the identical config.
`scripts/verify_shard_parity.sh` asserts both agree on placement for
10,000 sample IDs — because if they ever silently disagreed, a signup
could land on shard 2 by one service's math and have related data
written to shard 3 by another's, corrupting the data model with no
error at the time it happens. The same "prove it, don't assume it"
standard is applied to generated code:
`scripts/verify_schema_gen.sh` diffs freshly-regenerated Protobuf/
FlatBuffers bindings against what's committed.

**gRPC/Protobuf for typed internal RPC; FlatBuffers where parse cost
compounds across fan-out.** Two internal calls moved from hand-rolled
JSON to gRPC/Protobuf, for a typed, compile-time-checked contract. The
Kafka event moved to FlatBuffers instead, because it's produced once
and independently parsed by three consumers in three languages — a
Protobuf message still needs a full parse-and-allocate pass before any
field is readable, paid once per consumer per message; FlatBuffers
reads a field straight out of the received bytes via an offset table.
The ranking call goes further and layers FlatBuffers *inside* a gRPC
envelope, keeping gRPC as the typed transport while opting out of its
Protobuf payload codec specifically because that call is list-heavy
(a full candidate set, up to ~750 items) and on the system's most
latency-sensitive path. Full reasoning, including the real bug this
change surfaced (silently-dropped ranking-probability fields in the
old JSON client): [wire-protocols.md](wire-protocols.md).

**An edge gateway, not a service mesh.** Envoy fronts all client
traffic and is the only door in; the two purely internal calls
(fan-out's cold-tier append, the ranking call) go direct over the
Compose network, bypassing Envoy entirely. This is a deliberate
scope boundary, not an oversight: a Docker-bridge hop has no packet
loss and sub-millisecond RTT, so neither a service mesh's mTLS/retry
machinery nor HTTP/3's loss-recovery machinery is solving a problem
that exists on that hop. QUIC is scoped identically — terminated at
the Envoy edge, and only on the feed *read* path, because that's the
path exposed to real, lossy client networks at the QPS the source doc
actually targets. Both choices, and how HTTP/3 was verified with a
real QUIC client rather than assumed from config: [gateway.md](gateway.md).

## 10. Trade-offs and Known Limitations

The full 19-row trade-off table, each row naming what this repo does,
what a production system would do instead, and why production wins at
scale, lives in [trade-offs.md](trade-offs.md) — written to stand alone
as system-design-interview prep. The six most consequential:

1. **No resharding tooling.** The ID scheme makes growing from 4 to N
   shards *tractable* (shard ID is 8 bits, independent of instance
   count) but nothing here executes a shard split.
2. **Single-instance Neo4j, Kafka broker, and BadgerDB tier.** Each is a
   named single point of failure; a production deployment replicates
   all three.
3. **Heuristic ranking and recall, not trained models.** An accurate
   model of *stage one* of a real ranking system's life cycle — you need
   logged production traffic before you can train anything.
4. **Two unreconciled cross-store drift windows**: the Neo4j `User` node
   vs. its Postgres row, and the Redis username directory vs. source of
   truth. Both are two independent writes with no shared transaction;
   an outbox pattern is the standard fix, not implemented here.
5. **No observability.** Structured logs only — no metrics, no tracing.
   At real QPS, consumer lag and per-shard latency are the two signals
   that matter most for knowing whether the system is keeping up, and
   neither exists here.
6. **No AuthN/AuthZ.** Any caller can post as any `userId`. Orthogonal
   to the sharding/feed architecture this RFC is about, but a hard
   prerequisite before this touches real users.

## 11. Prioritized Follow-up (if this were going to production)

In the order I'd actually build them, because each is either a
data-loss prevention or a prerequisite for safely changing anything
else:

1. Observability (metrics + tracing) — can't safely operate or change
   anything else without it.
2. Dead-letter handling for the three Kafka consumers.
3. Kafka replication factor > 1.
4. Neo4j replication (causal cluster) — the social graph currently has
   exactly one copy in the entire system.
5. An outbox for the two named drift windows.
6. Resharding tooling — only once a shard is an actual bottleneck.
7. A trained ranking model — requires (1) to know if it's even better
   than the heuristic it replaces.
8. TLS on the plaintext edge listener and Envoy HA.

Full reasoning per item: [trade-offs.md](trade-offs.md#if-asked-what-would-you-build-first-to-make-this-production-ready).

## 12. Appendix: Verification Artifacts

Claims in this system are backed by something that checks them, not
just configuration that "should" work:

- `scripts/verify_shard_parity.sh` — Go/Java shard-routing agreement
  over 10,000 sample IDs.
- `scripts/verify_schema_gen.sh` — committed Protobuf/FlatBuffers
  bindings match fresh codegen from `schemas/`.
- HTTP/3 verified with a real QUIC client (`aioquic`) against the feed
  read path, cross-checked against Envoy's own connection counters —
  not asserted from the listener config. [gateway.md](gateway.md#verification-a-real-http3-request-not-an-assumed-one)
- The consistent-hash ring's virtual-node key format was corrected
  after a real measured 29% max deviation across shards, brought under
  2% by changing the key layout — not assumed correct because "a hash
  function was used." [sharding.md](sharding.md#a-real-measurement-not-an-assumed-one)
