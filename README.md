# Feed Generation Service

A runnable, application-level-sharded implementation of the [Instagram Feed
Architecture design doc](doc/instagram_feed_design_doc.pdf): 4 physical
Postgres shards with self-routing IDs, a graph database for the social
graph, hybrid fan-out, an event bus decoupling writes from
fan-out/vector/notification work, a hot/cold-tiered timeline cache, and
vector-based out-of-network recall.

This isn't a single-database demo with sharding "documented as a trade-off."
It's actually sharded: every entity ID embeds its own shard number, every
repository routes through a shard-aware connection pool, and
[`scripts/verify_shard_parity.sh`](scripts/verify_shard_parity.sh) proves
the Go and Java services agree, byte-for-byte, on where a new entity lands.
See [doc/sharding.md](doc/sharding.md) for the full design record, including
a bug the parity tests actually caught during development.

Each service is written in a **different language**, matching the doc's own
(explicit or implied) tech choices, so the codebase doubles as a hands-on
comparison of how the same kind of problem (HTTP API, Kafka consumer, ANN
search, ranking, shard routing) looks in each ecosystem -- and, per
[doc/trade-offs.md](doc/trade-offs.md) row 13, an honest accounting of what
that polyglot choice actually costs operationally.

| Service | Language | Why | Doc reference |
|---|---|---|---|
| [post-ingestion-service](services/post-ingestion-service) | **Go** | Fast, low-overhead HTTP write path; owns sharded Postgres + Neo4j writes | §3 write path |
| [fanout-worker](services/fanout-worker) | **Java** | Kafka's native/most common consumer ecosystem; hybrid fan-out across shards + graph | §3.4 Hybrid Fan-Out Logic |
| [vector-pipeline](services/vector-pipeline) | **Python** | Doc names Sentence-Transformers explicitly | §4 "Consumer 2: Vector Pipeline" |
| [notification-service](services/notification-service) | **Node/TypeScript** | Lightweight I/O-bound event consumer; BigInt-based shard-aware ID minting | §4 "Consumer 3: Notification Service" |
| [feed-aggregation-service](services/feed-aggregation-service) | **Go** | Explicit in the doc; owns the hot/cold feed-storage tier | §5 diagram: "Feed Aggregation Service (Go)" |
| [ranking-service](services/ranking-service) | **Rust** | Explicit alternative in the doc | §5 diagram: "Ranking Service (Rust / C++)" |

Every entity ID, shard-routing decision, and cross-language interface above
is implemented twice (Go in `pkg/sharding`, Java in
`com.feed.sharding`) against the identical algorithm -- see
[doc/sharding.md](doc/sharding.md#cross-language-parity) for why that's
load-bearing, not decorative.

## Documentation

This README covers running it. For the deeper material, see [doc/](doc/index.md):

- **[doc/architecture.md](doc/architecture.md)** -- every service, the sharded/graph/hot-cold data model, Redis/Kafka/Qdrant/Neo4j schemas
- **[doc/sharding.md](doc/sharding.md)** -- application-level sharding design: self-routing IDs, consistent hashing, why the social graph lives in Neo4j instead of sharded Postgres, what's still unsolved
- **[doc/caching.md](doc/caching.md)** -- the hot (Redis) / cold (BadgerDB) feed storage tier, and the RocksDB→BadgerDB substitution reasoning
- **[doc/engagement-at-scale.md](doc/engagement-at-scale.md)** -- likes/comments at hyperscale: sharded Redis counters, read-your-own-writes, rate limiting, moderation, cache-stampede protection, and what's deliberately deferred
- **[doc/flow.md](doc/flow.md)** -- step-by-step walkthrough of one post's write path and one feed's read path through the sharded system, with file references
- **[doc/gateway.md](doc/gateway.md)** -- the Envoy gateway: routing table, why service-to-service calls bypass it, and the HTTP/3 (QUIC) listener scoped to the feed read path
- **[doc/trade-offs.md](doc/trade-offs.md)** -- remaining local-demo choice vs. real production-grade choice, written for system-design-interview prep
- **[doc/learning-notes.md](doc/learning-notes.md)** -- what building the same service in 6 languages actually teaches you, plus the real bugs (including a process-crashing nil-pointer panic from a malformed shard ID) hit building this repo and the general lesson each is an instance of

## Architecture

Every application service runs as a Docker Compose container now, and
the **Envoy gateway is the only door in** for client traffic -- see
[doc/gateway.md](doc/gateway.md) for the full routing design, including
why 2 internal service-to-service calls deliberately bypass it.

```
                              Client
                                 |
                     ┌───────────────────────┐
                     │   Envoy gateway (:8080  │
                     │   HTTP, :8443 HTTP/3)    │
                     └───┬───────────┬────────┘
              /api/ingest│  /api/feed│         /*
                         ▼           ▼           ▼
      post-ingestion-service   feed-aggregation-service   web-ui (nginx)
             (Go)                      (Go)
               |                        |  ▲
               ▼                        |  └── ranking-service (Rust)
   shard N of 4 Postgres instances      |      composite score, direct
   Neo4j (social graph)                 |      call, NOT via Envoy
   Redis (sharded counters,             |
     rate limiting, RYOW cache)         ▼
               |                Redis hot inbox / celebrity outbox
               ▼                BadgerDB cold tier (promotes on read)
        Kafka "post-created"    Qdrant ANN search
               |
   ┌───────────┼───────────────────┐
   ▼           ▼                   ▼
fanout-worker  vector-pipeline   notification-service
(Java)         (Python)          (Node)
  |               |                  |
Neo4j "who     Qdrant upsert      Redis username directory ->
follows me" ->                    sharded Postgres notifications
cross-shard active-filter ->
Redis hot inbox / celebrity
outbox / BadgerDB cold tier
(direct call to feed-aggregation-
 service, NOT via Envoy)
```

## Prerequisites

- Docker + `docker-compose` -- this is now the **only** requirement to
  actually run the system.
- Go, Rust, Java 21 + Maven, Python 3, Node.js are needed only for the
  local dev/test loop (`scripts/build_all.sh`: `go test`, `cargo build`,
  `mvn test`, `npm test`, the shard-parity gate) -- see
  [Toolchain notes](#toolchain-notes) if any are missing and you want
  that inner loop.

## Quick start

```bash
bash scripts/run_all.sh     # docker-compose up --build: 4 Postgres shards, Neo4j,
                             # Redis, Kafka, Qdrant, MinIO, all 7 app services, and Envoy
python3 scripts/seed.py     # demo users, follow graph, and ~14 sample posts (via the gateway)
```

Then open **http://localhost:8080** — a small vanilla HTML/JS UI (no build step, no framework), served by nginx behind Envoy, with:
- a **user switcher** to view the app as any seeded account (including the celebrity),
- a **People** panel to **follow/unfollow** and watch follower counts move (backed by Neo4j),
- a **composer** to create a post (with a caption and a placeholder-image color swatch — mentioning e.g. `@user_3` produces a real notification),
- a **feed** showing each item's ranking score and which candidate source it came from (in-network / celebrity / vector / ad), so you can see the design doc's funnel in action -- **like and comment** on posts to watch their score move, and use **Reset seen (demo)** if you've paged through the small sample pool and want to re-explore it.

Or drive it with curl directly, through the gateway — the write path
(`userId` is a real self-routing Snowflake ID minted by `POST /v1/users`,
not a small integer you can pick yourself):

```bash
curl -X POST http://localhost:8080/api/ingest/v1/posts \
  -H 'Content-Type: application/json' \
  -d '{"userId":"357748213593194496","mediaUrl":"https://example.com/img.jpg","mediaType":1,"caption":"hello @user_3"}'
```

...and the read path a couple seconds later (once the async consumers have processed the event):

```bash
curl "http://localhost:8080/api/feed/v1/feed?userId=357748214239117312"
```

The same feed route also speaks real HTTP/3 on `https://localhost:8443`
-- see [doc/gateway.md](doc/gateway.md#verification-a-real-http3-request-not-an-assumed-one)
for how that's actually verified (the host's `curl` here predates
HTTP/3 support, so verification uses a real QUIC client instead of
asserting the config "should" work).

Tail logs with `docker-compose logs -f <service>`. Stop everything with `bash scripts/stop_all.sh`.

Verify Go/Java agree on shard placement independently of the running stack:

```bash
bash scripts/verify_shard_parity.sh 10000
```

## Services in detail

Port numbers below (`:4001`, `:4002`, `:4003`) are each service's
**container-internal** port -- none of them are published to the host
anymore. Everything client-facing goes through Envoy at `:8080`/`:8443`
(see [doc/gateway.md](doc/gateway.md)); reaching a service directly by
port is only possible from inside the Compose network (e.g. one
container calling another by service name), which is exactly how
fanout-worker's cold-tier call and feed-aggregation-service's ranking
call work.

### post-ingestion-service (Go) -- `:4001`
- `POST /v1/uploads/presign` -- returns a MinIO pre-signed PUT URL (direct-to-blob upload, bypassing the app server, per §3).
- `POST /v1/users` -- mints a self-routing Snowflake ID via the consistent-hash ring (`pkg/sharding`), inserts into that shard's Postgres, and `MERGE`s a lightweight node into Neo4j. Duplicate usernames return `409` (mapped from Postgres's unique-constraint violation).
- `POST /v1/posts` -- mints a Snowflake ID that **inherits the author's shard bits** (no re-hashing), commits to that shard's Postgres, publishes `post-created` to Kafka (keyed by `userId`), returns 201 immediately -- the Kafka publish does not block the response. Accepts optional `likeCount`/`commentCount` for seeding realistic engagement numbers; real client posts omit them (start at 0).
- `GET /v1/users`, `GET /v1/users/{id}/following`, `POST /v1/follow`, `POST /v1/unfollow` -- social graph reads/writes against **Neo4j**, not sharded Postgres (see [doc/sharding.md](doc/sharding.md#the-social-graph-lives-in-neo4j-not-sharded-postgres)). Follow/unfollow update `followerCount`/`isCelebrity` atomically in the same Cypher transaction as the edge.
- `POST /v1/posts/{id}/like`, `POST /v1/posts/{id}/unlike` -- idempotent per (post, user); increments a **sharded Redis counter** (16 sub-keys, see [doc/engagement-at-scale.md](doc/engagement-at-scale.md)), updates a read-your-own-writes Redis Set, and is rate-limited (20 toggles/user/post/minute).
- `POST /v1/posts/{id}/comments`, `GET /v1/posts/{id}/comments` -- real comment text in Postgres, co-located with the **post's** shard; writes pass a synchronous moderation blocklist; reads are protected from cache stampede via `singleflight`.
- CORS is wide open (`Access-Control-Allow-Origin: *`) -- a leftover from before Envoy existed, when the web UI (a different port) needed it to call this service directly. Everything now goes through Envoy on one origin (`:8080`), so this is no longer load-bearing, but it's also not yet removed. Fine for a local demo either way; lock it down before exposing this anywhere real.

### fanout-worker (Java)
Kafka consumer implementing the hybrid fan-out rule from §3.4, now across
shards and Neo4j: reads `isCelebrity` off the author's Neo4j node; if
true, a single append to `celebrity:outbox:<id>`. Otherwise it queries
Neo4j for the full follower list, then does a cross-shard, concurrent
Postgres lookup (`ExecutorService`-based scatter-gather, grouping follower
IDs by shard via bit-shift, dropping any malformed/out-of-range ID rather
than failing the whole fan-out) to split followers into active vs.
dormant. Active followers get pushed straight into their Redis hot inbox
(`feed:user:<id>`, capped at `FEED_INBOX_MAX_ITEMS`); dormant followers
are appended to the **BadgerDB cold tier** via an HTTP call to
feed-aggregation-service, instead of being dropped.

### vector-pipeline (Python)
Computes a real sentence embedding of the post caption (`all-MiniLM-L6-v2`,
384-dim) and upserts it into Qdrant. The doc names Sentence-Transformers
explicitly; we use it as-is rather than a literal 128-d two-tower model,
since training a real two-tower retrieval model is out of scope for a local
demo -- this still gives genuine semantic embeddings for real ANN search.
Unaffected by sharding: it never touches Postgres.

### notification-service (Node/TypeScript)
Parses `@mentions` out of the caption, resolves the username via the Redis
username directory (not a Postgres lookup -- there's no single users table
anymore), mints the notification's ID **inheriting the recipient's shard**
using a BigInt Snowflake generator (IDs exceed `Number.MAX_SAFE_INTEGER`),
and writes the row to that shard's Postgres.

### ranking-service (Rust) -- `:4003`
`POST /rank` implements the composite scoring objective from §4:

```
Score = w1*P(Like) + w2*P(Comment) + w3*P(Share) + w4*P(Dwell>5s) - w5*P(Hide)
```

The doc's production ranker is a two-pass LightGBM + multi-task deep model;
training real models is out of scope here, so `P(Like)` etc. are estimated
with transparent heuristics (recency decay x normalized engagement counts)
instead. Fully stateless -- no shard-awareness needed, since candidates
arrive pre-hydrated with the counts it needs.

### feed-aggregation-service (Go) -- `:4002`
`GET /v1/feed?userId=&cursor=` runs the full read-path funnel from §4:
1. Fan-in candidate retrieval, concurrently: Redis hot inbox (falling back to and promoting from the BadgerDB cold tier if empty, see [doc/caching.md](doc/caching.md)), celebrity outboxes of celebrities the user follows (via Neo4j), and Qdrant ANN search against a "taste vector" (the average embedding of the user's own/followed-users' recent posts, standing in for a learned two-tower user embedding -- itself a cross-shard scatter-gather over post metadata, since followees' posts live on their own shards).
2. Seen-state filter -- a Redis ZSET of last-shown timestamps per user, dropping anything shown in the last 48h.
3. Hydration -- post metadata and like/comment counts, fetched by grouping candidate IDs by shard (bit-shift, no lookup) and fanning out concurrently; any candidate with a malformed/out-of-range shard ID is dropped and logged rather than failing the whole batch (see [doc/learning-notes.md](doc/learning-notes.md), bug #6).
4. Calls `ranking-service` for composite scoring.
5. Applies the "max 2 posts per author" diversity rule and inserts synthetic ads at slots 3 and 8.
6. Returns an AES-256-GCM encrypted opaque cursor for the next page.

Each returned item also carries `likedByMe` (read straight from the
read-your-own-writes Redis Set, not a per-shard Postgres query) and
`likeCount`/`commentCount` (summed from the sharded Redis counters) --
which is exactly what liking/commenting on a post moves, so you can watch
a post's rank change in real time by engaging with it.

`POST /v1/feed/reset-seen` clears a user's seen-state history. **This is a local demo convenience, not a real product feature** -- it exists because this repo's sample content pool is small and fixed, so paging through it once exhausts what's unseen (a real feed never needs this; it refills continuously from new posts by people you follow).

### web-ui (static HTML/CSS/JS) -- `:80` (container-internal; reached via Envoy at `:8080`)
No framework, no build step -- just static files, served by `nginx:alpine`
in the Docker Compose deployment. The browser only ever talks to Envoy's
port 8080; it never makes a cross-origin request, and never needs to --
Envoy does the `/api/ingest` -> post-ingestion-service and `/api/feed` ->
feed-aggregation-service routing that a hand-rolled reverse proxy used to
do (see [doc/gateway.md](doc/gateway.md)). [`serve.py`](web-ui/serve.py)
still exists and still does that same proxying itself, for one specific
case: iterating on the web UI directly on the host, without Docker, where
there's no Envoy in front of it. It's not used in the containerized
deployment (`web-ui/Dockerfile` uses nginx instead) and is otherwise
redundant with Envoy now.

Post images are generated client-side (and server-side, for seeded content) as inline SVG `data:` URIs -- colored background + emoji -- so the whole demo works fully offline with no image hosting or MinIO upload flow wired in (the real presigned-upload endpoint still exists at `POST /v1/uploads/presign` for a production client to use).

## What's simplified vs. the production doc

This runs on one laptop, not 500M DAU across a real data center fleet: 4
Postgres shards instead of hundreds, one Neo4j instance instead of a
causal cluster, one embedded BadgerDB cold tier instead of a distributed
LSM fleet, heuristics instead of trained ranking/recall models, a
single-broker Kafka instead of a replicated cluster, and no
observability/auth/rate-limiting on the API surface itself (engagement
rate-limiting against flapping *is* implemented -- see
[doc/engagement-at-scale.md](doc/engagement-at-scale.md)). Every one of
those is a deliberate, named trade-off -- see
**[doc/trade-offs.md](doc/trade-offs.md)** for the full breakdown of what's
still simplified, why the production-grade choice wins at scale, and what
an interviewer would likely ask as a follow-up. What's **not** on that
list anymore -- because it's genuinely implemented, not just discussed --
is sharding itself, the social graph store, hot/cold feed storage, and a
real edge gateway with HTTP/3 on the feed read path; see
[doc/sharding.md](doc/sharding.md), [doc/caching.md](doc/caching.md),
[doc/engagement-at-scale.md](doc/engagement-at-scale.md), and
[doc/gateway.md](doc/gateway.md) for those.

## Toolchain notes

- **Running the system only needs Docker** -- `scripts/run_all.sh` is
  `docker-compose up --build` under the hood. Everything below is about
  the local dev/test loop (`scripts/build_all.sh`), not about running
  the app.
- Go and Rust are **not** installed via apt/sudo here -- Go was unpacked from the official tarball into `~/go-toolchain`, Rust via `rustup` into `~/.cargo`. `scripts/env.sh` puts both on `PATH`; source it (or use the provided scripts, which already do) rather than relying on a system-wide install.
- Java: the system default `java` may point at an older JDK; the fan-out worker needs 17+. `scripts/env.sh` pins `JAVA_HOME` to a JDK 21 install if present.
- Python deps are installed with `pip3 install --user` (no venv) because `python3-venv` isn't installed and requires `sudo apt install python3-venv`. If you'd rather use a venv, install that package and adjust `scripts/build_all.sh`. The `vector-pipeline` Docker image is pinned to **Python 3.10** (matching this host toolchain), not 3.12 -- `kafka-python==2.0.2`'s vendored `six` shim breaks on 3.12, found by actually running the container.
- **MinIO's `minio/minio` Docker Hub image now requires a paid login.** `docker-compose.yml` uses `quay.io/minio/minio:latest` instead (MinIO's official free mirror).
- The 4 Postgres shards are mapped to host ports **5441-5444** (not 5432), and Neo4j's Bolt port is the standard **7687**. There are **two** shard config files: `config/shards.json` (host ports, used by `go test`, `verify_shard_parity.sh`, and anything run directly on the host) and `config/shards.docker.json` (container-network addresses like `shard-0:5432`, used by every containerized app service). Same topology, different addressing -- see `doc/architecture.md`.
- Kafka topics are pre-created by the one-shot `kafka-init` Compose service against the internal listener (`kafka:29092`) before any consumer starts -- auto-create-on-first-produce is racy (the first publish can land before the topic finishes creating and gets dropped). `scripts/create_topics.sh` still exists for host-mode use against the external listener (`localhost:9092`).
- `go.work` ties `pkg/sharding` into both Go services as a local module for host development (no package registry needed). Each Go service's own `go.mod` also carries a `replace sharding => ../../pkg/sharding` fallback specifically for its Docker build (`GOWORK=off`), since a container build only copies one service, not the whole workspace, and `go.work` would otherwise demand every module it lists exist on disk. `scripts/build_all.sh` builds and tests the workspace first, then runs `scripts/verify_shard_parity.sh` as a build gate before considering the build done.

## Repo layout

```
docker-compose.yml       infra + all 7 app services + envoy, one Compose file
.dockerignore            keeps the two Go services' build context small (they build from the repo root)
envoy/envoy.yaml         the gateway: routing, health checks, the HTTP/3 listener (see doc/gateway.md)
envoy/certs/             self-signed dev TLS cert, generated by scripts/gen_dev_certs.sh, gitignored
db/shard-schema.sql      schema applied identically to all 4 shards (no `follows` table -- see doc/sharding.md)
config/shards.json       shard topology, HOST ports -- read by go test / verify_shard_parity.sh / host-mode runs
config/shards.docker.json  same topology, CONTAINER-network addresses -- read by every containerized app service
pkg/sharding/            shared Go module: consistent-hash ring + self-routing Snowflake IDs (also a CLI for parity checks)
.env / .env.example      config for host-mode dev/tooling (docker-compose.yml sets its own container-network env vars inline)
doc/                     architecture, sharding, caching, engagement-at-scale, gateway, flow, trade-offs, learning notes (see doc/index.md)
scripts/
  env.sh                 puts Go/Rust/JDK21 on PATH (for build_all.sh, not for running the app)
  build_all.sh           local dev/test loop: go/cargo/mvn/npm test + the shard-parity gate (does NOT run the app)
  gen_dev_certs.sh        generates Envoy's self-signed dev TLS cert if missing
  run_all.sh             docker-compose up --build -- the only way the app actually runs
  stop_all.sh            docker-compose down
  migrate.sh             re-applies db/shard-schema.sql idempotently to all 4 shards (host-mode tool)
  create_topics.sh       pre-creates Kafka topics against the host-mode listener (kafka-init does this for Compose)
  verify_shard_parity.sh builds and diffs the Go and Java shard routers against thousands of sample keys
  seed.py                demo users, follow graph, and sample posts (via the gateway + a documented Neo4j seed-only shortcut for celebrity status)
services/
  post-ingestion-service/    Go (clean architecture: domain/service/storage/transport/cmd), test/unit/, Dockerfile
  fanout-worker/             Java, Maven (clean architecture, same layering), src/test/java/, Dockerfile
  vector-pipeline/           Python, Dockerfile
  notification-service/      Node/TypeScript (clean architecture, same layering), test/unit/, Dockerfile
  feed-aggregation-service/  Go (clean architecture, same layering), test/unit/, Dockerfile
  ranking-service/           Rust (Cargo), Dockerfile
web-ui/                      Static HTML/CSS/JS sample client (feed, post, follow/unfollow, like/comment), Dockerfile (nginx), serve.py (host-mode-only fallback)
```
