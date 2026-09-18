# Feed Generation Service

A runnable implementation of a public [Instagram-style feed design
doc](doc/instagram_feed_design_doc.pdf) — actually sharded, not a
single-database demo with sharding described as a hypothetical. Four
independent Postgres shards, self-routing IDs, a real graph database for
the social graph, hybrid fan-out, a hot/cold feed cache, and vector-based
recommendations for posts outside your network.

Every entity ID carries its own shard number, and a script
([`scripts/verify_shard_parity.sh`](scripts/verify_shard_parity.sh))
proves the Go and Java services agree, byte-for-byte, on where a new
entity lands — see [doc/DESIGN.md](doc/DESIGN.md) for why that matters.

Each service is written in a different language, matching the role it
would realistically use in production:

| Service | Language | Job |
|---|---|---|
| [post-ingestion-service](services/post-ingestion-service) | Go | Every write: posts, follows, likes, comments |
| [fanout-worker](services/fanout-worker) | Java | Decides whose inbox a new post lands in |
| [vector-pipeline](services/vector-pipeline) | Python | Embeds post captions for similarity search |
| [notification-service](services/notification-service) | Node/TS | `@mention` notifications |
| [feed-aggregation-service](services/feed-aggregation-service) | Go | Builds and ranks a user's feed |
| [ranking-service](services/ranking-service) | Rust | Scores a list of candidate posts |

## Read this first

- **[doc/DESIGN.md](doc/DESIGN.md)** — the whole system, in plain English: architecture, data model, write/read paths, and why the harder decisions were made the way they were.
- **[doc/TRADE-OFFS.md](doc/TRADE-OFFS.md)** — what's simplified vs. real production scale, and what to build first. Written for system-design interview prep.

## Prerequisites

- Docker + `docker-compose` — the only requirement to run the system.
- Go, Rust, Java 21+, Python 3, Node.js — only needed for the local
  dev/test loop (`scripts/build_all.sh`), not for running the app.

## Quick start

```bash
bash scripts/run_all.sh     # starts everything: 4 Postgres shards, Neo4j,
                             # Redis, Kafka, Qdrant, MinIO, all 7 services, Envoy
python3 scripts/seed.py     # demo users, follow graph, ~14 sample posts
```

Then open **http://localhost:8080** — a small HTML/JS UI with a user
switcher, a follow/unfollow panel, a post composer (mention `@user_3` to
trigger a real notification), and a feed that shows each post's score and
where it came from (in-network / celebrity / recommended / ad). Like and
comment on posts to watch their rank move.

Or drive it directly with curl:

```bash
curl -X POST http://localhost:8080/api/ingest/v1/posts \
  -H 'Content-Type: application/json' \
  -d '{"userId":"357748213593194496","mediaUrl":"https://example.com/img.jpg","mediaType":1,"caption":"hello @user_3"}'

curl "http://localhost:8080/api/feed/v1/feed?userId=357748214239117312"
```

Tail logs with `docker-compose logs -f <service>`. Stop everything with
`bash scripts/stop_all.sh`. Check that Go and Java agree on shard
placement, independent of the running stack:

```bash
bash scripts/verify_shard_parity.sh 10000
```

## Toolchain notes

- `scripts/env.sh` puts Go, Rust, JDK 21, and the schema compilers
  (`protoc`, `flatc`) on `PATH` for the dev loop.
- `vector-pipeline`'s image is pinned to Python 3.10 — a dependency's
  vendored shim breaks on 3.12.
- The 4 Postgres shards map to host ports 5441–5444 (5432 is often
  already taken on a dev machine). `config/shards.json` has host
  addresses; `config/shards.docker.json` has container addresses — same
  topology, different addressing, since containerized services and
  host-mode tools reach the shards differently.
- Kafka topics are pre-created before any consumer starts — creating a
  topic on first publish is racy and can drop the very first message.

## Repo layout

```
docker-compose.yml       infra + all 7 app services + envoy, one Compose file
envoy/envoy.yaml          the gateway: routing, health checks, HTTP/3 listener
db/shard-schema.sql       schema applied identically to all 4 shards
config/shards.json        shard topology, host ports
config/shards.docker.json same topology, container addresses
pkg/sharding/             shared Go module: hash ring + self-routing IDs
schemas/proto/, schemas/fbs/   gRPC and FlatBuffers schemas for internal calls
doc/                      DESIGN.md, TRADE-OFFS.md
scripts/
  run_all.sh              docker-compose up --build -- how the app actually runs
  stop_all.sh              docker-compose down
  seed.py                  demo users, follow graph, sample posts
  build_all.sh             local dev/test loop (go/cargo/mvn/npm test)
  verify_shard_parity.sh   diffs the Go and Java shard routers
  verify_schema_gen.sh     checks committed generated code matches schemas/
services/                 one directory per service (Go, Java, Python, Rust, Node)
web-ui/                   static HTML/CSS/JS sample client
```
