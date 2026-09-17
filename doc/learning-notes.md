# Learning Notes

Two kinds of notes here: what building the *same* kind of service
(HTTP API, Kafka consumer, ANN search, scoring endpoint) back-to-back in
five languages actually teaches you, and the real bugs hit while building
this repo -- kept because each one is an instance of a general lesson, not
just a one-off fix.

## Per-language notes

### Go (post-ingestion-service, feed-aggregation-service)

- Go 1.22's `net/http` added method-and-path routing patterns
  (`mux.HandleFunc("POST /v1/posts", ...)`) directly in the standard
  library -- for a service this size, that removed any reason to reach for
  a router package at all.
- `feed-aggregation-service`'s three-way fan-in (Redis inbox, celebrity
  outboxes, Qdrant search) is a `sync.WaitGroup` with three goroutines --
  about as close as code gets to reading like the doc's own "Worker 1 /
  Worker 2 / Worker 3, in parallel" diagram.
- `context.Context` threaded through every DB/Redis/HTTP call by
  convention (not enforced by the compiler) is the idiomatic way
  cancellation and timeouts propagate -- there's no equivalent single
  mechanism in the other four languages used here.
- Adding `pgx/v5` as a dependency caused `go get` to print
  `go: downloading go1.25.0` and switch toolchains automatically, because
  that dependency's `go.mod` declared a minimum Go version newer than what
  was installed. This is expected modern-Go behavior (the `go` command
  manages its own toolchain versions per-module since Go 1.21), not a
  sandboxing quirk -- worth knowing so it doesn't look alarming the first
  time it happens.

### Java (fanout-worker)

- Kafka's official client library is Java, and it shows: consumer group
  membership, partition rebalancing, and manual offset commits
  (`enable.auto.commit=false` + `commitSync()` after a batch succeeds) are
  most naturally documented and most directly expressed here, since the
  rest of the Kafka ecosystem's own internals and docs assume this client.
- Producing a single runnable artifact needs an explicit step (the
  `maven-assembly-plugin` "jar with dependencies") that Go's static
  binary and Rust's `cargo build` give you for free. Not better or worse,
  just a build step that doesn't exist in the other four services here.
- Hit a real transitive-dependency version conflict here -- see "slf4j"
  below.

### Python (vector-pipeline)

- `sentence-transformers` made "compute a real embedding" a five-line
  service; the actual engineering cost was environment setup (`torch` is
  large, and see the `python3-venv` note below), not the ML code.
- `kafka-python` is pure Python with no native dependencies, unlike
  `confluent-kafka` (which wraps `librdkafka`, faster but needs a system
  library present). For a demo meant to run on an unknown machine, the
  pure-Python client is the safer default -- a real production consumer,
  chasing lower latency and CPU, would more likely pick the native-backed
  client instead.
- The GIL is a non-issue for a service like this: it spends essentially
  all its time waiting on the Kafka socket, Qdrant's HTTP API, or the
  (CPU-bound, but single-request-at-a-time here) embedding model -- there's
  no concurrent Python code trying to share the interpreter.

### Rust (ranking-service)

- `axum` + `tokio` is the idiomatic modern choice for a small async HTTP
  service, and reads almost identically to what you'd write in Go, just
  with explicit `async fn` and `.await`.
- The type system earned its keep once, directly: a field-name mismatch
  between the JSON request struct and what `feed-aggregation-service`
  actually sent would have been a silent `null`/zero-value bug at runtime
  in Go, Python, or Node (all of which happily deserialize partial JSON);
  in Rust, `serde`'s `#[serde(rename = "...")]` attributes made the exact
  wire field names explicit, and a typo there is a compile-time field name
  that just doesn't match anything, not a runtime surprise.
- Ownership/borrowing never had to fight the code here, because this
  service deliberately holds no shared mutable state -- it's a pure
  function from request to response. That's exactly the kind of service
  Rust's ownership rules have the least to say about; they'd matter far
  more in a service maintaining, say, an in-memory LRU cache across
  requests.
- First `cargo build` pulled ~60 transitive crates and took the longest
  wall-clock time of any service's first build here. Incremental builds
  after that were fast.

### Node/TypeScript (notification-service)

- `kafkajs`'s manual-commit API (`consumer.run({ autoCommit: false, ... })`
  + `commitOffsets(...)`) is more explicit/lower-level than either the
  Java or Python client's equivalent -- you construct the offset-plus-one
  object yourself rather than the library inferring it.
- Async/await over a single-threaded event loop is the most natural fit of
  any of the five languages here for a service that's *purely*
  I/O-bound (Kafka in, two Postgres queries out, nothing CPU-heavy at all).
- This service's `PostCreatedEvent` interface used to be a hand-written
  TypeScript type next to the Go producer's matching struct, agreeing on
  JSON field names purely by convention -- exactly the gap a shared
  schema closes. It's since been replaced by FlatBuffers codegen (see
  [wire-protocols.md](wire-protocols.md)); the flatbuffers-js runtime's
  choice to decode 64-bit fields as native `bigint` turned out to be a
  bonus for this specific service, since it already mints its own
  `bigint` Snowflake IDs -- one less string-to-bigint conversion, not one
  more.

## Real bugs hit building this repo

Each of these actually happened while building this system, not
hypothetical -- kept here because each is a specific instance of a general
lesson worth carrying to other projects.

### 1. Kafka topic auto-create race

**Symptom:** the very first post published from `post-ingestion-service`
failed with `[3] Unknown Topic Or Partition`, even though the writer was
configured with `AllowAutoTopicCreation: true`.
**Root cause:** the producer's metadata request *triggers* topic creation
asynchronously, but the write attempt that triggered it doesn't wait for
creation to finish -- it just fails, once, before the topic exists.
**Fix:** [`scripts/create_topics.sh`](../scripts/create_topics.sh)
pre-creates the topic explicitly before any service starts.
**General lesson:** never rely on lazy resource creation being ready by
the time the same code path needs it -- if creation is async, either wait
for it explicitly or provision the resource ahead of time.

### 2. MinIO's Docker Hub image requires a login now

**Symptom:** `docker pull minio/minio:latest` returned
`pull access denied for minio/minio ... denied: requested access to the
resource is denied`.
**Root cause:** MinIO changed their Docker Hub distribution terms; the
`minio/minio` image now requires authentication for anything beyond very
old pinned tags.
**Fix:** `docker-compose.yml` pulls `quay.io/minio/minio:latest` instead
(MinIO's own free mirror).
**General lesson:** a well-known image name in a tutorial or doc can stop
working with zero warning because the vendor changed distribution terms,
not because anything in your setup is wrong. Verify pulls actually work;
don't trust that a familiar image name still resolves the way it used to.

### 3. Silent logging failure from an slf4j version conflict

**Symptom:** `fanout-worker` printed
`SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder"` and then
every `log.info(...)` call silently no-op'd, despite `slf4j-simple` being
a direct Maven dependency.
**Root cause:** `kafka-clients` transitively depends on `slf4j-api 1.7.x`;
Maven's "nearest wins" dependency mediation resolved that transitive
version over the 2.x line `slf4j-simple` actually needed, so the two
libraries loaded incompatible major versions of the same API at runtime.
`mvn dependency:tree` confirmed it immediately once checked.
**Fix:** pinned `slf4j-api` explicitly as a direct dependency at the same
version as `slf4j-simple`, so it wins dependency mediation.
**General lesson:** "I declared the dependency I need" doesn't guarantee
"the version that actually loads at runtime is the one I expect," in any
build system with transitive dependency resolution. When a library's
observed behavior doesn't match its documentation, check the *resolved*
dependency tree before assuming the library itself is broken.

### 4. Postgres port collision with an unrelated project

**Symptom:** `docker-compose up -d` needed a port remap before it would
even start cleanly.
**Root cause:** the dev machine already had an unrelated project's
Postgres container bound to host port 5432.
**Fix:** this repo's 4 Postgres shards map to host ports **5441-5444**
instead (see `docker-compose.yml` and `.env`).
**General lesson:** never assume a service's default port is free on a
shared or long-lived dev machine -- check with `ss -ltn` / `docker ps`
before binding, especially for infra with well-known default ports
(5432, 6379, 9092, ...) that many unrelated projects all reach for.

### 5. A malformed shard-routed ID crashed the whole process, not just one request

**Symptom:** appending a test candidate with a made-up post ID (not a
real self-routing ID minted by the system) to a user's cold-tier inbox,
then reading that user's feed, crashed feed-aggregation-service entirely
with a nil-pointer panic -- taking down every other concurrent request
with it, not just the one bad candidate.
**Root cause:** `ExtractShardID` on an out-of-range ID happily returns
some integer, and `ShardedPool.PoolForShard` looked that up in a
`map[int]*pgxpool.Pool` with no existence check -- a miss returns Go's
nil zero-value silently, and the first `.Query()` call on that nil pool
panics. `PoolForShard`/`PoolForExistingID` had no way to say "that shard
doesn't exist" short of crashing.
**Fix:** both methods now return `(*pgxpool.Pool, error)`, and every
caller that builds a shard-grouped batch (post metadata hydration,
taste-vector seed-post lookup) validates each ID's shard during grouping
and drops -- with a logged warning -- any ID that doesn't map to a real
shard, rather than including it and discovering the problem inside a
goroutine later. Fixed identically in both post-ingestion-service and
feed-aggregation-service, since both have the same `ShardedPool` shape.
**General lesson:** a sharding layer's routing function needs a real
error path for "no such shard," not just a fallback value -- and where it
gets checked matters: validating during grouping means one bad ID quietly
drops out of a batch; validating only inside the per-shard goroutine
means one bad ID either kills the whole request (if the error propagates)
or kills the whole process (if it doesn't, as `nil` pointer derefs don't).
The second case is far worse than the first, and both are worse than
catching it before the fan-out even starts.

### 6. FlatBuffers Java wouldn't link, one `flatc` release too new

**Symptom:** `fanout-worker` failed to compile with `cannot find symbol:
method FLATBUFFERS_25_12_19()` inside FlatBuffers' own generated
`PostCreatedEvent.java`, immediately after wiring up
[schemas/fbs/post_created.fbs](../schemas/fbs/post_created.fbs) (see
[wire-protocols.md](wire-protocols.md)).
**Root cause:** FlatBuffers' generated Java code calls a
version-checking method (`Constants.FLATBUFFERS_<exact-version>()`) that
only exists if the `flatbuffers-java` runtime jar is the *exact* version
`flatc` generated against -- `flatc` was pinned to the newest release at
the time (`25.12.19`), but Maven Central's `flatbuffers-java` artifact
only went up to `25.2.10`. Go, Rust, and Python's generated code carry no
such check, so the same schema built cleanly for every other language and
only failed here.
**Fix:** pinned `flatc` itself to `25.2.10` -- the newest version with a
published Maven Central jar -- and regenerated every language's bindings
against it, rather than trying to work around the mismatch in one
language.
**General lesson:** when one schema feeds codegen for N languages,
"newest compiler" isn't automatically the right choice -- the constraint
is the *oldest* published runtime library across every target language,
and that's worth checking before picking a version, not after a build
failure points at it.

### 7. gRPC DNS resolution silently broke inside the shaded fat jar, not the code

**Symptom:** `fanout-worker` crashed on startup with `IllegalArgumentException:
Address types of NameResolver 'unix' for 'feed-aggregation-service:4102'
not supported by transport` -- but only when run from the built
`fanout-worker-jar-with-dependencies.jar` (in Docker); the exact same
`ColdTierGrpcClient` code, hitting the exact same gRPC APIs, passed every
local `mvn test` run against `localhost:<port>` with no error at all.
**Root cause:** `grpc-netty-shaded`, `grpc-core`, and `grpc-protobuf`
each ship their own `META-INF/services/io.grpc.NameResolverProvider`
file (standard Java `ServiceLoader` registration). `maven-assembly-plugin`'s
`jar-with-dependencies` descriptor copies dependency JAR contents into
one fat jar but does **not** merge same-path resource files across
JARs -- whichever dependency's copy of that exact path landed last in
the merge silently overwrote the others, leaving only a Unix-domain-socket
resolver registered and none of the normal DNS resolver. Unit tests never
caught this because `mvn test` runs against the normal multi-JAR
classpath (every `META-INF/services` file present, unmerged, un-collided);
only the actual shaded artifact -- the one thing Docker actually runs --
was affected.
**Fix:** replaced `maven-assembly-plugin` with `maven-shade-plugin`'s
`ServicesResourceTransformer`, which merges same-path service files
line-by-line instead of overwriting. Confirmed by inspecting the built
jar directly (`unzip -p ... META-INF/services/io.grpc.NameResolverProvider`)
rather than trusting that a green test suite meant the packaged artifact
was correct.
**General lesson:** a passing test suite proves the *code* works against
whatever classpath the test runner assembles -- it says nothing about
whether the *packaging step* reproduces that same classpath faithfully.
Uber-jar builds are exactly where those two things diverge, and
`ServiceLoader`-based libraries (gRPC, JDBC drivers, many others) are
exactly the mechanism most likely to break silently when they do.
