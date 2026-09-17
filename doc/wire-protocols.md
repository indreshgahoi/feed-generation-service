# Internal Wire Protocols: gRPC/Protobuf and FlatBuffers

Every wire format in this system used to be JSON: the two internal,
service-to-service calls that bypass Envoy (see
[gateway.md](gateway.md#what-s-deliberately-not-behind-the-gateway)), and
the `post-created` Kafka event. All client-facing traffic through Envoy
(`/api/ingest`, `/api/feed`) is still plain JSON/HTTP -- that's what the
README's curl examples and the web UI depend on, and there's no reason to
touch it. This document covers replacing the *internal* wire formats with
gRPC/Protobuf and FlatBuffers, and why each call got the format it did --
including one call that deliberately gets both.

## The two internal calls: HTTP/JSON → gRPC/Protobuf

| Call | Schema | Port |
|---|---|---|
| fanout-worker (Java) → feed-aggregation-service (Go), cold-tier append | [`schemas/proto/cold_tier.proto`](../schemas/proto/cold_tier.proto) | `feed-aggregation-service:4102` |
| feed-aggregation-service (Go) → ranking-service (Rust), composite scoring | [`schemas/proto/ranking.proto`](../schemas/proto/ranking.proto) | `ranking-service:4103` |

Both used to be hand-rolled JSON over each language's stdlib HTTP client,
with a real, checkable cost: every 64-bit Snowflake ID crossing the
cold-tier call was stringified (`"userId":"357748213593194496"`) purely
because that's the safe default for JSON, even though neither Go nor Java
has JSON's IEEE-754-precision problem with large integers -- that's a
JavaScript-specific workaround being paid by two languages that didn't
need it. Protobuf's `uint64` fields cross both directions natively; the
Go server and Java client now exchange the ID as what it actually is.
And neither call had a single test exercising the real wire format before
this -- every existing test mocked the domain interface. Both gRPC clients
now have one (`ColdTierGrpcClientTest`, and `ranking-service`'s own
`#[cfg(test)]` module), built the same way `scripts/verify_shard_parity.sh`
treats every other cross-language contract in this repo: prove it, don't
assert it.

Why gRPC specifically, not just "Protobuf over HTTP":
- **A typed contract, enforced by codegen, not convention.**
  `learning-notes.md` already named this gap in the old JSON path: *"No
  schema registry, no codegen -- a hand-written TypeScript interface next
  to the Go producer's matching struct was enough, because both sides
  just agree on JSON field names by convention."* gRPC's generated client
  and server stubs make that agreement a compile-time fact instead of a
  convention four codebases have to independently honor.
- **HTTP/2 underneath**, without hand-rolling that decision -- connection
  multiplexing and header compression, for calls this system will likely
  want to make more frequently (or stream) as it grows, without a
  transport rewrite.
- It's the standard, boring choice for internal RPC at this call volume --
  not a novel one. The interesting decision in this repo is the *next*
  one.

## The `post-created` Kafka event: JSON → FlatBuffers

Schema: [`schemas/fbs/post_created.fbs`](../schemas/fbs/post_created.fbs).
Produced once by post-ingestion-service (Go); deserialized independently
by three consumers in three languages -- fanout-worker (Java),
vector-pipeline (Python), notification-service (Node/TS) -- each with its
own consumer group and its own copy of every message (see
[architecture.md](architecture.md#kafka)). That fan-out is exactly the
shape where FlatBuffers earns its keep over Protobuf: a Protobuf message
still has to be fully parsed into a materialized object graph before any
field is readable, paid once per consumer per message. A FlatBuffer is
read directly out of the received byte buffer via offset tables -- no
parse pass, no allocation, at the cost of a less friendly in-memory
representation than a real object.

Two side effects of the schema, both improvements over the JSON path, not
just a format swap:
- **`created_at` is `uint64` unix-epoch-millis, not an RFC3339 string.**
  FlatBuffers has no datetime type, so this was a forced choice -- but it
  also deletes a string-parse step from every consumer. Java goes from
  `OffsetDateTime.parse(event.createdAt)` to reading a `long` directly;
  Python and Node never parse a timestamp at all now.
- **notification-service's IDs arrive as `bigint`, not strings.** The
  FlatBuffers TypeScript runtime decodes 64-bit fields as native `bigint`.
  This service already mints its own `bigint` Snowflake IDs (see
  [sharding.md](sharding.md)), so this removes a `BigInt(event.userId)`
  string-to-bigint conversion that used to happen on every message,
  instead of adding one.

`vector-pipeline` got its first automated test of any kind as part of
this change (`test/test_post_created_decode.py`) -- it previously had
none, JSON-era or otherwise.

## The ranking call: gRPC *and* FlatBuffers, deliberately layered

The feed-aggregation-service → ranking-service call doesn't pick one wire
format -- it uses both, at different layers, on purpose:

```
feed-aggregation-service                          ranking-service
  CandidateList (FlatBuffers)  --RankRequest.candidates_fb-->  decode, score
                              <--RankResponse.ranked_fb--  RankedList (FlatBuffers)
```

`schemas/proto/ranking.proto` defines the RPC (`Rank`) and its message
envelope, but the envelope's only field is `bytes` --
[`schemas/fbs/ranking.fbs`](../schemas/fbs/ranking.fbs) defines what's
actually inside. gRPC's own Protobuf codec is deliberately not used for
the payload itself.

**Why not just let gRPC use its own Protobuf codec here, like the
cold-tier call does?** Because this is the one call in the system that's
list-heavy and latency-sensitive at the same time -- a feed request's
candidate set (in-network + celebrity + vector recall combined) is
exactly the shape FlatBuffers is built for, and Protobuf's per-message
parse+allocate cost is real: decoding a repeated field of N candidate
messages means N heap allocations before the first field is readable,
every single call, on the hot read path this system's own design doc
targets at ~80k QPS. FlatBuffers reads a `Candidate` straight out of the
received bytes via its offset table -- no parse pass, no per-item
allocation. gRPC is still worth keeping *as the transport* (the typed
`Rank(RankRequest) -> RankResponse` contract, HTTP/2, the option to stream
later) -- it's specifically gRPC's payload codec this call skips.

This also happens to fix a silent bug in the old JSON path: ranking-service
has always computed and returned `pLike`/`pComment`/`pShare`/`pDwell`/
`pHide` alongside each item's score, but the Go client's JSON struct never
declared those fields, so `json.Decode` silently dropped them on the
floor. `ranking.fbs`'s `RankedItem` table carries all five; nothing forces
a Go caller to read them, but they're no longer *unreadable*.

**The real cost, named rather than glossed over:** wrapping opaque bytes
inside a gRPC message means generic Protobuf tooling (`grpcurl`, gRPC
reflection, anything that inspects a message by field) can see that a
`Rank` call happened but not what candidates were in it -- the FlatBuffer
payload is opaque to anything that doesn't also know
`schemas/fbs/ranking.fbs`. For a two-service internal call with an
already-narrow blast radius (see
[gateway.md](gateway.md#what-s-deliberately-not-behind-the-gateway)),
that's a reasonable trade; it would be a real cost at higher internal
call-site counts, where generic RPC tooling earns its keep specifically
by being generic.

## Codegen: one schema, five languages, nothing generated in Docker

There was no `.proto`/`.fbs` file or `protoc`/`flatc` invocation anywhere
in this repo before this change -- this is genuinely new infrastructure,
not a migration of an existing one.

- **`schemas/proto/`** and **`schemas/fbs/`** hold the source of truth,
  shared across every language that needs them.
- **[`scripts/gen_schemas.sh`](../scripts/gen_schemas.sh)** generates Go
  protobuf/gRPC code, and FlatBuffers bindings for every language that
  needs them (Go, Java, Python, TypeScript, Rust) -- and the result is
  **committed**, not generated fresh inside a Dockerfile. That keeps every
  app-service Dockerfile exactly as simple as it was (running the system
  still only needs Docker, per the README); `protoc`/`flatc` join
  Go/Rust/Java/Python/Node as dev-loop-only tools (see
  [`scripts/env.sh`](../scripts/env.sh)).
- **Two exceptions, both idiomatic for their language rather than
  arbitrary:** Java's `cold_tier.proto` gRPC stub is generated by Maven's
  `protobuf-maven-plugin` at `mvn generate-sources` time (it resolves the
  right native `protoc`/`protoc-gen-grpc-java` binaries per platform from
  Maven Central automatically); Rust's `ranking.proto` gRPC stub is
  generated by `tonic-build` in `ranking-service/build.rs` at
  `cargo build` time. Neither is committed -- both regenerate on every
  build of their own service, which is the normal way each ecosystem
  already does codegen.
- **[`scripts/verify_schema_gen.sh`](../scripts/verify_schema_gen.sh)**
  regenerates everything into a scratch directory and diffs it against
  what's committed, failing if they've drifted -- the same "prove it,
  don't assume it" idiom `verify_shard_parity.sh` already established for
  the Go/Java sharding algorithms. Wired into
  [`scripts/build_all.sh`](../scripts/build_all.sh) as a build gate.

One version constraint worth naming, because it cost real time to find:
FlatBuffers' generated Java code calls a version-checking method
(`Constants.FLATBUFFERS_<exact-version>()`) that only exists if the
`flatbuffers-java` runtime jar is the *exact* version `flatc` generated
against -- unlike Go/Rust/Python/TypeScript, which don't enforce this.
`flatc` is pinned to `25.2.10` here specifically because that's the
newest version with a published Maven Central jar; a newer `flatc`
(`25.12.19` was current at the time of writing) generates Java code that
simply won't link against anything on Maven Central yet.

## What this doesn't solve

- **No schema versioning/compatibility story.** Protobuf and FlatBuffers
  both support additive field evolution, but nothing here tests for it --
  there's no contract test that changing a `.proto`/`.fbs` file and
  rolling one service at a time doesn't break the other side mid-deploy.
  At 2 internal call sites and one topic, this is coordinated by hand
  (change the schema, regenerate, redeploy everything together); a real
  multi-team system would want a schema registry with compatibility
  checks enforced in CI, the same gap `learning-notes.md` already named
  for the old JSON path.
- **Plaintext gRPC, no mTLS.** Same boundary [gateway.md](gateway.md)
  already draws for these two call sites generally -- internal traffic on
  the Docker bridge network, not hardened the way a real service mesh
  would.
- **No retry/circuit-breaking policy on either gRPC client.** A transient
  failure on the ranking call currently fails that feed request outright
  (see [trade-offs.md](trade-offs.md) row 10) -- gRPC didn't add this,
  and moving to gRPC didn't fix it either.
