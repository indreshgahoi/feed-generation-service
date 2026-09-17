# Build context is the REPO ROOT, not this directory -- build.rs compiles
# ../../schemas/proto/ranking.proto (relative to this crate) at build
# time, so schemas/ has to be a sibling of services/ inside the image too.
# Mirrors the same pattern the two Go services already use for pkg/sharding.
# Build with: docker build -f services/ranking-service/Dockerfile .

FROM rust:1.98-bookworm AS builder
# tonic-build/prost-build need a real protoc binary at `cargo build` time
# (see build.rs) -- unlike Go's protoc-gen-go (a pure-Go plugin fed by the
# host's protoc, run outside Docker per scripts/gen_schemas.sh), Rust's
# codegen runs inside this build stage itself.
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src

COPY schemas ./schemas
COPY services/ranking-service ./services/ranking-service

WORKDIR /src/services/ranking-service
RUN cargo build --release

# axum/tokio/tonic have no native deps beyond glibc, so a slim runtime
# (not distroless) keeps this simple without needing a musl target.
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates wget \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /src/services/ranking-service/target/release/ranking-service /app/ranking-service
WORKDIR /app
EXPOSE 4003
EXPOSE 4103
ENTRYPOINT ["/app/ranking-service"]
