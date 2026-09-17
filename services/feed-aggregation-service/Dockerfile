# Build context is the REPO ROOT (see post-ingestion-service/Dockerfile
# for why -- same GOWORK=off + go.mod replace fallback pattern here).
# Build with: docker build -f services/feed-aggregation-service/Dockerfile .

FROM golang:1.26-bookworm AS builder
WORKDIR /src

COPY pkg/sharding ./pkg/sharding
COPY services/feed-aggregation-service ./services/feed-aggregation-service

WORKDIR /src/services/feed-aggregation-service
RUN CGO_ENABLED=0 GOWORK=off go build -o /out/feed-aggregation-service ./cmd/server

# badger/v4, pgx/v5, go-redis, and the Neo4j/Qdrant clients are all pure
# Go -- no CGO, so distroless/static is enough (no libc, no shell).
FROM gcr.io/distroless/static-debian12
COPY --from=builder /out/feed-aggregation-service /app/feed-aggregation-service
WORKDIR /app
# config/shards.docker.json is mounted at runtime (docker-compose.yml),
# not baked in here -- see post-ingestion-service/Dockerfile for why.
# BadgerDB cold-tier data -- see doc/caching.md. A named volume keeps this
# across container restarts; it's still explicitly a rebuildable cache,
# not a system of record, so losing it is a degradation, not data loss.
VOLUME ["/app/data/cold-tier"]
EXPOSE 4002
EXPOSE 4102
ENTRYPOINT ["/app/feed-aggregation-service"]
