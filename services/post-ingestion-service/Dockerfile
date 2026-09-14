# Build context is the REPO ROOT, not this directory -- this service
# imports the local pkg/sharding module. Host dev resolves that import
# via go.work; a container build only copies this module + pkg/sharding
# (not the whole workspace), so it builds with GOWORK=off instead,
# falling back to the `replace sharding => ../../pkg/sharding` directive
# in this module's own go.mod.
# Build with: docker build -f services/post-ingestion-service/Dockerfile .

FROM golang:1.26-bookworm AS builder
WORKDIR /src

COPY pkg/sharding ./pkg/sharding
COPY services/post-ingestion-service ./services/post-ingestion-service

WORKDIR /src/services/post-ingestion-service
RUN CGO_ENABLED=0 GOWORK=off go build -o /out/post-ingestion-service ./cmd/server

# No CGO in this service's dependency tree (pgx/v5, go-redis, the Neo4j
# and Kafka drivers, minio-go are all pure Go), so a distroless static
# base works -- no libc, no shell, smallest possible attack surface.
FROM gcr.io/distroless/static-debian12
COPY --from=builder /out/post-ingestion-service /app/post-ingestion-service
WORKDIR /app
# config/shards.docker.json is mounted at runtime (docker-compose.yml),
# not baked in here -- it's shared by 4 services across 3 languages with
# different build contexts (Go builds from repo root, Java/Node build
# from their own service dir), so a runtime mount is the one approach
# that works identically for all of them.
EXPOSE 4001
ENTRYPOINT ["/app/post-ingestion-service"]
