module feed-aggregation-service

go 1.25.0

require (
	github.com/dgraph-io/badger/v4 v4.9.6
	github.com/google/flatbuffers v25.2.10+incompatible
	github.com/jackc/pgx/v5 v5.11.0
	github.com/neo4j/neo4j-go-driver/v5 v5.28.4
	github.com/redis/go-redis/v9 v9.22.0
	google.golang.org/grpc v1.83.2
	google.golang.org/protobuf v1.36.11
	sharding v0.0.0-00010101000000-000000000000
)

// Resolved via go.work's `use` directive for local/host development;
// this replace is the fallback that makes a standalone build -- e.g. the
// Dockerfile, which copies only this module and pkg/sharding, not the
// whole workspace -- resolvable with GOWORK=off.
replace sharding => ../../pkg/sharding

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgraph-io/ristretto/v2 v2.2.0 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel v1.44.0 // indirect
	go.opentelemetry.io/otel/metric v1.44.0 // indirect
	go.opentelemetry.io/otel/trace v1.44.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260526163538-3dc84a4a5aaa // indirect
)
