// Package sharding is the single source of truth for how entities are
// routed across physical Postgres shards. See /doc/sharding.md for the
// full design rationale -- this package is deliberately small: a config
// loader, a consistent-hash ring used only for new-entity placement, and
// a self-routing Snowflake-style ID generator. Everything else (which
// tables live where, cross-shard fan-out, the follows dual-write) is
// application logic in each service, not hidden in here.
package sharding

import (
	"encoding/json"
	"fmt"
	"os"
)

type ShardConfig struct {
	ID          int    `json:"id"`
	PostgresURL string `json:"postgresUrl"`
}

type Config struct {
	Shards               []ShardConfig `json:"shards"`
	VirtualNodesPerShard int           `json:"virtualNodesPerShard"`
}

// LoadConfig reads the shard topology from a JSON file (see
// config/shards.json at the repo root). Every service that touches
// sharded data loads the same file, so the topology has exactly one
// source of truth.
func LoadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("sharding: read config %q: %w", path, err)
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("sharding: parse config %q: %w", path, err)
	}
	if len(cfg.Shards) == 0 {
		return Config{}, fmt.Errorf("sharding: config %q defines no shards", path)
	}
	if cfg.VirtualNodesPerShard <= 0 {
		cfg.VirtualNodesPerShard = 150
	}
	return cfg, nil
}
