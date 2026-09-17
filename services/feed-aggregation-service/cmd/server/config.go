package main

import (
	"os"
	"strconv"
)

type Config struct {
	Port                    string
	GRPCPort                string
	ShardConfigPath         string
	RedisAddr               string
	Neo4jURI                string
	Neo4jUsername           string
	Neo4jPassword           string
	QdrantURL               string
	RankingServiceGRPCAddr  string
	ColdTierDataDir         string
	InNetworkCandidateLimit int
	CelebrityCandidateLimit int
	VectorCandidateLimit    int
	SeenStateTTLSeconds     int
	PageSize                int
	MaxPostsPerAuthor       int
	TasteSeedPostLimit      int
	CursorSecret            string
}

func loadConfig() Config {
	return Config{
		Port:                    getEnv("FEED_PORT", "4002"),
		GRPCPort:                getEnv("FEED_AGGREGATION_GRPC_PORT", "4102"),
		ShardConfigPath:         getEnv("SHARD_CONFIG_PATH", "../../config/shards.json"),
		RedisAddr:               getEnv("REDIS_ADDR", "localhost:6379"),
		Neo4jURI:                getEnv("NEO4J_URI", "bolt://localhost:7687"),
		Neo4jUsername:           getEnv("NEO4J_USERNAME", "neo4j"),
		Neo4jPassword:           getEnv("NEO4J_PASSWORD", "feedpassword"),
		QdrantURL:               getEnv("QDRANT_URL", "http://localhost:6333"),
		RankingServiceGRPCAddr:  getEnv("RANKING_SERVICE_GRPC_ADDR", "localhost:4103"),
		ColdTierDataDir:         getEnv("COLD_TIER_DATA_DIR", "./data/cold-tier"),
		InNetworkCandidateLimit: getEnvInt("IN_NETWORK_CANDIDATE_LIMIT", 500),
		CelebrityCandidateLimit: getEnvInt("CELEBRITY_CANDIDATE_LIMIT", 50),
		VectorCandidateLimit:    getEnvInt("VECTOR_CANDIDATE_LIMIT", 250),
		SeenStateTTLSeconds:     getEnvInt("SEEN_STATE_TTL_SECONDS", 172800),
		PageSize:                getEnvInt("FEED_PAGE_SIZE", 20),
		MaxPostsPerAuthor:       getEnvInt("MAX_POSTS_PER_AUTHOR", 2),
		TasteSeedPostLimit:      getEnvInt("TASTE_SEED_POST_LIMIT", 5),
		CursorSecret:            getEnv("FEED_CURSOR_SECRET", "change-me-32-byte-local-dev-secret"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}
