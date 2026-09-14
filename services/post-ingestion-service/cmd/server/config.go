package main

import (
	"os"
	"strings"
)

type Config struct {
	Port            string
	ShardConfigPath string
	RedisAddr       string
	Neo4jURI        string
	Neo4jUsername   string
	Neo4jPassword   string
	KafkaBrokers    []string
	MinioEndpoint   string
	MinioAccessKey  string
	MinioSecretKey  string
	MinioBucket     string
	MinioUseSSL     bool
}

func loadConfig() Config {
	return Config{
		Port:            getEnv("INGESTION_PORT", "4001"),
		ShardConfigPath: getEnv("SHARD_CONFIG_PATH", "../../config/shards.json"),
		RedisAddr:       getEnv("REDIS_ADDR", "localhost:6379"),
		Neo4jURI:        getEnv("NEO4J_URI", "bolt://localhost:7687"),
		Neo4jUsername:   getEnv("NEO4J_USERNAME", "neo4j"),
		Neo4jPassword:   getEnv("NEO4J_PASSWORD", "feedpassword"),
		KafkaBrokers:    strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ","),
		MinioEndpoint:   getEnv("MINIO_ENDPOINT", "localhost") + ":" + getEnv("MINIO_PORT", "9000"),
		MinioAccessKey:  getEnv("MINIO_ACCESS_KEY", "minioadmin"),
		MinioSecretKey:  getEnv("MINIO_SECRET_KEY", "minioadminpassword"),
		MinioBucket:     getEnv("MINIO_BUCKET", "ig-media"),
		MinioUseSSL:     getEnv("MINIO_USE_SSL", "false") == "true",
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
