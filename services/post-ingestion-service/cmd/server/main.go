// Command server is the composition root: it loads config, constructs
// every concrete dependency (Postgres shards, Neo4j, Redis, Kafka,
// MinIO), wires them into services, wires services into HTTP handlers,
// and owns process lifecycle (listen, graceful shutdown on SIGINT/
// SIGTERM). Nothing below cmd/ imports anything from cmd/ -- dependencies
// point inward (transport -> service -> domain), and this file is the
// only place that wires the outward-pointing concrete implementations
// back in.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"sharding"

	"post-ingestion-service/internal/service"
	"post-ingestion-service/internal/storage/kafka"
	"post-ingestion-service/internal/storage/minio"
	"post-ingestion-service/internal/storage/moderation"
	"post-ingestion-service/internal/storage/neo4j"
	"post-ingestion-service/internal/storage/postgres"
	redisstore "post-ingestion-service/internal/storage/redis"
	transporthttp "post-ingestion-service/internal/transport/http"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg := loadConfig()
	ctx := context.Background()

	shardCfg, err := sharding.LoadConfig(cfg.ShardConfigPath)
	if err != nil {
		logger.Error("failed to load shard config", "error", err, "path", cfg.ShardConfigPath)
		os.Exit(1)
	}

	shardedPool, err := postgres.NewShardedPool(ctx, shardCfg)
	if err != nil {
		logger.Error("failed to connect to Postgres shards", "error", err)
		os.Exit(1)
	}
	defer shardedPool.Close()
	logger.Info("connected to all Postgres shards", "numShards", shardedPool.NumShards())

	graphRepo, err := neo4j.NewGraphRepo(cfg.Neo4jURI, cfg.Neo4jUsername, cfg.Neo4jPassword)
	if err != nil {
		logger.Error("failed to create Neo4j driver", "error", err)
		os.Exit(1)
	}
	defer graphRepo.Close(ctx)
	if err := graphRepo.VerifyConnectivity(ctx); err != nil {
		logger.Error("failed to connect to Neo4j", "error", err)
		os.Exit(1)
	}
	logger.Info("connected to Neo4j graph database")

	redisClient := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	if err := redisClient.Ping(ctx).Err(); err != nil {
		logger.Error("failed to connect to Redis", "error", err)
		os.Exit(1)
	}
	defer redisClient.Close()

	publisher := kafka.NewPublisher(cfg.KafkaBrokers)
	defer publisher.Close()

	blobStore, err := minio.NewBlobStore(cfg.MinioEndpoint, cfg.MinioAccessKey, cfg.MinioSecretKey, cfg.MinioBucket, cfg.MinioUseSSL)
	if err != nil {
		logger.Error("failed to create MinIO client", "error", err)
		os.Exit(1)
	}
	if err := blobStore.EnsureBucket(ctx); err != nil {
		logger.Warn("could not ensure MinIO bucket exists", "error", err)
	}

	// --- Repositories ---
	userRepo := postgres.NewUserRepo(shardedPool)
	postRepo := postgres.NewPostRepo(shardedPool)
	likeRepo := postgres.NewLikeRepo(shardedPool)
	commentRepo := postgres.NewCommentRepo(shardedPool)
	counterRepo := redisstore.NewCounterRepo(redisClient)
	usernameDirectory := redisstore.NewUsernameDirectory(redisClient)
	likeStateRepo := redisstore.NewLikeStateRepo(redisClient)
	// 20 like/unlike toggles per (user, post) per minute -- generous
	// enough for a genuine double-tap-to-correct-a-mistake, tight enough
	// to stop bot-driven flapping. See doc/DESIGN.md.
	likeRateLimiter := redisstore.NewRateLimiter(redisClient, 20, time.Minute)
	commentModerator := moderation.NewBlocklistModerator(moderation.DefaultBlocklist)

	// --- Services ---
	userService := service.NewUserService(userRepo, graphRepo, usernameDirectory, shardedPool)
	followService := service.NewFollowService(graphRepo)
	postService := service.NewPostService(postRepo, publisher, shardedPool)
	engagementService := service.NewEngagementService(
		likeRepo, commentRepo, userRepo, counterRepo, likeStateRepo, likeRateLimiter, commentModerator, shardedPool,
	)

	// --- HTTP transport ---
	handlers := transporthttp.Handlers{
		User:       transporthttp.NewUserHandler(userService, followService),
		Post:       transporthttp.NewPostHandler(postService),
		Engagement: transporthttp.NewEngagementHandler(engagementService),
		Upload:     transporthttp.NewUploadHandler(blobStore),
	}
	router := transporthttp.NewRouter(handlers)

	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      router,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		logger.Info("post-ingestion-service listening", "port", cfg.Port)
		serverErr <- srv.ListenAndServe()
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-serverErr:
		if err != nil && err != http.ErrServerClosed {
			logger.Error("server failed", "error", err)
			os.Exit(1)
		}
	case sig := <-stop:
		logger.Info("received shutdown signal, draining in-flight requests", "signal", sig.String())
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Error("graceful shutdown failed", "error", err)
		}
	}
}
