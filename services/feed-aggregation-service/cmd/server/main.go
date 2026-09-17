// Command server is feed-aggregation-service's composition root -- see
// post-ingestion-service/cmd/server/main.go for the same convention.
package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc"

	"sharding"

	coldtierpb "feed-aggregation-service/internal/genproto/coldtier"
	"feed-aggregation-service/internal/service"
	"feed-aggregation-service/internal/storage/badger"
	"feed-aggregation-service/internal/storage/neo4j"
	"feed-aggregation-service/internal/storage/postgres"
	"feed-aggregation-service/internal/storage/qdrant"
	"feed-aggregation-service/internal/storage/rankingclient"
	redisstore "feed-aggregation-service/internal/storage/redis"
	transportgrpc "feed-aggregation-service/internal/transport/grpc"
	transporthttp "feed-aggregation-service/internal/transport/http"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg := loadConfig()
	ctx := context.Background()

	shardCfg, err := sharding.LoadConfig(cfg.ShardConfigPath)
	if err != nil {
		logger.Error("failed to load shard config", "error", err)
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

	redisClient := goredis.NewClient(&goredis.Options{Addr: cfg.RedisAddr})
	if err := redisClient.Ping(ctx).Err(); err != nil {
		logger.Error("failed to connect to Redis", "error", err)
		os.Exit(1)
	}
	defer redisClient.Close()

	coldInbox, err := badger.Open(cfg.ColdTierDataDir)
	if err != nil {
		logger.Error("failed to open cold-tier store", "error", err, "dataDir", cfg.ColdTierDataDir)
		os.Exit(1)
	}
	defer coldInbox.Close()
	logger.Info("opened BadgerDB cold-tier store", "dataDir", cfg.ColdTierDataDir)

	vectorRepo := qdrant.NewVectorRepo(cfg.QdrantURL)
	rankingClient, err := rankingclient.New(cfg.RankingServiceGRPCAddr)
	if err != nil {
		logger.Error("failed to create ranking-service gRPC client", "error", err)
		os.Exit(1)
	}
	defer rankingClient.Close()

	// --- Repositories ---
	hotInbox := redisstore.NewHotInboxRepo(redisClient)
	seenState := redisstore.NewSeenStateRepo(redisClient)
	counters := redisstore.NewCounterRepo(redisClient)
	postMeta := postgres.NewPostMetaRepo(shardedPool)
	liked := redisstore.NewLikedRepo(redisClient)

	feedService := service.NewFeedService(
		hotInbox, coldInbox, seenState, graphRepo, postMeta, counters, liked, vectorRepo, rankingClient,
		service.NewCursorCodec(cfg.CursorSecret),
		service.FeedConfig{
			InNetworkCandidateLimit: cfg.InNetworkCandidateLimit,
			CelebrityCandidateLimit: cfg.CelebrityCandidateLimit,
			VectorCandidateLimit:    cfg.VectorCandidateLimit,
			SeenStateTTLSeconds:     cfg.SeenStateTTLSeconds,
			PageSize:                cfg.PageSize,
			MaxPostsPerAuthor:       cfg.MaxPostsPerAuthor,
			TasteSeedPostLimit:      cfg.TasteSeedPostLimit,
		},
	)

	handlers := transporthttp.Handlers{
		Feed: transporthttp.NewFeedHandler(feedService),
	}
	router := transporthttp.NewRouter(handlers)

	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      router,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	// gRPC server for the cold-tier append call -- internal,
	// service-to-service only (called by fanout-worker), never routed
	// through the Envoy gateway. See doc/gateway.md and doc/wire-protocols.md.
	grpcServer := grpc.NewServer()
	coldtierpb.RegisterColdTierServiceServer(grpcServer, transportgrpc.NewColdTierServer(coldInbox))
	grpcListener, err := net.Listen("tcp", ":"+cfg.GRPCPort)
	if err != nil {
		logger.Error("failed to open gRPC listener", "error", err, "port", cfg.GRPCPort)
		os.Exit(1)
	}

	serverErr := make(chan error, 1)
	go func() {
		logger.Info("feed-aggregation-service HTTP listening", "port", cfg.Port)
		serverErr <- srv.ListenAndServe()
	}()
	go func() {
		logger.Info("feed-aggregation-service gRPC listening", "port", cfg.GRPCPort)
		serverErr <- grpcServer.Serve(grpcListener)
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-serverErr:
		if err != nil && err != http.ErrServerClosed && err != grpc.ErrServerStopped {
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
		grpcServer.GracefulStop()
	}
}
