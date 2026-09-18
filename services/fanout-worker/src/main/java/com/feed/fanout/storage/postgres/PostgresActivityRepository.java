package com.feed.fanout.storage.postgres;

import com.feed.fanout.domain.ActivityRepository;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Followers can live on any of the 4 Postgres shards regardless of which
 * shard the author (or Neo4j) put them on, so "filter to active" is a
 * cross-shard scatter-gather: group by shard (bit-shift, no lookup), fan
 * out concurrently, merge. See doc/DESIGN.md. This is the direct Java
 * counterpart of the same pattern in both Go services'
 * PostMetaRepo.RecentByAuthors.
 */
public class PostgresActivityRepository implements ActivityRepository {
    private final ShardedDataSource dataSource;
    private final ExecutorService executor;

    public PostgresActivityRepository(ShardedDataSource dataSource) {
        this.dataSource = dataSource;
        this.executor = Executors.newFixedThreadPool(Math.max(dataSource.numShards(), 1));
    }

    @Override
    public List<Long> filterActive(List<Long> userIds, int activeWithinDays) {
        Map<Integer, List<Long>> byShard = new HashMap<>();
        for (Long id : userIds) {
            int shardId = ShardedDataSource.extractShardId(id);
            if (!dataSource.shardIds().contains(shardId)) {
                // A malformed/forged ID resolves to a shard outside our
                // topology -- drop just this ID rather than failing the
                // whole batch. Same principle as the Go services'
                // PoolForShard error path.
                continue;
            }
            byShard.computeIfAbsent(shardId, k -> new ArrayList<>()).add(id);
        }

        Instant cutoff = Instant.now().minus(activeWithinDays, ChronoUnit.DAYS);
        List<Future<List<Long>>> futures = new ArrayList<>();
        for (Map.Entry<Integer, List<Long>> entry : byShard.entrySet()) {
            int shardId = entry.getKey();
            List<Long> ids = entry.getValue();
            futures.add(executor.submit(() -> queryActive(shardId, ids, cutoff)));
        }

        List<Long> active = new ArrayList<>();
        for (Future<List<Long>> future : futures) {
            try {
                active.addAll(future.get(10, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("activity check interrupted", e);
            } catch (ExecutionException | TimeoutException e) {
                throw new RuntimeException("activity check failed for one shard", e);
            }
        }
        return active;
    }

    private List<Long> queryActive(int shardId, List<Long> userIds, Instant cutoff) throws SQLException {
        String placeholders = String.join(",", userIds.stream().map(id -> "?").toList());
        String sql = "SELECT user_id FROM users WHERE user_id IN (" + placeholders + ") AND last_active_at >= ?";

        try (Connection conn = dataSource.connect(shardId);
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            int i = 1;
            for (Long id : userIds) {
                stmt.setLong(i++, id);
            }
            stmt.setTimestamp(i, Timestamp.from(cutoff));

            List<Long> active = new ArrayList<>();
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    active.add(rs.getLong("user_id"));
                }
            }
            return active;
        }
    }
}
