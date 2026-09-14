package com.feed.fanout.storage.postgres;

import com.feed.sharding.ShardConfig;
import com.feed.sharding.ShardTopology;
import com.feed.sharding.SnowflakeIdGenerator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * One JDBC connection per physical shard, opened on demand -- the
 * Java-side equivalent of the Go services' ShardedPool. fanout-worker's
 * query volume (one activity check per post, not per request) doesn't
 * justify pulling in a pooling library; each call opens and closes its
 * own short-lived connection.
 */
public class ShardedDataSource {
    private record ShardConnectionInfo(String jdbcUrl, String username, String password) {}

    private final Map<Integer, ShardConnectionInfo> shards = new HashMap<>();

    public ShardedDataSource(ShardTopology topology) {
        for (ShardConfig shard : topology.shards) {
            String[] creds = extractCredentials(shard.postgresUrl);
            String jdbcUrl = "jdbc:" + shard.postgresUrl.replaceFirst("postgresql://[^:]+:[^@]+@", "postgresql://");
            shards.put(shard.id, new ShardConnectionInfo(jdbcUrl, creds[0], creds[1]));
        }
    }

    private static String[] extractCredentials(String postgresUrl) {
        String withoutScheme = postgresUrl.substring(postgresUrl.indexOf("://") + 3);
        String userInfo = withoutScheme.substring(0, withoutScheme.indexOf('@'));
        String[] parts = userInfo.split(":", 2);
        return new String[]{parts[0], parts.length > 1 ? parts[1] : ""};
    }

    public Connection connect(int shardId) throws SQLException {
        ShardConnectionInfo info = shards.get(shardId);
        if (info == null) {
            throw new SQLException("no shard configured with id " + shardId + " (have " + shards.size() + " shards)");
        }
        return DriverManager.getConnection(info.jdbcUrl(), info.username(), info.password());
    }

    public Set<Integer> shardIds() {
        return shards.keySet();
    }

    public int numShards() {
        return shards.size();
    }

    /** Recovers the shard that minted id -- a bit-shift, never a lookup. */
    public static int extractShardId(long id) {
        return SnowflakeIdGenerator.extractShardId(id);
    }
}
