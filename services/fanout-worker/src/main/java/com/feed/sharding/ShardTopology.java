package com.feed.sharding;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Loads the shard topology from the same config/shards.json file the Go
 * services read -- one source of truth for shard membership across
 * languages. See /doc/sharding.md.
 */
public class ShardTopology {
    public List<ShardConfig> shards;
    public int virtualNodesPerShard = 150;

    public static ShardTopology load(String path) throws IOException {
        File file = new File(path);
        if (!file.exists()) {
            throw new IOException("sharding: config file not found: " + path);
        }
        ObjectMapper mapper = new ObjectMapper();
        ShardTopology topology = mapper.readValue(file, ShardTopology.class);
        if (topology.shards == null || topology.shards.isEmpty()) {
            throw new IOException("sharding: config " + path + " defines no shards");
        }
        if (topology.virtualNodesPerShard <= 0) {
            topology.virtualNodesPerShard = 150;
        }
        return topology;
    }
}
