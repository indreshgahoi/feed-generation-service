package com.feed.sharding;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** One physical shard entry from config/shards.json. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ShardConfig {
    public int id;
    public String postgresUrl;
}
