package com.feed.sharding;

/**
 * Not part of the running service -- a small CLI used purely so
 * scripts/verify_shard_parity.sh can compare this Java ring's placement
 * decisions against pkg/sharding's Go ring for the same keys, from
 * outside either language's test suite. Usage:
 * {@code java -cp ... com.feed.sharding.ShardCli <configPath> <key1> <key2> ...}
 * Prints one "{@code key:shardId}" line per key, matching the Go CLI's
 * output format exactly.
 */
public final class ShardCli {
    private ShardCli() {}

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("usage: ShardCli <configPath> <key1> [key2 ...]");
            System.exit(2);
        }
        ShardTopology topology = ShardTopology.load(args[0]);
        ShardRing ring = new ShardRing(topology);

        StringBuilder out = new StringBuilder();
        for (int i = 1; i < args.length; i++) {
            out.append(args[i]).append(':').append(ring.shardForNewEntity(args[i])).append('\n');
        }
        System.out.print(out);
    }
}
