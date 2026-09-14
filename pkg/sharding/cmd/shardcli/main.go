// Command shardcli is not part of any running service -- it exists purely
// so scripts/verify_shard_parity.sh can compare this Go ring's placement
// decisions against fanout-worker's Java ShardCli for the same keys, from
// outside either language's test suite.
package main

import (
	"fmt"
	"os"

	"sharding"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: shardcli <configPath> <key1> [key2 ...]")
		os.Exit(2)
	}

	cfg, err := sharding.LoadConfig(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	ring := sharding.NewRing(cfg)

	for _, key := range os.Args[2:] {
		fmt.Printf("%s:%d\n", key, ring.ShardForNewEntity(key))
	}
}
