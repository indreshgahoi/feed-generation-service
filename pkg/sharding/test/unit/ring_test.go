package sharding_test

import (
	"fmt"
	"math"
	"testing"

	"sharding"
)

func testConfig(numShards int) sharding.Config {
	cfg := sharding.Config{VirtualNodesPerShard: 150}
	for i := 0; i < numShards; i++ {
		cfg.Shards = append(cfg.Shards, sharding.ShardConfig{ID: i})
	}
	return cfg
}

func TestShardForNewEntity_Deterministic(t *testing.T) {
	ring := sharding.NewRing(testConfig(4))
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("user-%d", i)
		first := ring.ShardForNewEntity(key)
		second := ring.ShardForNewEntity(key)
		if first != second {
			t.Fatalf("ShardForNewEntity(%q) not deterministic: %d then %d", key, first, second)
		}
		if first < 0 || first >= 4 {
			t.Fatalf("ShardForNewEntity(%q) returned out-of-range shard %d", key, first)
		}
	}
}

func TestShardForNewEntity_RoughlyEvenDistribution(t *testing.T) {
	ring := sharding.NewRing(testConfig(4))
	counts := make(map[int]int)
	const n = 20000
	for i := 0; i < n; i++ {
		shard := ring.ShardForNewEntity(fmt.Sprintf("placement-key-%d", i))
		counts[shard]++
	}

	expected := float64(n) / 4
	for shard, count := range counts {
		deviation := math.Abs(float64(count)-expected) / expected
		if deviation > 0.15 {
			t.Errorf("shard %d got %d placements (%.1f%% of %d expected), deviation %.1f%% exceeds 15%% tolerance",
				shard, count, 100*float64(count)/n, int(expected), deviation*100)
		}
	}
}

// TestConsistentHashing_MinimalDisruptionOnResize is the actual point of
// using a ring instead of `hash(key) % N`: growing the shard count should
// only remap a small fraction of placement decisions, not most of them.
func TestConsistentHashing_MinimalDisruptionOnResize(t *testing.T) {
	const n = 5000
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("user-%d", i)
	}

	before := sharding.NewRing(testConfig(4))
	after := sharding.NewRing(testConfig(5)) // simulate adding a 5th shard

	moved := 0
	for _, key := range keys {
		if before.ShardForNewEntity(key) != after.ShardForNewEntity(key) {
			moved++
		}
	}

	movedRatio := float64(moved) / n
	// Naive `hash % N` would remap ~(1 - 4/5) = 80% of keys going from 4->5
	// shards. Consistent hashing should remap roughly 1/5 (~20%) or less.
	if movedRatio > 0.30 {
		t.Errorf("resizing 4->5 shards remapped %.1f%% of placements; expected well under the ~80%% a naive modulo scheme would cause", movedRatio*100)
	}
	t.Logf("resizing 4->5 shards remapped %.1f%% of new-entity placements", movedRatio*100)
}

func TestIDGenerator_ExtractShardID_RoundTrips(t *testing.T) {
	for shardID := 0; shardID <= sharding.MaxShardID; shardID += 17 {
		gen := sharding.NewIDGenerator(shardID)
		id := gen.Next()
		if got := sharding.ExtractShardID(id); got != shardID {
			t.Errorf("ExtractShardID(%d) = %d, want %d", id, got, shardID)
		}
	}
}

func TestIDGenerator_MonotonicWithinShard(t *testing.T) {
	gen := sharding.NewIDGenerator(2)
	var last int64
	for i := 0; i < 10000; i++ {
		id := gen.Next()
		if id <= last {
			t.Fatalf("IDGenerator produced non-increasing ID: %d after %d", id, last)
		}
		last = id
	}
}

func TestIDGenerator_RejectsOutOfRangeShardID(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected NewIDGenerator to panic for shardID > 255")
		}
	}()
	sharding.NewIDGenerator(256)
}

func TestLoadConfig_MissingFile(t *testing.T) {
	if _, err := sharding.LoadConfig("/nonexistent/shards.json"); err == nil {
		t.Fatal("expected error loading nonexistent config file")
	}
}
