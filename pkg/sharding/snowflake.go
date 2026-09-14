package sharding

import (
	"sync"
	"time"
)

const epochMillis int64 = 1704067200000 // 2024-01-01T00:00:00Z

// 64-bit layout: 41 bits timestamp | 8 bits shard ID | 14 bits sequence.
// Modeled on Instagram's own published sharded-ID scheme, for the same
// reason they use it: the shard that minted an ID is recoverable forever
// with a bit-shift, no directory lookup, and it can never go stale. See
// doc/sharding.md.
const (
	sequenceBits   = 14
	shardIDBits    = 8
	shardIDShift   = sequenceBits
	timestampShift = sequenceBits + shardIDBits
	maxSequence    = (1 << sequenceBits) - 1

	// MaxShardID is the largest shard ID an 8-bit shard field can hold --
	// the hard ceiling on shard count this ID scheme supports without
	// changing the bit layout (see doc/sharding.md).
	MaxShardID = (1 << shardIDBits) - 1
)

// IDGenerator mints k-sortable, self-routing IDs for one specific shard.
// Each shard's writers run their own generator instance seeded with that
// shard's ID -- there is no coordination between shards because the
// shard ID is baked into every ID this generator produces.
type IDGenerator struct {
	mu            sync.Mutex
	shardID       int
	sequence      int64
	lastTimestamp int64
}

func NewIDGenerator(shardID int) *IDGenerator {
	if shardID < 0 || shardID > MaxShardID {
		panic("sharding: shardID out of range for an 8-bit ID field")
	}
	return &IDGenerator{shardID: shardID, lastTimestamp: -1}
}

func (g *IDGenerator) Next() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()

	ts := time.Now().UnixMilli() - epochMillis
	if ts == g.lastTimestamp {
		g.sequence = (g.sequence + 1) & maxSequence
		if g.sequence == 0 {
			for ts <= g.lastTimestamp {
				ts = time.Now().UnixMilli() - epochMillis
			}
		}
	} else {
		g.sequence = 0
	}
	g.lastTimestamp = ts

	return (ts << timestampShift) | (int64(g.shardID) << shardIDShift) | g.sequence
}

// ExtractShardID recovers the shard that minted id -- a pure bit-shift,
// never a network call or a lookup table.
func ExtractShardID(id int64) int {
	return int(id>>shardIDShift) & MaxShardID
}
