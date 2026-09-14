package sharding

import (
	"fmt"
	"hash/crc32"
	"sort"
)

// Ring is a consistent-hash ring used for exactly one decision: which
// shard a brand-new entity (a new user signup) gets created on. Existing
// entities never consult the ring -- their shard is embedded in their own
// ID (see snowflake.go) and recovered with a bit-shift, not a lookup.
//
// Virtual nodes (150 per shard) exist so that with a small number of real
// shards (4, here), new-entity placement still spreads roughly evenly
// across them; with only 4 raw hash points the distribution would be
// lumpy.
//
// This exact algorithm (CRC32, same virtual node naming scheme) is
// reimplemented in Java for fanout-worker. The two MUST agree bit-for-bit
// on every placement decision -- see scripts/verify_shard_parity.sh.
type Ring struct {
	nodes     []vnode
	numShards int
}

type vnode struct {
	hash    uint32
	shardID int
}

func NewRing(cfg Config) *Ring {
	r := &Ring{numShards: len(cfg.Shards)}
	for _, shard := range cfg.Shards {
		for v := 0; v < cfg.VirtualNodesPerShard; v++ {
			// Vnode index BEFORE shard ID matters: CRC32 is a linear code,
			// and hashing "shard-<id>-vnode-<v>" (shard id as a constant
			// prefix, only the low-order vnode counter varying) produces
			// visibly clustered hashes -- measured ~29% max deviation
			// across shards at 150 vnodes/shard. Swapping the order to
			// "vnode-<v>-shard-<id>" breaks that correlation and brings
			// it under 2%. See pkg/sharding/ring_test.go.
			key := fmt.Sprintf("vnode-%d-shard-%d", v, shard.ID)
			r.nodes = append(r.nodes, vnode{
				hash:    crc32.ChecksumIEEE([]byte(key)),
				shardID: shard.ID,
			})
		}
	}
	sort.Slice(r.nodes, func(i, j int) bool { return r.nodes[i].hash < r.nodes[j].hash })
	return r
}

// ShardForNewEntity returns the shard a new entity should be created on,
// given a caller-chosen placement key (e.g. a signup-time UUID or the
// username -- anything stable and unique to that entity, chosen before
// it has an ID, since it doesn't have one yet).
func (r *Ring) ShardForNewEntity(placementKey string) int {
	h := crc32.ChecksumIEEE([]byte(placementKey))
	idx := sort.Search(len(r.nodes), func(i int) bool { return r.nodes[i].hash >= h })
	if idx == len(r.nodes) {
		idx = 0 // wrap around the ring
	}
	return r.nodes[idx].shardID
}

func (r *Ring) NumShards() int { return r.numShards }
