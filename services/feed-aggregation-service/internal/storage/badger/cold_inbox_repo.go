// Package badger implements domain.ColdInboxRepository using BadgerDB, a
// pure-Go embedded LSM-tree store standing in for the design doc's
// RocksDB -- see doc/caching.md for why (CGO/native-toolchain avoidance,
// the same trade-off CockroachDB made building Pebble).
package badger

import (
	"context"
	"encoding/json"
	"errors"

	badgerdb "github.com/dgraph-io/badger/v4"

	"feed-aggregation-service/internal/domain"
)

type ColdInboxRepo struct {
	db *badgerdb.DB
}

// Open opens (or creates) the embedded LSM-tree store at dataDir. This is
// a single local instance with no replication -- see doc/caching.md
// "What's genuinely different from real RocksDB-on-NVMe" for the
// consequences of that.
func Open(dataDir string) (*ColdInboxRepo, error) {
	opts := badgerdb.DefaultOptions(dataDir).WithLoggingLevel(badgerdb.WARNING)
	db, err := badgerdb.Open(opts)
	if err != nil {
		return nil, err
	}
	return &ColdInboxRepo{db: db}, nil
}

func (r *ColdInboxRepo) Close() error {
	return r.db.Close()
}

func coldKey(userID string) []byte {
	return []byte("cold:inbox:" + userID)
}

func (r *ColdInboxRepo) Get(_ context.Context, userID string) ([]domain.Candidate, error) {
	var candidates []domain.Candidate
	err := r.db.View(func(txn *badgerdb.Txn) error {
		item, err := txn.Get(coldKey(userID))
		if errors.Is(err, badgerdb.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &candidates)
		})
	})
	return candidates, err
}

// Append is a read-modify-write, not an atomic list append -- BadgerDB
// has no native list type, and this cold tier's write volume (fan-out to
// DORMANT followers only) is low enough that the race window (two
// concurrent fan-outs to the same dormant user's cold tier) is an
// accepted, documented gap rather than something worth a more complex
// CRDT-style merge for. See doc/caching.md.
func (r *ColdInboxRepo) Append(_ context.Context, userID string, candidate domain.Candidate) error {
	return r.db.Update(func(txn *badgerdb.Txn) error {
		var existing []domain.Candidate
		item, err := txn.Get(coldKey(userID))
		if err == nil {
			_ = item.Value(func(val []byte) error {
				return json.Unmarshal(val, &existing)
			})
		} else if !errors.Is(err, badgerdb.ErrKeyNotFound) {
			return err
		}

		existing = append(existing, candidate)
		const maxColdTierItems = 800 // same cap as the hot tier
		if len(existing) > maxColdTierItems {
			existing = existing[len(existing)-maxColdTierItems:]
		}

		encoded, err := json.Marshal(existing)
		if err != nil {
			return err
		}
		return txn.Set(coldKey(userID), encoded)
	})
}
