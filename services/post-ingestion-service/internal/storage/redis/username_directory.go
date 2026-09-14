package redis

import (
	"context"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// UsernameDirectory solves the classic global-secondary-index problem
// sharding creates: username isn't the shard key, so there's no
// bit-shift shortcut to find a user by username. See doc/sharding.md
// "The username problem (a global secondary index)".
type UsernameDirectory struct {
	client *redis.Client
}

func NewUsernameDirectory(client *redis.Client) *UsernameDirectory {
	return &UsernameDirectory{client: client}
}

func directoryKey(username string) string { return "username:" + username }

func (d *UsernameDirectory) Set(ctx context.Context, username string, userID int64) error {
	return d.client.Set(ctx, directoryKey(username), userID, 0).Err()
}

func (d *UsernameDirectory) Lookup(ctx context.Context, username string) (int64, bool, error) {
	val, err := d.client.Get(ctx, directoryKey(username)).Result()
	if err == redis.Nil {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("username directory lookup: %w", err)
	}
	userID, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("username directory: corrupt value for %q: %w", username, err)
	}
	return userID, true, nil
}
