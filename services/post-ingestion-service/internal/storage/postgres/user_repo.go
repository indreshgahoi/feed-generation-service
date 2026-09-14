package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"post-ingestion-service/internal/domain"
)

const pgUniqueViolation = "23505"

type UserRepo struct {
	pool *ShardedPool
}

func NewUserRepo(pool *ShardedPool) *UserRepo {
	return &UserRepo{pool: pool}
}

func (r *UserRepo) Create(ctx context.Context, user domain.User) error {
	pool, err := r.pool.PoolForExistingID(user.UserID)
	if err != nil {
		return err
	}
	_, err = pool.Exec(ctx,
		`INSERT INTO users (user_id, username, last_active_at) VALUES ($1, $2, $3)`,
		user.UserID, user.Username, user.LastActiveAt)

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation {
		// Duplicate username (the UNIQUE constraint, not the PK -- a
		// user_id collision would mean the ID generator itself is
		// broken, which is a different, much worse problem than
		// "someone already took this name").
		return fmt.Errorf("%w: username %q is taken", domain.ErrAlreadyExists, user.Username)
	}
	return err
}

func (r *UserRepo) GetByID(ctx context.Context, userID int64) (domain.User, error) {
	pool, err := r.pool.PoolForExistingID(userID)
	if err != nil {
		return domain.User{}, err
	}
	var u domain.User
	err = pool.QueryRow(ctx,
		`SELECT user_id, username, last_active_at FROM users WHERE user_id = $1`, userID,
	).Scan(&u.UserID, &u.Username, &u.LastActiveAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.User{}, domain.ErrNotFound
	}
	return u, err
}

// ListAll fans out to every shard concurrently and merges. See
// doc/sharding.md: this is explicitly a demo/admin-scale query, not
// something a 500M-user system would ever expose unbounded like this.
func (r *UserRepo) ListAll(ctx context.Context) ([]domain.User, error) {
	type result struct {
		shardID int
		users   []domain.User
		err     error
	}

	pools := r.pool.AllPools()
	results := make(chan result, len(pools))
	var wg sync.WaitGroup

	for shardID, pool := range pools {
		wg.Add(1)
		go func(shardID int, pool *pgxpool.Pool) {
			defer wg.Done()
			users, err := queryUsers(ctx, pool)
			if err != nil {
				results <- result{shardID: shardID, err: fmt.Errorf("shard %d: %w", shardID, err)}
				return
			}
			results <- result{shardID: shardID, users: users}
		}(shardID, pool)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var (
		all      []domain.User
		firstErr error
	)
	for res := range results {
		if res.err != nil {
			if firstErr == nil {
				firstErr = res.err
			}
			continue
		}
		all = append(all, res.users...)
	}
	if firstErr != nil {
		return nil, firstErr
	}
	return all, nil
}

func queryUsers(ctx context.Context, pool *pgxpool.Pool) ([]domain.User, error) {
	rows, err := pool.Query(ctx, `SELECT user_id, username, last_active_at FROM users ORDER BY user_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var users []domain.User
	for rows.Next() {
		var u domain.User
		if err := rows.Scan(&u.UserID, &u.Username, &u.LastActiveAt); err != nil {
			return nil, err
		}
		users = append(users, u)
	}
	return users, rows.Err()
}
