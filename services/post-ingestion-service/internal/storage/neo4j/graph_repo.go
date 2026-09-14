// Package neo4j implements domain.SocialGraphRepository against Neo4j.
// See doc/sharding.md, "The social graph lives in Neo4j, not sharded
// Postgres": a follow edge connects two users who can be on any two
// shards, which is exactly the shape a graph database is for and a
// sharded relational table is not.
package neo4j

import (
	"context"
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"

	"post-ingestion-service/internal/domain"
)

const celebrityFollowerCutoff = 25000

type GraphRepo struct {
	driver neo4j.DriverWithContext
}

func NewGraphRepo(uri, username, password string) (*GraphRepo, error) {
	driver, err := neo4j.NewDriverWithContext(uri, neo4j.BasicAuth(username, password, ""))
	if err != nil {
		return nil, fmt.Errorf("neo4j: create driver: %w", err)
	}
	return &GraphRepo{driver: driver}, nil
}

func (r *GraphRepo) VerifyConnectivity(ctx context.Context) error {
	return r.driver.VerifyConnectivity(ctx)
}

func (r *GraphRepo) Close(ctx context.Context) error {
	return r.driver.Close(ctx)
}

// EnsureUserNode upserts a lightweight User node so FOLLOWS edges have
// somewhere to attach. Called alongside the sharded Postgres user INSERT
// -- see doc/sharding.md "What still has to stay in sync" for the
// two-writes-no-shared-transaction gap this creates.
func (r *GraphRepo) EnsureUserNode(ctx context.Context, userID int64, username string) error {
	_, err := neo4j.ExecuteQuery(ctx, r.driver, `
		MERGE (u:User {userId: $userId})
		ON CREATE SET u.username = $username, u.followerCount = 0, u.isCelebrity = false
	`, map[string]any{"userId": userID, "username": username},
		neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase("neo4j"))
	return err
}

// Follow creates the edge and increments the followee's followerCount in
// one Cypher transaction -- one database, one transaction, no
// cross-store consistency problem (contrast with the Redis counters,
// which genuinely do straddle two stores). Idempotent: following twice
// does not double-count, thanks to MERGE on the relationship.
func (r *GraphRepo) Follow(ctx context.Context, followerID, followeeID int64) error {
	if followerID == followeeID {
		return domain.ErrSelfFollow
	}
	result, err := neo4j.ExecuteQuery(ctx, r.driver, `
		MATCH (follower:User {userId: $followerId})
		MATCH (followee:User {userId: $followeeId})
		MERGE (follower)-[edge:FOLLOWS]->(followee)
		ON CREATE SET edge.createdAt = datetime(), followee.followerCount = followee.followerCount + 1
		SET followee.isCelebrity = followee.followerCount > $celebrityCutoff
		RETURN edge.createdAt AS createdAt
	`, map[string]any{"followerId": followerID, "followeeId": followeeID, "celebrityCutoff": celebrityFollowerCutoff},
		neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase("neo4j"))
	if err != nil {
		return fmt.Errorf("neo4j: follow: %w", err)
	}
	if len(result.Records) == 0 {
		return domain.ErrNotFound // one of the two User nodes doesn't exist
	}
	return nil
}

// Unfollow deletes the edge and decrements followerCount atomically.
func (r *GraphRepo) Unfollow(ctx context.Context, followerID, followeeID int64) error {
	_, err := neo4j.ExecuteQuery(ctx, r.driver, `
		MATCH (follower:User {userId: $followerId})-[edge:FOLLOWS]->(followee:User {userId: $followeeId})
		DELETE edge
		SET followee.followerCount = CASE WHEN followee.followerCount > 0 THEN followee.followerCount - 1 ELSE 0 END,
		    followee.isCelebrity = followee.followerCount > $celebrityCutoff
	`, map[string]any{"followerId": followerID, "followeeId": followeeID, "celebrityCutoff": celebrityFollowerCutoff},
		neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase("neo4j"))
	if err != nil {
		return fmt.Errorf("neo4j: unfollow: %w", err)
	}
	return nil
}

// Following returns who userID follows -- a single-hop traversal,
// regardless of which shard(s) those followees' post/profile data live on.
func (r *GraphRepo) Following(ctx context.Context, userID int64) ([]int64, error) {
	result, err := neo4j.ExecuteQuery(ctx, r.driver, `
		MATCH (:User {userId: $userId})-[:FOLLOWS]->(followee:User)
		RETURN followee.userId AS followeeId
	`, map[string]any{"userId": userID},
		neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase("neo4j"))
	if err != nil {
		return nil, fmt.Errorf("neo4j: following: %w", err)
	}
	ids := make([]int64, 0, len(result.Records))
	for _, record := range result.Records {
		v, _ := record.Get("followeeId")
		ids = append(ids, v.(int64))
	}
	return ids, nil
}

// GetProfile reads the social-graph-derived facts about a user: follower
// count and celebrity status, both maintained transactionally by
// Follow/Unfollow above.
func (r *GraphRepo) GetProfile(ctx context.Context, userID int64) (domain.SocialProfile, error) {
	result, err := neo4j.ExecuteQuery(ctx, r.driver, `
		MATCH (u:User {userId: $userId})
		RETURN u.userId AS userId, u.username AS username, u.followerCount AS followerCount, u.isCelebrity AS isCelebrity
	`, map[string]any{"userId": userID},
		neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase("neo4j"))
	if err != nil {
		return domain.SocialProfile{}, fmt.Errorf("neo4j: get profile: %w", err)
	}
	if len(result.Records) == 0 {
		return domain.SocialProfile{}, domain.ErrNotFound
	}
	record := result.Records[0]
	userIDVal, _ := record.Get("userId")
	usernameVal, _ := record.Get("username")
	followerCountVal, _ := record.Get("followerCount")
	isCelebrityVal, _ := record.Get("isCelebrity")
	return domain.SocialProfile{
		UserID:        userIDVal.(int64),
		Username:      usernameVal.(string),
		FollowerCount: int(followerCountVal.(int64)),
		IsCelebrity:   isCelebrityVal.(bool),
	}, nil
}
