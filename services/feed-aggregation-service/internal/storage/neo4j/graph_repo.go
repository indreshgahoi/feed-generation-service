// Package neo4j implements domain.SocialGraphRepository, read-only from
// this service's perspective (writes happen in post-ingestion-service).
// See doc/DESIGN.md "The social graph lives in Neo4j, not sharded
// Postgres".
package neo4j

import (
	"context"
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

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

// FollowedCelebrities replaces what used to be a cross-shard
// scatter-gather ("is each followee a celebrity") with a single Cypher
// query -- this is the single biggest simplification Neo4j buys the read
// path. See doc/DESIGN.md.
func (r *GraphRepo) FollowedCelebrities(ctx context.Context, userID string) ([]string, error) {
	result, err := neo4j.ExecuteQuery(ctx, r.driver, `
		MATCH (:User {userId: $userId})-[:FOLLOWS]->(followee:User)
		WHERE followee.isCelebrity = true
		RETURN followee.userId AS followeeId
	`, map[string]any{"userId": mustParseInt64(userID)},
		neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase("neo4j"))
	if err != nil {
		return nil, fmt.Errorf("neo4j: followed celebrities: %w", err)
	}
	return extractIDColumn(result, "followeeId"), nil
}

func (r *GraphRepo) Following(ctx context.Context, userID string) ([]string, error) {
	result, err := neo4j.ExecuteQuery(ctx, r.driver, `
		MATCH (:User {userId: $userId})-[:FOLLOWS]->(followee:User)
		RETURN followee.userId AS followeeId
	`, map[string]any{"userId": mustParseInt64(userID)},
		neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase("neo4j"))
	if err != nil {
		return nil, fmt.Errorf("neo4j: following: %w", err)
	}
	return extractIDColumn(result, "followeeId"), nil
}

func extractIDColumn(result *neo4j.EagerResult, column string) []string {
	ids := make([]string, 0, len(result.Records))
	for _, record := range result.Records {
		v, ok := record.Get(column)
		if !ok {
			continue
		}
		ids = append(ids, formatInt64(v.(int64)))
	}
	return ids
}
