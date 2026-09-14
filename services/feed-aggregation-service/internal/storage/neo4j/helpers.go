package neo4j

import "strconv"

// mustParseInt64 panics on malformed input deliberately: userID strings
// reaching this layer originate from validated path/query parameters
// upstream, so a parse failure here indicates a programming error, not
// bad user input.
func mustParseInt64(s string) int64 {
	id, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		panic("neo4j: invalid user ID string: " + s)
	}
	return id
}

func formatInt64(id int64) string {
	return strconv.FormatInt(id, 10)
}
