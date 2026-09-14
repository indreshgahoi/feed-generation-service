#!/usr/bin/env python3
"""Seeds demo users, a follow graph, and sample posts -- entirely through
the real HTTP APIs (POST /v1/users, /v1/follow, /v1/posts), not direct
database writes. This matters now in a way it didn't for the original
single-Postgres version: user IDs are self-routing Snowflake IDs minted
by post-ingestion-service's consistent-hash ring (see doc/sharding.md),
not small sequential integers a script could just assign -- there is no
shortcut that bypasses the API and still produces valid IDs.

One deliberate exception, clearly called out where it happens: making a
user an actual celebrity (>25,000 real followers) would mean 25,001 real
POST /v1/follow calls. Instead this script creates a normal-sized follow
graph and then directly patches the celebrity's Neo4j node -- a seed-only
shortcut, not how celebrity status is achieved in the running system
(that's always the atomic increment inside GraphRepo.Follow, one real
follow at a time).
"""
import json
import os
import random
import time
import urllib.error
import urllib.parse
import urllib.request

from neo4j import GraphDatabase

INGESTION_URL = os.environ.get("INGESTION_URL", "http://localhost:8080/api/ingest")
NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USERNAME = os.environ.get("NEO4J_USERNAME", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "feedpassword")

REGULAR_USERNAMES = [f"user_{i}" for i in range(2, 14)]
CELEBRITY_USERNAME = "celeb_taylor"
SIMULATED_CELEBRITY_FOLLOWER_COUNT = 30000

SWATCHES = [
    ("#F5B14C", "\U0001F305"),
    ("#4C9AF5", "\U0001F30A"),
    ("#7ED957", "\U0001F33F"),
    ("#F5588A", "\U0001F3A8"),
    ("#8A3AB9", "✨"),
    ("#2D2D2D", "\U0001F319"),
]

SAMPLE_POSTS = [
    ("user_2", "Coffee first, code second ☕ @user_5", 0, 12, 2),
    ("user_3", "Sunset chasing again", 0, 34, 5),
    ("user_4", "New paint set arrived, time to create @user_9", 3, 8, 1),
    ("user_5", "Weekend hike recap \U0001f33f", 2, 21, 3),
    (CELEBRITY_USERNAME, "Dropping something special today ✨", 4, 4200, 380),
    ("user_6", "Ocean breeze and good vibes @user_3", 1, 15, 2),
    ("user_7", "Late night thoughts", 5, 6, 0),
    ("user_8", "Studio session with @celeb_taylor today, unreal", 4, 58, 9),
    ("user_9", "Can't stop won't stop @user_11", 3, 27, 4),
    ("user_10", "Sunrise chasing again \U0001f305", 0, 19, 2),
    (CELEBRITY_USERNAME, "Thank you for 30k -- this community is everything", 3, 5100, 610),
    ("user_11", "Small plants, big joy \U0001f33f", 2, 11, 1),
    ("user_12", "Trying a new recipe tonight @user_2", 1, 9, 3),
    ("user_13", "Quiet morning, loud thoughts", 5, 14, 2),
]


def http_json(method: str, path: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(f"{INGESTION_URL}{path}", data=data, method=method,
                                  headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


def wait_for_ingestion(timeout_seconds: int = 60) -> bool:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            urllib.request.urlopen(f"{INGESTION_URL}/healthz", timeout=2)
            return True
        except (urllib.error.URLError, ConnectionError):
            time.sleep(1)
    return False


def placeholder_image(color: str, emoji: str) -> str:
    svg = (
        f"<svg xmlns='http://www.w3.org/2000/svg' width='400' height='300'>"
        f"<rect width='100%' height='100%' fill='{color}'/>"
        f"<text x='50%' y='53%' font-size='90' text-anchor='middle' dominant-baseline='middle'>{emoji}</text>"
        f"</svg>"
    )
    return "data:image/svg+xml," + urllib.parse.quote(svg)


def create_users() -> dict[str, str]:
    print(f"Creating users via {INGESTION_URL}/v1/users ...")
    user_ids: dict[str, str] = {}
    for username in [CELEBRITY_USERNAME] + REGULAR_USERNAMES:
        try:
            user = http_json("POST", "/v1/users", {"username": username})
        except urllib.error.HTTPError as e:
            if e.code == 409:  # already exists from a prior seed run
                print(f"  {username} already exists, looking it up")
                users = http_json("GET", "/v1/users")
                user = next(u for u in users if u["username"] == username)
            else:
                raise
        user_ids[username] = user["userId"]
        print(f"  {username} -> user_id={user['userId']}")
    return user_ids


def create_follow_graph(user_ids: dict[str, str]) -> None:
    print("\nBuilding follow graph via /v1/follow ...")
    celeb_id = user_ids[CELEBRITY_USERNAME]
    regular_ids = [user_ids[u] for u in REGULAR_USERNAMES]

    for uid in regular_ids:
        http_json("POST", "/v1/follow", {"followerId": uid, "followeeId": celeb_id})

    for uid in regular_ids:
        others = [o for o in regular_ids if o != uid]
        for followee in random.sample(others, k=min(4, len(others))):
            http_json("POST", "/v1/follow", {"followerId": uid, "followeeId": followee})
    print(f"  everyone follows {CELEBRITY_USERNAME}; random mesh among regular users built")


def simulate_celebrity_status(celeb_user_id: str) -> None:
    """Seed-only shortcut -- see module docstring. The running system
    never sets these fields directly; GraphRepo.Follow increments
    followerCount atomically, one real follow at a time."""
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))
    with driver.session() as session:
        session.run(
            "MATCH (u:User {userId: $userId}) "
            "SET u.followerCount = $count, u.isCelebrity = true",
            userId=int(celeb_user_id), count=SIMULATED_CELEBRITY_FOLLOWER_COUNT,
        )
    driver.close()
    print(f"\nSimulated {SIMULATED_CELEBRITY_FOLLOWER_COUNT} followers for {CELEBRITY_USERNAME} "
          f"(seed-only Neo4j patch, not a real follow count)")


def create_sample_posts(user_ids: dict[str, str]) -> None:
    print(f"\nCreating {len(SAMPLE_POSTS)} sample posts ...")
    for username, caption, swatch_idx, like_count, comment_count in SAMPLE_POSTS:
        color, emoji = SWATCHES[swatch_idx]
        body = {
            "userId": user_ids[username],
            "mediaUrl": placeholder_image(color, emoji),
            "mediaType": 1,
            "caption": caption,
            "likeCount": like_count,
            "commentCount": comment_count,
        }
        try:
            http_json("POST", "/v1/posts", body)
            print(f"  posted as {username}: {caption[:50]!r}")
        except urllib.error.HTTPError as e:
            print(f"  FAILED for {username}: {e}")
        time.sleep(0.3)  # spread out created_at timestamps a little

    print("Waiting a few seconds for async fan-out/vector/notification workers to catch up...")
    time.sleep(5)


def main() -> None:
    if not wait_for_ingestion():
        print(f"ERROR: post-ingestion-service not reachable at {INGESTION_URL}.")
        print("Start the stack with scripts/run_all.sh, then re-run this script.")
        return

    user_ids = create_users()
    create_follow_graph(user_ids)
    simulate_celebrity_status(user_ids[CELEBRITY_USERNAME])
    create_sample_posts(user_ids)

    print("\nDone. Open the web UI (http://localhost:8080, behind the Envoy gateway) or hit the feed API directly, e.g.:")
    print(f"  curl 'http://localhost:8080/api/feed?userId={user_ids['user_7']}'")


if __name__ == "__main__":
    main()
