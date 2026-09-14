#!/usr/bin/env python3
"""Vector Pipeline (Consumer 2 in the design doc's read/write diagram).

Consumes post-created events, computes a real sentence embedding of the
caption via Sentence-Transformers (as named in the doc -- "Compute 128-d
embedding via Sentence-Transformer & index in Qdrant (HNSW)"), and upserts
it into Qdrant for out-of-network / vector-recall candidate generation.

Note: we use the 384-dim all-MiniLM-L6-v2 model rather than a literal
128-d two-tower model -- training a real two-tower retrieval model is out
of scope for a local demo; this gives genuine semantic embeddings that the
feed-aggregation-service can do real ANN search against.
"""
import json
import logging
import os

from kafka import KafkaConsumer
from qdrant_client import QdrantClient
from qdrant_client.http import models as qmodels
from sentence_transformers import SentenceTransformer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("vector-pipeline")

KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9092").split(",")
QDRANT_URL = os.environ.get("QDRANT_URL", "http://localhost:6333")
TOPIC = "post-created"
COLLECTION = "post_embeddings"
EMBEDDING_MODEL = "all-MiniLM-L6-v2"
EMBEDDING_DIM = 384


def ensure_collection(client: QdrantClient) -> None:
    existing = {c.name for c in client.get_collections().collections}
    if COLLECTION not in existing:
        client.create_collection(
            collection_name=COLLECTION,
            vectors_config=qmodels.VectorParams(size=EMBEDDING_DIM, distance=qmodels.Distance.COSINE),
        )
        log.info("created qdrant collection '%s' (dim=%d, cosine)", COLLECTION, EMBEDDING_DIM)


def main() -> None:
    log.info("loading sentence-transformer model '%s' (first run downloads it)...", EMBEDDING_MODEL)
    model = SentenceTransformer(EMBEDDING_MODEL)

    qdrant = QdrantClient(url=QDRANT_URL)
    ensure_collection(qdrant)

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        group_id="vector-pipeline",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    log.info("vector-pipeline subscribed to '%s'", TOPIC)
    for message in consumer:
        event = message.value
        try:
            text = event.get("caption") or f"post by user {event['userId']}"
            embedding = model.encode(text, normalize_embeddings=True).tolist()

            qdrant.upsert(
                collection_name=COLLECTION,
                points=[
                    qmodels.PointStruct(
                        id=int(event["postId"]),
                        vector=embedding,
                        payload={
                            "user_id": event["userId"],
                            "created_at": event["createdAt"],
                            "caption": event.get("caption", ""),
                        },
                    )
                ],
            )
            consumer.commit()
            log.info("indexed embedding for post %s (author %s)", event["postId"], event["userId"])
        except Exception:
            log.exception("failed to index post %s", event.get("postId"))


if __name__ == "__main__":
    main()
