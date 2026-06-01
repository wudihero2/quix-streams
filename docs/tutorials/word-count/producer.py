import json
import logging
import os
import time
from random import choice

from quixstreams import Application
from quixstreams.models.topics import TopicConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# The topic the producer writes reviews to and the consumers read from.
REVIEWS_TOPIC = "product_reviews"
NUM_PARTITIONS = int(os.getenv("NUM_PARTITIONS", "4"))

REVIEW_LIST = [
    "This is the best thing since sliced bread. The best I say.",
    "This is terrible. Could not get it working.",
    "I was paid to write this. Seems good.",
    "Great product. Would recommend.",
    "Not sure who this is for. Seems like it will break after 5 minutes.",
    "I bought their competitors product and it is way worse. Use this one instead.",
    "I would buy it again. In fact I would buy several of them.",
    "Great great GREAT",
]

PRODUCT_LIST = ["product_a", "product_b", "product_c", "product_d", "product_e"]


def main():
    app = Application(
        broker_address=os.getenv("BROKER_ADDRESS", "127.0.0.1:9094"),
        # Throughput tuning (keeps the default idempotent/acks=all safety).
        producer_extra_config={
            "linger.ms": 50,  # wait up to 50ms to build bigger batches
            "batch.size": 1048576,  # up to 1MB per batch
            "compression.type": "lz4",  # compress batches
            "queue.buffering.max.messages": 1000000,
        },
    )

    # Create the topic with 4 partitions so up to 4 consumers can run in parallel.
    # Calling app.topic() with auto_create_topics (default) creates it eagerly.
    reviews_topic = app.topic(
        name=REVIEWS_TOPIC,
        config=TopicConfig(num_partitions=NUM_PARTITIONS, replication_factor=1),
    )

    total = int(os.getenv("TOTAL_REVIEWS", "5000000"))
    sleep_s = float(os.getenv("PRODUCE_SLEEP", "0"))

    # A plain producer (no consumer), so the script exits as soon as it's done.
    start = time.monotonic()
    with app.get_producer() as producer:
        for i in range(total):
            review = choice(REVIEW_LIST)
            product = choice(PRODUCT_LIST)
            producer.produce(
                topic=reviews_topic.name,
                # The consumer reads values with the default JSON deserializer.
                value=json.dumps(review).encode("utf-8"),
                # `key_deserializer="str"` on the consumer keeps this readable.
                key=product.encode("utf-8"),
                # Round-robin the partition so ALL 4 partitions get an even share.
                # (Hashing the few product keys can leave some partitions empty.)
                partition=i % NUM_PARTITIONS,
            )
            if sleep_s:
                time.sleep(sleep_s)
    elapsed = time.monotonic() - start
    logger.info(
        f"Produced {total} reviews to '{REVIEWS_TOPIC}' "
        f"({total / elapsed:,.0f} msg/s in {elapsed:.2f}s)"
    )


if __name__ == "__main__":
    main()
