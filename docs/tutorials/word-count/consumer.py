import logging
import os
import random
import string
from collections import Counter

from quixstreams import Application
from quixstreams.sinks.community.postgresql import PostgreSQLSink

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Must match the topic the producer writes to.
REVIEWS_TOPIC = "product_reviews"

# Must match the table pre-created by init.sql.
TABLE_NAME = "product_review_word_counts"


def tokenize_and_count(text):
    return list(Counter(text.lower().replace(".", " ").split()).items())


def should_skip(word_count_pair):
    word, count = word_count_pair
    return word not in ["i", "a", "we", "it", "is", "and", "or", "the"]


# Number of randomly-generated extra columns, by type (8 + 6 + 4 + 2 = 20).
# These MUST match the columns pre-created in init.sql.
NUM_INT_COLS = 8
NUM_FLOAT_COLS = 6
NUM_TEXT_COLS = 4
NUM_BOOL_COLS = 2


def _random_text(n: int = 12) -> str:
    return "".join(random.choices(string.ascii_lowercase, k=n))


def to_row(word_count_pair):
    # PostgreSQLSink only accepts dictionaries (each key becomes a column).
    word, count = word_count_pair
    row = {"word": word, "count": count}
    # Pad with 20 randomly-generated columns of mixed types to make a wide row.
    for i in range(1, NUM_INT_COLS + 1):
        row[f"int_col_{i:02d}"] = random.randint(0, 1_000_000)
    for i in range(1, NUM_FLOAT_COLS + 1):
        row[f"float_col_{i:02d}"] = round(random.uniform(0, 1000), 4)
    for i in range(1, NUM_TEXT_COLS + 1):
        row[f"text_col_{i:02d}"] = _random_text()
    for i in range(1, NUM_BOOL_COLS + 1):
        row[f"bool_col_{i:02d}"] = random.random() < 0.5
    return row


def main():
    # The table already exists (init.sql), so we turn OFF schema auto-update and
    # do NOT upsert: every processed pair becomes its own row. That keeps the
    # `processed_at` timestamps, which we use to measure how long all consumers
    # took:
    #   SELECT max(processed_at) - min(processed_at) FROM product_review_word_counts;
    postgres_sink = PostgreSQLSink(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5433")),
        dbname=os.getenv("POSTGRES_DBNAME", "quixstreams"),
        user=os.getenv("POSTGRES_USER", "quixstreams"),
        password=os.getenv("POSTGRES_PASSWORD", "quixstreams"),
        table_name=TABLE_NAME,
        schema_auto_update=False,
    )

    app = Application(
        broker_address=os.getenv("BROKER_ADDRESS", "127.0.0.1:9094"),
        # Every consumer instance MUST share the same consumer group. Kafka then
        # spreads the topic's 4 partitions across the running instances, so each
        # of the 4 `python consumer.py` processes is assigned its own partition(s).
        consumer_group=os.getenv("CONSUMER_GROUP", "product_review_word_counter"),
        auto_offset_reset="earliest",
        commit_every=5000,
        # Fetch tuning: pull larger chunks per request for higher throughput.
        consumer_extra_config={
            "fetch.min.bytes": 1048576,  # wait for ~1MB before returning
            "fetch.wait.max.ms": 200,  # ...but no longer than 200ms
            "max.partition.fetch.bytes": 4194304,
            "queued.max.messages.kbytes": 256000,
        },
    )

    # Consume from the topic the producer created. `key_deserializer="str"` keeps
    # the product key readable in the `__key` TEXT column.
    reviews_topic = app.topic(name=REVIEWS_TOPIC, key_deserializer="str")

    sdf = app.dataframe(topic=reviews_topic)
    sdf = sdf.apply(tokenize_and_count, expand=True)
    sdf = sdf.filter(should_skip)
    sdf = sdf.apply(to_row)
    # `metadata=True` prints the partition/offset so you can see which partition
    # this particular consumer instance is handling.
    # sdf.print(metadata=True)
    sdf.sink(postgres_sink)

    app.run()


if __name__ == "__main__":
    main()
