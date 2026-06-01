import logging
import os
import random
import string
from collections import Counter

from quixstreams import Application
from quixstreams.sinks.community.doris import DorisSink

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Must match the topic the producer writes to.
REVIEWS_TOPIC = "product_reviews"

# Must match the table pre-created by init_doris.sql.
TABLE_NAME = "product_review_word_counts"


def tokenize_and_count(text):
    return list(Counter(text.lower().replace(".", " ").split()).items())


def should_skip(word_count_pair):
    word, count = word_count_pair
    return word not in ["i", "a", "we", "it", "is", "and", "or", "the"]


# Number of randomly-generated extra columns, by type (8 + 6 + 4 + 2 = 20).
# These MUST match the columns pre-created in init_doris.sql.
NUM_INT_COLS = 8
NUM_FLOAT_COLS = 6
NUM_TEXT_COLS = 4
NUM_BOOL_COLS = 2


def _random_text(n: int = 12) -> str:
    return "".join(random.choices(string.ascii_lowercase, k=n))


def to_row(word_count_pair):
    # DorisSink (flatten_value=True) only accepts dictionaries — each key becomes
    # a column. Pad with 20 randomly-generated columns to make a wide row.
    word, count = word_count_pair
    row = {"word": word, "count": count}
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
    # DorisSink writes batches via the Stream Load HTTP API. The table must
    # already exist (init_doris.sql) — Doris does not auto-create schema.
    #
    # DORIS_HTTP_PORT defaults to 8040 (the BE webserver) so Stream Load is sent
    # straight to the BE and skips the FE(8030)->BE(8040) redirect, which a
    # host-based app can't follow into the Docker network. If you run this app
    # INSIDE the compose network instead, set DORIS_HOST=doris DORIS_HTTP_PORT=8030.
    doris_sink = DorisSink(
        host=os.getenv("DORIS_HOST", "localhost"),
        http_port=int(os.getenv("DORIS_HTTP_PORT", "8040")),
        username=os.getenv("DORIS_USER", "root"),
        password=os.getenv("DORIS_PASSWORD", ""),
        database=os.getenv("DORIS_DATABASE", "quixstreams"),
        table_name=TABLE_NAME,
        # Keep only __key + __timestamp (matches the columns in init_doris.sql).
        include_metadata={"key", "timestamp"},
    )

    app = Application(
        broker_address=os.getenv("BROKER_ADDRESS", "127.0.0.1:9094"),
        # Every consumer instance MUST share the same consumer group. Kafka then
        # spreads the topic's 4 partitions across the running instances, so each
        # of the 4 `python consumer_doris.py` processes gets its own partition(s).
        consumer_group=os.getenv("CONSUMER_GROUP", "product_review_word_counter_doris"),
        auto_offset_reset="earliest",
        commit_every=10000,
        # Fetch tuning: pull larger chunks per request for higher throughput.
        consumer_extra_config={
            "fetch.min.bytes": 1048576,  # wait for ~1MB before returning
            "fetch.wait.max.ms": 200,  # ...but no longer than 200ms
            "max.partition.fetch.bytes": 4194304,
            "queued.max.messages.kbytes": 256000,
        },
    )

    # `key_deserializer="str"` keeps the product key readable in __key.
    reviews_topic = app.topic(name=REVIEWS_TOPIC, key_deserializer="str")

    sdf = app.dataframe(topic=reviews_topic)
    sdf = sdf.apply(tokenize_and_count, expand=True)
    sdf = sdf.filter(should_skip)
    sdf = sdf.apply(to_row)
    # sdf.print(metadata=True)
    sdf.sink(doris_sink)

    app.run()


if __name__ == "__main__":
    main()
