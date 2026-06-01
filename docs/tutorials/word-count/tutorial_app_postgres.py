import logging
import os
import time
from collections import Counter
from random import choice

from quixstreams import Application
from quixstreams.sinks.community.postgresql import PostgreSQLSink
from quixstreams.sources import Source

logger = logging.getLogger(__name__)


class ReviewGenerator(Source):
    _review_list = [
        "This is the best thing since sliced bread. The best I say.",
        "This is terrible. Could not get it working.",
        "I was paid to write this. Seems good.",
        "Great product. Would recommend.",
        "Not sure who this is for. Seems like it will break after 5 minutes.",
        "I bought their competitors product and it is way worse. Use this one instead.",
        "I would buy it again. In fact I would buy several of them.",
        "Great great GREAT",
    ]

    _product_list = ["product_a", "product_b", "product_c"]

    def __init__(self):
        super().__init__(name="customer-reviews")

    def run(self):
        for review in self._review_list:
            event = self.serialize(key=choice(self._product_list), value=review)
            logger.debug(f"Generating review for {event.key}: {event.value}")
            self.produce(key=event.key, value=event.value)
            time.sleep(0.5)  # just to make things easier to follow along
        logger.info("Sent all product reviews.")


def tokenize_and_count(text):
    return list(Counter(text.lower().replace(".", " ").split()).items())


def should_skip(word_count_pair):
    word, count = word_count_pair
    return word not in ["i", "a", "we", "it", "is", "and", "or", "the"]


def to_row(word_count_pair):
    # The PostgreSQLSink only accepts dictionaries, where each key becomes a
    # column. So convert the (word, count) tuple into a row.
    word, count = word_count_pair
    return {"word": word, "count": count}


def main():
    # The PostgreSQLSink writes batches of records to a PostgreSQL table.
    # The table (and any missing columns) is created automatically when
    # `schema_auto_update=True` (the default).
    postgres_sink = PostgreSQLSink(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5433")),
        dbname=os.getenv("POSTGRES_DBNAME", "quixstreams"),
        user=os.getenv("POSTGRES_USER", "quixstreams"),
        password=os.getenv("POSTGRES_PASSWORD", "quixstreams"),
        table_name="product_review_word_counts",
        # Upsert the running count per word so each word has a single row.
        primary_key_columns="word",
        upsert_on_primary_key=True,
    )

    app = Application(
        broker_address=os.getenv("BROKER_ADDRESS", "127.0.0.1:9094"),
        consumer_group="product_review_word_counter",
        auto_offset_reset="earliest",
    )

    # If reading from a Kafka topic, pass topic=<Topic> instead of a source
    sdf = app.dataframe(source=ReviewGenerator())
    sdf = sdf.apply(tokenize_and_count, expand=True)
    sdf = sdf.filter(should_skip)
    sdf = sdf.apply(to_row)
    sdf.print()
    sdf.sink(postgres_sink)

    # Start the application
    app.run()


# This approach is necessary since we are using a Source
if __name__ == "__main__":
    main()
