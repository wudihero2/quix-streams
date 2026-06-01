-- Pre-creates the table the consumers sink word counts into.
--
-- One row is written per processed (word, count) pair. `processed_at` is stamped
-- by PostgreSQL at INSERT time, so the total time taken by ALL consumers together
-- is simply the span between the first and last row written:
--
--   SELECT max(processed_at) - min(processed_at) AS total_elapsed,
--          count(*)                              AS rows_written
--   FROM   product_review_word_counts;
--
-- NOTE: this script only runs the FIRST time the postgres data volume is created.
-- If you changed it, recreate the volume: `docker compose down -v && docker compose up -d`.

CREATE TABLE IF NOT EXISTS product_review_word_counts (
    "timestamp"  TIMESTAMP   NOT NULL,
    "__key"      TEXT,
    word         TEXT        NOT NULL,
    count        BIGINT      NOT NULL,
    -- 20 randomly-generated columns written by the consumer (8 int / 6 float /
    -- 4 text / 2 bool). These MUST match to_row() in consumer.py.
    int_col_01   BIGINT,
    int_col_02   BIGINT,
    int_col_03   BIGINT,
    int_col_04   BIGINT,
    int_col_05   BIGINT,
    int_col_06   BIGINT,
    int_col_07   BIGINT,
    int_col_08   BIGINT,
    float_col_01 DOUBLE PRECISION,
    float_col_02 DOUBLE PRECISION,
    float_col_03 DOUBLE PRECISION,
    float_col_04 DOUBLE PRECISION,
    float_col_05 DOUBLE PRECISION,
    float_col_06 DOUBLE PRECISION,
    text_col_01  TEXT,
    text_col_02  TEXT,
    text_col_03  TEXT,
    text_col_04  TEXT,
    bool_col_01  BOOLEAN,
    bool_col_02  BOOLEAN,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_word_counts_processed_at
    ON product_review_word_counts (processed_at);
