-- Pre-creates the Doris database + table the Doris consumer sinks word counts into.
--
-- Unlike the PostgreSQL sink, DorisSink does NOT auto-create tables or columns,
-- so this schema must exist before running consumer_doris.py. The columns here
-- must match to_row() in consumer.py PLUS the metadata columns DorisSink adds
-- (__key, __timestamp — selected via include_metadata={"key", "timestamp"}).
--
-- This image does NOT auto-run init scripts, so apply it manually once the FE
-- is up (port 9030, user root, no password):
--
--   docker exec -i doris-fe1 mysql -uroot -P9030 -h127.0.0.1 < init_doris.sql
--   # or, from the host with a local mysql client:
--   mysql -uroot -h127.0.0.1 -P9030 < init_doris.sql
--
-- A DUPLICATE KEY model is used (every processed pair becomes its own row, like
-- the non-upsert PostgreSQL consumer). `processed_at` defaults to the Doris
-- insert time, so total wall-clock for ALL consumers is:
--
--   SELECT max(processed_at) - min(processed_at) AS total_elapsed_s,
--          count(*)                              AS rows_written
--   FROM   quixstreams.product_review_word_counts;

CREATE DATABASE IF NOT EXISTS quixstreams;

CREATE TABLE IF NOT EXISTS quixstreams.product_review_word_counts (
    -- DUPLICATE KEY columns must be the leading columns in definition order.
    `word`         VARCHAR(256) NOT NULL,
    `count`        BIGINT       NOT NULL,
    -- 20 randomly-generated columns written by the consumer (8 int / 6 float /
    -- 4 text / 2 bool). These MUST match to_row() in consumer.py.
    `int_col_01`   BIGINT,
    `int_col_02`   BIGINT,
    `int_col_03`   BIGINT,
    `int_col_04`   BIGINT,
    `int_col_05`   BIGINT,
    `int_col_06`   BIGINT,
    `int_col_07`   BIGINT,
    `int_col_08`   BIGINT,
    `float_col_01` DOUBLE,
    `float_col_02` DOUBLE,
    `float_col_03` DOUBLE,
    `float_col_04` DOUBLE,
    `float_col_05` DOUBLE,
    `float_col_06` DOUBLE,
    `text_col_01`  VARCHAR(64),
    `text_col_02`  VARCHAR(64),
    `text_col_03`  VARCHAR(64),
    `text_col_04`  VARCHAR(64),
    `bool_col_01`  BOOLEAN,
    `bool_col_02`  BOOLEAN,
    -- Metadata columns added by DorisSink (include_metadata={"key","timestamp"}).
    `__key`        VARCHAR(256),
    `__timestamp`  DATETIME,
    -- Stamped by Doris at insert time; used to measure write throughput.
    -- DATETIME(6) keeps microsecond precision so short runs still measure well.
    `processed_at` DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
)
DUPLICATE KEY(`word`, `count`)
DISTRIBUTED BY HASH(`word`) BUCKETS AUTO
PROPERTIES ("replication_num" = "3");   -- 3-BE cluster: 3 replicas per tablet (HA)
