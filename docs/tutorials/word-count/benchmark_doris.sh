#!/usr/bin/env bash
#
# Partition-scaling benchmark for the word-count pipeline (Doris sink).
#
# For each partition count P (consumers scale to match): reset the topic + the
# Doris table, produce TOTAL reviews, start P consumers, wait for the topic to
# fully drain (consumer-group lag -> 0), then measure the write throughput from
# Doris (max(processed_at) - min(processed_at)).
#
# Usage:
#   ./benchmark_doris.sh                   # defaults: TOTAL=100000, configs "4 8 16"
#   TOTAL=5000000 ./benchmark_doris.sh     # 5M reviews per run
#   CONFIGS="4 8" TOTAL=200000 ./benchmark_doris.sh
#
# Requirements:
#   - the docker-compose stack up (broker + doris)
#   - the Doris schema applied once:
#       docker exec -i doris-fe1 mysql -uroot -P9030 -h127.0.0.1 < init_doris.sql
#   - the repo's virtualenv at .venv (override with PY=...)
#
# consumer_doris.py defaults to DORIS_HTTP_PORT=8040 (BE webserver), so Stream
# Load is sent straight to the BE and skips the FE->BE redirect a host-based app
# can't follow into the Docker network.
set -u

# --- paths (relative to this script) ---------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJ="$(cd "$SCRIPT_DIR/../../.." && pwd)"
PY="${PY:-$PROJ/.venv/bin/python}"

# DorisSink is unreleased, so it only exists in the repo source — not in any
# published quixstreams in site-packages. Put the repo root first on the import
# path so `python consumer_doris.py` finds the local DorisSink. (Alternatively
# install the repo editable once: `.venv/bin/pip install -e .[doris]`.)
export PYTHONPATH="$PROJ${PYTHONPATH:+:$PYTHONPATH}"

# --- knobs -----------------------------------------------------------------
TOTAL="${TOTAL:-100000}"          # reviews produced per run
CONFIGS="${CONFIGS:-4 8 16}"      # partition counts to test (= consumer counts)
BROKER="${BROKER:-127.0.0.1:9094}"
TOPIC="product_reviews"
TABLE="product_review_word_counts"
DB="quixstreams"

# Batch, no-header mysql client against the Doris FE. `-N -B` => tab-separated.
DORIS="docker exec -i doris-fe1 mysql -uroot -P9030 -h127.0.0.1 -N -B $DB"
KCG="docker exec broker kafka-consumer-groups --bootstrap-server broker:29092"
KT="docker exec broker kafka-topics --bootstrap-server broker:29092"

OUTDIR="$SCRIPT_DIR/.bench_doris"
LOGDIR="$OUTDIR/logs"
RESULTS="$OUTDIR/results.tsv"
mkdir -p "$LOGDIR"
echo -e "partitions\tconsumers\treviews\trows\telapsed_s\trows_per_sec\tprod_msg_s" > "$RESULTS"

reset_topic() {  # $1 = partition count
  $KT --delete --topic "$TOPIC" >/dev/null 2>&1
  sleep 3
  NUM_PARTITIONS="$1" TOTAL_REVIEWS=0 "$PY" "$SCRIPT_DIR/producer.py" >/dev/null 2>&1
}

group_status() {  # echoes "<lag_sum> <end_sum> <committed_parts>" for group $1
  # committed_parts = partitions reporting a NUMERIC lag. Right after start /
  # during rebalance a partition's LAG shows "-" (no committed offset yet); we
  # must NOT treat that as lag 0, or the drain check breaks before consumers
  # have actually processed anything. So count only numeric-lag partitions and
  # require that count to equal the partition total before declaring "drained".
  $KCG --describe --group "$1" 2>/dev/null | awk '
    NR>1 && $5 ~ /^[0-9]+$/ { end+=$5 }
    NR>1 && $6 ~ /^[0-9]+$/ { lag+=$6; committed++ }
    END { printf "%d %d %d", lag+0, end+0, committed+0 }'
}

topic_end_sum() {  # echoes the sum of end offsets across all partitions of $TOPIC
  docker exec broker kafka-get-offsets --bootstrap-server broker:29092 \
    --topic "$TOPIC" --time -1 2>/dev/null | awk -F: '{s+=$3} END {print s+0}'
}

run() {  # $1 = partitions, $2 = consumers
  local P="$1" N="$2" GRP="wc_doris_p$1"
  echo "=== partitions=$P consumers=$N reviews=$TOTAL ==="

  pkill -f "$SCRIPT_DIR/consumer_doris.py" >/dev/null 2>&1; sleep 1
  $DORIS -e "TRUNCATE TABLE $TABLE;" >/dev/null 2>&1
  $KCG --delete --group "$GRP" >/dev/null 2>&1
  reset_topic "$P"

  # 1) Produce all reviews first (topic then holds TOTAL messages).
  local plog="$LOGDIR/producer_p${P}.log"
  NUM_PARTITIONS="$P" TOTAL_REVIEWS="$TOTAL" "$PY" "$SCRIPT_DIR/producer.py" >"$plog" 2>&1
  local prod_rate
  prod_rate=$(grep -aoE '\(([0-9,]+) msg/s' "$plog" | head -1 | tr -d '(,' | awk '{print $1}')
  echo "  produced: ${prod_rate:-?} msg/s"

  # 1b) Barrier: producer.py exits synchronously above, but confirm all TOTAL
  #     messages are durably in the topic (end-offset sum >= TOTAL) before any
  #     consumer starts, so consumers never race ahead of the producer.
  local bit=0
  while (( bit < 60 )); do
    (( $(topic_end_sum) >= TOTAL )) && break
    sleep 1; ((bit++))
  done
  echo "  topic ready: end_offsets=$(topic_end_sum) (>= $TOTAL)"

  # 2) Start N consumers (same group -> Kafka splits the partitions across them).
  local pids=()
  for ((i=1; i<=N; i++)); do
    CONSUMER_GROUP="$GRP" "$PY" "$SCRIPT_DIR/consumer_doris.py" >"$LOGDIR/cons_p${P}_${i}.log" 2>&1 &
    pids+=($!)
  done

  # 3) Wait for full drain. Done only when: every partition has committed
  #    (committed == P), the topic is fully produced (end >= TOTAL), and lag == 0.
  local it=0 lag end committed
  while (( it < 1800 )); do
    read lag end committed < <(group_status "$GRP")
    (( committed == P && end >= TOTAL && lag == 0 )) && break
    sleep 2; ((it++))
  done

  # 3b) Safety net: lag==0 implies writes flushed (at-least-once commits offsets
  #     only after a successful sink write), but wait until the Doris row count
  #     stops growing before measuring, so we never read a half-written table.
  local prev=-1 cur
  while :; do
    cur=$($DORIS -e "SELECT count(*) FROM $TABLE;")
    [ "$cur" = "$prev" ] && break
    prev="$cur"; sleep 2
  done

  # 4) Measure write throughput from Doris.
  #    UNIX_TIMESTAMP() on DATETIME(6) yields fractional seconds.
  local row rows elapsed rps
  row=$($DORIS -e \
    "SELECT count(*),
            round(UNIX_TIMESTAMP(max(processed_at)) - UNIX_TIMESTAMP(min(processed_at)), 2),
            round(count(*) / NULLIF(UNIX_TIMESTAMP(max(processed_at)) - UNIX_TIMESTAMP(min(processed_at)), 0), 1)
     FROM $TABLE;")
  IFS=$'\t' read rows elapsed rps <<< "$row"
  echo "  rows=$rows elapsed=${elapsed}s rows/s=$rps"
  echo -e "${P}\t${N}\t${TOTAL}\t${rows}\t${elapsed}\t${rps}\t${prod_rate:-}" >> "$RESULTS"

  # 5) Tear down consumers + group for the next run.
  kill -INT "${pids[@]}" >/dev/null 2>&1; sleep 2
  kill -9 "${pids[@]}" >/dev/null 2>&1
  $KCG --delete --group "$GRP" >/dev/null 2>&1
}

for P in $CONFIGS; do
  run "$P" "$P"
done

echo "=== DONE ==="
column -t -s $'\t' "$RESULTS"
echo "results: $RESULTS"
