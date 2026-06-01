#!/usr/bin/env bash
#
# Partition-scaling benchmark for the word-count pipeline.
#
# For each partition count P (consumers scale to match): reset the topic + the
# PostgreSQL table, produce TOTAL reviews, start P consumers, wait for the topic
# to fully drain (consumer-group lag -> 0), then measure the write throughput
# from PostgreSQL (max(processed_at) - min(processed_at)).
#
# Usage:
#   ./benchmark.sh                       # defaults: TOTAL=100000, configs "4 8 16"
#   TOTAL=5000000 ./benchmark.sh         # 5M reviews per run
#   CONFIGS="4 8" TOTAL=200000 ./benchmark.sh
#
# Requirements: the docker-compose stack up (broker + postgres), and the repo's
# virtualenv at .venv (override with PY=...).
set -u

# --- paths (relative to this script) ---------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJ="$(cd "$SCRIPT_DIR/../../.." && pwd)"
PY="${PY:-$PROJ/.venv/bin/python}"

# --- knobs -----------------------------------------------------------------
TOTAL="${TOTAL:-100000}"          # reviews produced per run
CONFIGS="${CONFIGS:-4 8 16}"      # partition counts to test (= consumer counts)
BROKER="${BROKER:-127.0.0.1:9094}"
TOPIC="product_reviews"
TABLE="product_review_word_counts"

PSQL="docker exec postgres psql -U quixstreams"
KCG="docker exec broker kafka-consumer-groups --bootstrap-server broker:29092"
KT="docker exec broker kafka-topics --bootstrap-server broker:29092"

OUTDIR="$SCRIPT_DIR/.bench"
LOGDIR="$OUTDIR/logs"
RESULTS="$OUTDIR/results.tsv"
mkdir -p "$LOGDIR"
echo -e "partitions\tconsumers\treviews\trows\telapsed_s\trows_per_sec\tprod_msg_s" > "$RESULTS"

reset_topic() {  # $1 = partition count
  $KT --delete --topic "$TOPIC" >/dev/null 2>&1
  sleep 3
  NUM_PARTITIONS="$1" TOTAL_REVIEWS=0 "$PY" "$SCRIPT_DIR/producer.py" >/dev/null 2>&1
}

lag_and_end() {  # echoes "<lag_sum> <end_offset_sum>" for group $1
  $KCG --describe --group "$1" 2>/dev/null | awk '
    NR>1 && $6 ~ /^[0-9]+$/ { lag+=$6 }
    NR>1 && $5 ~ /^[0-9]+$/ { end+=$5 }
    END { printf "%d %d", lag+0, end+0 }'
}

run() {  # $1 = partitions, $2 = consumers
  local P="$1" N="$2" GRP="wc_p$1"
  echo "=== partitions=$P consumers=$N reviews=$TOTAL ==="

  pkill -f "$SCRIPT_DIR/consumer.py" >/dev/null 2>&1; sleep 1
  $PSQL -q -c "TRUNCATE $TABLE;" >/dev/null 2>&1
  $KCG --delete --group "$GRP" >/dev/null 2>&1
  reset_topic "$P"

  # 1) Produce all reviews first (topic then holds TOTAL messages).
  local plog="$LOGDIR/producer_p${P}.log"
  NUM_PARTITIONS="$P" TOTAL_REVIEWS="$TOTAL" "$PY" "$SCRIPT_DIR/producer.py" >"$plog" 2>&1
  local prod_rate
  prod_rate=$(grep -aoE '\(([0-9,]+) msg/s' "$plog" | head -1 | tr -d '(,' | awk '{print $1}')
  echo "  produced: ${prod_rate:-?} msg/s"

  # 2) Start N consumers (same group -> Kafka splits the partitions across them).
  local pids=()
  for ((i=1; i<=N; i++)); do
    CONSUMER_GROUP="$GRP" "$PY" "$SCRIPT_DIR/consumer.py" >"$LOGDIR/cons_p${P}_${i}.log" 2>&1 &
    pids+=($!)
  done

  # 3) Wait for full drain: all messages produced AND lag == 0.
  local it=0 lag end
  while (( it < 1800 )); do
    read lag end < <(lag_and_end "$GRP")
    (( end >= TOTAL && lag == 0 )) && break
    sleep 2; ((it++))
  done
  sleep 3  # let the final commits/writes settle

  # 4) Measure write throughput from PostgreSQL.
  local row rows elapsed rps
  row=$($PSQL -tAF$'\t' -c \
    "SELECT count(*),
            round(extract(epoch FROM max(processed_at)-min(processed_at))::numeric,2),
            round(count(*)/NULLIF(extract(epoch FROM max(processed_at)-min(processed_at)),0),1)
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
