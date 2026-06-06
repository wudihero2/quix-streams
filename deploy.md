# Quix Streams Passthrough Pipeline — K8s Deployment Guide

## Architecture Overview

```
CDC (Debezium)
    │
    ▼
Source Kafka Cluster
    │
    ▼
┌──────────────────────────────────┐
│  Quix Streams Passthrough Pods   │
│                                  │
│  Group: orders (replicas: 2)     │
│  Group: users  (replicas: 1)    │
│  Group: logs   (replicas: 3)    │
│  ...           (~50 groups)      │
│                                  │
│  All pods use the SAME image     │
│  Config driven by:               │
│   - ConfigMap (groups.yaml)      │
│   - Vault (secrets)              │
│   - Helm values (replicas, etc.) │
└──────────────────────────────────┘
    │
    ▼
Downstream (Kafka / DB / S3 ...)
```

**Core Principles:**

- **一個 Image 打天下** — 所有 sink types 預先打包在 image 裡，靠 config 切換，極少 rebuild
- **一份 ConfigMap** — 所有分組定義在 `groups.yaml`，不產生大量 ConfigMap
- **Helm 驅動** — 一個 chart 管理所有分組的 Deployments、ConfigMap、KEDA ScaledObjects
- **Vault 管敏感資訊** — DSN、password 走 External Secrets Operator，不進 git

---

## 目錄結構

```
deploy/
├── chart/
│   └── quix-passthrough/
│       ├── Chart.yaml
│       ├── values.yaml                # 所有設定：groups 定義 + sink 設定 + resource limits
│       ├── templates/
│       │   ├── _helpers.tpl
│       │   ├── configmap.yaml         # 從 values.yaml groups 生成 groups.yaml
│       │   ├── deployment.yaml        # range over groups → 一個 group 一個 Deployment
│       │   ├── externalsecret.yaml    # Vault → K8s Secret
│       │   ├── serviceaccount.yaml
│       │   ├── scaledobject.yaml      # KEDA（per-group 可選）
│       │   └── servicemonitor.yaml    # Prometheus
└── Dockerfile
```

---

## Dockerfile

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/

ENTRYPOINT ["python", "-m", "app.main"]
```

`requirements.txt`:
```
quixstreams
psycopg2-binary    # postgres sink
boto3              # s3 sink (future)
```

Image rebuild 時機：升級 quixstreams 版本、新增 sink type。**日常操作不需要 rebuild。**

---

## App 程式碼 (`app/main.py`)

```python
import os
import yaml
import orjson
from quixstreams import Application


def load_group_config():
    config_path = os.environ.get("GROUPS_CONFIG", "/config/groups.yaml")
    group_name = os.environ["GROUP_NAME"]

    with open(config_path) as f:
        config = yaml.safe_load(f)

    group = config["groups"][group_name]
    return group


def resolve_target_name(routing_config, topic_name=None):
    """根據 routing config 回傳目標名稱（str 或 callable）。
    通用於 Doris table_name 和 Kafka target_topic。
    """
    if routing_config is None:
        return None

    if isinstance(routing_config, str):
        return routing_config

    strategy = routing_config.get("strategy", "static")

    if strategy == "static":
        return routing_config.get("table") or routing_config.get("topic")

    if strategy == "from_topic":
        # "cdc.public.orders" → split(".") → 取 index（預設 -1 = 最後一段）
        index = routing_config.get("index", -1)
        prefix = routing_config.get("prefix", "")
        if topic_name:
            return prefix + topic_name.split(".")[index]
        return routing_config.get("default", "unknown")

    if strategy == "from_field":
        field = routing_config["field"]
        default = routing_config.get("default", "unknown")
        return lambda item: item.value.get(field, default)

    if strategy == "mapping":
        table_map = routing_config.get("map", {})
        return table_map.get(topic_name, routing_config.get("default", topic_name))

    raise ValueError(f"Unknown routing strategy: {strategy}")


def build_sink(sink_config, topic_name=None):
    """根據 config 中的 type 欄位回傳對應的 sink instance。"""
    sink_type = sink_config["type"]
    routing = sink_config.get("topic_routing") or sink_config.get("table_routing")

    if sink_type == "kafka":
        from quixstreams.sinks.community.kafka import KafkaSink
        target = resolve_target_name(routing, topic_name) or sink_config.get("target_topic")
        return KafkaSink(
            broker_address=sink_config["broker"],
            topic=target,
        )
    elif sink_type == "postgres":
        from quixstreams.sinks.community.postgresql import PostgreSQLSink
        return PostgreSQLSink(
            connection_string=sink_config["dsn"],
            table_name=sink_config.get("target_table"),
            schema_auto_update=sink_config.get("schema_auto_update", True),
        )
    elif sink_type == "doris":
        from quixstreams.sinks.community.doris import DorisSink
        target = resolve_target_name(routing, topic_name) or sink_config.get("target_table")
        return DorisSink(
            host=sink_config["host"],
            http_port=sink_config.get("http_port", 8030),
            username=sink_config.get("username", "root"),
            password=sink_config.get("password", ""),
            database=sink_config["database"],
            table_name=target,
            flatten_value=sink_config.get("flatten_value", True),
            include_metadata=sink_config.get("include_metadata", True),
            partial_update=sink_config.get("partial_update", "none"),
            sequence_column=sink_config.get("sequence_column"),
            merge_type=sink_config.get("merge_type", "APPEND"),
        )
    else:
        raise ValueError(f"Unknown sink type: {sink_type}")


def resolve_sinks(topic_name, topic_overrides, default_sink_configs):
    """回傳該 topic 的 sink list。支援單一 sink 和 fan-out 多 sink。"""
    override = topic_overrides.get("sinks") or topic_overrides.get("sink")
    configs = override if override else default_sink_configs

    # 統一成 list：單一 dict → [dict]，已經是 list 則不動
    if isinstance(configs, dict):
        configs = [configs]

    return [build_sink(c, topic_name=topic_name) for c in configs]


def make_kafka_dlq_handler(app, dlq_topic_name):
    """DLQ 方式 1：失敗的 rows 寫到 Kafka DLQ topic。"""
    def handler(table, rows, exception):
        with app.get_producer() as producer:
            for row in rows:
                producer.produce(
                    topic=dlq_topic_name,
                    value=orjson.dumps({
                        "failed_table": table,
                        "error": str(exception),
                        "row": row,
                    }),
                )
    return handler


def make_doris_dlq_handler(
    host, http_port, username, password, database, error_table="__dlq",
):
    """DLQ 方式 2：失敗的 rows 寫到 Doris error table。

    Error table schema（Doris Duplicate Key 表，不做 dedup，保留所有 error 記錄）：
        CREATE TABLE `{database}`.`{error_table}` (
            `error_time`    DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
            `failed_table`  VARCHAR(256)    NOT NULL,
            `error_message` TEXT            NOT NULL,
            `row_data`      JSON            NOT NULL
        )
        DUPLICATE KEY(`error_time`, `failed_table`)
        DISTRIBUTED BY HASH(`failed_table`) BUCKETS AUTO;

    寫入的每筆 row 包含：
      - error_time:    Stream Load 失敗的時間（由 Doris DEFAULT CURRENT_TIMESTAMP 填入）
      - failed_table:  原本要寫入的目標 table name
      - error_message: DorisSinkException 的錯誤訊息（含 Doris ErrorURL）
      - row_data:      原始資料（JSON 格式，包含所有欄位 + metadata）
    """
    from quixstreams.sinks.community.doris import DorisSink

    error_sink = DorisSink(
        host=host,
        http_port=http_port,
        username=username,
        password=password,
        database=database,
        table_name=error_table,
        flatten_value=False,           # 整個 row 塞進 __value，不展開
        include_metadata=False,        # error table 有自己的 schema
        # 不設 on_stream_load_error — error sink 自己失敗就讓 app crash
        # 避免無限遞迴（error sink 失敗 → 再寫 error sink → ...）
    )
    error_sink.setup()

    import logging
    _logger = logging.getLogger(__name__)

    def handler(table, rows, exception):
        error_rows = [
            {
                "failed_table": table,
                "error_message": str(exception),
                "row_data": row,
            }
            for row in rows
        ]
        try:
            error_sink._stream_load(error_table, error_rows)
            _logger.info(
                f"Wrote {len(error_rows)} failed rows to "
                f"Doris {database}.{error_table}"
            )
        except Exception as e:
            # error sink 也失敗 → 不吞，讓 app crash，避免資料靜默丟失
            _logger.error(
                f"Failed to write to error table "
                f"{database}.{error_table}: {e}"
            )
            raise

    return handler


def build_dlq_handler(prefix, app, source_label):
    """根據環境變數 DLQ_{prefix}_MODE 建立對應的 DLQ handler。

    prefix: "PROCESSING" 或 "SINK"
    source_label: 寫入 error record 的 source 欄位（"processing_error" 或 "sink_error"）
    """
    mode = os.environ.get(f"DLQ_{prefix}_MODE", "crash")

    if mode == "kafka":
        topic = os.environ.get(
            f"DLQ_{prefix}_TOPIC",
            f"dlq.{source_label}-{os.environ['GROUP_NAME']}",
        )
        return make_kafka_dlq_handler(app, topic)

    if mode == "doris":
        return make_doris_dlq_handler(
            host=os.environ.get(f"DLQ_{prefix}_DORIS_HOST", "doris-fe"),
            http_port=int(os.environ.get(f"DLQ_{prefix}_DORIS_HTTP_PORT", "8030")),
            username=os.environ.get(f"DLQ_{prefix}_DORIS_USERNAME", "root"),
            password=os.environ.get(f"DLQ_{prefix}_DORIS_PASSWORD", ""),
            database=os.environ.get(f"DLQ_{prefix}_DORIS_DATABASE", "error_log"),
            error_table=os.environ.get(f"DLQ_{prefix}_DORIS_TABLE", f"__dlq_{source_label}"),
        )

    if mode == "skip":
        return lambda table, rows, exc: logger.warning(
            f"Skipped {len(rows)} failed rows for table '{table}': {exc}"
        )

    # mode == "crash" 或其他 → 回傳 None，讓錯誤直接 raise
    return None


def make_processing_error_handler(dlq_handler):
    """建立 on_processing_error callback。

    如果 dlq_handler 有設定，把失敗的 row 寫到 DLQ 並跳過。
    如果沒有（crash mode），回傳 False 讓 app crash。
    """
    if dlq_handler is None:
        return lambda exc, row, log: False  # crash

    def handler(exc, row, log):
        try:
            row_data = {"value": row.value, "key": row.key} if row else {}
            dlq_handler(
                table="__processing_error",
                rows=[row_data],
                exception=exc,
            )
        except Exception as dlq_exc:
            log.error(f"Failed to write processing error to DLQ: {dlq_exc}")
            return False  # DLQ 也失敗 → crash
        return True  # 寫 DLQ 成功 → 跳過該筆

    return handler


def _safe_decode(b):
    """raw Kafka key/value 是 bytes，orjson 不能直接序列化 → 盡力 decode 成 str。"""
    if isinstance(b, (bytes, bytearray)):
        return bytes(b).decode("utf-8", errors="replace")
    return b


def make_consumer_error_handler(dlq_handler):
    """建立 on_consumer_error callback（poll / 反序列化階段）。

    這層攔的是「訊息進 SDF 之前」就失敗的錯誤（例如壞掉的 JSON、schema 不符）。
    此時還沒有 Row，只有 raw message（bytes），所以盡力 decode 後寫 DLQ。
    回 True = 跳過該筆壞訊息、繼續；回 False = crash（重啟後 replay → 可能 crash-loop）。
    """
    if dlq_handler is None:
        return lambda exc, message, log: False  # crash

    def handler(exc, message, log):
        try:
            row_data = {
                "topic": message.topic() if message else None,
                "partition": message.partition() if message else None,
                "offset": message.offset() if message else None,
                "value": _safe_decode(message.value()) if message else None,
                "key": _safe_decode(message.key()) if message else None,
            }
            dlq_handler(table="__consumer_error", rows=[row_data], exception=exc)
        except Exception as dlq_exc:
            log.error(f"Failed to write consumer error to DLQ: {dlq_exc}")
            return False  # DLQ 也失敗 → crash
        return True  # 寫 DLQ 成功 → 跳過該筆壞訊息

    return handler


def make_producer_error_handler(dlq_handler):
    """建立 on_producer_error callback（序列化 / produce 到 Kafka 階段）。

    這層攔的是 to_topic() / changelog 寫 Kafka 時序列化或投遞失敗。
    此時是 Row（已反序列化的物件），跟 processing_error 一樣取 value/key。
    """
    if dlq_handler is None:
        return lambda exc, row, log: False  # crash

    def handler(exc, row, log):
        try:
            row_data = {"value": row.value, "key": row.key} if row else {}
            dlq_handler(table="__producer_error", rows=[row_data], exception=exc)
        except Exception as dlq_exc:
            log.error(f"Failed to write producer error to DLQ: {dlq_exc}")
            return False  # DLQ 也失敗 → crash
        return True  # 寫 DLQ 成功 → 跳過該筆

    return handler


def main():
    group_config = load_group_config()
    group_name = os.environ["GROUP_NAME"]

    # ── 建立四層 DLQ handlers ──
    # on_consumer_error / on_producer_error 必須在「建構 Application 時」就傳入：
    # 它們會被綁進 internal consumer / producer，事後再改屬性沒有用。
    # 但 DLQ handler 需要 app（kafka mode 會用 app.get_producer()），形成雞生蛋。
    # 解法：建構時先傳 late-bound 包裝 lambda，app 建好後再填真正的 handler。
    # （這兩個 callback 只在 app.run() 期間被呼叫，那時 handler 早已填好。）
    _consumer_error = None
    _producer_error = None

    app = Application(
        broker_address=os.environ["BROKER_ADDRESS"],
        consumer_group=f"passthrough-{group_name}",
        auto_offset_reset="earliest",
        loglevel=os.environ.get("LOG_LEVEL", "INFO"),
        # consumer/producer 階段的錯誤（反序列化 / produce 失敗）
        on_consumer_error=lambda exc, msg, log: _consumer_error(exc, msg, log),
        on_producer_error=lambda exc, row, log: _producer_error(exc, row, log),
    )

    processing_dlq = build_dlq_handler("PROCESSING", app, "processing")
    sink_dlq = build_dlq_handler("SINK", app, "sink")
    consumer_dlq = build_dlq_handler("CONSUMER", app, "consumer")
    producer_dlq = build_dlq_handler("PRODUCER", app, "producer")

    # on_processing_error 可事後注入；consumer/producer 填回上面 late-bound 的洞
    app._on_processing_error = make_processing_error_handler(processing_dlq)
    _consumer_error = make_consumer_error_handler(consumer_dlq)
    _producer_error = make_producer_error_handler(producer_dlq)

    # default_sinks 支援單一 dict 或 list（fan-out）
    default_sinks = group_config.get("default_sinks") or group_config["default_sink"]
    topics_config = group_config["topics"]

    for topic_name, topic_opts in topics_config.items():
        topic_opts = topic_opts or {}
        sinks = resolve_sinks(topic_name, topic_opts, default_sinks)

        topic = app.topic(topic_name)
        sdf = app.dataframe(topic)
        for sink in sinks:
            if hasattr(sink, '_on_stream_load_error') and sink._on_stream_load_error is None:
                sink._on_stream_load_error = sink_dlq
            sdf.sink(sink)

    app.run()


if __name__ == "__main__":
    main()
```

---

## Helm Chart

### `Chart.yaml`

```yaml
apiVersion: v2
name: quix-passthrough
description: Quix Streams passthrough pipeline
version: 0.1.0
appVersion: "1.0.0"
```

### `values.yaml`

```yaml
image:
  repository: your-registry.io/quix-passthrough
  tag: "1.0.0"
  pullPolicy: IfNotPresent

broker:
  address: kafka-bootstrap:9092

vault:
  enabled: true
  path: secret/data/quix-passthrough   # Vault path
  keys:
    - name: POSTGRES_DSN
      vaultKey: postgres-dsn
    # 新增敏感資訊只需要在這裡加一行

resources:
  requests:
    memory: 128Mi
    cpu: 100m
  limits:
    memory: 512Mi
    cpu: 500m

# ── DLQ 設定 ──────────────────────────────────────────────────
# 四層錯誤各自獨立選擇 DLQ 目的地：
#   consumer_error   — poll / 反序列化錯誤（訊息進 SDF 之前就壞，如髒 JSON）
#   processing_error — SDF pipeline 內的錯誤（apply/filter/update 拋異常）
#   producer_error   — 序列化 / produce 到 Kafka 失敗（to_topic / changelog）
#   sink_error       — Sink flush 失敗（Stream Load / DB write 錯誤）
#
# 每層的 mode：kafka | doris | skip | crash
#   kafka — 寫到 Kafka DLQ topic
#   doris — 寫到 Doris error table
#   skip  — 跳過該筆/該 batch，不寫 DLQ，pipeline 繼續（靜默丟棄）
#   crash — 不處理，直接 crash app（重啟後 replay；預設）
#
# 每層可以指向同一個目的地，也可以各走各的。沒寫的層 = 預設 crash。
dlq:
  # ── poll / 反序列化錯誤（on_consumer_error）──
  # 訊息在進 SDF 之前就壞掉（解析不出 Row）。這是反序列化錯誤的唯一攔截點，
  # processing_error 的 DLQ 攔不到。skip/kafka/doris = 跳過該筆壞訊息繼續；
  # crash = 整條 pipeline 停在這筆（重啟 replay → 同筆再炸 → CrashLoopBackOff）。
  consumer_error:
    mode: skip                         # kafka | doris | skip | crash（建議 skip 或 kafka）
    # kafka:
    #   topic: "dlq.consumer-errors"

  # ── SDF pipeline 錯誤（on_processing_error）──
  # apply/filter/update 拋異常時，該筆 message 怎麼處理
  processing_error:
    mode: kafka                        # kafka | doris | skip | crash
    kafka:
      topic: "dlq.processing-errors"   # 固定 topic name，或留空用預設 dlq.processing-{group}
    # doris:                           # mode=doris 時取消註解
    #   host: doris-fe
    #   http_port: 8030
    #   database: error_log
    #   table: __dlq_processing

  # ── produce 到 Kafka 錯誤（on_producer_error）──
  # to_topic() / changelog 寫 Kafka 時序列化或投遞失敗
  producer_error:
    mode: crash                        # kafka | doris | skip | crash
    # kafka:
    #   topic: "dlq.producer-errors"

  # ── Sink flush 錯誤（on_stream_load_error）──
  # DorisSink Stream Load 失敗時，整個 failed batch 怎麼處理
  sink_error:
    mode: doris                        # kafka | doris | skip | crash
    # kafka:
    #   topic: "dlq.sink-errors"
    doris:
      host: doris-fe
      http_port: 8030
      database: error_log
      table: __dlq_sink
      username: root
      passwordSecretRef:
        name: quix-passthrough-vault
        key: doris-dlq-password

  # ── 共用 Doris error table 設定（可選）──
  # 如果多層都用 doris 且想寫到同一張表，可以用 shared_doris 省掉重複設定
  # 任何一層（consumer/processing/producer/sink）沒寫自己的 doris 就 fallback 到這裡
  # shared_doris:
  #   host: doris-fe
  #   http_port: 8030
  #   database: error_log
  #   table: __dlq
  #   username: root
  #   passwordSecretRef:
  #     name: quix-passthrough-vault
  #     key: doris-dlq-password

# ── 組合範例 ──────────────────────────────────────────────────
#
# 範例 1：兩層都走 Kafka DLQ（最簡單）
#   dlq:
#     processing_error:
#       mode: kafka
#     sink_error:
#       mode: kafka
#
# 範例 2：SDF 錯誤跳過，Sink 錯誤寫 Doris（推薦 CDC 場景）
#   dlq:
#     processing_error:
#       mode: skip              # 格式錯的資料直接丟棄
#     sink_error:
#       mode: doris             # Doris 暫時掛了 → 寫 error table
#
# 範例 3：兩層都寫到同一張 Doris error table
#   dlq:
#     processing_error:
#       mode: doris
#     sink_error:
#       mode: doris
#     shared_doris:
#       host: doris-fe
#       database: error_log
#       table: __dlq
#
# 範例 4：Sink 錯誤直接 crash（at-least-once 最安全）
#   dlq:
#     processing_error:
#       mode: skip
#     sink_error:
#       mode: crash
#
# 使用 mode=doris 前需要先在 Doris 建立 error table：
#
#   CREATE TABLE `error_log`.`__dlq_sink` (
#       `error_time`    DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
#       `source`        VARCHAR(64)     NOT NULL COMMENT 'processing_error or sink_error',
#       `failed_table`  VARCHAR(256)    NOT NULL,
#       `error_message` TEXT            NOT NULL,
#       `row_data`      JSON            NOT NULL
#   )
#   DUPLICATE KEY(`error_time`, `source`, `failed_table`)
#   DISTRIBUTED BY HASH(`failed_table`) BUCKETS AUTO;

# ── Group 定義 ──────────────────────────────────────
# 所有設定（K8s 部署 + topic 分組 + sink）都在這裡，一個檔案管所有
# 新增/移除 group 只需修改這裡，helm upgrade 一次部署完成
groups:
  # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  # Group 1: orders — 單一 sink + per-table 覆寫
  # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  #
  # 場景：訂單相關的 3 張 OLTP table，由 Debezium CDC 產生 change event
  #       寫入 source Kafka cluster。Quix Streams 從這些 topic 消費後：
  #       - orders, order_items → 轉發到下游 Kafka cluster 給即時服務消費
  #       - order_history → 寫入 Postgres 做報表查詢（不需要即時）
  #
  # Source topics（Debezium CDC 產生，格式：dbserver.schema.table）：
  #   cdc.public.orders       — 訂單主表（order_id, user_id, total, created_at）
  #   cdc.public.order_items  — 訂單明細（item_id, order_id, product_id, qty, price）
  #   cdc.public.order_history — 訂單狀態變更紀錄（order_id, old_status, new_status, changed_at）
  #
  # 資料流：
  #   cdc.public.orders       ──→ downstream-kafka:9092 topic "cdc.public.orders"（同名轉發）
  #   cdc.public.order_items  ──→ downstream-kafka:9092 topic "cdc.public.order_items"（同名轉發）
  #   cdc.public.order_history ──→ Postgres table "order_history"（per-table 覆寫）
  #
  # Kafka sink 沒指定 target_topic 時，預設寫到和 source 同名的 topic。
  # {} 代表「用 default_sink、不覆寫」。
  #
  orders:
    replicas: 2
    autoscaling:
      enabled: true
      lagThreshold: 1000
      maxReplicas: 5
    config:
      default_sink:
        type: kafka
        broker: downstream-kafka:9092
      topics:
        cdc.public.orders: {}          # → downstream-kafka topic "cdc.public.orders"
        cdc.public.order_items: {}     # → downstream-kafka topic "cdc.public.order_items"
        cdc.public.order_history:      # → Postgres table "order_history"
          sink:
            type: postgres
            dsn: ${POSTGRES_DSN}
            target_table: order_history

  # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  # Group 2: users — 不同 table 不同 sink
  # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  #
  # 場景：用戶相關的 3 張 OLTP table，大部分寫到 Doris 做 OLAP 分析，
  #       但 user_preferences 是高頻小更新（每秒數千筆），不適合頻繁
  #       Stream Load 到 Doris，改寫 Kafka 給下游即時推薦系統消費。
  #
  # Source topics：
  #   cdc.public.users            — 用戶主表（user_id, name, email, created_at）
  #   cdc.public.user_profiles    — 用戶個人資料（user_id, avatar, bio, updated_at）
  #   cdc.public.user_preferences — 用戶偏好設定（user_id, key, value, updated_at）
  #
  # 資料流：
  #   cdc.public.users         ──→ Doris database "dwd", table "dwd_users"
  #   cdc.public.user_profiles ──→ Doris database "dwd", table "dwd_users"（同一張表）
  #   cdc.public.user_preferences ──→ downstream-kafka topic "cdc.public.user_preferences"
  #
  # users 和 user_profiles 兩張 source table 寫到同一張 Doris table "dwd_users"，
  # 因為它們共用 user_id 作為 key，在 Doris 用 partial_update: flexible 合併欄位。
  # CDC event 可能只帶 name 或只帶 avatar，flexible 模式讓 Doris 只更新有帶的欄位。
  #
  users:
    replicas: 1
    autoscaling:
      enabled: false
    config:
      default_sink:
        type: doris
        host: doris-fe
        http_port: 8030
        database: dwd
        target_table: dwd_users
        partial_update: flexible
      topics:
        cdc.public.users: {}           # → Doris dwd.dwd_users
        cdc.public.user_profiles: {}   # → Doris dwd.dwd_users（和 users 合併到同一張表）
        cdc.public.user_preferences:   # → downstream-kafka（覆寫，不走 Doris）
          sink:
            type: kafka
            broker: downstream-kafka:9092
            # 沒指定 target_topic，寫到同名 topic "cdc.public.user_preferences"

  # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  # Group 3: payments — Fan-out（同時寫到多個 sink）
  # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  #
  # 場景：支付資料需要同時送到兩個目的地：
  #   1. 下游 Kafka → 即時風控系統消費（毫秒級延遲）
  #   2. Doris → 交易分析報表（秒級延遲可接受）
  #   但 refunds 只需要寫 Postgres 做金額對帳，不需要即時也不需要 OLAP。
  #
  # Source topics：
  #   cdc.public.payments        — 支付記錄（payment_id, order_id, amount, method, status, paid_at）
  #   cdc.public.payment_methods — 支付方式（method_id, user_id, type, card_last4, created_at）
  #   cdc.public.refunds         — 退款記錄（refund_id, payment_id, amount, reason, refunded_at）
  #
  # 資料流（fan-out = 一筆資料同時寫到多個目的地）：
  #   cdc.public.payments        ──→ downstream-kafka topic "cdc.public.payments"
  #                               ──→ Doris dwd.dwd_payments（同時）
  #   cdc.public.payment_methods ──→ downstream-kafka topic "cdc.public.payment_methods"
  #                               ──→ Doris dwd.dwd_payments（同時）
  #   cdc.public.refunds         ──→ Postgres table "refunds"（覆寫，不 fan-out）
  #
  # default_sinks（複數 s）是 list，每筆消費到的 message 會同時寫到 list 裡所有 sink。
  # per-table 用 sink（單數 dict）覆寫可以跳脫 fan-out，改成只寫一個 sink。
  #
  payments:
    replicas: 2
    autoscaling:
      enabled: true
      lagThreshold: 500
      maxReplicas: 8
    config:
      default_sinks:                   # ← list = fan-out
        - type: kafka                  # sink 1: 下游 Kafka
          broker: downstream-kafka:9092
        - type: doris                  # sink 2: Doris
          host: doris-fe
          http_port: 8030
          database: dwd
          target_table: dwd_payments
      topics:
        cdc.public.payments: {}        # → kafka "cdc.public.payments" + Doris dwd.dwd_payments
        cdc.public.payment_methods: {} # → kafka "cdc.public.payment_methods" + Doris dwd.dwd_payments
        cdc.public.refunds:            # → Postgres "refunds"（覆寫，只走 Postgres）
          sink:
            type: postgres
            dsn: ${POSTGRES_DSN}
            target_table: refunds

  # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  # Group 4: logs — per-table fan-out
  # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  #
  # 場景：log 類資料大部分只轉發 Kafka 給 ELK/Loki 等 log pipeline 消費，
  #       但 audit_logs 有合規需求，除了 Kafka 還要寫到 Doris 做長期留存和查詢。
  #       app_logs 和 access_logs 不需要持久化到 DB。
  #
  # Source topics：
  #   cdc.public.app_logs    — 應用程式 log（app_id, level, message, stack_trace, ts）
  #   cdc.public.audit_logs  — 審計 log（user_id, action, resource, ip, ts）—— 合規必須留存
  #   cdc.public.access_logs — HTTP access log（method, path, status, latency, ts）
  #
  # 資料流：
  #   cdc.public.app_logs    ──→ downstream-kafka topic "cdc.public.app_logs"
  #   cdc.public.audit_logs  ──→ downstream-kafka topic "cdc.public.audit_logs"
  #                           ──→ Doris audit.audit_logs（同時，per-table fan-out）
  #   cdc.public.access_logs ──→ downstream-kafka topic "cdc.public.access_logs"
  #
  # 和 Group 3（payments）的差別：
  #   Group 3 = 全 group 預設 fan-out（default_sinks list）
  #   Group 4 = 預設單一 sink，只有 audit_logs 用 sinks（複數 list）做 per-table fan-out
  #
  logs:
    replicas: 3
    autoscaling:
      enabled: false
    resources:
      requests:
        memory: 256Mi
        cpu: 200m
      limits:
        memory: 1Gi
        cpu: "1"
    config:
      default_sink:                    # 單一 dict，不是 list
        type: kafka
        broker: downstream-kafka:9092
      topics:
        cdc.public.app_logs: {}        # → kafka "cdc.public.app_logs"
        cdc.public.audit_logs:         # → kafka + doris（per-table fan-out）
          sinks:                       # ← 複數 list
            - type: kafka
              broker: downstream-kafka:9092
              # → downstream-kafka topic "cdc.public.audit_logs"
            - type: doris
              host: doris-fe
              http_port: 8030
              database: audit
              target_table: audit_logs
              # → Doris audit.audit_logs
        cdc.public.access_logs: {}     # → kafka "cdc.public.access_logs"

  # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  # Group 5: analytics — 動態 table routing (from_topic)
  # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  #
  # 場景：多張 CDC table 都寫到同一個 Doris database "ods"（Operational Data Store），
  #       Doris table 名從 source topic name 自動推導，不需要每張都手寫 target_table。
  #       未來新增 table 只要加一行 topic name，不用寫 target。
  #
  # Source topics：
  #   cdc.public.orders   — 訂單主表（order_id, user_id, total, created_at）
  #   cdc.public.users    — 用戶主表（user_id, name, email, created_at）
  #   cdc.public.payments — 支付記錄（payment_id, order_id, amount, paid_at）
  #
  # 轉換邏輯（resolve_target_name 執行）：
  #   1. 拿 source topic name   → "cdc.public.orders"
  #   2. split(".")              → ["cdc", "public", "orders"]
  #   3. 取 index: -1（最後一段）→ "orders"
  #   4. 加 prefix: "ods_"       → "ods_orders"
  #   5. 最終寫到 Doris           → database "ods", table "ods_orders"
  #
  # 資料流：
  #   cdc.public.orders   ──→ Doris ods.ods_orders
  #   cdc.public.users    ──→ Doris ods.ods_users
  #   cdc.public.payments ──→ Doris ods.ods_payments
  #
  # 新增一張 table 只要加一行：
  #   cdc.public.products: {}    # → 自動推導為 Doris ods.ods_products
  #
  analytics:
    replicas: 2
    autoscaling:
      enabled: false
    config:
      default_sink:
        type: doris
        host: doris-fe
        http_port: 8030
        database: ods
        table_routing:               # ← 取代 target_table
          strategy: from_topic
          index: -1                  # "cdc.public.orders" → "orders"
          prefix: "ods_"            # "orders" → "ods_orders"
      topics:
        cdc.public.orders: {}        # → Doris ods.ods_orders
        cdc.public.users: {}         # → Doris ods.ods_users
        cdc.public.payments: {}      # → Doris ods.ods_payments

  # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  # Group 6: multi_tenant — 動態 table routing (from_field)
  # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  #
  # 場景：多租戶 SaaS 系統，所有租戶的 event 混在同一個 Kafka topic 裡，
  #       每筆 message 的 value 裡有 tenant_id 欄位標示來自哪個租戶。
  #       需要根據 tenant_id 把資料寫到不同的 Doris table（一個租戶一張表）。
  #
  # Source topic：
  #   events.multi_tenant — 多租戶 event（tenant_id, event, user_id, payload, ts）
  #
  # Kafka message 範例（同一個 topic 裡的不同 message）：
  #   {"tenant_id": "acme",   "event": "click",    "user_id": 1, "ts": "..."}
  #   {"tenant_id": "globex", "event": "purchase", "user_id": 2, "ts": "..."}
  #   {"tenant_id": "acme",   "event": "signup",   "user_id": 3, "ts": "..."}
  #
  # 轉換邏輯（resolve_target_name 執行）：
  #   1. 讀取 message value 的 field "tenant_id"
  #   2. 用該值作為 Doris table name
  #   3. 如果 tenant_id 欄位不存在 → 用 default "unknown_tenant"
  #
  # 資料流（同一個 topic，不同 message 寫到不同 table）：
  #   tenant_id="acme"    ──→ Doris tenant_data.acme
  #   tenant_id="globex"  ──→ Doris tenant_data.globex
  #   tenant_id 缺失       ──→ Doris tenant_data.unknown_tenant
  #
  # DorisSink 內部的 table_name 會變成 callable：
  #   lambda item: item.value.get("tenant_id", "unknown_tenant")
  # write() 時 DorisSink 會自動按回傳的 table name 分組，每個 tenant 一次 Stream Load。
  #
  multi_tenant:
    replicas: 2
    autoscaling:
      enabled: false
    config:
      default_sink:
        type: doris
        host: doris-fe
        http_port: 8030
        database: tenant_data
        table_routing:
          strategy: from_field
          field: "tenant_id"         # 讀 value["tenant_id"] 作為 table name
          default: "unknown_tenant"  # tenant_id 缺失時的 fallback
      topics:
        events.multi_tenant: {}      # 一個 topic，根據 tenant_id 寫到多張 Doris table

  # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  # Group 7: etl — 動態 table routing (mapping)
  # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  #
  # 場景：CDC source table 用的是 OLTP 命名（orders, users），
  #       但 Doris 數倉用的是分層命名（dwd_fact_orders, dwd_dim_users），
  #       source → target 沒有統一規則，需要明確的對照表。
  #
  # Source topics：
  #   cdc.public.orders      — 訂單主表（order_id, user_id, total, created_at）
  #   cdc.public.order_items — 訂單明細（item_id, order_id, product_id, qty, price）
  #   cdc.public.users       — 用戶主表（user_id, name, email, created_at）
  #   cdc.public.products    — 商品主表（product_id, name, category, price）
  #
  # 對照表（map 裡的 key 是 source topic，value 是 target Doris table）：
  #   cdc.public.orders      → dwd_fact_orders      (事實表，交易相關)
  #   cdc.public.order_items → dwd_fact_order_items  (事實表，交易明細)
  #   cdc.public.users       → dwd_dim_users         (維度表，用戶屬性)
  #   cdc.public.products    → dwd_dim_products       (維度表，商品屬性)
  #
  # 資料流：
  #   cdc.public.orders      ──→ Doris dwd.dwd_fact_orders
  #   cdc.public.order_items ──→ Doris dwd.dwd_fact_order_items
  #   cdc.public.users       ──→ Doris dwd.dwd_dim_users
  #   cdc.public.products    ──→ Doris dwd.dwd_dim_products
  #
  # 和 from_topic 的差別：
  #   from_topic = 自動推導（split + prefix），適合命名有規則的場景
  #   mapping    = 手動指定，適合 source/target 命名規則不同的場景
  # default 是找不到對照時的 fallback，避免新增 topic 忘了加 map 時直接報錯。
  #
  etl:
    replicas: 1
    autoscaling:
      enabled: false
    config:
      default_sink:
        type: doris
        host: doris-fe
        http_port: 8030
        database: dwd
        table_routing:
          strategy: mapping
          map:
            cdc.public.orders: dwd_fact_orders
            cdc.public.order_items: dwd_fact_order_items
            cdc.public.users: dwd_dim_users
            cdc.public.products: dwd_dim_products
          default: dwd_unknown       # 找不到對照時的 fallback table
      topics:
        cdc.public.orders: {}        # → Doris dwd.dwd_fact_orders
        cdc.public.order_items: {}   # → Doris dwd.dwd_fact_order_items
        cdc.public.users: {}         # → Doris dwd.dwd_dim_users
        cdc.public.products: {}      # → Doris dwd.dwd_dim_products

  # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  # Group 8: kafka_transform — Kafka topic routing (from_topic)
  # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  #
  # 場景：CDC topics 從 source Kafka cluster 轉發到另一個 downstream Kafka cluster，
  #       但 target topic name 需要轉換：去掉 CDC prefix，加上 "processed." 前綴。
  #       下游消費者不需要知道資料來自 CDC，只看到 "processed.orders" 這樣的 topic。
  #
  # Source topics（source Kafka cluster）：
  #   cdc.public.orders   — 訂單 CDC event
  #   cdc.public.users    — 用戶 CDC event
  #   cdc.public.payments — 支付 CDC event
  #
  # 轉換邏輯（和 Group 5 的 Doris table_routing 完全一樣）：
  #   1. 拿 source topic name     → "cdc.public.orders"
  #   2. split(".")                → ["cdc", "public", "orders"]
  #   3. 取 index: -1（最後一段）  → "orders"
  #   4. 加 prefix: "processed."   → "processed.orders"
  #   5. 寫到 downstream Kafka     → topic "processed.orders"
  #
  # 資料流：
  #   source cdc.public.orders   ──→ downstream-kafka topic "processed.orders"
  #   source cdc.public.users    ──→ downstream-kafka topic "processed.users"
  #   source cdc.public.payments ──→ downstream-kafka topic "processed.payments"
  #
  # Kafka sink 用 topic_routing（不是 table_routing），語法完全一樣。
  # 底層共用 resolve_target_name()。
  #
  kafka_transform:
    replicas: 2
    autoscaling:
      enabled: false
    config:
      default_sink:
        type: kafka
        broker: downstream-kafka:9092
        topic_routing:               # ← Kafka 用 topic_routing
          strategy: from_topic
          index: -1                  # "cdc.public.orders" → "orders"
          prefix: "processed."      # "orders" → "processed.orders"
      topics:
        cdc.public.orders: {}        # → downstream-kafka topic "processed.orders"
        cdc.public.users: {}         # → downstream-kafka topic "processed.users"
        cdc.public.payments: {}      # → downstream-kafka topic "processed.payments"
```

#### Routing 策略總覽

`table_routing`（Doris/Postgres）和 `topic_routing`（Kafka）使用相同的 `strategy` 引擎：

| `strategy` | 行為 | Config 範例 | 結果 |
|------------|------|------------|------|
| `static` | 固定名稱 | `table: "my_table"` 或 `topic: "out.orders"` | `"my_table"` |
| `from_topic` | 從 source topic name 擷取 | `index: -1, prefix: "ods_"` | `cdc.public.orders → ods_orders` |
| `from_field` | 從 message value 欄位取 | `field: "tenant_id"` | `value["tenant_id"]` 作為目標名稱 |
| `mapping` | 明確對照表 | `map: {cdc.public.orders: out.orders}` | per-topic 查表 |

- Doris / Postgres 用 `table_routing`，決定寫到哪張 table
- Kafka 用 `topic_routing`，決定寫到哪個 downstream topic
- 兩者語法完全一樣，底層共用 `resolve_target_name()`
- 不用 routing 時，直接用 `target_table` 或 `target_topic` 即可（向後相容）

**Kafka routing 範例：**

```yaml
# source topics → downstream topics，自動加前綴
config:
  default_sink:
    type: kafka
    broker: downstream-kafka:9092
    topic_routing:
      strategy: from_topic
      index: -1                    # cdc.public.orders → orders
      prefix: "processed."        # → processed.orders
  topics:
    cdc.public.orders: {}          # → processed.orders
    cdc.public.users: {}           # → processed.users
```

```yaml
# 明確 source → target topic 對照
config:
  default_sink:
    type: kafka
    broker: downstream-kafka:9092
    topic_routing:
      strategy: mapping
      map:
        cdc.public.orders: analytics.orders
        cdc.public.users: analytics.users
      default: analytics.unknown
  topics:
    cdc.public.orders: {}          # → analytics.orders
    cdc.public.users: {}           # → analytics.users
```

### `templates/configmap.yaml`

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ include "quix-passthrough.fullname" . }}-groups
data:
  groups.yaml: |
    groups:
    {{- range $name, $group := .Values.groups }}
      {{ $name }}:
        {{- toYaml $group.config | nindent 8 }}
    {{- end }}
```

從 `values.yaml` 每個 group 的 `config` 欄位動態生成 `groups.yaml`。
不需要額外維護 `configs/` 目錄，所有設定集中在 `values.yaml` 一個檔案。

### `templates/deployment.yaml`

```yaml
{{- range $name, $group := .Values.groups }}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ include "quix-passthrough.fullname" $ }}-{{ $name }}
  labels:
    {{- include "quix-passthrough.labels" $ | nindent 4 }}
    app.kubernetes.io/group: {{ $name }}
spec:
  {{- if not ($group.autoscaling).enabled }}
  replicas: {{ $group.replicas | default 1 }}
  {{- end }}
  selector:
    matchLabels:
      {{- include "quix-passthrough.selectorLabels" $ | nindent 6 }}
      app.kubernetes.io/group: {{ $name }}
  template:
    metadata:
      labels:
        {{- include "quix-passthrough.selectorLabels" $ | nindent 8 }}
        app.kubernetes.io/group: {{ $name }}
      annotations:
        checksum/config: {{ include (print $.Template.BasePath "/configmap.yaml") $ | sha256sum }}
    spec:
      serviceAccountName: {{ include "quix-passthrough.fullname" $ }}
      containers:
        - name: passthrough
          image: "{{ $.Values.image.repository }}:{{ $.Values.image.tag }}"
          imagePullPolicy: {{ $.Values.image.pullPolicy }}
          env:
            - name: GROUP_NAME
              value: {{ $name | quote }}
            - name: BROKER_ADDRESS
              value: {{ $.Values.broker.address | quote }}
            {{- if $.Values.vault.enabled }}
            {{- range $.Values.vault.keys }}
            - name: {{ .name }}
              valueFrom:
                secretKeyRef:
                  name: {{ include "quix-passthrough.fullname" $ }}-vault
                  key: {{ .vaultKey }}
            {{- end }}
            {{- end }}
            {{- with $.Values.dlq }}
            {{- $shared := .shared_doris }}
            {{- /* 四層各自獨立；沒設的層不產生 env（app 端預設 crash）。
                   每層的 env 由 quix-passthrough.dlqEnv helper 統一渲染。*/}}
            {{- with .consumer_error }}
            {{- include "quix-passthrough.dlqEnv" (dict "prefix" "CONSUMER" "layer" . "group" $name "shared" $shared) | nindent 12 }}
            {{- end }}
            {{- with .processing_error }}
            {{- include "quix-passthrough.dlqEnv" (dict "prefix" "PROCESSING" "layer" . "group" $name "shared" $shared) | nindent 12 }}
            {{- end }}
            {{- with .producer_error }}
            {{- include "quix-passthrough.dlqEnv" (dict "prefix" "PRODUCER" "layer" . "group" $name "shared" $shared) | nindent 12 }}
            {{- end }}
            {{- with .sink_error }}
            {{- include "quix-passthrough.dlqEnv" (dict "prefix" "SINK" "layer" . "group" $name "shared" $shared) | nindent 12 }}
            {{- end }}
            {{- end }}
          volumeMounts:
            - name: config
              mountPath: /config
              readOnly: true
          resources:
            {{- $res := $group.resources | default $.Values.resources }}
            {{- toYaml $res | nindent 12 }}
          livenessProbe:
            httpGet:
              path: /health
              port: 8080
            initialDelaySeconds: 10
            periodSeconds: 30
          readinessProbe:
            httpGet:
              path: /health
              port: 8080
            initialDelaySeconds: 5
            periodSeconds: 10
      volumes:
        - name: config
          configMap:
            name: {{ include "quix-passthrough.fullname" $ }}-groups
{{- end }}
```

### `templates/externalsecret.yaml`

```yaml
{{- if .Values.vault.enabled }}
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: {{ include "quix-passthrough.fullname" . }}-vault
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: vault-backend        # 你的 ClusterSecretStore 名稱
    kind: ClusterSecretStore
  target:
    name: {{ include "quix-passthrough.fullname" . }}-vault
    creationPolicy: Owner
  data:
    {{- range .Values.vault.keys }}
    - secretKey: {{ .vaultKey }}
      remoteRef:
        key: {{ $.Values.vault.path }}
        property: {{ .vaultKey }}
    {{- end }}
{{- end }}
```

### `templates/scaledobject.yaml`

```yaml
{{- range $name, $group := .Values.groups }}
{{- if ($group.autoscaling).enabled }}
---
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: {{ include "quix-passthrough.fullname" $ }}-{{ $name }}
  labels:
    {{- include "quix-passthrough.labels" $ | nindent 4 }}
spec:
  scaleTargetRef:
    name: {{ include "quix-passthrough.fullname" $ }}-{{ $name }}
  minReplicaCount: {{ $group.replicas | default 1 }}
  maxReplicaCount: {{ ($group.autoscaling).maxReplicas | default 10 }}
  triggers:
    - type: kafka
      metadata:
        bootstrapServers: {{ $.Values.broker.address }}
        consumerGroup: passthrough-{{ $name }}
        lagThreshold: {{ ($group.autoscaling).lagThreshold | default 1000 | quote }}
        offsetResetPolicy: earliest
{{- end }}
{{- end }}
```

### `templates/servicemonitor.yaml`

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: {{ include "quix-passthrough.fullname" . }}
  labels:
    {{- include "quix-passthrough.labels" . | nindent 4 }}
spec:
  selector:
    matchLabels:
      {{- include "quix-passthrough.selectorLabels" . | nindent 6 }}
  endpoints:
    - port: metrics
      interval: 30s
      path: /metrics
```

### `templates/_helpers.tpl`

```yaml
{{- define "quix-passthrough.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "quix-passthrough.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "quix-passthrough.labels" -}}
helm.sh/chart: {{ include "quix-passthrough.name" . }}
{{ include "quix-passthrough.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "quix-passthrough.selectorLabels" -}}
app.kubernetes.io/name: {{ include "quix-passthrough.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- /*
quix-passthrough.dlqEnv — 渲染「一層」DLQ 的 DLQ_<PREFIX>_* env vars。
四層（CONSUMER / PROCESSING / PRODUCER / SINK）共用這個 helper，避免重複。
呼叫方式（傳一個 dict）：
  {{ include "quix-passthrough.dlqEnv" (dict "prefix" "CONSUMER" "layer" . "group" $name "shared" $shared) }}
  prefix : env 前綴，如 "PROCESSING"（app 端對應 DLQ_PROCESSING_MODE 等）
  layer  : 該層的設定（.consumer_error / .processing_error / ...）
  group  : group name，用來組預設 topic / table 名稱
  shared : .Values.dlq.shared_doris（doris mode 的 fallback）
預設 topic = dlq.<prefix小寫>-<group>；預設 doris table = __dlq_<prefix小寫>。
*/}}
{{- define "quix-passthrough.dlqEnv" -}}
{{- $prefix := .prefix }}
{{- $layer := .layer }}
{{- $group := .group }}
{{- $lower := lower $prefix }}
{{- $mode := $layer.mode | default "crash" }}
- name: DLQ_{{ $prefix }}_MODE
  value: {{ $mode | quote }}
{{- if eq $mode "kafka" }}
- name: DLQ_{{ $prefix }}_TOPIC
  value: {{ ($layer.kafka).topic | default (printf "dlq.%s-%s" $lower $group) | quote }}
{{- end }}
{{- if eq $mode "doris" }}
{{- $d := $layer.doris | default .shared | default dict }}
- name: DLQ_{{ $prefix }}_DORIS_HOST
  value: {{ $d.host | quote }}
- name: DLQ_{{ $prefix }}_DORIS_HTTP_PORT
  value: {{ $d.http_port | default 8030 | quote }}
- name: DLQ_{{ $prefix }}_DORIS_DATABASE
  value: {{ $d.database | default "error_log" | quote }}
- name: DLQ_{{ $prefix }}_DORIS_TABLE
  value: {{ $d.table | default (printf "__dlq_%s" $lower) | quote }}
- name: DLQ_{{ $prefix }}_DORIS_USERNAME
  value: {{ $d.username | default "root" | quote }}
{{- if $d.passwordSecretRef }}
- name: DLQ_{{ $prefix }}_DORIS_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ $d.passwordSecretRef.name }}
      key: {{ $d.passwordSecretRef.key }}
{{- end }}
{{- end }}
{{- end }}
```

### `templates/_validate.tpl`

Helm template 層的 pre-deploy 檢查。`helm install/upgrade` 時如果設定不合法，
直接 `fail` 擋住，不會產生任何 K8s 資源。

```yaml
{{- /* ── DLQ 設定驗證 ── */}}

{{- $validModes := list "kafka" "doris" "skip" "crash" }}

{{- with .Values.dlq }}
{{- $dlq := . }}
{{- /* 四層共用同一套驗證：mode 合法 + doris mode 需要 doris/shared_doris 設定 */}}
{{- range $layer := list "consumer_error" "processing_error" "producer_error" "sink_error" }}
{{- with index $dlq $layer }}
{{- if not (has .mode $validModes) }}
{{- fail (printf "dlq.%s.mode must be one of %s, got '%s'" $layer ($validModes | join ", ") .mode) }}
{{- end }}
{{- if and (eq .mode "doris") (not (or .doris $dlq.shared_doris)) }}
{{- fail (printf "dlq.%s.mode=doris requires either %s.doris or shared_doris config" $layer $layer) }}
{{- end }}
{{- end }}
{{- end }}

{{- end }}

{{- /* ── Group 設定驗證 ── */}}

{{- range $name, $group := .Values.groups }}

{{- /* 每個 group 必須有 config */}}
{{- if not $group.config }}
{{- fail (printf "groups.%s.config is required" $name) }}
{{- end }}

{{- /* config 必須有 default_sink 或 default_sinks */}}
{{- if and (not $group.config.default_sink) (not $group.config.default_sinks) }}
{{- fail (printf "groups.%s.config must have default_sink or default_sinks" $name) }}
{{- end }}

{{- /* config 必須有 topics */}}
{{- if not $group.config.topics }}
{{- fail (printf "groups.%s.config.topics is required" $name) }}
{{- end }}

{{- /* sink_error DLQ 設定了非 crash mode，但 group 沒有用 doris sink → 警告 */}}
{{- /* （Helm 沒有 warn function，用 printf 到 Notes.txt 提醒） */}}

{{- end }}
```

在 `templates/deployment.yaml` 最上方加一行引用驗證：

```yaml
{{- include "quix-passthrough._validate" . }}
{{- range $name, $group := .Values.groups }}
...
```

這些檢查在 `helm install/upgrade` 時執行，錯誤範例：

```
$ helm upgrade quix-passthrough ./chart/quix-passthrough
Error: execution error at (quix-passthrough/templates/deployment.yaml:1):
  dlq.sink_error.mode=doris requires either sink_error.doris or shared_doris config
```

---

## 日常操作 Runbook

### 新增一個分組

**只需要改 `values.yaml` 一個檔案。** 不需要改 template、不需要改其他檔案。
所有 template 都用 `range .Values.groups` 迴圈，新 group 加進 values 後會自動生成
對應的 Deployment、ConfigMap、ScaledObject 等 K8s 資源。

1. 在 `values.yaml` 加入新 group（K8s 部署 + topic/sink 設定一起寫）：
   ```yaml
   groups:
     new-group:
       replicas: 1
       autoscaling:
         enabled: false
       config:
         default_sink:
           type: kafka
           broker: downstream-kafka:9092
         topics:
           cdc.public.new_table_a: {}
           cdc.public.new_table_b: {}
   ```
2. 部署：
   ```bash
   helm upgrade quix-passthrough ./chart/quix-passthrough -n quix
   ```

`helm upgrade` 會自動：
- 建立新 Deployment `quix-passthrough-new-group`
- 更新 ConfigMap（checksum 變更觸發所有 pods rolling restart）
- 如果 `autoscaling.enabled: true`，建立對應的 KEDA ScaledObject

### 新增 table 到既有分組

1. 在 `values.yaml` 對應 group 的 `config.topics` 下新增 topic：
   ```yaml
   groups:
     orders:
       config:
         topics:
           cdc.public.new_table: {}   # 新增這行
   ```
2. 部署（ConfigMap checksum 變更會觸發 rolling restart）：
   ```bash
   helm upgrade quix-passthrough ./chart/quix-passthrough -n quix
   ```

### 調整 replicas / 開關 KEDA

修改 `values.yaml` 對應 group 的 `replicas` 或 `autoscaling` 設定，然後 `helm upgrade`。

### Rebuild Image（罕見）

```bash
docker build -t your-registry.io/quix-passthrough:1.1.0 -f deploy/Dockerfile .
docker push your-registry.io/quix-passthrough:1.1.0

# 更新 values.yaml image.tag，然後
helm upgrade quix-passthrough ./chart/quix-passthrough -n quix
```

---

## Monitoring Checklist

| 指標 | 來源 | Alert 建議 |
|------|------|-----------|
| Consumer lag per group | KEDA metrics / kafka_exporter | lag > 10000 持續 5 分鐘 |
| Pod restarts | K8s metrics | restart > 3 in 10 min |
| Pod ready status | K8s | ready pods < desired replicas 持續 3 分鐘 |
| Sink write errors | App /metrics endpoint | error rate > 0 持續 1 分鐘 |
| Memory usage | cAdvisor | > 80% of limit 持續 5 分鐘 |
| DLQ topic lag | kafka_exporter | `dlq.processing-*` 或 `dlq.sink-*` 有新 message |
| DLQ Doris error count | Doris SQL | `SELECT COUNT(*) FROM error_log.__dlq_sink WHERE error_time > NOW() - INTERVAL 5 MINUTE` |

---

## 錯誤處理架構

```
Kafka poll → 反序列化成 Row
    │     └── 錯誤？ → on_consumer_error callback        ← 進 SDF 之前；唯一能攔反序列化的點
    │           │       ┌──────────────────────────────────────────┐
    │           │       │ DLQ_CONSUMER_MODE 決定去向：              │
    │           │       │  kafka/doris/skip → 跳過該筆壞訊息，繼續  │
    │           │       │  crash → app crash（同筆會 replay 再炸）  │
    │           │       └──────────────────────────────────────────┘
    ▼
Quix Streams app
    │
    ├── SDF pipeline（apply / filter / update）
    │     └── 錯誤？ → on_processing_error callback
    │           │       ┌──────────────────────────────────────────┐
    │           │       │ DLQ_PROCESSING_MODE 決定去向：            │
    │           │       │  kafka → Kafka DLQ topic                 │
    │           │       │  doris → Doris error table               │
    │           │       │  skip  → 跳過，不寫 DLQ                  │
    │           │       │  crash → app crash，重啟後 replay        │
    │           │       └──────────────────────────────────────────┘
    │
    ├── to_topic() / changelog → produce 到 Kafka
    │     └── 序列化 / 投遞失敗？ → on_producer_error callback
    │           │       ┌──────────────────────────────────────────┐
    │           │       │ DLQ_PRODUCER_MODE 決定去向：              │
    │           │       │  kafka / doris / skip / crash            │
    │           │       └──────────────────────────────────────────┘
    │
    ├── Checkpoint commit → Sink flush
    │     └── DorisSink.write() → _stream_load()
    │           ├── 成功 → 繼續
    │           ├── Publish Timeout → warning，繼續
    │           └── 失敗 → DorisSinkException
    │                 │       ┌──────────────────────────────────────────┐
    │                 │       │ DLQ_SINK_MODE 決定去向：                  │
    │                 │       │  kafka → Kafka DLQ topic                 │
    │                 │       │  doris → Doris error table               │
    │                 │       │  skip  → 跳過整個 batch，不寫 DLQ        │
    │                 │       │  crash → app crash，重啟後 replay        │
    │                 │       └──────────────────────────────────────────┘
    │
    └── Side output（SDF 層級，在 sink 之前）
          sdf.filter(is_invalid).to_topic(dlq_topic)
```

### 四層 DLQ 獨立設定

四層**各自獨立選擇** DLQ 目的地，互不影響。沒設的層 = 預設 `crash`：

| 層級 | 環境變數 prefix | callback | 攔截什麼 | mode 選項 |
|------|----------------|----------|---------|----------|
| **consumer_error** | `DLQ_CONSUMER_*` | `on_consumer_error` | poll / 反序列化失敗（髒 JSON、schema 不符），**進 SDF 前** | kafka / doris / skip / crash |
| **processing_error** | `DLQ_PROCESSING_*` | `on_processing_error` | SDF pipeline 內 apply/filter/update 拋的異常 | kafka / doris / skip / crash |
| **producer_error** | `DLQ_PRODUCER_*` | `on_producer_error` | to_topic / changelog 序列化 / produce 到 Kafka 失敗 | kafka / doris / skip / crash |
| **sink_error** | `DLQ_SINK_*` | `on_stream_load_error` | DorisSink Stream Load 失敗 | kafka / doris / skip / crash |

> `consumer_error` / `processing_error` / `producer_error` 是 Quix Streams 框架層級 callback，跟 sink 類型無關；
> `sink_error` 則只對有實作 error callback 的 sink 有效（見下方「Sink DLQ 支援範圍」）。

每層的 mode：

| mode | 行為 |
|------|------|
| `kafka` | 失敗的 row/batch 寫到 Kafka DLQ topic，pipeline 繼續 |
| `doris` | 失敗的 row/batch 寫到 Doris error table，pipeline 繼續 |
| `skip` | 跳過（丟棄），log warning，pipeline 繼續 |
| `crash` | 不處理，直接 crash app，重啟後從 last committed offset replay |

### Sink DLQ 支援範圍

**`sink_error` DLQ 只對有實作 error callback 的 sink 有效。**
其他 sink 寫入失敗會直接 crash app，`dlq.sink_error` 的設定對它們無效。

| Sink | sink_error DLQ | 失敗行為 |
|------|---------------|---------|
| **DorisSink** | 有效（`on_stream_load_error`） | 根據 `DLQ_SINK_MODE` 處理 |
| **KafkaSink** | 無效 | 直接 crash，重啟後 replay |
| **PostgreSQLSink** | 無效 | 直接 crash，重啟後 replay |
| **其他 community sinks** | 無效 | 直接 crash，重啟後 replay |

`main()` 裡用 `hasattr(sink, '_on_stream_load_error')` 檢查，只對支援的 sink 注入 DLQ handler。
不支援的 sink 不受影響，行為和沒有 `dlq` 設定時完全一樣。

如果你的 group 同時使用 DorisSink + KafkaSink（fan-out），DorisSink 失敗時走 DLQ 繼續，
但 KafkaSink 失敗時仍然會 crash。兩者是獨立的 sink instance，互不影響。

`consumer_error` / `processing_error` / `producer_error` 都是 Quix Streams 框架層級的 callback，
跟 sink 類型無關，對所有 sink 都有效 — 因為它們攔截的是 sink 之前各階段的錯誤。

### 四個 Application callback 一覽

`Application` 接受**四個** callback,對應 pipeline 不同階段。本 chart 已把其中三個 error callback
接進可設定 DLQ(第四個 `on_message_processed` 是觀測用,不是錯誤處理):

| callback | 觸發階段 | 攔截什麼 | 預設行為 | 本 chart |
|----------|---------|---------|---------|:--------:|
| `on_consumer_error` | poll Kafka / **反序列化** | value/key 反序列化失敗(壞 JSON、schema 不符) | `return False` → **crash** | ✓ DLQ_CONSUMER |
| `on_processing_error` | SDF `.process()` | apply / filter / update 內拋的異常 | `return False` → crash | ✓ DLQ_PROCESSING |
| `on_producer_error` | 序列化 / produce 到 Kafka | `to_topic()` / changelog 寫 Kafka 時序列化/送出失敗 | `return False` → crash | ✓ DLQ_PRODUCER |
| `on_message_processed` | 每筆成功處理**之後** | 不是錯誤 — 觀測 hook(topic, partition, offset) | `None`(不做事) | — |

關鍵點：

- **三個 error callback 的約定一致**:callback 回 `True` → 忽略該例外、繼續;回 `False`(或用預設)→ 例外往上拋,app 最終停掉(K8s 重啟 → 從 last committed offset replay)。定義在 `quixstreams/error_callbacks.py`,三個預設(`default_on_consumer_error` / `default_on_processing_error` / `default_on_producer_error`)**都 `return False`**。本 chart 的 `make_*_error_handler` 會在有設定 DLQ 時改成「寫 DLQ + 回 True 跳過」。
- **`on_consumer_error` 是反序列化錯誤的唯一攔截點**:一筆壞掉的訊息會在**進 SDF 之前**就炸,`processing_error` 的 DLQ 攔不到。所以 `consumer_error` 建議設 `skip` 或 `kafka`,否則一筆髒資料會 crash-loop(預設 `crash`)。
- **`on_consumer_error` / `on_producer_error` 必須在建構 `Application` 時傳入**(綁進 internal consumer/producer),不能像 `on_processing_error` 那樣事後設屬性。`main()` 用 late-bound lambda 解掉「callback 需要在建構時給、但 DLQ handler 需要 app」的雞生蛋(見 App 程式碼)。
- **`on_message_processed` 是觀測用**,不是錯誤處理。每筆成功處理後被呼叫,常用來做吞吐統計 — `dashboard/collector`(quix-metrics)的 `MetricsAgent` 就是 monkey-patch 這個 hook 來數每 partition 的訊息量。預設 `None`(零成本),有掛才有事做。本 chart 沒接它(交給獨立的 metrics agent)。

### `values.yaml` 組合範例

```yaml
# 範例 1：SDF 錯誤跳過，Sink 錯誤寫 Doris（推薦 CDC 場景）
dlq:
  processing_error:
    mode: skip                     # 格式錯的 message 直接丟棄
  sink_error:
    mode: doris                    # Doris 暫時掛了 → 寫 error table
    doris:
      host: doris-fe
      database: error_log
      table: __dlq_sink

# 範例 2：兩層都走 Kafka DLQ，但寫到不同的 topic
dlq:
  processing_error:
    mode: kafka
    kafka:
      topic: dlq.processing       # SDF 錯誤 → 這個 topic
  sink_error:
    mode: kafka
    kafka:
      topic: dlq.sink             # Sink 錯誤 → 另一個 topic

# 範例 3：兩層都寫到同一張 Doris error table
dlq:
  processing_error:
    mode: doris
  sink_error:
    mode: doris
  shared_doris:                    # 多層共用同一份 Doris 設定
    host: doris-fe
    database: error_log
    table: __dlq                   # 同一張表，用 source 欄位區分
    username: root
    passwordSecretRef:
      name: quix-passthrough-vault
      key: doris-dlq-password

# 範例 4：Sink 錯誤直接 crash（at-least-once 最安全）
dlq:
  processing_error:
    mode: skip
  sink_error:
    mode: crash                    # Stream Load 失敗 → 停掉，人工介入
```

### DLQ 寫入格式

**Kafka DLQ topic** — 每筆 message：

```json
{
  "failed_table": "ods_orders",
  "error": "Stream Load failed: Status=Fail, Message=...",
  "row": {"order_id": 1001, "amount": 99.5, "__key": "k1", ...}
}
```

**Doris error table** — 每筆 row：

| 欄位 | 範例 | 說明 |
|------|------|------|
| `error_time` | `2025-01-15 10:30:00` | Doris DEFAULT CURRENT_TIMESTAMP |
| `source` | `sink_error` 或 `processing_error` | 哪一層的錯誤 |
| `failed_table` | `ods_orders` | 原本要寫入的目標 table |
| `error_message` | `Stream Load failed: ...` | 完整錯誤訊息 |
| `row_data` | `{"order_id":1001,...}` | 原始 row 資料 JSON |

後續處理：
- Kafka DLQ → 另一個 consumer 消費，人工檢查 / 修正 / 重新投入
- Doris error table → SQL 查詢、Grafana dashboard 接 alert
- 兩者都可以接 S3/GCS 做長期留存

失敗的 rows 直接寫到 Doris 的 error table，不經過 Kafka。
適合你希望所有資料（包括 error）都在 Doris 內查詢的場景。

Error table 需要預先建立（Duplicate Key 表，不做 dedup，保留所有 error 記錄）：

```sql
CREATE TABLE `error_log`.`__dlq` (
    `error_time`    DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `failed_table`  VARCHAR(256)    NOT NULL,
    `error_message` TEXT            NOT NULL,
    `row_data`      JSON            NOT NULL
)
DUPLICATE KEY(`error_time`, `failed_table`)
DISTRIBUTED BY HASH(`failed_table`) BUCKETS AUTO;
```

每筆寫入的 error row：

| 欄位 | 值 | 說明 |
|------|-----|------|
| `error_time` | `2025-01-15 10:30:00` | Doris DEFAULT CURRENT_TIMESTAMP 自動填入 |
| `failed_table` | `ods_orders` | 原本要寫入的目標 table |
| `error_message` | `Stream Load failed: Status=Fail, ...` | 完整錯誤訊息，含 Doris ErrorURL |
| `row_data` | `{"order_id":1001,"amount":99.5,...}` | 原始 row 資料（JSON） |

查詢 error 記錄：

```sql
-- 查看最近的 error
SELECT * FROM error_log.__dlq
ORDER BY error_time DESC
LIMIT 100;

-- 查看某張 table 的 error
SELECT error_time, error_message, JSON_EXTRACT(row_data, '$.order_id') AS order_id
FROM error_log.__dlq
WHERE failed_table = 'ods_orders'
  AND error_time > '2025-01-15';

-- 統計各 table 的 error 數量
SELECT failed_table, COUNT(*) AS error_count
FROM error_log.__dlq
WHERE error_time > NOW() - INTERVAL 1 HOUR
GROUP BY failed_table
ORDER BY error_count DESC;
```

相關環境變數（在 Helm `values.yaml` 的 Deployment env 或 Vault 中設定）：

| 環境變數 | 預設值 | 說明 |
|---------|--------|------|
| `DLQ_MODE` | `kafka` | DLQ 模式：`kafka` / `doris` / `none` |
| `DORIS_DLQ_HOST` | 繼承 `DORIS_HOST` | Error table 所在的 Doris FE host |
| `DORIS_DLQ_HTTP_PORT` | `8030` | Doris FE HTTP port |
| `DORIS_DLQ_USERNAME` | `root` | Doris username |
| `DORIS_DLQ_PASSWORD` | `` | Doris password（建議走 Vault） |
| `DORIS_DLQ_DATABASE` | `error_log` | Error table 所在的 database |
| `DORIS_DLQ_TABLE` | `__dlq` | Error table 名稱 |

**重要**：如果 Doris error table 的 Stream Load 也失敗（例如 Doris 整個掛了），
handler 不會吞錯誤 — 會讓 app crash，避免資料靜默丟失。
這種情況下 app 重啟後會從 last committed offset 重新消費。

---

## FAQ

**Q: 為什麼不用一個 Deployment 管所有 groups？**
A: 不同 group 需要獨立 scale、獨立 restart。一個 group 掛了不應影響其他 group。

**Q: ConfigMap 更新後 pods 會自動重啟嗎？**
A: 會。Deployment template 有 `checksum/config` annotation，ConfigMap 變更會觸發 rolling update。

**Q: 1000 張 tables 的 ConfigMap 會不會超過 1MB 上限？**
A: 不會。每張 table 大約 50 bytes，1000 張 = ~50KB，遠低於 1MB。
即使加上所有 group 的 sink 設定，整個 values.yaml 頂多幾百 KB。

**Q: 怎麼處理 groups.yaml 裡的 `${POSTGRES_DSN}` 變數？**
A: App 啟動時讀取 YAML 後，對 sink config 中的 `${VAR}` 做環境變數替換。環境變數由 Vault → ExternalSecret → K8s Secret → Pod env 注入。
