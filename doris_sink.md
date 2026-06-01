# Doris Sink — Stream Load 設計與實作

## Overview

基於 Apache Doris 的 [Stream Load](https://doris.apache.org/docs/data-operate/import/import-way/stream-load-manual/) HTTP API，實作 Quix Streams 的 `BatchingSink`。

**不依賴 pydoris**（它是 SQLAlchemy dialect），直接用 `requests` 對 Doris FE 發 HTTP PUT，走 JSON 格式寫入。
使用 **orjson** 做 JSON 序列化（比標準 `json` 快 3-10x，原生支援 `datetime`，輸出 `bytes` 免二次 encode）。

## Architecture

```
Quix Streams App
    │
    ▼
BatchingSink.flush()
    │  收集一個 checkpoint 週期內的 SinkItems
    ▼
DorisSink.write(batch)
    │  1. 將 batch 按 table_name 分組
    │  2. 每組序列化成 JSON Lines (ndjson) via orjson
    │  3. HTTP PUT → Doris FE Stream Load API
    ▼
Doris FE → BE
    │  FE 轉發到 BE 執行實際寫入
    ▼
Doris Table
```

## Stream Load API 摘要

| 項目 | 值 |
|------|-----|
| Endpoint | `PUT http://{fe_host}:{fe_http_port}/api/{db}/{table}/_stream_load` |
| Auth | HTTP Basic Auth |
| Format | JSON（`read_json_by_line: true` + `strip_outer_array: false`） |
| Body | JSON Lines — 每行一個 JSON object |
| Response | JSON with `Status: Success/Fail`，`NumberTotalRows`，`NumberLoadedRows` 等 |

關鍵 Headers：

```
format: json
read_json_by_line: true
Expect: 100-continue
strip_outer_array: false
```

## 設計決策

### 為什麼用 orjson 而非標準 json？

- 序列化速度快 3-10x，1000 tables 高吞吐場景效能有感
- 原生支援 `datetime`（自動轉 ISO 格式），不需要自訂 serializer
- 輸出直接是 `bytes`，省掉 `.encode("utf-8")` 的記憶體複製
- `_orjson_default` 只需處理 `Decimal`，其餘 orjson 內建搞定

### 為什麼用 JSON Lines 而非 CSV？

- CDC 來源的 schema 可能動態變化，JSON 不需要預先定義 column order
- 避免 CSV escape 問題（value 含逗號、換行等）
- Doris Stream Load 原生支援 `read_json_by_line`

### 為什麼不用 pydoris？

- pydoris 是 SQLAlchemy dialect（走 MySQL protocol），不是 Stream Load client
- pydoris 的 `DorisClient.write()` 過於簡陋（無 retry、無錯誤處理、無 JSON 支援預設）
- 直接用 `requests` 更輕量，只需要一個 HTTP PUT

### Table Name 策略

與 PostgreSQL sink 相同，支援：
- 固定 string：所有 records 寫入同一張 table
- Callable：根據 `SinkItem` 動態決定 table name（適用多 topic → 多 table）

### Partial Column Update（部分列更新）

Doris Unique Key 表支援只更新部分欄位，而不需要每次都寫完整 row。
DorisSink 透過 `partial_update` 參數支援三種模式：

| 模式 | `partial_update=` | 行為 | 適用場景 |
|------|-------------------|------|---------|
| **完整寫入** | `"none"`（預設） | 每筆 row 包含所有欄位，整行 insert 或 replace | 資料完整的一般寫入 |
| **固定欄位** | `"fixed"` | 整個 batch 只更新 `partial_update_columns` 指定的欄位 | 定期更新特定欄位（如 `order_status`） |
| **彈性欄位** | `"flexible"` | 每筆 row 可以更新不同的欄位（Doris 3.1+） | CDC 場景：每筆 event 可能帶不同欄位 |

前置條件：
- Doris table 必須是 **Unique Key 模型**（Merge-on-Write）
- 每筆 row 必須包含**所有 Key columns**
- `"flexible"` 模式只支援 JSON 格式（本 sink 預設就是 JSON，符合要求）
- `"fixed"` 模式的 `partial_update_columns` 必須包含所有 Key columns

底層 Stream Load headers：
- `"fixed"` → `partial_columns: true` + `columns: col1,col2,...`
- `"flexible"` → `unique_key_update_mode: UPDATE_FLEXIBLE_COLUMNS`

### Merge Type（資料合併策略）

控制匯入資料如何與 Doris 表中既有資料合併，適用於 Unique Key 表。

| `merge_type=` | 行為 | 適用場景 |
|----------------|------|---------|
| `"APPEND"`（預設） | 所有 row 直接新增或覆蓋（Unique Key 表按 key 覆蓋） | 一般寫入 |
| `"DELETE"` | 刪除 key 匹配的既有 row | CDC DELETE event |
| `"MERGE"` | 符合 `delete_condition` 的 row 刪除，其餘新增 | CDC 混合 INSERT + DELETE |

`"MERGE"` 模式必須搭配 `delete_condition`（SQL WHERE 表達式），例如：
- `delete_condition="op_type='DELETE'"` — 當 CDC event 的 `op_type` 欄位為 DELETE 時刪除該 row

### Sequence Column（版本排序控制）

Unique Key 表中，當多筆 row 有相同 key 時，Doris 需要決定哪筆「贏」。
`sequence_column` 指定用哪個欄位的值來比較 — 值大的保留。

典型用途：
- CDC 場景用 `updated_at` 或 `binlog_position` 確保亂序到達的 event 不會用舊值覆蓋新值
- 搭配 `merge_type="DELETE"` 時，只有 sequence 值 >= 既有 row 的才能成功刪除

底層 header：`function_column.sequence_col: <column_name>`

### Hidden Columns（系統隱藏欄位）

Doris 表有兩個隱藏的系統欄位，可以直接在資料中控制：

| 隱藏欄位 | 用途 | 值 |
|---------|------|-----|
| `__DORIS_DELETE_SIGN__` | 標記 row 為刪除（軟刪除） | `1` = 刪除, `0` = 保留 |
| `__DORIS_SEQUENCE_COL__` | 控制 row 替換順序 | 任意可比較的值 |

當你的 CDC event 已經帶了 delete flag（例如 Debezium 的 `__deleted`），可以直接映射到
`__DORIS_DELETE_SIGN__`，不需要用 `merge_type="DELETE"` 分兩次寫入。

使用時需要在 `hidden_columns` 中聲明，讓 Doris 知道資料裡包含這些系統欄位。

### Send Batch Parallelism（寫入並行度）

`send_batch_parallelism` 控制 BE 節點間批次資料傳送的並行度。
預設由 BE 的 `max_send_batch_parallelism_per_job` 限制。
資料量大、BE 節點多時可以適當調高（如 `4` 或 `8`）來提升吞吐。

### 錯誤處理與 Dead Letter Queue

#### 錯誤傳播路徑

```
DorisSink._stream_load()
    │
    ├── 成功 → log info，繼續
    ├── Publish Timeout → log warning，繼續（資料已 commit）
    └── 失敗 → DorisSinkException
              │
              ├── on_stream_load_error 有設定？
              │     ├── 有 → 呼叫 callback(table, rows, exception)，不 raise，pipeline 繼續
              │     └── 沒有 → raise → BatchingSink.flush() → Checkpoint.commit() → app crash
              │
              └── app 重啟後從 last committed offset 重新消費
```

#### 預設行為（不設 callback）

Stream Load 失敗 → `DorisSinkException` → 整個 app crash → 重啟後 replay。
這是最安全的，保證 at-least-once，但任何一張 table 失敗會卡住所有 table。

#### `on_stream_load_error` callback（Dead Letter / Error Routing）

設定 `on_stream_load_error` 後，Stream Load 失敗**不會 crash app**。
Callback 接收 `(table_name, failed_rows, exception)`，你可以：
- 寫到 DLQ Kafka topic
- 寫到 Doris error table
- 寫到本地檔案
- 送 alert

**重要**：使用 callback 代表你接受這些 row 不會寫到原本的目標 table。
如果 callback 本身也失敗（拋出異常），該異常會直接 propagate，app 仍然會 crash。

#### Side Output（SDF 層級）

Quix Streams 的 `StreamingDataFrame` 支援 filter + branch，可以在 SDF 層級做 side output：

```python
sdf = app.dataframe(topic)

# 正常資料 → Doris
sdf.filter(lambda v: v.get("is_valid", True)).sink(doris_sink)

# 異常資料 → DLQ topic
dlq_topic = app.topic("dlq.my_topic")
sdf.filter(lambda v: not v.get("is_valid", True)).to_topic(dlq_topic)
```

#### `on_processing_error`（SDF pipeline 層級）

`Application(on_processing_error=callback)` 可以攔截 SDF pipeline 內的錯誤（如 apply/filter/update 拋的異常）。
callback 回傳 `True` 跳過該筆 message，回傳 `False` 讓 app crash。

**限制**：`on_processing_error` **不 cover sink flush 錯誤**。
Sink 的 `write()` 發生在 `Checkpoint.commit()` 階段，不在 SDF pipeline 內。
所以 DorisSink 的 Stream Load 失敗只能靠 `on_stream_load_error` 處理。

#### 三層錯誤處理總覽

| 層級 | 機制 | 攔截什麼 | 適用 sink | 範例 |
|------|------|---------|----------|------|
| **SDF pipeline** | `sdf.filter()` + side output | 資料本身有問題（格式、欄位缺失） | 所有 sink | 無效資料 → DLQ topic |
| **SDF pipeline** | `on_processing_error` | apply/filter/update 拋的異常 | 所有 sink | JSON parse 失敗 → 跳過 |
| **Sink flush** | `on_stream_load_error` | Stream Load HTTP 失敗 | **僅 DorisSink** | Doris 掛了 → 寫 DLQ |

**Sink DLQ 支援範圍：**

| Sink | sink error DLQ | 失敗行為 |
|------|---------------|---------|
| **DorisSink** | 支援（`on_stream_load_error`） | 根據 callback 處理（寫 DLQ / skip / crash） |
| **KafkaSink** | 不支援 | 直接 crash，重啟後 replay |
| **PostgreSQLSink** | 不支援 | 直接 crash，重啟後 replay |

前兩層（side output + `on_processing_error`）是 Quix Streams 框架層級的，和 sink 類型無關，
發生在資料到達 sink 之前，對所有 sink 都有效。

第三層（`on_stream_load_error`）是 DorisSink 自己實作的，其他 sink 沒有對應的 error callback。
如果 fan-out 同時用 DorisSink + KafkaSink，DorisSink 失敗可以走 DLQ 繼續，
KafkaSink 失敗仍然 crash，兩者互不影響。

- `max_filter_ratio` 預設為 `0`（zero tolerance），可由使用者調整
- Stream Load 是 atomic 的 — 整個 batch 成功或失敗，沒有 partial success

## Usage

### 動態 Table Name（Callable）

當你有多個 Kafka topics 的資料要寫到不同的 Doris tables 時，可以傳入一個 function 作為 `table_name`。
這個 function 接收 `SinkItem`，回傳 table name 字串。

```python
# ── 方式 1：根據 Kafka message 內的欄位決定 table ──
# CDC event 通常帶有來源 table 資訊
# message: {"_table": "orders", "order_id": 1, "amount": 99.5}
doris_sink = DorisSink(
    ...,
    table_name=lambda item: item.value["_table"],
)
# → _table="orders" 的 message 寫到 Doris 的 orders 表
# → _table="users"  的 message 寫到 Doris 的 users 表
```

```python
# ── 方式 2：根據 Kafka message key 決定 table ──
# key: "cdc.public.orders" → table: "orders"
doris_sink = DorisSink(
    ...,
    table_name=lambda item: item.key.split(".")[-1] if item.key else "default",
)
```

```python
# ── 方式 3：自訂 mapping function ──
TABLE_MAP = {
    "cdc.public.orders": "dwd_orders",
    "cdc.public.users": "dwd_users",
    "cdc.public.payments": "dwd_payments",
}

def resolve_table(item):
    # item.value, item.key, item.timestamp, item.offset, item.headers 都可用
    source = item.value.get("_source_table", "unknown")
    return TABLE_MAP.get(source, f"raw_{source}")

doris_sink = DorisSink(
    ...,
    table_name=resolve_table,
)
```

`write()` 內部會按回傳的 table name 自動分組，對每個 table 分別發一次 Stream Load：

```
SinkBatch (mixed messages)
    │
    ├── table_name(item1) → "orders"  ─┐
    ├── table_name(item2) → "users"   ─┤
    ├── table_name(item3) → "orders"  ─┤
    │                                   ▼
    │                          分組：
    │                          orders: [item1, item3] → Stream Load PUT /api/db/orders
    │                          users:  [item2]        → Stream Load PUT /api/db/users
```

注意：每個 table name 會觸發一次獨立的 HTTP PUT 請求。如果一個 batch 裡有太多不同的 table，
會產生大量小請求。建議搭配 deploy.md 裡的分組策略，讓同一個 pod 處理的 topics 盡量寫同一張 table。

### 基本用法 — value 展開寫入

```python
from quixstreams import Application
from quixstreams.sinks.community.doris import DorisSink

app = Application(broker_address="kafka:9092", consumer_group="my-group")

doris_sink = DorisSink(
    host="doris-fe",
    http_port=8030,
    username="root",
    password="",
    database="my_db",
    table_name="my_table",
)

topic = app.topic("cdc.public.orders")
sdf = app.dataframe(topic)
sdf.sink(doris_sink)
app.run()
```

Kafka message `{"user_id": 1, "amount": 99.5}` 寫入 Doris 的 row：

```json
{"user_id": 1, "amount": 99.5, "__key": "order-123", "__topic": "cdc.public.orders", "__partition": 0, "__offset": 42, "__headers": {}, "__timestamp": "2025-01-15T08:30:00+00:00"}
```

### value 不展開 — 塞進 `__value` JSON 欄位

```python
doris_sink = DorisSink(
    ...,
    flatten_value=False,   # 整個 value dict 存成一個 JSON 欄位
)
```

寫入 Doris 的 row：

```json
{"__value": {"user_id": 1, "amount": 99.5}, "__key": "order-123", "__topic": "cdc.public.orders", ...}
```

Doris table 只需要一個 `__value JSON` 欄位，不需要為每個業務欄位建 column。
適合 schema 經常變動或你只想存原始 JSON 的場景。

### 選擇性寫入 metadata

```python
# 全部 metadata（預設）
DorisSink(..., include_metadata=True)

# 不要 metadata — 只寫業務欄位
DorisSink(..., include_metadata=False)

# 只要特定欄位
DorisSink(..., include_metadata={"key", "offset", "timestamp"})
```

可選的 metadata 欄位：`"key"`, `"topic"`, `"partition"`, `"offset"`, `"headers"`, `"timestamp"`

### 組合範例

| `flatten_value` | `include_metadata` | Doris table 需要的欄位 |
|-----|-----|------|
| `True` | `True` | 業務欄位 + 全部 `__*` 欄位 |
| `True` | `False` | 只有業務欄位 |
| `True` | `{"key", "timestamp"}` | 業務欄位 + `__key` + `__timestamp` |
| `False` | `True` | `__value` (JSON) + 全部 `__*` 欄位 |
| `False` | `False` | 只有 `__value` (JSON) |
| `False` | `{"offset"}` | `__value` (JSON) + `__offset` |

### Partial Column Update（部分列更新）

```python
# ── 固定欄位更新 ──
# 整個 batch 只更新 order_id (key) + order_status
# Doris table: Unique Key on order_id
doris_sink = DorisSink(
    ...,
    partial_update="fixed",
    partial_update_columns=["order_id", "order_status"],
    include_metadata=False,        # partial update 通常不需要 metadata
)
# Kafka message: {"order_id": 1001, "order_status": "shipped"}
# → Stream Load headers: partial_columns:true, columns:order_id,order_status
# → Doris 只更新 order_status，其他欄位保持不變
```

```python
# ── 彈性欄位更新（CDC 場景推薦）──
# 每筆 CDC event 可能帶不同的欄位，Doris 3.1+ 支援
doris_sink = DorisSink(
    ...,
    partial_update="flexible",
    include_metadata=False,
)
# Kafka message 1: {"order_id": 1001, "order_status": "shipped"}
# Kafka message 2: {"order_id": 1002, "amount": 199.5, "updated_at": "2025-01-15"}
# → 每筆 row 各自更新它帶的欄位，不需要一致
# → Stream Load header: unique_key_update_mode:UPDATE_FLEXIBLE_COLUMNS
```

### Merge Type — CDC DELETE / MERGE

```python
# ── 純刪除 ──
# 收到的每筆 message 都代表「刪除這個 key 的 row」
doris_sink = DorisSink(
    ...,
    merge_type="DELETE",
    include_metadata=False,
)
# Kafka message: {"order_id": 1001}
# → Doris 刪除 order_id=1001 的 row
```

```python
# ── 混合 INSERT + DELETE (MERGE) ──
# CDC event 帶 op_type 欄位，DELETE 的刪、其餘的寫入
doris_sink = DorisSink(
    ...,
    merge_type="MERGE",
    delete_condition="op_type='DELETE'",
    include_metadata=False,
)
# Kafka message 1: {"order_id": 1001, "amount": 99.5, "op_type": "INSERT"}  → 寫入
# Kafka message 2: {"order_id": 1002, "op_type": "DELETE"}                  → 刪除
```

### Sequence Column — 防亂序覆蓋

```python
# CDC event 可能亂序到達，用 updated_at 確保舊 event 不覆蓋新 event
doris_sink = DorisSink(
    ...,
    sequence_column="updated_at",
)
# Event 1 (先到): {"order_id": 1, "status": "paid",    "updated_at": "2025-01-15 10:00"}
# Event 2 (後到): {"order_id": 1, "status": "created", "updated_at": "2025-01-15 09:00"}
# → Doris 保留 Event 1（updated_at 較大），Event 2 被丟棄
```

### Hidden Columns — 軟刪除 via __DORIS_DELETE_SIGN__

```python
# Debezium CDC 已經帶了 delete flag，直接映射到 Doris 系統欄位
# 不需要分兩批寫入（一批 INSERT、一批 DELETE），一次搞定
doris_sink = DorisSink(
    ...,
    hidden_columns=["__DORIS_DELETE_SIGN__"],
    include_metadata=False,
)
# Kafka message: {"order_id": 1001, "amount": 99.5, "__DORIS_DELETE_SIGN__": 0}  → 寫入
# Kafka message: {"order_id": 1002, "__DORIS_DELETE_SIGN__": 1}                  → 刪除
```

### Error Handling — DLQ / Error Table

```python
# ── 方式 1：失敗的 batch 寫到 Kafka DLQ topic ──
from quixstreams import Application

app = Application(broker_address="kafka:9092", consumer_group="my-group")
dlq_topic = app.topic("dlq.doris_errors")

def send_to_dlq(table, rows, exception):
    """Stream Load 失敗時，把 failed rows 逐筆寫到 DLQ topic。"""
    with app.get_producer() as producer:
        for row in rows:
            producer.produce(
                topic=dlq_topic.name,
                value=orjson.dumps({
                    "failed_table": table,
                    "error": str(exception),
                    "row": row,
                }),
            )

doris_sink = DorisSink(
    ...,
    on_stream_load_error=send_to_dlq,   # 失敗不 crash，寫 DLQ
)
```

```python
# ── 方式 2：失敗的 batch 寫到 Doris error table ──
#
# 適合所有資料都在 Doris 內查詢的場景，不需要額外的 Kafka DLQ topic。
#
# Step 1: 在 Doris 建立 error table（Duplicate Key，保留所有 error 記錄）
#
#   CREATE TABLE `error_log`.`__dlq` (
#       `error_time`    DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
#       `failed_table`  VARCHAR(256)    NOT NULL,
#       `error_message` TEXT            NOT NULL,
#       `row_data`      JSON            NOT NULL
#   )
#   DUPLICATE KEY(`error_time`, `failed_table`)
#   DISTRIBUTED BY HASH(`failed_table`) BUCKETS AUTO;
#
# Step 2: 建立 error sink + callback

import logging
_logger = logging.getLogger(__name__)

error_sink = DorisSink(
    host="doris-fe",
    http_port=8030,
    username="root",
    password="",
    database="error_log",
    table_name="__dlq",
    flatten_value=False,               # row_data 欄位用 JSON 存整個原始 row
    include_metadata=False,
    # 不設 on_stream_load_error — error sink 失敗就讓 app crash
    # 避免無限遞迴（error sink 失敗 → 再寫 error sink → ...）
)
error_sink.setup()

def send_to_error_table(table, rows, exception):
    """失敗的 rows 連同 error 資訊寫到 Doris error table。

    寫入的每筆 row：
      - error_time:    由 Doris DEFAULT CURRENT_TIMESTAMP 自動填入
      - failed_table:  原本要寫入的目標 table（如 "ods_orders"）
      - error_message: DorisSinkException 完整錯誤訊息（含 ErrorURL）
      - row_data:      原始資料 JSON（包含所有欄位 + metadata）
    """
    error_rows = [
        {
            "failed_table": table,
            "error_message": str(exception),
            "row_data": row,
        }
        for row in rows
    ]
    try:
        error_sink._stream_load("__dlq", error_rows)
        _logger.info(f"Wrote {len(error_rows)} failed rows to error_log.__dlq")
    except Exception as e:
        # error sink 也失敗 → 不吞，讓 app crash，避免資料靜默丟失
        _logger.error(f"Failed to write to error table: {e}")
        raise

doris_sink = DorisSink(
    ...,
    on_stream_load_error=send_to_error_table,
)

# Step 3: 查詢 error 記錄
#
#   -- 最近 100 筆 error
#   SELECT * FROM error_log.__dlq ORDER BY error_time DESC LIMIT 100;
#
#   -- 某張 table 的 error
#   SELECT error_time, error_message, JSON_EXTRACT(row_data, '$.order_id')
#   FROM error_log.__dlq
#   WHERE failed_table = 'ods_orders' AND error_time > '2025-01-15';
#
#   -- 統計各 table error 數量
#   SELECT failed_table, COUNT(*) AS cnt
#   FROM error_log.__dlq
#   WHERE error_time > NOW() - INTERVAL 1 HOUR
#   GROUP BY failed_table ORDER BY cnt DESC;
```

```python
# ── 方式 3：搭配 SDF side output 做完整的三層 error handling ──
app = Application(
    broker_address="kafka:9092",
    consumer_group="my-group",
    # 層級 2：SDF pipeline 內的錯誤（apply/filter 拋異常）→ 跳過該筆
    on_processing_error=lambda exc, row, log: True,
)

topic = app.topic("cdc.public.orders")
dlq_topic = app.topic("dlq.orders")
sdf = app.dataframe(topic)

# 層級 1：SDF side output — 資料本身有問題的提早分流到 DLQ
sdf.filter(lambda v: not v.get("is_valid", True)).to_topic(dlq_topic)
valid = sdf.filter(lambda v: v.get("is_valid", True))

# 層級 3：Sink flush 失敗 — Stream Load error → DLQ
def on_error(table, rows, exc):
    with app.get_producer() as p:
        for row in rows:
            p.produce(topic=dlq_topic.name, value=orjson.dumps(row))

doris_sink = DorisSink(..., on_stream_load_error=on_error)
valid.sink(doris_sink)

app.run()
```

### 進階設定 — 完整 CDC pipeline

```python
doris_sink = DorisSink(
    host="doris-fe",
    http_port=8030,
    username="root",
    password="",
    database="my_db",
    table_name=lambda item: item.value.get("_table", "default"),
    # value 處理
    flatten_value=True,
    include_metadata={"key", "offset", "timestamp"},
    # CDC 寫入策略
    partial_update="flexible",
    sequence_column="updated_at",
    merge_type="MERGE",
    delete_condition="__deleted=1",
    # 效能
    send_batch_parallelism=4,
    timeout_seconds=120,
    max_filter_ratio=0.1,
    extra_headers={"timezone": "Asia/Taipei"},
    # 錯誤處理
    on_stream_load_error=send_to_dlq,  # 失敗不 crash
)
```

## File

`quixstreams/sinks/community/doris.py`

---

## 源碼解說

### 繼承結構

```
BaseSink                    # 定義 lifecycle: setup() → add() → flush() → cleanup()
  └── BatchingSink          # 自動按 (topic, partition) 累積 SinkBatch，flush 時呼叫 write()
        └── DorisSink       # 實作 write()：將 batch 序列化成 JSON Lines，HTTP PUT 到 Doris
```

`BatchingSink` 已經幫你處理了：
- 每筆 message 進來時呼叫 `add()` → 存入 in-memory `_batches` dict
- Checkpoint 時呼叫 `flush()` → 遍歷所有 `_batches` 呼叫 `write(batch)` → 清空 batches
- Backpressure 時呼叫 `on_paused()` → 丟棄 batches

`DorisSink` 只需實作四個 method：`setup()`、`cleanup()`、`add()`、`write()`。

### Lifecycle 流程

```
Application.run()
    │
    ├── 1. BaseSink.start()          # 只呼叫一次
    │       └── DorisSink.setup()    # 建立 requests.Session，驗證 FE 連通性
    │
    ├── 2. 每筆 Kafka message:
    │       └── DorisSink.add()      # 驗證 value 是 dict，交給 BatchingSink 累積
    │
    ├── 3. Checkpoint 到了:
    │       └── BatchingSink.flush()
    │             └── DorisSink.write(batch)
    │                   ├── 按 table_name 分組
    │                   ├── _item_to_row()：根據 flatten_value + metadata_fields 組裝 row
    │                   └── 每組呼叫 _stream_load()（orjson 序列化 → HTTP PUT）
    │
    └── 4. 應用結束:
            └── DorisSink.cleanup()  # 關閉 requests.Session
```

### 逐段解說

#### Import 與依賴隔離 (L1-24)

```python
try:
    import orjson
except ImportError as exc:
    raise ImportError(...) from exc

try:
    import requests
    from requests.auth import HTTPBasicAuth
except ImportError as exc:
    raise ImportError(...) from exc
```

`orjson` 和 `requests` 都是 optional dependency，只有 `pip install quixstreams[doris]` 才會安裝。
分開兩個 `try/except`，讓錯誤訊息明確指出缺的是哪個套件。
這是所有 community sinks 的共同 pattern（參考 `bigquery.py`、`postgresql.py`）。

#### `__init__()` 建構子 (L60-120)

```python
self._table_name = _table_name_setter(table_name)
self._auth = HTTPBasicAuth(username, password)
self._flatten_value = flatten_value
self._partial_update = partial_update
if partial_update == "fixed" and not partial_update_columns:
    raise ValueError(...)
self._partial_update_columns = partial_update_columns
# include_metadata: True → ALL_METADATA_FIELDS, False → set(), Set → 直接使用
if include_metadata is True:
    self._metadata_fields = ALL_METADATA_FIELDS
elif include_metadata is False:
    self._metadata_fields: Set[MetadataField] = set()
else:
    self._metadata_fields = include_metadata
self._session: Optional[requests.Session] = None
```

重點：
- `table_name` 透過 `_table_name_setter()` 統一包裝成 callable。
  如果傳入 `"my_table"`（string），會變成 `lambda sink_item: "my_table"`。
  如果傳入 function，直接使用。這讓 `write()` 裡可以統一用 `self._table_name(item)` 呼叫。
- `include_metadata` 統一轉成 `Set[MetadataField]`，`_item_to_row()` 裡用 `in` 檢查。
  三種輸入（`True`/`False`/`Set`）在 `__init__` 就 normalize 完，後續不需要判斷型別。
- `_flatten_value` 控制 value 展開或塞進 `__value`。
- `_partial_update` 和 `_partial_update_columns`：控制 Doris 部分列更新。
  `"fixed"` 模式強制要求 `partial_update_columns`，在 `__init__` 就驗證。
- `_session` 此時是 `None`，等 `setup()` 才建立。
- `extra_headers` 讓使用者可以傳入任意 Stream Load header（如 `timezone`、`jsonpaths`），
  在 `_stream_load()` 裡會 `headers.update(self._extra_headers)` 覆蓋預設值。

#### `setup()` 連線建立 (L122-134)

```python
def setup(self):
    self._session = requests.Session()
    self._session.should_strip_auth = lambda old_url, new_url: False
    self._session.auth = self._auth

    url = f"http://{self._host}:{self._http_port}/api/bootstrap"
    try:
        self._session.request("GET", url, timeout=10)
    except requests.ConnectionError as e:
        raise DorisSinkException(...) from e
```

- `should_strip_auth = lambda ...: False`：Doris FE 收到 Stream Load 請求後會 302 redirect 到 BE。
  `requests` 預設在跨 host redirect 時會移除 Auth header，這行阻止它這麼做，
  確保 redirect 後 BE 也能收到 Basic Auth credentials。
  參考 pydoris `DorisClient` 也有同樣的處理。
- 連通性檢查打 `GET /api/bootstrap`（Doris FE 已知 endpoint），只 catch `ConnectionError`
  （網路不通、DNS 解析失敗等）。不檢查 HTTP status code，因為 Doris 對此 endpoint 可能回各種 status
  但只要能收到回應就代表網路是通的。
- 如果連不上會拋 `DorisSinkException`，`BaseSink.start()` 會 catch 並觸發
  `_on_client_connect_failure` callback。

#### `cleanup()` 資源釋放 (L136-139)

```python
def cleanup(self):
    if self._session is not None:
        self._session.close()
        self._session = None
```

關閉 `requests.Session` 底層的 TCP connection pool。設為 `None` 避免 cleanup 後誤用。

#### `add()` 輸入驗證 (L155-179)

```python
def add(self, value, key, timestamp, headers, topic, partition, offset):
    if not isinstance(value, Mapping):
        raise TypeError(...)
    return super().add(...)
```

唯一的邏輯：驗證 `value` 必須是 dict-like（`Mapping`），因為不論是展開還是塞 `__value`，
原始資料都必須是可序列化的 dict。
驗證通過後交給 `BatchingSink.add()` 放入 `_batches[(topic, partition)]` 的 `SinkBatch` 裡。

#### `_build_url()` URL 組裝 (L181-186)

```python
def _build_url(self, table: str) -> str:
    return (
        f"http://{self._host}:{self._http_port}"
        f"/api/{quote(self._database, safe='')}"
        f"/{quote(table, safe='')}/_stream_load"
    )
```

`quote(... safe='')` 對 database 和 table name 做 URL encoding。
`safe=''` 表示**所有**特殊字元都要 encode（包括 `/`、`?`、`#`）。
這防止 table name 含特殊字元時改變 URL 結構（例如 `table_name="a/b"` 會變成 `a%2Fb`）。

#### `write()` 批次分組 (L141-155)

```python
def write(self, batch: SinkBatch):
    tables: dict[str, list[dict]] = {}
    for item in batch:
        table = self._table_name(item)
        rows = tables.setdefault(table, [])
        row = _item_to_row(
            item,
            topic=batch.topic,
            partition=batch.partition,
            metadata_fields=self._metadata_fields,
            flatten_value=self._flatten_value,
        )
        rows.append(row)

    for table, rows in tables.items():
        self._stream_load(table, rows)
```

一個 `SinkBatch` 可能包含多個 table 的資料（當 `table_name` 是 callable 時）。
先按 table name 分組，再對每個 table 分別發一次 Stream Load。
Doris Stream Load 每次請求只能寫一張 table，所以這裡必須分組。
`batch.topic` 和 `batch.partition` 是 batch 層級的屬性，傳入 `_item_to_row()` 寫入 metadata。

#### `_stream_load()` 核心寫入 (L188-221)

```python
body = b"\n".join(
    orjson.dumps(row, default=_orjson_default) for row in rows
)

headers = {
    "Expect": "100-continue",
    "format": "json",
    "read_json_by_line": "true",
    "strip_outer_array": "false",
    "label": f"quix_{_sanitize_label(table)}_{uuid.uuid4().hex}",
    "timeout": str(self._timeout_seconds),
    "max_filter_ratio": str(self._max_filter_ratio),
    **self._extra_headers,
}
```

- **序列化**：`orjson.dumps()` 直接輸出 `bytes`，`b"\n".join()` 也是 bytes 操作，
  全程零字串轉換。比原本 `json.dumps() → str → .encode()` 少一次記憶體複製。
- **Body 格式**：JSON Lines（每行一個 JSON object，用 `\n` 分隔）。
  搭配 `read_json_by_line: true`，Doris 會逐行解析。
- **`Expect: 100-continue`**：HTTP 1.1 機制，client 先送 headers，server 回 `100 Continue` 後
  才送 body。避免大 body 被 server 拒絕後白傳。
- **`label`**：Doris 用 label 做冪等性保證 — 相同 label 的 Stream Load 不會重複執行。
  格式 `quix_{sanitized_table}_{uuid}`，其中 `_sanitize_label()` 把非 `[a-zA-Z0-9_]` 的字元
  替換成 `_`（Doris label 只允許這些字元）。完整 32 字元 UUID hex，碰撞機率趨近零。
- **Partial Update headers**：根據 `_partial_update` 模式注入對應 headers：
  - `"fixed"` → `partial_columns: true` + `columns: col1,col2,...`
    告訴 Doris 這個 batch 只會更新指定的欄位，其餘欄位保持原值。
  - `"flexible"` → `unique_key_update_mode: UPDATE_FLEXIBLE_COLUMNS`
    告訴 Doris 每筆 row 可能帶不同的欄位，缺少的欄位保持原值。
  - `"none"` → 不加任何 partial update header，走完整寫入。
- **`headers.update(self._extra_headers)`**：使用者傳入的 headers 最後覆蓋，
  確保 `extra_headers` 有最高優先級。

#### `_handle_response()` 回應處理 (L223-259)

```python
status = result.get("Status")
if status == "Success":
    # log 成功資訊，return
if status == "Publish Timeout":
    # log warning，return（資料已 commit，只是可見性延遲）
# 其他狀態都是失敗，raise DorisSinkException
```

Doris Stream Load 回應三種狀態：
- **`Success`**：寫入成功，log 載入行數和耗時。
- **`Publish Timeout`**：資料已 commit 到 BE 但尚未 publish（可見性延遲）。
  不算失敗，只 warning。資料最終會可見。
- **其他（`Fail`、`Label Already Exists` 等）**：拋 `DorisSinkException`，
  包含 `ErrorURL`（Doris 提供的錯誤詳情連結）供 debug。
  異常會讓 Quix Streams 的 checkpoint 機制 abort 並 retry。

### Helper Functions

#### `_item_to_row()` — SinkItem → dict (L262-283)

```python
def _item_to_row(item, topic, partition, metadata_fields, flatten_value):
    if flatten_value:
        row = dict(item.value)          # 展開：{"user_id": 1, "amount": 99.5}
    else:
        row = {"__value": item.value}   # 不展開：{"__value": {"user_id": 1, ...}}

    if "key" in metadata_fields:
        row["__key"] = item.key
    if "topic" in metadata_fields:
        row["__topic"] = topic
    if "partition" in metadata_fields:
        row["__partition"] = partition
    if "offset" in metadata_fields:
        row["__offset"] = item.offset
    if "headers" in metadata_fields:
        row["__headers"] = _serialize_headers(item.headers)
    if "timestamp" in metadata_fields:
        row["__timestamp"] = datetime.fromtimestamp(item.timestamp / 1000, tz=timezone.utc)
    return row
```

兩種 value 處理模式：
- **`flatten_value=True`**（預設）：`dict(item.value)` 展開所有 key-value 到 row 頂層。
  Doris table 需要對應每個業務欄位的 column。適合 schema 穩定的場景。
- **`flatten_value=False`**：整個 value dict 塞進 `__value` 欄位。
  Doris table 只需要一個 `__value JSON` 或 `__value VARCHAR` column。
  適合 schema 經常變動、或你只想存原始 JSON 再在查詢時 parse。

Metadata 欄位按 `metadata_fields` set 逐一檢查，只加入被選中的。
每個欄位前綴 `__` 避免與業務欄位衝突。
Timestamp 使用 UTC（`tz=timezone.utc`），orjson 會自動序列化成 ISO 格式。

#### `_serialize_headers()` — Kafka Headers → dict (L286-294)

```python
def _serialize_headers(headers: HeadersTuples) -> dict[str, str]:
    if not headers:
        return {}
    result = {}
    for key, value in headers:
        if isinstance(value, bytes):
            result[key] = value.decode("utf-8", errors="replace")
        else:
            result[key] = str(value) if value is not None else None
    return result
```

Kafka headers 是 `list[tuple[str, bytes]]`，轉成 JSON-friendly 的 `dict[str, str]`。
bytes 值 decode 成 UTF-8，無法 decode 的用 `�` 取代。
空 headers 回傳 `{}`（不是 `null`），確保 Doris JSON column 不會收到 null。

#### `_orjson_default()` — orjson 序列化擴充 (L297-300)

```python
def _orjson_default(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, (bytes, bytearray)):
        return bytes(obj).decode("utf-8", errors="replace")
    raise TypeError(...)
```

orjson 的 `default` callback，處理 `Decimal` 和 `bytes`。
其他型別 orjson 原生支援：
- `datetime` → ISO 格式（自動處理時區）
- `int`, `float`, `str`, `bool`, `None` → 原生 JSON
- `list`, `dict` → 遞迴序列化
- `UUID` → orjson 原生支援

注意：orjson **不**原生序列化 `bytes`（會丟 `TypeError`）。Kafka 的 key 在沒有設
`key_deserializer="str"` 時是 `bytes`，所以 `_orjson_default` 比照 `_serialize_headers`
把 `bytes`/`bytearray` decode 成 UTF-8 字串（無法 decode 的字元用 `�` 取代）。

比之前用標準 `json` 時的 `_json_serializer` 精簡很多。

#### `_sanitize_label()` — Label 清理 (L303-306)

```python
_LABEL_INVALID_CHARS = re.compile(r"[^a-zA-Z0-9_]")

def _sanitize_label(name: str) -> str:
    return _LABEL_INVALID_CHARS.sub("_", name)
```

Doris Stream Load label 只允許字母、數字、底線。
例如 `cdc.public.orders` → `cdc_public_orders`。
Regex 在 module level 預編譯，避免每次呼叫重新編譯。

#### `_table_name_setter()` — Table Name 統一化 (L309-314)

```python
def _table_name_setter(table_name):
    if isinstance(table_name, str):
        return lambda sink_item: table_name
    return table_name
```

將 string 或 callable 統一包裝成 callable，讓 `write()` 裡可以一致地用 `self._table_name(item)` 呼叫。

### 與 pydoris DorisClient 的對比

| | pydoris `DorisClient` | `DorisSink` |
|--|----------------------|-------------|
| 序列化 | 標準 `json`（預設 CSV） | `orjson`（JSON Lines，3-10x 更快） |
| Auth redirect | `should_strip_auth = False` | 同 |
| 錯誤處理 | 只 check `status == 200` 回 bool | 解析 response JSON，區分 Success/Timeout/Fail，拋有意義的異常 |
| Label | 無（或手動 set） | 自動 `quix_{table}_{uuid}`，帶 sanitize |
| URL safety | 無 encoding | `quote(safe='')` |
| Session 管理 | 無 close | 有 `cleanup()` |
| Value 模式 | 無選擇 | `flatten_value=True/False` |
| Metadata | 無 | 可選 `include_metadata=True/False/Set` |
| Lifecycle | 無 | 整合 Quix Streams 的 `setup/add/write/flush/cleanup` |
