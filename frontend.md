# Quix Streams 監控前端計劃書

## 1. 整體架構

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Quix Pod (Python)                           │
│                                                                     │
│  ┌───────────┐   ┌──────────────┐   ┌───────────────────────────┐  │
│  │ Application│──▶│ StreamingData │──▶│  MetricsCollector         │  │
│  │  .run()   │   │  Frame(s)    │   │  (統一介面)                │  │
│  └───────────┘   └──────────────┘   │                           │  │
│                                      │  ┌─────────────────────┐ │  │
│                                      │  │ KafkaMetricsExporter│ │  │
│                                      │  │ (預設, 最輕量)      │ │  │
│                                      │  └────────┬────────────┘ │  │
│                                      │           │              │  │
│                                      │  ┌────────┴────────────┐ │  │
│                                      │  │ OTelMetricsExporter │ │  │
│                                      │  │ (業界標準, 選配)    │ │  │
│                                      │  └────────┬────────────┘ │  │
│                                      └───────────┼──────────────┘  │
└──────────────────────────────────────────────────┼──────────────────┘
                                                   │
                    ┌──────────────────────────────┼──────────────┐
                    │              傳輸層                         │
                    │                                             │
                    │  路線 A: Kafka metrics topic                │
                    │  ┌──────────────────────────┐              │
                    │  │ __quix_metrics (compacted)│              │
                    │  └─────────────┬────────────┘              │
                    │                │                            │
                    │  路線 B: OTel Collector                     │
                    │  ┌──────────────────────────┐              │
                    │  │ OTLP gRPC/HTTP → Collector│             │
                    │  │ → Prometheus / PG / etc. │              │
                    │  └─────────────┬────────────┘              │
                    └────────────────┼───────────────────────────┘
                                     │
                    ┌────────────────┼───────────────────────────┐
                    │         Backend Service (Python/Go)        │
                    │                                            │
                    │  ┌────────────┴──────────────┐             │
                    │  │ Kafka Consumer / OTel Recv │             │
                    │  └────────────┬──────────────┘             │
                    │  ┌────────────┴──────────────┐             │
                    │  │ Aggregator + In-Memory     │             │
                    │  │ (滑動視窗聚合)             │             │
                    │  └────────────┬──────────────┘             │
                    │  ┌────────────┴──────────────┐             │
                    │  │ REST API + WebSocket       │             │
                    │  └────────────┬──────────────┘             │
                    └────────────────┼───────────────────────────┘
                                     │
                    ┌────────────────┼───────────────────────────┐
                    │         Svelte Frontend                    │
                    │                                            │
                    │  ┌────────┐ ┌────────┐ ┌───────────────┐  │
                    │  │ DAG    │ │ Offset │ │ 節點流量      │  │
                    │  │ 視覺化 │ │ 監控   │ │ 即時面板      │  │
                    │  └────────┘ └────────┘ └───────────────┘  │
                    │  ┌────────┐ ┌────────┐ ┌───────────────┐  │
                    │  │ 資源   │ │ 錯誤   │ │ Broker        │  │
                    │  │ 監控   │ │ 統計   │ │ 健康狀態      │  │
                    │  └────────┘ └────────┘ └───────────────┘  │
                    └───────────────────────────────────────────┘
```

### 設計原則

- **零侵入**：所有 metrics 收集都透過現有 callback 和 public API，不改動 Quix 核心處理路徑
- **可選啟用**：metrics 收集預設關閉，透過 `Application(enable_metrics=True)` 啟用
- **雙通道**：Kafka topic（零依賴、最輕量）和 OTel（業界標準、可接任何 backend）同時支援

---

## 2. Metrics 傳輸雙通道設計

### 2.1 共用介面：MetricsCollector

```python
class MetricsCollector(Protocol):
    def emit_dag(self, dag_json: dict) -> None: ...
    def emit_throughput(self, node_id: str, count: int, bytes: int, ts: float) -> None: ...
    def emit_offset(self, topic: str, partition: int, committed: int, high_watermark: int) -> None: ...
    def emit_resource(self, cpu: float, mem_mb: float, disk_mb: float) -> None: ...
    def emit_error(self, error_type: str, topic: str, partition: int, detail: str) -> None: ...
    def flush(self) -> None: ...
```

所有 exporter 都實作這個 Protocol，上層程式碼只呼叫 `MetricsCollector`，不關心底層傳輸。

### 2.2 路線 A：KafkaMetricsExporter（預設）

| 項目 | 設計 |
|------|------|
| Topic | `__quix_metrics`，compacted，partition=1 |
| 序列化 | JSON（人類可讀，方便 debug） |
| 傳輸方式 | 複用現有 `InternalProducer`，async produce，不阻塞主處理迴圈 |
| Message key | `{consumer_group}.{metric_type}`（利用 compaction 保留最新狀態） |
| 優點 | 零額外依賴、零額外基礎設施 |
| 缺點 | 只能被自己的 backend 消費 |

**現有可用 API**：
- `InternalProducer.produce()` — 已支援 async produce with delivery callback（`quixstreams/internal_producer.py:181`）
- Producer 已有 `poll()` 機制處理 delivery report，不需額外 thread

**需要新增**：
- `__quix_metrics` topic 的自動建立（可在 `TopicManager` 中加一個 internal topic）
- `KafkaMetricsExporter` class 封裝 produce 邏輯

### 2.3 路線 B：OTelMetricsExporter（選配）

| 項目 | 設計 |
|------|------|
| SDK | `opentelemetry-sdk` + `opentelemetry-exporter-otlp` |
| 傳輸 | 背景 thread，`PeriodicExportingMetricReader`（預設 30s batch） |
| Collector 端 | OTel Collector → Prometheus / PostgreSQL / 任何 OTLP-compatible backend |
| 優點 | 業界標準、可接 Grafana/Datadog/任何 observability 平台 |
| 缺點 | 額外 pip 依賴（`opentelemetry-api`, `opentelemetry-sdk`） |

**需要新增**：
- `OTelMetricsExporter` class，使用 `opentelemetry.metrics` API
- `extras` 依賴組：`pip install quixstreams[otel]`

### 2.4 多 Exporter 組合

```python
class CompositeExporter:
    """同時推送到多個 exporter"""
    def __init__(self, exporters: list[MetricsCollector]):
        self._exporters = exporters

    def emit_throughput(self, ...):
        for exp in self._exporters:
            exp.emit_throughput(...)
```

用戶使用方式：
```python
app = Application(
    broker_address="localhost:9092",
    enable_metrics=True,                    # 啟用 Kafka topic exporter（預設）
    otel_endpoint="http://collector:4317",  # 同時啟用 OTel exporter
)
```

---

## 3. DAG 視覺化

### 3.1 現有 API 分析

**`Stream.full_tree()`**（`quixstreams/core/stream/stream.py:378`）：
- 使用 `graphlib.TopologicalSorter` 遍歷整棵 Stream 樹
- 回傳拓撲排序後的 `List[Stream]`
- 每個 Stream 有 `parents`、`children`、`func`（StreamFunction）
- `func.__class__.__name__` 可得到操作類型（ApplyFunction、FilterFunction 等）
- `func.func.__qualname__` 可得到 user function 名稱

**`DataFrameRegistry._registry`**（`quixstreams/dataframe/registry.py:24`）：
- `dict[str, Stream]`，key 是 topic name，value 是 root Stream
- `compose_all()` 遍歷所有 registry 中的 Stream 並 compose
- `_topics_to_stream_ids` 和 `_stream_ids_to_topics` 提供 topic ↔ stream_id 映射

**`Stream.compose()`**（`quixstreams/core/stream/stream.py:404`）：
- 呼叫 `full_tree()` 取得所有節點
- 反轉後按拓撲序 compose，從 leaf 往 root

### 3.2 DAG JSON 提取方案

```python
def extract_dag(registry: DataFrameRegistry) -> dict:
    """從 DataFrameRegistry 提取完整 DAG 結構"""
    nodes = []
    edges = []
    node_ids = {}  # Stream -> id mapping

    for topic_name, root_stream in registry._registry.items():
        for i, stream in enumerate(root_stream.full_tree()):
            node_id = f"{topic_name}_{id(stream)}"
            node_ids[stream] = node_id
            nodes.append({
                "id": node_id,
                "type": stream.func.__class__.__name__,        # "ApplyFunction"
                "label": stream.func.func.__qualname__,        # "my_transform"
                "source_topic": topic_name,
                "is_root": len(stream.parents) == 0,
                "is_leaf": len(stream.children) == 0,
                "is_merged": stream.is_merged(),
                "is_branched": stream.is_branched(),
            })
            for parent in stream.parents:
                if parent in node_ids:
                    edges.append({
                        "source": node_ids[parent],
                        "target": node_id,
                    })

    return {
        "type": "dag",
        "consumer_group": ...,
        "nodes": nodes,
        "edges": edges,
        "timestamp": time.time(),
    }
```

### 3.3 推送時機

- **啟動時 push 一次**到 metrics topic，key = `{consumer_group}.dag`
- Topic 設為 compacted → 始終保留最新 DAG 快照
- 如果 pipeline 結構變更（理論上不會在 runtime 變），重新 push

### 3.4 前端渲染

使用 **@xyflow/svelte**（前身 Svelte Flow）渲染 DAG：
- 每個 Stream node 渲染為一個方塊，顯示操作類型 + function 名
- Topic 渲染為特殊形狀（圓角）
- merged/branched 節點用不同顏色標示
- 點擊節點顯示即時 throughput + error 資訊

---

## 4. Kafka Offset 監控

### 4.1 現有 API 分析

**`BaseConsumer`**（`quixstreams/kafka/consumer.py:86`）提供完整的 offset 查詢：

| 方法 | 回傳 | 用途 |
|------|------|------|
| `get_watermark_offsets(tp)` | `(low, high)` | partition 的最低和最高 offset |
| `position([tp])` | `list[TopicPartition]` | consumer 目前讀取位置 |
| `committed([tp])` | `list[TopicPartition]` | 最近一次 commit 的 offset |

**Consumer lag 計算**：
```
lag = high_watermark - committed_offset
```

**`BaseCheckpoint._tp_offsets`**（`quixstreams/checkpointing/checkpoint.py:46`）：
- `Dict[Tuple[str, int], int]` — 記錄每個 (topic, partition) 的已處理 offset
- `_total_offsets_processed` — 這個 checkpoint 週期內處理的總 offset 數

### 4.2 Metrics 收集方式

在每次 checkpoint commit 後，收集 offset 資訊：

```python
# 在 checkpoint commit 成功後觸發
for (topic, partition), offset in self._tp_offsets.items():
    low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition))
    committed_list = consumer.committed([TopicPartition(topic, partition)])
    committed_offset = committed_list[0].offset

    metrics_collector.emit_offset(
        topic=topic,
        partition=partition,
        current_offset=offset,
        committed_offset=committed_offset,
        high_watermark=high,
        low_watermark=low,
        lag=high - committed_offset,
    )
```

### 4.3 需要注意

- `get_watermark_offsets()` 是同步 Kafka 呼叫，有網路延遲
- 建議每 N 次 checkpoint 才查一次（如每 30 秒），不要每條訊息都查
- `committed()` 也是同步呼叫，同理需要節流

---

## 5. 節點流量監控

### 5.1 現有 API 分析

**`on_message_processed` callback**（`quixstreams/app.py:1037-1038`）：
```python
# Application 中已有此 callback
if self._on_message_processed is not None:
    self._on_message_processed(topic_name, partition, offset)
```
- 簽名：`Callable[[str, int, int], None]` — (topic, partition, offset)
- 在每條訊息成功處理後觸發

**`MessageContext.size`**（`quixstreams/models/messagecontext.py:49`）：
- `int` 類型，表示訊息的原始大小（bytes）
- 在 `set_message_context()` 時設定

### 5.2 流量追蹤方案

```python
class ThroughputTracker:
    """追蹤每個 topic-partition 的訊息流量"""

    def __init__(self, collector: MetricsCollector, report_interval: float = 10.0):
        self._counts: dict[tuple[str, int], int] = defaultdict(int)
        self._bytes: dict[tuple[str, int], int] = defaultdict(int)
        self._last_report = time.monotonic()
        self._collector = collector
        self._interval = report_interval

    def on_message(self, topic: str, partition: int, offset: int):
        """作為 on_message_processed callback"""
        ctx = message_context()  # 取得當前 MessageContext
        self._counts[(topic, partition)] += 1
        self._bytes[(topic, partition)] += ctx.size

        if time.monotonic() - self._last_report >= self._interval:
            self._flush()

    def _flush(self):
        for (topic, partition), count in self._counts.items():
            bytes_total = self._bytes[(topic, partition)]
            self._collector.emit_throughput(
                node_id=f"{topic}:{partition}",
                count=count,
                bytes=bytes_total,
                ts=time.time(),
            )
        self._counts.clear()
        self._bytes.clear()
        self._last_report = time.monotonic()
```

### 5.3 需要新增

- 在 `Application` 中建立 `ThroughputTracker`，接入 `on_message_processed`
- 如果用戶自己也設了 `on_message_processed`，需要 chain 兩個 callback
- 考慮用 `set_message_context()` 取得 `MessageContext` 來獲取 `size`

---

## 6. 資源監控

### 6.1 資料來源

| 指標 | 來源 | 現有/新增 |
|------|------|-----------|
| CPU 使用率 | `psutil.cpu_percent()` | 新增（psutil 已在很多環境預裝） |
| Memory 使用 | `psutil.Process().memory_info().rss` | 新增 |
| RocksDB 磁碟用量 | `os.path.getsize()` 遍歷 state dir | 新增 |
| State partition info | `StateStoreManager._stores` | 現有 |

### 6.2 現有 API 分析

**`StateStoreManager`**（`quixstreams/state/manager.py:32`）：
- `_state_dir: Path` — state 資料夾路徑
- `_stores: Dict[Optional[str], Dict[str, Store]]` — 所有 store 的 registry
- `stores` property 可取得 `{stream_id: {store_name: store}}`

**`RocksDBStorePartition`**（`quixstreams/state/rocksdb/partition.py:35`）：
- `path: str` — RocksDB 資料夾的絕對路徑
- 可用 `os.path.getsize()` 遍歷計算磁碟用量

### 6.3 收集方案

```python
import psutil
import os

class ResourceCollector:
    def __init__(self, state_manager: StateStoreManager, interval: float = 30.0):
        self._state_manager = state_manager
        self._process = psutil.Process()
        self._interval = interval

    def collect(self) -> dict:
        mem = self._process.memory_info()
        state_dir = self._state_manager._state_dir

        # RocksDB disk usage
        disk_bytes = 0
        if state_dir and state_dir.exists():
            for f in state_dir.rglob("*"):
                if f.is_file():
                    disk_bytes += f.stat().st_size

        return {
            "type": "resource",
            "cpu_percent": psutil.cpu_percent(interval=None),
            "mem_rss_mb": mem.rss / (1024 * 1024),
            "mem_vms_mb": mem.vms / (1024 * 1024),
            "state_disk_mb": disk_bytes / (1024 * 1024),
            "store_count": sum(
                len(stores) for stores in self._state_manager.stores.values()
            ),
            "partition_count": sum(
                len(store.partitions)
                for stores in self._state_manager.stores.values()
                for store in stores.values()
            ),
            "timestamp": time.time(),
        }
```

### 6.4 注意事項

- `psutil` 需要作為 optional dependency：`pip install quixstreams[metrics]`
- 磁碟掃描 (`rglob`) 在大 state 時可能較慢，建議 30s+ 間隔
- CPU 用 non-blocking `interval=None`，回傳上次呼叫以來的平均值

---

## 7. 錯誤統計

### 7.1 現有 API 分析

**三種 error callback**（`quixstreams/error_callbacks.py`）：

| Callback 類型 | 簽名 | 觸發時機 |
|---------------|------|----------|
| `ProcessingErrorCallback` | `(Exception, Optional[Row], Logger) -> bool` | 訊息處理邏輯拋出例外 |
| `ConsumerErrorCallback` | `(Exception, Optional[Message], Logger) -> bool` | Kafka consumer 錯誤 |
| `ProducerErrorCallback` | `(Exception, Optional[Row], Logger) -> bool` | Kafka producer 錯誤 |

**Application 中的使用**（`quixstreams/app.py`）：
- `on_processing_error` — 傳入 `Application.__init__()`
- `on_consumer_error` — 傳入 `Application.__init__()`
- `on_producer_error` — 傳入 `InternalProducer.__init__()`

### 7.2 錯誤攔截方案

包裝原有 callback，在呼叫前攔截錯誤資訊：

```python
class ErrorInterceptor:
    def __init__(self, collector: MetricsCollector, original_cb):
        self._collector = collector
        self._original = original_cb
        self._counts: dict[str, int] = defaultdict(int)

    def __call__(self, exc, context, logger):
        error_type = type(exc).__name__
        self._counts[error_type] += 1
        self._collector.emit_error(
            error_type=error_type,
            detail=str(exc),
            topic=getattr(context, 'topic', None),
            partition=getattr(context, 'partition', None),
        )
        return self._original(exc, context, logger)
```

使用方式：在 `Application.__init__()` 中，如果 `enable_metrics=True`，自動包裝三個 error callback。

### 7.3 Broker 健康狀態

**現有 API**（`quixstreams/kafka/consumer.py:133-228`）：
- `_broker_states: dict[str, str]` — 每個 broker 的狀態（UP/DOWN/INIT 等）
- `_stats_cb()` — 解析 librdkafka stats JSON，追蹤 broker 狀態轉換
- `_broker_unavailable_since` — all-brokers-down 的起始時間

這些資訊已經在 consumer 內部追蹤，可以直接暴露為 metrics：
```python
def collect_broker_health(consumer: BaseConsumer) -> dict:
    return {
        "type": "broker_health",
        "broker_states": dict(consumer._broker_states),
        "all_brokers_down_since": consumer._broker_unavailable_since,
        "brokers_seen_up": list(consumer._brokers_seen_up),
    }
```

---

## 8. Backend Service 設計

### 8.1 架構

```
┌─────────────────────────────────────────┐
│           Backend Service               │
│                                         │
│  ┌─────────────┐  ┌─────────────────┐  │
│  │ Kafka        │  │ OTel Receiver   │  │
│  │ Consumer     │  │ (OTLP gRPC)    │  │
│  │ (__quix_     │  │                 │  │
│  │  metrics)    │  │                 │  │
│  └──────┬───────┘  └───────┬────────┘  │
│         │                  │            │
│  ┌──────┴──────────────────┴────────┐  │
│  │          Aggregator              │  │
│  │  - 滑動視窗聚合 (1m/5m/15m)     │  │
│  │  - DAG 快照暫存                  │  │
│  │  - 最近 N 條錯誤紀錄             │  │
│  └──────────────┬───────────────────┘  │
│                 │                       │
│  ┌──────────────┴───────────────────┐  │
│  │        API Layer                  │  │
│  │  GET  /api/dag                   │  │
│  │  GET  /api/offsets               │  │
│  │  GET  /api/throughput            │  │
│  │  GET  /api/resources             │  │
│  │  GET  /api/errors                │  │
│  │  GET  /api/brokers               │  │
│  │  WS   /ws/live                   │  │
│  └──────────────────────────────────┘  │
└─────────────────────────────────────────┘
```

### 8.2 技術選型

| 元件 | 建議 | 理由 |
|------|------|------|
| Web 框架 | FastAPI | async 原生、WebSocket 支援、自動 OpenAPI 文件 |
| Kafka 消費 | `confluent-kafka` 或 `quixstreams` 本身 | 保持一致性 |
| 即時推送 | WebSocket | 低延遲、Svelte 原生支援 |
| 聚合儲存 | In-memory (dict + deque) | 監控資料不需持久化，重啟後自動重建 |

### 8.3 聚合策略

- **Throughput**：維護 1m / 5m / 15m 滑動視窗，每秒更新
- **Offset lag**：保留最近 100 筆歷史，前端繪製趨勢線
- **錯誤紀錄**：ring buffer 保留最近 1000 條
- **DAG**：最後一筆 compacted message，啟動時從 topic 讀取
- **資源監控**：保留最近 100 筆（每 30s 一筆 ≈ 50 分鐘歷史）

### 8.4 WebSocket 協議

```json
// Client → Server: 訂閱
{ "action": "subscribe", "channels": ["throughput", "offsets", "errors"] }

// Server → Client: 推送
{ "channel": "throughput", "data": { ... }, "ts": 1700000000.0 }
```

---

## 9. Svelte 前端設計

### 9.1 技術棧

| 元件 | 選擇 | 用途 |
|------|------|------|
| Framework | SvelteKit | 路由、SSR |
| DAG 渲染 | @xyflow/svelte | 互動式 DAG 圖 |
| 圖表 | Chart.js + svelte-chartjs | 時序圖（throughput, lag, CPU） |
| UI | Skeleton UI 或 Tailwind | 元件庫 + utility CSS |
| 即時通訊 | 原生 WebSocket | 不需額外庫 |

### 9.2 頁面結構

```
/                        → Dashboard（全域概覽）
/dag                     → DAG 視覺化（全螢幕互動）
/topics/:name            → 單一 Topic 詳情（offset, throughput, errors）
/errors                  → 錯誤列表 + 統計
/resources               → 資源監控面板
```

### 9.3 Dashboard 面板配置

```
┌──────────────────────────────────────────────────────┐
│  Header: consumer_group / app_id / uptime / status   │
├────────────────────────┬─────────────────────────────┤
│                        │  Total Throughput            │
│    DAG Mini View       │  ████████ 1.2k msg/s        │
│    (縮圖 + 點擊展開)   │  ████████ 4.5 MB/s          │
│                        │                             │
├────────────────────────┼─────────────────────────────┤
│  Offset Lag            │  Resources                  │
│  topic-a [0]: 12       │  CPU:  ██████░░ 62%         │
│  topic-a [1]: 0        │  MEM:  ████░░░░ 340 MB      │
│  topic-b [0]: 1,204    │  Disk: ██░░░░░░ 1.2 GB      │
├────────────────────────┼─────────────────────────────┤
│  Recent Errors                                       │
│  ⚠ ProcessingError: KeyError 'x'   3m ago   topic-a │
│  ⚠ ProducerError: BufferFull       12m ago  topic-b │
└──────────────────────────────────────────────────────┘
```

### 9.4 DAG 互動功能

- **節點顏色**：依據即時 throughput 做 heatmap（冷→藍, 熱→紅）
- **點擊節點**：顯示該 Stream 的 function 名稱、throughput、error count
- **Hover 邊**：顯示兩節點間的 message 流量
- **Layout**：Dagre 自動排版（左到右）
- **搜尋**：快速定位 function 名稱

---

## 10. Metrics Message Schema 設計

所有 metrics 共用信封格式，每種 metric 有自己的 payload：

### 10.1 信封（Envelope）

```json
{
  "version": 1,
  "consumer_group": "my-group",
  "app_id": "my-app-12345",
  "hostname": "pod-xyz",
  "type": "throughput | offset | resource | error | dag | broker_health",
  "timestamp": 1700000000.123,
  "payload": { ... }
}
```

**Kafka topic key**：`{consumer_group}.{type}`（配合 compaction）

### 10.2 各類型 Payload

**DAG**（啟動時 push 一次）：
```json
{
  "nodes": [
    {
      "id": "topic-a_140234567",
      "type": "ApplyFunction",
      "label": "transform_value",
      "source_topic": "topic-a",
      "is_root": true,
      "is_leaf": false,
      "is_merged": false,
      "is_branched": false
    }
  ],
  "edges": [
    { "source": "topic-a_140234567", "target": "topic-a_140234890" }
  ]
}
```

**Throughput**（每 10 秒彙報一次）：
```json
{
  "partitions": {
    "topic-a:0": { "count": 1234, "bytes": 5678900, "rate_msg_s": 123.4, "rate_bytes_s": 567890 },
    "topic-a:1": { "count": 1100, "bytes": 5100000, "rate_msg_s": 110.0, "rate_bytes_s": 510000 }
  },
  "total_count": 2334,
  "total_bytes": 10778900,
  "window_seconds": 10
}
```

**Offset**（每 30 秒彙報一次）：
```json
{
  "partitions": {
    "topic-a:0": {
      "committed": 99000,
      "position": 99050,
      "high_watermark": 99100,
      "low_watermark": 50000,
      "lag": 100
    }
  }
}
```

**Resource**（每 30 秒彙報一次）：
```json
{
  "cpu_percent": 62.3,
  "mem_rss_mb": 340.5,
  "mem_vms_mb": 1200.0,
  "state_disk_mb": 1234.5,
  "store_count": 3,
  "partition_count": 6
}
```

**Error**（即時推送）：
```json
{
  "error_type": "ProcessingError",
  "exception_class": "KeyError",
  "message": "'missing_field'",
  "topic": "topic-a",
  "partition": 0,
  "offset": 99042,
  "traceback": "Traceback (most recent call last):\n  ..."
}
```

**Broker Health**（每 30 秒彙報一次）：
```json
{
  "brokers": {
    "broker-1:9092": { "state": "UP", "node_id": 1 },
    "broker-2:9092": { "state": "UP", "node_id": 2 },
    "broker-3:9092": { "state": "DOWN", "node_id": 3 }
  },
  "all_brokers_down_since": null
}
```

---

## 11. 對 Pipeline 效能的影響分析

### 11.1 Kafka Topic Exporter（路線 A）

| 操作 | 成本 | 影響程度 |
|------|------|----------|
| `producer.produce()` (async) | ~10μs（放入 buffer 即返回） | 極低 |
| JSON 序列化 | ~5μs per message | 極低 |
| `get_watermark_offsets()` | ~1-5ms（Kafka broker 往返） | 中等，需節流 |
| `committed()` | ~1-5ms | 中等，需節流 |
| `psutil.cpu_percent()` | ~0.1ms | 極低 |
| 磁碟掃描 (state dir) | ~10-100ms（視 state 大小） | 中等，30s+ 間隔 |

**整體評估**：主要開銷在 offset 查詢和磁碟掃描，均已限制為 30 秒間隔。async produce 對主處理迴圈幾乎無影響。以 10 萬 msg/s 的 pipeline 為例：

- Throughput metrics：每 10s 一筆 produce ≈ 可忽略
- Offset metrics：每 30s 若有 10 個 partition ≈ 10 次 Kafka RPC ≈ 50ms
- Resource metrics：每 30s 一次 ≈ 100ms
- **總額外 CPU 開銷 < 0.1%**

### 11.2 OTel Exporter（路線 B）

| 操作 | 成本 | 影響程度 |
|------|------|----------|
| OTel SDK metric recording | ~1μs per data point | 極低 |
| Background export thread | 獨立 thread，30s batch | 零（不阻塞主迴圈） |
| gRPC/HTTP 傳輸 | 背景 thread 處理 | 零 |

**整體評估**：OTel SDK 設計上就是低開銷的。`PeriodicExportingMetricReader` 在獨立 thread 運作，主迴圈只需做 counter increment（原子操作）。

### 11.3 風險緩解

| 風險 | 緩解措施 |
|------|----------|
| Metrics topic produce 失敗 | fire-and-forget，不影響主迴圈，靜默記 log |
| OTel Collector 不可達 | SDK 內建 retry + 背景 thread，主迴圈不受影響 |
| psutil import 失敗 | Optional dependency，fallback 到不收集資源 metrics |
| 磁碟掃描太慢 | 設定上限（如 >10GB 就跳過掃描，只回報「超過 10GB」） |
| `get_watermark_offsets()` 延遲過高 | 設 timeout，失敗就跳過這輪 |

---

## 附錄：現有 Codebase 能力盤點

### 可直接使用（不需修改 Quix 核心）

| 能力 | 來源 | 檔案位置 |
|------|------|----------|
| DAG 拓撲提取 | `Stream.full_tree()` | `quixstreams/core/stream/stream.py:378` |
| Stream 操作類型 | `stream.func.__class__.__name__` | `quixstreams/core/stream/stream.py:102` |
| Function 名稱 | `stream.func.func.__qualname__` | `quixstreams/core/stream/stream.py:102` |
| Topic → Stream 映射 | `DataFrameRegistry._registry` | `quixstreams/dataframe/registry.py:24` |
| Stream ID ↔ Topic 映射 | `_topics_to_stream_ids` / `_stream_ids_to_topics` | `quixstreams/dataframe/registry.py:27-28` |
| Watermark offsets | `consumer.get_watermark_offsets()` | `quixstreams/kafka/consumer.py` |
| Consumer position | `consumer.position()` | `quixstreams/kafka/consumer.py` |
| Committed offsets | `consumer.committed()` | `quixstreams/kafka/consumer.py` |
| Message size | `MessageContext.size` | `quixstreams/models/messagecontext.py:49` |
| 訊息處理 callback | `on_message_processed` | `quixstreams/app.py:1037` |
| Error callbacks | 3 種 error callback | `quixstreams/error_callbacks.py` |
| Broker 狀態追蹤 | `_broker_states`, `_stats_cb()` | `quixstreams/kafka/consumer.py:133-228` |
| Checkpoint offset 追蹤 | `_tp_offsets`, `_total_offsets_processed` | `quixstreams/checkpointing/checkpoint.py:46-56` |
| State store 資訊 | `StateStoreManager.stores` | `quixstreams/state/manager.py:81` |
| Async produce | `InternalProducer.produce()` | `quixstreams/internal_producer.py:181` |
| State dir path | `StateStoreManager._state_dir` | `quixstreams/state/manager.py:57` |

### 需要新增的元件

| 元件 | 說明 | 複雜度 |
|------|------|--------|
| `MetricsCollector` Protocol | 統一 metrics 收集介面 | 低 |
| `KafkaMetricsExporter` | Produce metrics 到 Kafka topic | 低 |
| `OTelMetricsExporter` | 用 OTel SDK 推送 metrics | 中 |
| `ThroughputTracker` | 攔截 `on_message_processed`，計算流量 | 低 |
| `ErrorInterceptor` | 包裝 error callbacks，攔截錯誤 | 低 |
| `ResourceCollector` | psutil + 磁碟掃描 | 低 |
| `DAGExtractor` | 從 `DataFrameRegistry` 提取 DAG JSON | 低 |
| Backend Service | 獨立服務，consume metrics + REST/WS API | 中 |
| Svelte Frontend | Dashboard + DAG viewer + 各面板 | 高 |
| `__quix_metrics` topic 自動建立 | 在 `TopicManager` 中註冊 internal topic | 低 |
