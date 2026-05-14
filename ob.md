# Quix Streams: 可觀測性（Observability）源碼深度解析

## 目錄

1. [總覽：Quix 提供了哪些觀測手段](#1-總覽)
2. [on_message_processed — 每筆訊息回呼](#2-on_message_processed)
3. [print() / print_table() — 即時資料查看](#3-print--print_table)
4. [Printer / Table 類 — Rich 終端機輸出](#4-printer--table-類)
5. [RunTracker / RunCollector — 執行狀態追蹤](#5-runtracker--runcollector)
6. [error callbacks — 錯誤觀測](#6-error-callbacks)
7. [logging — 內建日誌](#7-logging)
8. [如何自建觀測層](#8-如何自建觀測層)

---

## 1. 總覽

Quix Streams **沒有內建 OpenTelemetry 或 Prometheus metrics**。觀測手段如下：

| 方式 | 用途 | 粒度 |
|------|------|------|
| `on_message_processed` | 每筆成功處理的訊息回呼 | per message |
| `sdf.print()` | 在 pipeline 中印出當前值 | per message |
| `sdf.print_table()` | 用 Rich 表格即時顯示最近 N 筆 | per message (throttled) |
| error callbacks | 錯誤時回呼（含 topic/partition/offset） | per error |
| Python logging | 框架內部 debug/info/warning 日誌 | 各種事件 |
| RunTracker | debug 用，追蹤已處理的訊息數量 | per run |

---

## 2. on_message_processed — 每筆訊息回呼

### 2.1 Application 接受 callback

**`quixstreams/app.py` (Application.__init__ 節錄)**
```python
class Application:
    def __init__(
        self,
        # ...
        on_message_processed: Optional[MessageProcessedCallback] = None,
        # ...
    ):
        # ...
        self._on_message_processed = on_message_processed
```

**類型定義**：
```python
MessageProcessedCallback = Callable[[str, int, int], None]
#                                   topic, partition, offset
```

### 2.2 呼叫位置

**`quixstreams/app.py` (_process_message 尾部)**
```python
def _process_message(self, dataframe_composed):
    # ... poll and process ...

    for row in rows:
        context = copy_context()
        context.run(set_message_context, row.context)
        try:
            context.run(
                dataframe_composed[topic_name],
                row.value, row.key, row.timestamp, row.headers,
            )
        except Exception as exc:
            to_suppress = self._on_processing_error(exc, row, logger)
            if not to_suppress:
                raise

    # Store the message offset after it's successfully processed
    self._processing_context.store_offset(
        topic=topic_name, partition=partition, offset=offset
    )
    self._run_tracker.set_message_consumed(True)
    self._producer._broker_available()
    self._consumer._broker_available()

    if self._on_message_processed is not None:
        self._on_message_processed(topic_name, partition, offset)   # <-- 在這裡
```

**重要**：只有**成功處理**的訊息才會觸發 callback（在 `store_offset` 之後）。

### 2.3 使用範例

```python
import time
from collections import defaultdict

# 方法 1: 簡單計數器
message_counts = defaultdict(int)

def on_processed(topic: str, partition: int, offset: int):
    message_counts[(topic, partition)] += 1
    if message_counts[(topic, partition)] % 1000 == 0:
        print(f"[{topic}][{partition}] processed {message_counts[(topic, partition)]} messages, latest offset: {offset}")

app = Application(
    broker_address="localhost:9092",
    on_message_processed=on_processed,
)
```

```python
# 方法 2: 推送 metrics 到 Prometheus
from prometheus_client import Counter, start_http_server

start_http_server(8000)
messages_processed = Counter(
    'quix_messages_processed_total',
    'Total messages processed',
    ['topic', 'partition']
)

def on_processed(topic: str, partition: int, offset: int):
    messages_processed.labels(topic=topic, partition=str(partition)).inc()

app = Application(
    broker_address="localhost:9092",
    on_message_processed=on_processed,
)
```

---

## 3. print() / print_table() — 即時資料查看

### 3.1 sdf.print()

**`quixstreams/dataframe/dataframe.py` (print 方法)**
```python
def print(
    self, pretty: bool = True, metadata: bool = False
) -> "StreamingDataFrame":
    """
    Print out the current message value (and optionally, the message metadata) to
    stdout (console) (like the built-in `print` function).

    Can also output a more dict-friendly format with `pretty=True`.

    This operation occurs in-place, meaning reassignment is entirely OPTIONAL: the
    original `StreamingDataFrame` is returned for chaining (`sdf.update().print()`).

    > NOTE: prints the current (edited) values, not the original values.
    """
    print_args = ["value", "key", "timestamp", "headers"]
    if pretty:
        printer: Callable[[Any], None] = functools.partial(
            pprint.pprint, indent=2, sort_dicts=False
        )
    else:
        printer = print
    return self._add_update(
        lambda *args: printer({print_args[i]: args[i] for i in range(len(args))}),
        metadata=metadata,
    )
```

**用法**：
```python
sdf = app.dataframe(topic)
sdf = sdf.apply(lambda v: {**v, "processed": True})
sdf.print()  # 每筆訊息都會 pprint 到 stdout
```

### 3.2 sdf.print_table()

**`quixstreams/dataframe/dataframe.py` (print_table 方法)**
```python
def print_table(
    self,
    size: int = 5,
    title: Optional[str] = None,
    metadata: bool = True,
    timeout: float = 5.0,
    live: bool = DEFAULT_LIVE,
    live_slowdown: float = DEFAULT_LIVE_SLOWDOWN,
    columns: Optional[List[str]] = None,
    column_widths: Optional[dict[str, int]] = None,
) -> "StreamingDataFrame":
    """
    Print a table with the most recent records.

    Creates a live table view that updates in real-time as new records are processed,
    showing the most recent N records in a formatted table.

    Printing Behavior:
    - Interactive mode (terminal/console): The table refreshes in-place
    - Non-interactive mode (output to file): Collects until full or timeout

    Example:
        sdf.print_table(size=5, title="Live Records", slowdown=1)

    Output:
    Live Records
    ┏━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━┳━━━━━┳━━━━━━━━━┳━━━━━━━┳━━━━━━━━━━┓
    ┃ _key       ┃ _timestamp ┃ active ┃ id  ┃ name    ┃ score ┃ status   ┃
    ┡━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━╇━━━━━╇━━━━━━━━━╇━━━━━━━╇━━━━━━━━━━┩
    │ b'53fe8e4' │ 1738685136 │ True   │ 876 │ Charlie │ 27.74 │ pending  │
    │ b'91bde51' │ 1738685137 │ True   │ 11  │         │       │ approved │
    └────────────┴────────────┴────────┴─────┴─────────┴───────┴──────────┘
    """
    self._processing_context.printer.configure_live(
        live=live, live_slowdown=live_slowdown
    )

    table = self._processing_context.printer.add_table(
        size=size,
        title=title,
        timeout=timeout,
        columns=columns,
        column_widths=column_widths,
    )

    if metadata:
        def _print_callback(value, key, timestamp, headers):
            row = {"_key": key, "_timestamp": timestamp}
            if isinstance(value, dict):
                row.update(value)
            else:
                row["value"] = value
            table.add_row(row)
    else:
        def _print_callback(value):
            if isinstance(value, dict):
                table.add_row(value)
            else:
                table.add_row({"value": value})

    return self._add_update(_print_callback, metadata=metadata)
```

---

## 4. Printer / Table 類 — Rich 終端機輸出

**`quixstreams/utils/printing.py` (完整源碼)**

```python
import sys
import time
from collections import deque
from typing import Any, Optional

from rich.console import Console
from rich.table import Table as RichTable

__all__ = ("Printer",)

DEFAULT_COLUMN_NAME = "0"
DEFAULT_LIVE = True
DEFAULT_LIVE_SLOWDOWN = 0.5


class Table:
    def __init__(
        self,
        size: int = 5,
        title: Optional[str] = None,
        timeout: float = 5.0,
        columns: Optional[list[str]] = None,
        column_widths: Optional[dict[str, int]] = None,
    ) -> None:
        self._rows: deque[dict[str, Any]] = deque(maxlen=size)   # <-- 固定大小的 deque
        self._title = title
        self._timeout = timeout
        self._auto_order = columns is None
        self._columns = columns or []
        self._column_widths = column_widths or {}
        self._has_new_data = False
        self._start = time.monotonic()

    def add_row(self, value: dict[str, Any]) -> None:
        if self._auto_order:
            for key in value.keys():
                if key not in self._columns:
                    self._columns.append(key)

        row = {column: value.get(column, "") for column in self._columns}
        self._rows.append(row)
        self._has_new_data = True

    def has_new_data(self) -> bool:
        return self._has_new_data

    def is_full(self) -> bool:
        return len(self._rows) >= (self._rows.maxlen or 0)

    def timeout_reached(self) -> bool:
        return time.monotonic() - self._start > self._timeout

    def clear(self) -> None:
        self._rows.clear()
        self._start = time.monotonic()

    def print(self, console: Console) -> None:
        if not self._rows:
            return

        table = RichTable(title=self._title, title_justify="left", highlight=True)

        for column in self._columns:
            table.add_column(column, width=self._column_widths.get(column))

        for row in self._rows:
            table.add_row(*[str(row.get(column, "")) for column in self._columns])

        console.print(table)
        self._has_new_data = False


class Printer:
    _console = Console()

    def __init__(self) -> None:
        self._tables: list[Table] = []
        self._active = False
        self._live = DEFAULT_LIVE
        self._live_slowdown = DEFAULT_LIVE_SLOWDOWN
        self._resolve_print_method()

    def _resolve_print_method(self) -> None:
        if self._live and sys.stdout.isatty():
            self._print = self._print_interactive
        else:
            self._print = self._print_non_interactive

    def configure_live(self, live: bool, live_slowdown: float) -> None:
        if live != DEFAULT_LIVE:
            self._live = live
            self._resolve_print_method()
        if live_slowdown != DEFAULT_LIVE_SLOWDOWN:
            self._live_slowdown = max(0.0, live_slowdown)

    def add_table(
        self,
        size: int = 5,
        title: Optional[str] = None,
        timeout: float = 5.0,
        columns: Optional[list[str]] = None,
        column_widths: Optional[dict[str, int]] = None,
    ) -> Table:
        table = Table(
            size=size, title=title, timeout=timeout,
            columns=columns, column_widths=column_widths,
        )
        self._tables.append(table)
        self._active = True
        return table

    def print(self) -> None:
        if self._active:
            self._print()

    def clear(self) -> None:
        for table in self._tables:
            table.clear()
        self._tables.clear()

    def _print_interactive(self) -> None:
        """Terminal mode: refresh in-place"""
        if not any(table.has_new_data() for table in self._tables):
            return

        self._console.clear()
        for table in self._tables:
            table.print(self._console)

        time.sleep(self._live_slowdown)              # <-- 節流

    def _print_non_interactive(self) -> None:
        """Non-interactive mode: collect until full or timeout"""
        for table in self._tables:
            if table.is_full() or table.timeout_reached():
                table.print(self._console)
                table.clear()
```

### Printer 在主循環中的呼叫

**`quixstreams/app.py` (_run_dataframe)**
```python
while run_tracker.running:
    if state_manager.recovery_required:
        state_manager.do_recovery()
    else:
        process_message(dataframes_composed)
        processing_context.commit_checkpoint()
        consumer.resume_backpressured()
        source_manager.raise_for_error()
        # ...
        printer.print()                  # <-- 每次循環呼叫
        run_tracker.update_status()
```

---

## 5. RunTracker / RunCollector — 執行狀態追蹤

**`quixstreams/runtracker.py` (完整源碼)**

```python
import logging
import time
from collections.abc import Mapping
from typing import Any, Optional

from .context import message_context
from .core.stream import VoidExecutor
from .models import Headers

__all__ = ("RunTracker", "RunCollector")

logger = logging.getLogger(__name__)


class RunCollector:
    """
    A simple sink to accumulate the outputs during the application run.
    """

    def __init__(self):
        self._items: list[dict] = []
        self._count: int = 0

    def add_value_and_metadata(
        self,
        value: Any, key: Any, timestamp_ms: int, headers: Headers,
        topic: str, partition: int, offset: int,
    ):
        if not isinstance(value, Mapping):
            value = {"_value": value}
        self._items.append({
            "_key": key, "_timestamp": timestamp_ms,
            "_headers": headers, "_topic": topic,
            "_partition": partition, "_offset": offset,
            **value,
        })
        self._count += 1

    def add_value(self, value: Any):
        if not isinstance(value, Mapping):
            value = {"_value": value}
        self._items.append(value)
        self._count += 1

    def increment_count(self):
        self._count += 1

    @property
    def items(self) -> list[dict]:
        return self._items

    @property
    def count(self) -> int:
        return self._count


class RunTracker:
    running: bool
    _has_stop_condition: bool
    _message_consumed: bool
    _collector: RunCollector
    _timeout: float
    _timeout_start_time: float
    _max_count: int

    def __init__(self):
        self.reset()

    @property
    def collected(self) -> list[dict]:
        return self._collector.items

    def collect_values_and_metadata(
        self, value: Any, key: Any, timestamp: int, headers: Any,
    ):
        ctx = message_context()
        self._collector.add_value_and_metadata(
            key=key, value=value, timestamp_ms=timestamp, headers=headers,
            offset=ctx.offset, partition=ctx.partition, topic=ctx.topic,
        )

    def collect_values(
        self, value: Any, key: Any, timestamp: int, headers: Any,
    ):
        self._collector.add_value(value=value)

    def increment_count(
        self, value: Any, key: Any, timestamp: int, headers: Any,
    ):
        self._collector.increment_count()

    def stop(self):
        self.running = False

    def reset(self):
        self.running = False
        self._collector = RunCollector()
        self._has_stop_condition = False
        self._timeout = 0.0
        self._timeout_start_time = 0.0
        self._message_consumed = False
        self._max_count = 0

    def update_status(self):
        if self._has_stop_condition and (self._at_timeout() or self._at_count()):
            self.stop()

    def set_as_running(self):
        self.running = True
        if self._timeout:
            self._timeout_start_time = time.monotonic() + 60

    def set_message_consumed(self, consumed: bool):
        self._message_consumed = consumed

    def timeout_refresh(self):
        self._timeout_start_time = time.monotonic()

    def set_stop_condition(self, timeout: float = 0.0, count: int = 0):
        if not ((timeout := max(timeout, 0.0)) or (count := max(count, 0))):
            return
        self._has_stop_condition = True
        self._timeout = timeout
        self._max_count = count

    def get_collector(self, collect: bool, metadata: bool) -> Optional[VoidExecutor]:
        if not self._has_stop_condition:
            return None
        elif not collect:
            return self.increment_count
        elif not metadata:
            return self.collect_values
        else:
            return self.collect_values_and_metadata

    def _at_count(self) -> bool:
        if self._max_count and self._collector.count >= self._max_count:
            logger.info(f"Count of {self._max_count} records reached.")
            return True
        return False

    def _at_timeout(self) -> bool:
        if not self._timeout:
            return False
        if self._message_consumed:
            self.timeout_refresh()
        elif (time.monotonic() - self._timeout_start_time) >= self._timeout:
            logger.info(f"Timeout of {self._timeout}s reached.")
            return True
        return False
```

**注意**：`RunTracker` 主要用於 debug/test 場景（`app.run(timeout=5, count=10)`），生產環境中通常不設定 stop condition。

---

## 6. error callbacks — 錯誤觀測

**`quixstreams/error_callbacks.py`**
```python
ProcessingErrorCallback = Callable[[Exception, Optional[Row], logging.Logger], bool]
ConsumerErrorCallback = Callable[
    [Exception, Optional[RawConfluentKafkaMessageProto], logging.Logger], bool
]
ProducerErrorCallback = Callable[[Exception, Optional[Row], logging.Logger], bool]
```

每個 callback 都收到完整的 `Row` 物件（含 topic, partition, offset, key, value），可以用來記錄詳細的錯誤資訊。

詳見 `log.md`。

---

## 7. logging — 內建日誌

Quix Streams 使用 Python 標準 `logging` 模組，logger 名稱是 `"quixstreams"`。

### 7.1 Application 初始化時配置

```python
# quixstreams/app.py
class Application:
    def __init__(self, ..., loglevel="INFO", ...):
        configure_logging(loglevel=loglevel)
```

### 7.2 關鍵日誌位置

| 元件 | 日誌 | 級別 |
|------|------|------|
| checkpoint commit | `"Committing a checkpoint"` | DEBUG |
| checkpoint commit | `"Checkpoint: flushing sinks"` | DEBUG |
| checkpoint commit | `"Checkpoint: flushing producer"` | DEBUG |
| backpressure | `"Backpressure for sink ... detected"` | WARNING |
| backpressure | `"Pausing topic partition..."` | DEBUG |
| backpressure | `"Resuming topic partition..."` | DEBUG |
| rebalance | `"Rebalancing: assigning partitions"` | DEBUG |
| rebalance | `"Rebalancing: revoking partitions"` | DEBUG |
| recovery | recovery manager 的各種日誌 | INFO/DEBUG |
| BatchingSink.flush | `"Flushing sink ... total_records=..."` | DEBUG |
| Application start | `"Starting the Application with the config..."` | INFO |

### 7.3 如何開啟 DEBUG 日誌

```python
app = Application(
    broker_address="localhost:9092",
    loglevel="DEBUG",    # <-- 開啟 DEBUG
)
```

或者自行配置：
```python
import logging
logging.getLogger("quixstreams").setLevel(logging.DEBUG)
```

---

## 8. 如何自建觀測層

### 8.1 方法 1: 在 SDF pipeline 中插入觀測 update

```python
from opentelemetry import trace, metrics

tracer = trace.get_tracer("quix-app")
meter = metrics.get_meter("quix-app")
message_counter = meter.create_counter("messages_processed")
processing_histogram = meter.create_histogram("processing_duration_ms")


def observe(value, key, timestamp, headers):
    """在 pipeline 中的任意位置插入"""
    with tracer.start_as_current_span("process_message") as span:
        span.set_attribute("kafka.key", str(key))
        span.set_attribute("kafka.timestamp", timestamp)
        message_counter.add(1, {"topic": "my-topic"})


sdf = app.dataframe(topic)
sdf = sdf.update(observe, metadata=True)     # <-- 插入觀測
sdf = sdf.apply(my_transform)
sdf.sink(my_sink)
```

### 8.2 方法 2: 用 on_message_processed 做全局觀測

```python
import time
from prometheus_client import Counter, Histogram, start_http_server

start_http_server(8000)

msg_total = Counter('quix_msg_total', 'Total messages', ['topic', 'partition'])
msg_lag = Histogram('quix_msg_lag_seconds', 'Message lag')

def on_processed(topic, partition, offset):
    msg_total.labels(topic=topic, partition=str(partition)).inc()

app = Application(
    broker_address="localhost:9092",
    on_message_processed=on_processed,
)
```

### 8.3 方法 3: 用 error callback 做錯誤觀測

```python
from prometheus_client import Counter

error_total = Counter('quix_errors_total', 'Total errors', ['topic', 'error_type'])

def on_error(exc, row, logger):
    error_total.labels(
        topic=row.topic if row else "unknown",
        error_type=type(exc).__name__,
    ).inc()
    logger.error(f"Error processing {row.topic}[{row.partition}]@{row.offset}: {exc}")
    return True  # 跳過繼續

app = Application(
    broker_address="localhost:9092",
    on_processing_error=on_error,
)
```

### 8.4 方法 4: 包裝 Sink 加入觀測

```python
class ObservableSink(BatchingSink):
    def __init__(self, inner_sink, metrics):
        super().__init__()
        self._inner = inner_sink
        self._metrics = metrics

    def write(self, batch):
        start = time.monotonic()
        try:
            self._inner.write(batch)
            self._metrics["sink_success"].inc(batch.size)
        except Exception as e:
            self._metrics["sink_errors"].inc()
            raise
        finally:
            duration = time.monotonic() - start
            self._metrics["sink_duration"].observe(duration)
```

### 8.5 觀測能力總結

| 想觀測什麼 | 使用方式 | 粒度 |
|------------|---------|------|
| 每筆訊息成功處理 | `on_message_processed` | topic/partition/offset |
| pipeline 中間值 | `sdf.print()` 或 `sdf.update(observe)` | 每筆 |
| 即時表格視圖 | `sdf.print_table()` | 最近 N 筆 |
| 處理錯誤 | `on_processing_error` | 每個錯誤 |
| Consumer 錯誤 | `on_consumer_error` | 每個錯誤 |
| Producer 錯誤 | `on_producer_error` | 每個錯誤 |
| Checkpoint 狀態 | 設定 `loglevel="DEBUG"` | 每次 commit |
| Rebalance 事件 | 設定 `loglevel="DEBUG"` | 每次 rebalance |
| Sink flush 狀態 | 設定 `loglevel="DEBUG"` | 每次 flush |
| 自定義 metrics | 在 `sdf.update()` 中推送到 Prometheus/OTel | 任意 |
