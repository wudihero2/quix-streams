# Quix Streams: Sink 機制與錯誤處理 源碼深度解析

## 目錄

1. [多個 Sink 會互卡嗎？Quix 用子進程還是線程？](#1-多個-sink-會互卡嗎)
2. [為什麼 Sink 不開子進程或線程 async 處理？](#2-為什麼-sink-不開子進程或線程-async-處理)
3. [錯誤處理：Quix 會卡住死掉還是跳過？如何記錄跳過的訊息？](#3-錯誤處理)

---

## 1. 多個 Sink 會互卡嗎？

### 結論（先講答案）

- **Quix 既不用子進程也不用線程** — Sink 是在**主線程中同步（inline）執行**的
- **多個 Sink 會互卡** — 在 checkpoint commit 時，所有 Sink 在同一個 for loop 中**依序 flush**，一個卡住後面全部等
- 如果某個 Sink 拋出 `SinkBackpressureError`，後面的 Sink 會被呼叫 `on_paused()` 直接丟棄已累積的資料

### 1.1 Sink 的註冊流程

當你在 SDF 上呼叫 `.sink()` 時：

**`quixstreams/dataframe/dataframe.py` (完整 sink 方法)**
```python
def sink(self, sink: BaseSink):
    """
    Sink the processed data to the specified destination.

    Internally, each processed record is added to a sink, and the sinks are
    flushed on each checkpoint.
    The offset will be committed only if all the sinks for all topic partitions
    are flushed successfully.

    Additionally, Sinks may signal the backpressure to the application
    (e.g., when the destination is rate-limited).
    When this happens, the application will pause the corresponding topic partition
    and resume again after the timeout.
    The backpressure handling and timeouts are defined by the specific sinks.

    Note: `sink()` is a terminal operation - it cannot receive any additional
    operations, but branches can still be generated from its originating SDF.

    """
    self._processing_context.sink_manager.register(sink)

    def _sink_callback(
        value: Any, key: Any, timestamp: int, headers: HeadersTuples
    ):
        ctx = message_context()
        sink.add(
            value=value,
            key=key,
            timestamp=timestamp,
            headers=headers,
            partition=ctx.partition,
            topic=ctx.topic,
            offset=ctx.offset,
        )

    # uses apply without returning to make this operation terminal
    self.apply(_sink_callback, metadata=True)
```

**關鍵點**：`sink.add()` 只是把資料放到記憶體 buffer 中（對 `BatchingSink` 而言），並沒有真正寫到外部系統。真正寫入是在 checkpoint commit 時呼叫 `flush()`。

### 1.2 SinkManager — 極簡的 dict 容器

**`quixstreams/sinks/base/manager.py` (完整源碼)**
```python
from typing import List

from .sink import BaseSink

__all__ = ("SinkManager",)


class SinkManager:
    def __init__(self):
        self._sinks = {}

    def register(self, sink: BaseSink):
        sink_id = id(sink)
        if sink_id not in self._sinks:
            self._sinks[id(sink)] = sink

    def start_sinks(self):
        for sink in self.sinks:
            sink.start()

    @property
    def sinks(self) -> List[BaseSink]:
        return list(self._sinks.values())
```

就是一個 `dict`。用 `id(sink)` 做 key 防止重複註冊。`start_sinks()` 在 `ProcessingContext.__enter__()` 中被呼叫：

**`quixstreams/processing/context.py` (ProcessingContext 進入)**
```python
def __enter__(self):
    self.sink_manager.start_sinks()
    return self
```

### 1.3 BaseSink — 抽象基類

**`quixstreams/sinks/base/sink.py` (完整源碼)**
```python
import abc
import logging
from typing import Any, Callable, Optional

from quixstreams.models import HeadersTuples
from quixstreams.sinks.base.batch import SinkBatch

logger = logging.getLogger(__name__)


ClientConnectSuccessCallback = Callable[[], None]
ClientConnectFailureCallback = Callable[[Optional[Exception]], None]


def _default_on_client_connect_success():
    logger.info("CONNECTED!")


def _default_on_client_connect_failure(exception: Exception):
    logger.error(f"ERROR! - Failed while connecting to client: {exception}")
    raise exception


class BaseSink(abc.ABC):
    """
    This is a base class for all sinks.

    Subclass it and implement its methods to create your own sink.

    Note that Sinks are currently in beta, and their design may change over time.
    """

    def __init__(
        self,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
    ):
        self._on_client_connect_success = (
            on_client_connect_success or _default_on_client_connect_success
        )
        self._on_client_connect_failure = (
            on_client_connect_failure or _default_on_client_connect_failure
        )

    @abc.abstractmethod
    def flush(self):
        """
        This method is triggered by the Checkpoint class when it commits.

        You can use `flush()` to write the batched data to the destination (in case of
        a batching sink), or confirm the delivery of the previously sent messages
        (in case of a streaming sink).

        If flush() fails, the checkpoint will be aborted.
        """

    @abc.abstractmethod
    def add(
        self,
        value: Any,
        key: Any,
        timestamp: int,
        headers: HeadersTuples,
        topic: str,
        partition: int,
        offset: int,
    ):
        """
        This method is triggered on every new processed record being sent to this sink.

        You can use it to accumulate batches of data before sending them outside, or
        to send results right away in a streaming manner and confirm a delivery later
        on flush().
        """

    def setup(self):
        """
        When applicable, set up the client here along with any validation to affirm a
        valid/successful authentication/connection.
        """

    def start(self):
        """
        Called as part of `Application.run()` to initialize the sink's client.
        Allows using a callback pattern around the connection attempt.
        """
        try:
            self.setup()
            self._on_client_connect_success()
        except Exception as e:
            self._on_client_connect_failure(e)

    def on_paused(self):
        """
        This method is triggered when the sink is paused due to backpressure, when
        the `SinkBackpressureError` is raised.

        Here you can react to the backpressure events.
        """
```

### 1.4 BatchingSink — 帶有記憶體 Buffer 的 Sink

```python
class BatchingSink(BaseSink):
    """
    A base class for batching sinks, that need to accumulate the data first before
    sending it to the external destinations.

    Examples: databases, objects stores, and other destinations where
    writing every message is not optimal.

    It automatically handles batching, keeping batches in memory per topic-partition.
    """

    _batches: dict[tuple[str, int], SinkBatch]

    def __init__(
        self,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
    ):
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure,
        )
        self._batches = {}

    def __repr__(self):
        return f"<BatchingSink: {self.__class__.__name__}>"

    @abc.abstractmethod
    def write(self, batch: SinkBatch):
        """
        This method implements actual writing to the external destination.

        It may also raise `SinkBackpressureError` if the destination cannot accept new
        writes at the moment.
        When this happens, the accumulated batch is dropped and the app pauses the
        corresponding topic partition.
        """

    def add(
        self,
        value: Any,
        key: Any,
        timestamp: int,
        headers: HeadersTuples,
        topic: str,
        partition: int,
        offset: int,
    ):
        """
        Add a new record to in-memory batch.
        """
        tp = (topic, partition)
        batch = self._batches.get(tp)
        if batch is None:
            batch = SinkBatch(topic=topic, partition=partition)
            self._batches[tp] = batch
        batch.append(
            value=value, key=key, timestamp=timestamp, headers=headers, offset=offset
        )

    def flush(self):
        """
        Flush accumulated batches to the destination and drop them afterward.
        """

        try:
            for (topic, partition), batch in self._batches.items():
                logger.debug(
                    f'Flushing sink "{self}" for partition "{topic}[{partition}]; '
                    f'total_records={batch.size}"'
                )
                # TODO: Some custom error handling may be needed here
                #   For now simply fail
                self.write(batch)
        finally:
            # Always drop batches after flushing
            self._batches.clear()

    def on_paused(self):
        """
        When the destination is already backpressured, drop the accumulated batches.
        """
        self._batches.clear()
```

### 1.5 SinkBatch — 每個 topic-partition 的 buffer

**`quixstreams/sinks/base/batch.py` (完整源碼)**
```python
from collections import deque
from itertools import islice
from typing import Any, Deque, Iterable, Iterator

from quixstreams.models import HeadersTuples

from .item import SinkItem

__all__ = ("SinkBatch",)


class SinkBatch:
    """
    A batch to accumulate processed data by `BatchingSink` between the checkpoints.

    Batches are created automatically by the implementations of `BatchingSink`.

    :param topic: a topic name
    :param partition: a partition number
    """

    _buffer: Deque[SinkItem]

    def __init__(self, topic: str, partition: int):
        self._buffer = deque()
        self._partition = partition
        self._topic = topic

    @property
    def topic(self) -> str:
        return self._topic

    @property
    def partition(self) -> int:
        return self._partition

    @property
    def size(self) -> int:
        return len(self._buffer)

    @property
    def start_offset(self) -> int:
        return self._buffer[0].offset

    def append(
        self,
        value: Any,
        key: Any,
        timestamp: int,
        headers: HeadersTuples,
        offset: int,
    ):
        self._buffer.append(
            SinkItem(
                value=value,
                key=key,
                timestamp=timestamp,
                headers=headers,
                offset=offset,
            )
        )

    def clear(self):
        self._buffer.clear()

    def empty(self) -> bool:
        return len(self._buffer) == 0

    def iter_chunks(self, n: int) -> Iterable[Iterable[SinkItem]]:
        """
        Iterate over batch data in chunks of length n.
        The last batch may be shorter.
        """
        if n < 1:
            raise ValueError("n must be at least one")
        it_ = iter(self)
        while batch := tuple(islice(it_, n)):
            yield batch

    def __iter__(self) -> Iterator[SinkItem]:
        return iter(self._buffer)
```

### 1.6 Checkpoint.commit() — Sink 在這裡被 flush（核心！）

**`quixstreams/checkpointing/checkpoint.py` (commit 方法 Step 1)**
```python
def commit(self):
    """
    Commit the checkpoint.

    This method will:
     1. Flush the registered sinks if any
     2. Produce the changelogs for each state store
     3. Flush the producer to ensure everything is delivered.
     4. Commit topic offsets.
     5. Flush each state store partition to the disk.
    """

    # Step 1. Flush sinks
    logger.debug("Checkpoint: flushing sinks")
    backpressured = False
    for sink in self._sink_manager.sinks:           # <-- 依序迭代所有 sink
        if backpressured:
            # Drop the accumulated data for the other sinks
            # if one of them is backpressured to limit the number of duplicates
            # when the data is reprocessed again
            sink.on_paused()                        # <-- 後面的 sink 直接丟棄資料
            continue

        try:
            sink.flush()                            # <-- 同步呼叫，會阻塞！
        except SinkBackpressureError as exc:
            logger.warning(
                f'Backpressure for sink "{sink}" is detected, '
                f"all partitions will be paused and resumed again "
                f"in {exc.retry_after}s"
            )
            # The backpressure is detected from the sink
            # Pause the assignment to let it cool down and seek it back to
            # the first processed offsets of this Checkpoint (it must be equal
            # to the last committed offset).
            self._consumer.trigger_backpressure(
                resume_after=exc.retry_after,
                offsets_to_seek=self._starting_tp_offsets.copy(),
            )
            backpressured = True
    if backpressured:
        # Exit early if backpressure is detected
        return

    # Step 2. Produce the changelogs
    # ...（後續步驟見 checkpoint.md）
```

### 1.7 SinkBackpressureError — 反壓信號

**`quixstreams/sinks/base/exceptions.py` (完整源碼)**
```python
from quixstreams.exceptions import QuixException

__all__ = ("SinkBackpressureError",)


class SinkBackpressureError(QuixException):
    """
    An exception to be raised by Sinks during flush() call
    to signal a backpressure event to the application.

    When raised, the app will drop the accumulated sink batches,
    pause all assigned topic partitions for
    a timeout specified in `retry_after`, and resume them when it's elapsed.

    :param retry_after: a timeout in seconds to pause for
    """

    def __init__(self, retry_after: float):
        self.retry_after = retry_after
```

### 1.8 整體流程圖

```
消息處理流程（單線程，同步）：

poll() → deserialize → SDF pipeline
                          │
           ┌──────────────┼──────────────┐
           ▼              ▼              ▼
      sink_A.add()   sink_B.add()   sink_C.add()
      (存到記憶體)    (存到記憶體)    (存到記憶體)
           │              │              │
           └──────────────┼──────────────┘
                          │
                    checkpoint 到期
                          │
                  commit() Step 1:
                          │
                   sink_A.flush()  ← 同步阻塞，寫入外部系統
                          │
                   sink_B.flush()  ← 等 A 完成才輪到 B
                          │
                   sink_C.flush()  ← 等 B 完成才輪到 C
                          │
                  commit() Step 2-5:
                  changelog → producer flush → commit offsets → flush state
```

### 1.9 總結：為什麼多個 Sink 會互卡

1. **沒有任何並行機制**：所有 Sink 在 `Checkpoint.commit()` 的 for loop 中**同步依序執行**
2. **一個 Sink 慢，全部等**：假設 Sink A 寫入 Postgres 花了 5 秒，在這 5 秒內 Sink B、C 都不會被 flush
3. **一個 Sink 反壓，其餘直接丟棄**：如果 Sink A 拋出 `SinkBackpressureError`，Sink B、C 的 `on_paused()` 會被呼叫，BatchingSink 的實作就是 `self._batches.clear()` 直接清空
4. **反壓會導致重新消費**：consumer 會 seek 回到這個 checkpoint 的起始 offset，所有訊息重新處理，所有 Sink 重新 add()

**如果你有多個獨立的 Sink 目的地，且擔心互卡**，建議：
- 用多個獨立的 Application（各自不同的 consumer group），每個只掛一個 Sink
- 或確保所有 Sink 的寫入速度相近

---

## 2. 為什麼 Sink 不開子進程或線程 async 處理？

### 結論（先講答案）

**因為 sink flush 必須在 checkpoint commit 的 Step 1 同步完成，否則一致性保證會崩壞。**

offset commit（Step 4）代表「我確認這些訊息已經被完整處理了」。
sink flush 在 Step 1，offset commit 在 Step 4。如果 sink 還沒確認寫入成功就 commit offset，之後 sink 失敗時資料已無法重新取得 → **資料遺失**。

### 2.1 如果 sink flush 改成 async 會怎樣？

```
假設 sink 是寫 PostgreSQL，改成 async（非阻塞）：

時間線：
T1: checkpoint expired → commit()
T2: sink.flush() → 提交到 thread pool，不等結果，立即返回
T3: Step 2-4 → commit offset = 150     ← offset 已經 commit 到 Kafka
T4: 主迴圈繼續處理 offset 151, 152...
T5: thread pool 裡的 PostgreSQL 寫入失敗了！
    ← ❌ offset 已經 commit 到 150，但 sink 的資料沒寫進去
    ← ❌ 這些資料不會被重新處理（因為 offset 已經推進了）
    ← ❌ 資料遺失！
```

```
正確的同步流程：

T1: checkpoint expired → commit()
T2: sink.flush() → 阻塞，等 PostgreSQL 寫入完成
    ├─ 成功 → 繼續 Step 2-4 → commit offset
    └─ 失敗 → SinkBackpressureError
              → 不 commit offset
              → seek 回 _starting_tp_offsets
              → 下次重新處理這批資料
              → ✅ 不會丟資料
```

### 2.2 源碼中 Step 1 的同步設計

```python
# quixstreams/checkpointing/checkpoint.py — commit()
def commit(self):
    # Step 1. Flush sinks          ← ★ 必須同步完成
    # Step 2. Produce changelogs
    # Step 3. Flush producer
    # Step 4. Commit offsets        ← ★ offset commit 在 sink flush 之後
    # Step 5. Flush state to disk
```

```python
# quixstreams/checkpointing/checkpoint.py:193-223
# Step 1. Flush sinks
logger.debug("Checkpoint: flushing sinks")
backpressured = False
for sink in self._sink_manager.sinks:
    if backpressured:
        sink.on_paused()        # ★ 一個 sink 出問題，其他 sink 的資料也丟掉
        continue

    try:
        sink.flush()            # ★ 同步阻塞！等 sink 確認完成
    except SinkBackpressureError as exc:
        self._consumer.trigger_backpressure(
            resume_after=exc.retry_after,
            offsets_to_seek=self._starting_tp_offsets.copy(),  # ★ seek 回起點
        )
        backpressured = True

if backpressured:
    return                      # ★ 提前返回，不執行 Step 2-5，不 commit offset
```

BaseSink 的文件也明確寫了這個意圖：

```python
# quixstreams/sinks/base/sink.py
class BaseSink(abc.ABC):
    @abc.abstractmethod
    def flush(self):
        """
        This method is triggered by the Checkpoint class when it commits.
        ★ flush 是 checkpoint commit 觸發的
        If flush() fails, the checkpoint will be aborted.
        ★ flush 失敗 → checkpoint abort → offset 不 commit → 重新處理
        """

    @abc.abstractmethod
    def add(self, value, key, timestamp, headers, topic, partition, offset):
        """
        This method is triggered on every new processed record being sent to this sink.
        ★ add 在處理每筆訊息時呼叫（累積到記憶體）
        ★ flush 在 checkpoint 時呼叫（批次寫出）
        """
```

BatchingSink 的 flush 也是同步的：

```python
# quixstreams/sinks/base/sink.py — BatchingSink
def flush(self):
    try:
        for (topic, partition), batch in self._batches.items():
            self.write(batch)   # ★ 同步呼叫子類別的 write()
    finally:
        self._batches.clear()   # ★ 無論成功失敗都清空 batch
```

### 2.3 「那 KinesisSink 不是有用 ThreadPoolExecutor 嗎？」

是的，但仔細看它的 `flush()` 最後還是**同步等待所有 future 完成**：

```python
# quixstreams/sinks/community/kinesis.py（完整源碼）
class KinesisSink(BaseSink):
    def __init__(
        self,
        stream_name: str,
        aws_access_key_id: Optional[str] = getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key: Optional[str] = getenv("AWS_SECRET_ACCESS_KEY"),
        region_name: Optional[str] = getenv("AWS_REGION", getenv("AWS_DEFAULT_REGION")),
        aws_endpoint_url: Optional[str] = getenv("AWS_ENDPOINT_URL_KINESIS"),
        value_serializer: Callable[[Any], str] = json.dumps,
        key_serializer: Callable[[Any], str] = bytes.decode,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
        **kwargs,
    ) -> None:
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure,
        )
        self._client: Optional[KinesisClient] = None
        self._stream_name = stream_name
        self._value_serializer = value_serializer
        self._key_serializer = key_serializer
        self._records = defaultdict(list)
        self._futures = defaultdict(list)
        self._credentials = {
            "endpoint_url": aws_endpoint_url,
            "region_name": region_name,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            **kwargs,
        }

        # ★ 只有 1 個 worker thread — 保證順序
        self._executor = ThreadPoolExecutor(max_workers=1)

    def setup(self):
        self._client = boto3.client("kinesis", **self._credentials)
        try:
            self._client.describe_stream(StreamName=self._stream_name)
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                raise KinesisStreamNotFoundError(
                    f"Kinesis stream `{self._stream_name}` does not exist."
                )
            raise

    def add(self, value, key, timestamp, headers, topic, partition, offset) -> None:
        topic_partition = (topic, partition)
        record = {
            "Data": self._value_serializer(value),
            "PartitionKey": self._key_serializer(key),
        }
        self._records[topic_partition].append(record)

        # ★ 累積到 500 筆就提前送出（Kinesis API 限制 500/batch）
        if len(self._records[topic_partition]) == 500:
            records = self._records.pop(topic_partition)
            self._submit(topic_partition, records)    # 提交到 thread pool

    def flush(self) -> None:
        # 送出剩餘的 records
        for tp, records in self._records.items():
            self._submit(tp, records)

        # ★ 關鍵：阻塞等待所有 future 完成！
        for futures in self._futures.values():
            done, not_done = wait(futures, return_when=FIRST_EXCEPTION)
            if not_done or any(f.exception() for f in done):
                raise SinkBackpressureError(retry_after=5.0)
                # ★ 有失敗 → backpressure → 不 commit offset → 重新處理

    def _submit(self, topic_partition, records):
        future = self._executor.submit(
            self._client.put_records,           # ★ 在 worker thread 中執行 I/O
            Records=records,
            StreamName=self._stream_name,
        )
        self._futures[topic_partition].append(future)
```

KinesisSink 的 thread 是一種**「add 時提前發送，flush 時等結果」**的最佳化：

```
沒有 thread 的 BatchingSink（PostgreSQL）：
  add → 記憶體累積
  add → 記憶體累積
  ...
  flush → 一次性寫 DB（阻塞 N 秒）← 所有 I/O 集中在 flush

有 thread 的 KinesisSink：
  add → 記憶體累積
  add → 累積到 500 筆 → submit 到 thread pool → thread 開始送 Kinesis API
  add → 記憶體累積
  add → 累積到 500 筆 → submit 到 thread pool → thread 開始送 Kinesis API
  ...
  flush → wait(所有 futures) ← I/O 已經在 add 階段開始了，flush 只是等結果
         ★ 但 flush 仍然阻塞到所有結果確認！

效果：KinesisSink 的 flush 等待時間更短（因為 I/O 已經提前開始），
      但從 checkpoint 的角度，flush() 仍然是同步阻塞的。
```

### 2.4 三種 Sink 模式對比

```
模式 1: BatchingSink（PostgreSQL, InfluxDB）
  ┌─────────────────────────────────────────────────────────┐
  │ add    add    add    add    add │ flush()              │
  │  ↓      ↓      ↓      ↓      ↓ │   ↓                  │
  │ 記憶體  記憶體  記憶體  記憶體  記憶體│ write(batch) ← 阻塞  │
  │                                │ ★ 所有 I/O 在 flush   │
  └─────────────────────────────────────────────────────────┘

模式 2: Streaming Sink（Kinesis, PubSub）
  ┌─────────────────────────────────────────────────────────┐
  │ add    add    add ─→ submit │ flush()                   │
  │  ↓      ↓      ↓    (thread)│   ↓                      │
  │ 記憶體  記憶體  記憶體  I/O開始 │ wait(futures) ← 阻塞    │
  │                             │ ★ I/O 提前開始，flush 等結果│
  └─────────────────────────────────────────────────────────┘

共同點：flush() 返回時 = 所有資料確認送達
       → checkpoint 才能安全 commit offset
```

### 2.5 如果真的想要 async sink，代價是什麼？

```
方案：sink.flush() 改成非阻塞，等下一次 checkpoint 才確認結果

問題 1: 至少需要 2 個 checkpoint 才能確認一批資料
  Checkpoint N: flush(batch_N) → 非阻塞返回 → commit offset_N
  Checkpoint N+1: 確認 batch_N 結果 → 失敗？但 offset_N 已經 commit 了 → ❌ 資料遺失

問題 2: backpressure 無法即時反應
  sink 過載 → 但 flush 已經返回 → offset 已 commit → 來不及 seek back

問題 3: 失敗重試的語意變複雜
  哪些資料需要重試？offset 已經推進了，怎麼重新取得那些資料？

結論：async sink 破壞了「flush 成功 = 可以 commit offset」的簡單不變式（invariant）。
     要支援它需要引入：
     - 預寫日誌（WAL）來持久化未確認的資料
     - 獨立的 offset 追蹤（不再跟 Kafka offset 綁定）
     - 複雜的失敗恢復邏輯
     → 代價遠大於收益，特別是在單 thread 架構下
```

### 2.6 那 Barrier 模型（Flink）的 Sink 能 async 嗎？

**可以。** 這是 barrier 模型比 periodic checkpoint 的一個結構性優勢。

Flink 的 sink 有兩種策略：

---

**策略 A：跟 Quix 一樣同步 flush（簡單但慢）**

```
records → [Sink operator] ──(同步寫)──▶ PostgreSQL
                  │
          收到 barrier → flush 等結果 → snapshot → checkpoint 完成
```

這跟 Quix 沒差別。但 Flink 也支援更進階的方式：

---

**策略 B：Async 寫入 + 兩階段提交（Two-Phase Commit）**

Flink 有 `TwoPhaseCommitSinkFunction`，把 checkpoint 拆成兩個階段：

```
Phase 1 — 收到 barrier 時：「預提交」(pre-commit)
  不需要等外部系統確認，只需要保存「哪些資料屬於這個 checkpoint」

Phase 2 — Checkpoint Coordinator 確認所有 operator 都 snapshot 完成後：「正式提交」(commit)
  這時才真正 commit 到外部系統
```

具體流程：

```
時間線：

T1: record_1 到達 Sink → async 寫入 DB（不等結果）
T2: record_2 到達 Sink → async 寫入 DB（不等結果）
T3: record_3 到達 Sink → async 寫入 DB（不等結果）
T4: [barrier cp=7] 到達 Sink
    │
    ▼ Phase 1: pre-commit
    │  - 開一個 DB transaction（或記錄 pending writes 的 ID）
    │  - snapshot 自己的 state:「cp=7 包含 record_1, record_2, record_3」
    │  - 回報 Checkpoint Coordinator：「我 snapshot 好了」
    │  ★ 不需要等 DB 寫入完成！只需要知道「哪些資料屬於 cp=7」
    │
T5: record_4 到達 Sink → 這屬於下一個 checkpoint，寫入新的 transaction
T6: record_5 到達 Sink → async 寫入
    │
    │  （此時 Sink 繼續處理新資料，不阻塞）
    │
T7: Checkpoint Coordinator 收到所有 operator 的 snapshot 回報
    → 宣布 cp=7 完成
    │
    ▼ Phase 2: commit
    │  - Sink 收到「cp=7 完成」的通知
    │  - DB transaction commit → record_1, record_2, record_3 正式可見
    │  ★ 如果 commit 失敗 → cp=7 標記為失敗 → 從上一個成功的 checkpoint 恢復
    │
T8: 繼續處理...
```

**關鍵：Phase 1 到 Phase 2 之間，Sink 不阻塞，繼續處理新資料。**

---

**Flink 兩階段的經典範例：KafkaSink**

```
Flink KafkaSink exactly-once：

Phase 1（收到 barrier）：
  - 呼叫 producer.flush()  ← 確保訊息送到 broker
  - 但不呼叫 commitTransaction()
  - snapshot: 記錄 transactional.id 和 epoch

Phase 2（checkpoint 確認完成）：
  - 呼叫 producer.commitTransaction()  ← 訊息正式對 consumer 可見
  - 開始新的 transaction 給下一個 checkpoint

如果 checkpoint 失敗：
  - 呼叫 producer.abortTransaction()  ← 訊息被丟棄，不會重複
```

---

**為什麼 Quix 做不到這件事？**

```
Quix 的 periodic checkpoint:

  add → add → add → add → [5秒到了] → flush → commit offset
                                        ↑
                              必須在這裡同步完成
                              因為下一步就是 commit offset
                              沒有「Phase 2 再 commit」的機會

問題 1: 沒有精確切點
  barrier 精確標記了「哪些 records 屬於這個 checkpoint」
  Quix 的 checkpoint 只有「時間到了就 commit」，沒有在資料流中標記邊界
  → 無法區分「checkpoint N 的資料」和「checkpoint N+1 的資料」在 sink 端

問題 2: 沒有兩階段協議
  Flink:  barrier → Phase 1 (snapshot) → 繼續處理 → Phase 2 (commit)
  Quix:   時間到 → flush + commit offset（一次性，沒有拆分）

問題 3: 沒有 Checkpoint Coordinator
  Flink 有一個 Coordinator 負責：
    - 告訴所有 Source 注入 barrier
    - 收集所有 operator 的 snapshot 回報
    - 確認全部完成後才觸發 Phase 2
  Quix 是單 process 單 thread，沒有這個角色
```

---

**對比總結**：

```
                        Quix (Periodic)              Flink (Barrier)
                     ─────────────────────      ─────────────────────
Sink 何時寫入？       flush 時一次性寫出           可以在 add 時就 async 寫出

Sink 何時確認？       flush 返回 = 確認完成        Phase 2 才正式 commit

flush 阻塞嗎？        ★ 必須阻塞                  Phase 1 不需要阻塞
                     （否則 offset 先 commit       （只需要 snapshot pending writes）
                       → 資料遺失）

失敗恢復？            seek back + 重新處理          abort transaction
                                                  + 從上一個 checkpoint 恢復

外部系統需要           不需要                       需要（DB transaction /
支援 transaction？    （flush 成功就是成功）          Kafka transaction /
                                                  idempotent writes）

吞吐量影響？          flush 期間完全阻塞            Phase 1 極快（只 snapshot）
                     → 處理暫停                    → 幾乎不影響吞吐量
```

**一句話**：Barrier 模型的 sink 能 async，是因為 barrier 提供了精確切點 +
兩階段提交讓「snapshot」和「commit」分離。Quix 的 periodic checkpoint 沒有這兩個東西，
所以 flush 必須同步完成才能保證「flush 成功 → 可以 commit offset」的不變式。

### 2.7 能不能多線程同時 flush 所有 sink，然後等全部完成？

```python
# 現在的做法：依序同步 flush，一個卡住後面全部等
for sink in sinks:
    sink.flush()

# 提議的做法：多線程平行 flush，最後阻塞等全部完成
with ThreadPoolExecutor() as pool:
    futures = {pool.submit(sink.flush): sink for sink in sinks}
    done, _ = wait(futures, return_when=ALL_COMPLETED)
    # 全部完成才繼續 Step 2-5
```

**一致性上完全可行。** 因為最終還是「全部 flush 完成 → 才 commit offset」，不變式沒被打破。

但 Quix 沒這樣做。以下分析為什麼，以及真的想改的話要注意什麼：

---

**問題 1：Backpressure 的語意變複雜**

現在的依序 flush 邏輯很清晰：

```python
# 現在的設計
for sink in sinks:
    if backpressured:
        sink.on_paused()    # 後面的 sink 直接丟棄（因為等等要 seek back 重新處理）
        continue
    try:
        sink.flush()
    except SinkBackpressureError:
        backpressured = True  # 一個出問題 → 全部停
```

如果改成平行：

```python
# 平行 flush
futures = {pool.submit(sink.flush): sink for sink in sinks}
done, _ = wait(futures, return_when=ALL_COMPLETED)

# 問題：怎麼處理 backpressure？
results = {}
for future in done:
    sink = futures[future]
    try:
        future.result()
        results[sink] = "ok"
    except SinkBackpressureError as e:
        results[sink] = e

# 場景：Sink A 成功，Sink B backpressure
# Sink A 已經寫到 PostgreSQL 了（不可撤銷！）
# 但 Sink B 要 seek back 重新處理
# → Sink A 會收到重複資料 ← ❌ 語意不對
```

依序 flush 的好處：Sink A backpressure 時，Sink B 還沒 flush，`on_paused()` 直接丟棄。
seek back 後 A 和 B 都重新處理，**兩者資料一致**。

平行 flush 的問題：Sink A 已經成功寫出，Sink B backpressure 要 seek back。
重新處理時 Sink A 會收到**重複的一批資料**。除非 Sink A 是冪等的（idempotent），否則會出錯。

```
依序 flush（現在的做法）：

  情況 1：第一個 sink 就 backpressure
    Sink A: flush → SinkBackpressureError!
    Sink B: on_paused() → 丟棄 batch
    Sink C: on_paused() → 丟棄 batch
    → seek back → 重新處理 → A, B, C 都重新 add + flush
    ★ 沒有任何 sink 寫出資料，乾淨重來

  情況 2：★ Sink A 成功，Sink B backpressure
    Sink A: flush → ✅ 成功！資料已寫入 PostgreSQL（不可撤銷！）
    Sink B: flush → SinkBackpressureError!
    Sink C: on_paused() → 丟棄 batch
    → offset 不 commit → seek back → 重新處理
    → Sink A 會收到同一批資料再寫一次 ← ⚠️ 重複！
    ★ 這是 at-least-once 語意：不丟資料，但可能重複
    ★ 源碼註解也承認了："to limit the number of duplicates"（減少，不是消除）

平行 flush（提議的做法）：
  Sink A: flush → ✅ 成功！資料已寫入 PostgreSQL
  Sink B: flush → SinkBackpressureError!
  Sink C: flush → ✅ 成功！資料已寫入 S3
  → 怎麼辦？
    → 不 seek back → Sink B 的資料遺失 ❌
    → seek back → Sink A 和 C 收到重複資料 ← ⚠️ 跟依序 flush 情況 2 一樣
```

**所以依序 flush 和平行 flush 在「A 成功 B 背壓」場景下，重複問題是一樣的。**
依序 flush 的真正優勢是：排在 backpressure sink **後面**的 sink 不會寫出（`on_paused` 丟棄），
減少了需要處理重複的 sink 數量。而平行 flush 所有 sink 同時跑，成功的都寫出去了，全部都要處理重複。

```
依序 flush：A 成功, B 背壓, C 丟棄  → 只有 A 需要處理重複
平行 flush：A 成功, B 背壓, C 成功  → A 和 C 都需要處理重複
```

**不管哪種方式，sink 都必須考慮冪等性（idempotent）或容忍重複。**
這就是為什麼 Quix 文件強調 at-least-once 語意：你的 sink 要能處理重複資料。

---

**問題 2：Thread Safety**

Sink 的 `flush()` 和 `write()` 可能不是 thread-safe 的：

```python
# 典型的 BatchingSink.flush()
def flush(self):
    try:
        for (topic, partition), batch in self._batches.items():
            self.write(batch)     # ★ 子類別實作，可能用了共享的 DB connection
    finally:
        self._batches.clear()     # ★ 修改 self._batches

# 如果 flush 在 thread pool 中執行：
# - self._batches 被多個 thread 同時讀寫？
#   → 不會，每個 sink 是獨立的物件，各自有自己的 _batches
# - 但子類別的 write() 可能用了共享資源（connection pool、file handle）
#   → 取決於使用者的實作，框架無法保證
```

每個 sink 是獨立物件，所以 `_batches` 不會衝突。但使用者自定義的 `write()` 可能用了
非 thread-safe 的資源（DB connection、file handle），框架無法保證。

---

**問題 3：錯誤處理的複雜度**

```python
# 平行 flush 的錯誤處理
futures = {pool.submit(sink.flush): sink for sink in sinks}
done, _ = wait(futures, return_when=ALL_COMPLETED)

errors = {}
for future in done:
    exc = future.exception()
    if exc:
        errors[futures[future]] = exc

# 可能的情況：
# 1. 全部成功 → OK
# 2. 一個 SinkBackpressureError → trigger_backpressure
# 3. 一個非 Backpressure 的 Exception → 崩潰
# 4. 多個 sink 同時失敗，有的是 Backpressure 有的不是 → ???
# 5. 一個 Backpressure + 其他成功 → 成功的 sink 已寫出，要 seek back → 重複
#
# 依序 flush 只需要處理 1, 2, 3，平行 flush 多了 4, 5
```

---

**如果真的想做，正確的做法**：

```python
# 方案：平行 flush + 冪等 sink + FIRST_EXCEPTION 策略
futures = {pool.submit(sink.flush): sink for sink in sinks}
done, not_done = wait(futures, return_when=FIRST_EXCEPTION)

# 如果有任何失敗
if not_done or any(f.exception() for f in done):
    # 取消未完成的
    for f in not_done:
        f.cancel()
    # 所有 sink 呼叫 on_paused（包括已成功的，因為等等要 seek back）
    for sink in sinks:
        sink.on_paused()
    # trigger backpressure
    self._consumer.trigger_backpressure(...)
    return

# 全部成功 → 繼續 Step 2-5
```

但這要求：
1. **所有 sink 必須是冪等的** — 因為成功的 sink 在 seek back 後會收到重複資料
2. **sink.flush() 必須是 thread-safe** — 框架無法替使用者保證
3. **on_paused() 要能處理「已 flush 成功但要放棄」** — 現在的 on_paused 只是清 batch，不涉及回滾

---

**結論**：

| | 依序 flush（現在） | 平行 flush（提議） |
|---|---|---|
| 一致性 | ✅ 簡單：一個失敗 → 後面都不 flush | ⚠️ 需要冪等 sink |
| 效能 | ❌ 最慢的 sink 決定整體速度 | ✅ 最慢的 sink 不卡其他 sink |
| Thread safety | ✅ 單 thread，不用擔心 | ⚠️ 使用者的 write() 可能不安全 |
| 錯誤處理 | ✅ 簡單 | ⚠️ 多種組合要處理 |
| Backpressure | ✅ 一個 BP → 後面直接丟棄 | ⚠️ 已成功的 sink 資料要重複 |

Quix 選擇依序 flush 是一個**簡單性換取正確性**的設計：犧牲多 sink 的平行效能，
換來極簡的錯誤處理和一致性保證。如果你只有一個 sink（大多數場景），效能完全一樣。

如果你真的有多個慢 sink 且需要平行，更好的做法是用**多個獨立的 Application**
（各自不同的 consumer group），每個只掛一個 sink。這樣每個 sink 獨立 checkpoint，
互不影響。

### 2.8 Checkpoint 才 flush？那下游延遲不就等於 commit_interval？

**只有 BatchingSink 才是 checkpoint 才 flush。Quix 其實設計了兩種模式。**

回去看 `BaseSink.add()` 的文件：

```python
# quixstreams/sinks/base/sink.py — BaseSink
@abc.abstractmethod
def add(self, value, key, timestamp, headers, topic, partition, offset):
    """
    This method is triggered on every new processed record being sent to this sink.

    You can use it to accumulate batches of data before sending them outside, or
    to send results right away in a streaming manner and confirm a delivery later
    on flush().
    ★ ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ★ 文件明確說了兩種用法：
    ★   1. accumulate batches → flush 時寫出（BatchingSink）
    ★   2. send right away → flush 時確認結果（Streaming Sink）
    """
```

**模式 1：BatchingSink — checkpoint 才寫（高延遲、高吞吐）**

```python
# PostgreSQL, InfluxDB 等用這種
class BatchingSink(BaseSink):
    def add(self, ...):
        self._batches[tp].append(item)  # ★ 只存記憶體

    def flush(self):
        for tp, batch in self._batches.items():
            self.write(batch)           # ★ checkpoint 時才寫 DB
        self._batches.clear()
```

```
add → 記憶體  add → 記憶體  add → 記憶體  ... [5秒到] → flush → 寫 DB
                                                         ↑
                                              下游延遲 = commit_interval
```

**模式 2：Streaming Sink — add 時就發送，flush 時等結果（低延遲）**

```python
# quixstreams/sinks/community/pubsub.py — PubSubSink（完整源碼）
class PubSubSink(BaseSink):
    def add(self, value, key, timestamp, headers, topic, partition, offset):
        # ★ add 時就立即發送！不等 checkpoint
        future = self._client.publish(
            topic=self._topic,
            data=data,
            _key=key,
            _timestamp=str(timestamp),
            _offset=str(offset),
            **dict(headers),
        )
        self._futures[(topic, partition)].append(future)

    def flush(self) -> None:
        # ★ flush 不是「寫出」，而是「確認之前發送的都成功了」
        for futures in self._futures.values():
            result = concurrent.futures.wait(
                futures,
                timeout=self._flush_timeout,
                return_when=concurrent.futures.FIRST_EXCEPTION,
            )
            if result.not_done or any(f.exception() for f in result.done):
                raise SinkBackpressureError(retry_after=5.0)
```

```
add → 立即發送 → add → 立即發送 → add → 立即發送 → [5秒到] → flush = 等結果
  ↓                ↓                ↓                              ↑
下游幾乎立即收到    下游幾乎立即收到    下游幾乎立即收到         只是確認全部成功
```

KinesisSink 也是同樣的模式（累積到 500 筆就提前 submit）：

```python
# quixstreams/sinks/community/kinesis.py — KinesisSink
class KinesisSink(BaseSink):
    def add(self, ...):
        self._records[topic_partition].append(record)
        # ★ 累積到 500 筆就立即送出（不等 checkpoint）
        if len(self._records[topic_partition]) == 500:
            records = self._records.pop(topic_partition)
            self._submit(topic_partition, records)  # → thread pool 送 Kinesis API

    def flush(self):
        # 送出剩餘不滿 500 的
        for tp, records in self._records.items():
            self._submit(tp, records)
        # ★ 等所有 future 完成
        for futures in self._futures.values():
            done, not_done = wait(futures, return_when=FIRST_EXCEPTION)
            if not_done or any(f.exception() for f in done):
                raise SinkBackpressureError(retry_after=5.0)
```

---

**兩種模式的延遲比較**：

```
commit_interval = 5 秒

BatchingSink（PostgreSQL）：
  T=0s    add(msg_1) → 記憶體
  T=0.1s  add(msg_2) → 記憶體
  T=0.2s  add(msg_3) → 記憶體
  ...
  T=5s    flush() → 批次寫入 DB → msg_1 到 msg_N 這時候才出現在 DB
  ★ 下游延遲 = 0~5 秒（平均 2.5 秒）

Streaming Sink（PubSub）：
  T=0s    add(msg_1) → 立即 publish → 下游幾十毫秒內收到
  T=0.1s  add(msg_2) → 立即 publish → 下游幾十毫秒內收到
  T=0.2s  add(msg_3) → 立即 publish → 下游幾十毫秒內收到
  ...
  T=5s    flush() → 只是確認之前的 publish 都成功了
  ★ 下游延遲 = 幾十毫秒
```

---

**為什麼 BatchingSink 還是有用？**

| | BatchingSink | Streaming Sink |
|---|---|---|
| 下游延遲 | 0 ~ commit_interval | 幾十毫秒 |
| 吞吐量 | ✅ 批次寫入效率高（bulk insert） | ❌ 逐筆寫入效率低 |
| 適用場景 | DB、Object Store、Data Lake | Message Queue、PubSub、Kinesis |
| 原因 | DB 的 bulk insert 比逐筆 insert 快 10-100 倍 | MQ 天然支援逐筆發送 |

如果你的下游是 DB 且需要低延遲，可以把 `commit_interval` 調小（例如 1 秒甚至 0.5 秒），
代價是更頻繁的 checkpoint → 更多 DB 寫入次數 → 更高的 DB 負載。

```python
# 調小 commit_interval
app = Application(
    broker_address="localhost:9092",
    consumer_group="my-group",
    commit_interval=0.5,  # ★ 每 0.5 秒 flush 一次（預設 5 秒）
)

# 或用 commit_every 按訊息數觸發
app = Application(
    broker_address="localhost:9092",
    consumer_group="my-group",
    commit_every=100,     # ★ 每處理 100 筆就 flush
)
```

---

## 3. 錯誤處理

### 結論（先講答案）

- **預設行為：Application 會崩潰停止**，不會跳過任何訊息
- 可以透過 `on_processing_error` callback 回傳 `True` 來跳過出錯的訊息
- 跳過的訊息需要**自己在 callback 中記錄**（寫 log、寫到 dead letter queue 等）
- Quix 本身不提供內建的 dead letter queue 機制

### 3.1 三個錯誤 Callback

**`quixstreams/error_callbacks.py` (完整源碼)**
```python
import logging
from typing import Callable, Optional

from .models import RawConfluentKafkaMessageProto, Row

ProcessingErrorCallback = Callable[[Exception, Optional[Row], logging.Logger], bool]
ConsumerErrorCallback = Callable[
    [Exception, Optional[RawConfluentKafkaMessageProto], logging.Logger], bool
]
ProducerErrorCallback = Callable[[Exception, Optional[Row], logging.Logger], bool]


def default_on_consumer_error(
    exc: Exception,
    message: Optional[RawConfluentKafkaMessageProto],
    logger: logging.Logger,
):
    topic, partition, offset = None, None, None
    if message is not None:
        topic, partition, offset = (
            message.topic(),
            message.partition(),
            message.offset(),
        )
    logger.exception(
        f"Failed to consume a message from Kafka: "
        f'partition="{topic}[{partition}]" offset="{offset}"',
    )
    return False       # <-- 回傳 False = 不跳過 = 崩潰


def default_on_processing_error(
    exc: Exception, row: Row, logger: logging.Logger
) -> bool:
    logger.exception(
        f"Failed to process a Row: "
        f'partition="{row.topic}[{row.partition}]" offset="{row.offset}"',
    )
    return False       # <-- 回傳 False = 不跳過 = 崩潰


def default_on_producer_error(
    exc: Exception, row: Optional[Row], logger: logging.Logger
) -> bool:
    topic, partition, offset = None, None, None
    if row is not None:
        topic, partition, offset = row.topic, row.partition, row.offset
    logger.exception(
        f"Failed to produce a message to Kafka: "
        f'partition="{topic}[{partition}]" offset="{offset}"',
    )
    return False       # <-- 回傳 False = 不跳過 = 崩潰
```

**所有預設 callback 都回傳 `False`，代表「不要吞掉這個錯誤」→ exception 會被 raise → Application 崩潰停止。**

### 3.2 Application 如何註冊 error callback

**`quixstreams/app.py` (Application.__init__ 節錄)**
```python
class Application:
    def __init__(
        self,
        broker_address: ...,
        # ...
        on_consumer_error: Optional[ConsumerErrorCallback] = None,
        on_processing_error: Optional[ProcessingErrorCallback] = None,
        on_producer_error: Optional[ProducerErrorCallback] = None,
        # ...
    ):
        # ...
        self._on_processing_error = on_processing_error or default_on_processing_error
        # ...
        self._consumer = self._get_internal_consumer(
            on_error=on_consumer_error,
            extra_config_overrides=consumer_extra_config_overrides,
        )
        self._producer = self._get_internal_producer(on_error=on_producer_error)
```

### 3.3 _process_message — 處理錯誤的核心位置

**`quixstreams/app.py` (完整 _process_message 方法)**
```python
def _process_message(self, dataframe_composed):
    # Serve producer callbacks
    self._producer.poll(self._config.producer_poll_timeout)
    rows = self._consumer.poll_row(
        timeout=self._config.consumer_poll_timeout,
        buffered=self._dataframe_registry.requires_time_alignment,
    )

    if rows is None:
        self._run_tracker.set_message_consumed(False)
        return

    # Deserializer may return multiple rows for a single message
    rows = rows if isinstance(rows, list) else [rows]
    if not rows:
        return

    first_row = rows[0]
    topic_name, partition, offset = (
        first_row.topic,
        first_row.partition,
        first_row.offset,
    )

    for row in rows:
        context = copy_context()
        context.run(set_message_context, row.context)
        try:
            # Execute StreamingDataFrame in a context
            context.run(
                dataframe_composed[topic_name],
                row.value,
                row.key,
                row.timestamp,
                row.headers,
            )
        except Exception as exc:
            # TODO: This callback might be triggered because of Producer
            #  errors too because they happen within ".process()"
            to_suppress = self._on_processing_error(exc, row, logger)
            if not to_suppress:
                raise                   # <-- 不跳過就 raise，Application 崩潰

    # Store the message offset after it's successfully processed
    self._processing_context.store_offset(
        topic=topic_name, partition=partition, offset=offset
    )
    self._run_tracker.set_message_consumed(True)
    self._producer._broker_available()
    self._consumer._broker_available()

    if self._on_message_processed is not None:
        self._on_message_processed(topic_name, partition, offset)
```

**關鍵邏輯**：
1. SDF pipeline 在 `context.run(dataframe_composed[topic_name], ...)` 執行
2. 如果拋出任何 `Exception`，呼叫 `self._on_processing_error(exc, row, logger)`
3. 如果 callback 回傳 `True`（suppress），**跳過這筆訊息**，繼續處理下一條
4. 如果 callback 回傳 `False`（不 suppress），**re-raise exception**

### 3.4 _exception_handler — Application 層級的最後防線

**`quixstreams/app.py` (_exception_handler)**
```python
def _exception_handler(self, exc_type, exc_val, exc_tb):
    fail = False

    # Sources and the application are independent.
    # If a source fails, the application can shutdown gracefully.
    if exc_val is not None and exc_type is not SourceException:
        fail = True

    self.stop(fail=fail)
```

**`quixstreams/app.py` (stop 方法)**
```python
def stop(self, fail: bool = False):
    """
    Stop the internal poll loop and the message processing.

    :param fail: if True, signals that application is stopped due
        to unhandled exception, and it shouldn't commit the current checkpoint.
    """

    self._run_tracker.stop()
    if fail:
        # Update "_failed" only when fail=True to prevent stop(failed=False) from
        # resetting it
        self._failed = True

    if self._state_manager.using_changelogs:
        self._state_manager.stop_recovery()
```

`_exception_handler` 是透過 `exit_stack.push()` 註冊的，是 `with` 區塊退出時的最後防線：

```python
def run(self, ...):
    # ...
    exit_stack = contextlib.ExitStack()
    exit_stack.enter_context(self._processing_context)
    exit_stack.enter_context(self._state_manager)
    exit_stack.enter_context(self._consumer)
    exit_stack.enter_context(self._source_manager)
    exit_stack.push(self._exception_handler)       # <-- 最後防線

    with exit_stack:
        if self._dataframe_registry.consumer_topics:
            collector = self._run_tracker.get_collector(
                collect=collect, metadata=metadata
            )
            self._run_dataframe(sink=collector)
        else:
            self._run_sources()
```

### 3.5 _run_dataframe — 主循環

**`quixstreams/app.py` (_run_dataframe)**
```python
def _run_dataframe(self, sink: Optional[VoidExecutor] = None):
    changelog_topics = self._topic_manager.changelog_topics_list

    state_manager = self._state_manager
    processing_context = self._processing_context
    source_manager = self._source_manager
    process_message = self._process_message
    printer = self._processing_context.printer
    run_tracker = self._run_tracker
    consumer = self._consumer

    consumer.subscribe(
        topics=self._dataframe_registry.consumer_topics + changelog_topics,
        on_assign=self._on_assign,
        on_revoke=self._on_revoke,
        on_lost=self._on_lost,
    )

    dataframes_composed = self._dataframe_registry.compose_all(sink=sink)

    processing_context.init_checkpoint()
    run_tracker.set_as_running()
    logger.info("The application started and is now processing incoming messages")
    # Start polling Kafka for messages and callbacks
    while run_tracker.running:
        if state_manager.recovery_required:
            state_manager.do_recovery()
            run_tracker.timeout_refresh()
        else:
            process_message(dataframes_composed)         # <-- 這裡如果 raise，整個 while 退出
            processing_context.commit_checkpoint()
            consumer.resume_backpressured()
            source_manager.raise_for_error()
            # ...
```

**如果 `process_message()` 中的 exception 沒有被 suppress，它會 raise 出來，跳出 while 迴圈，觸發 `exit_stack` 的 cleanup，`_exception_handler(fail=True)` 被呼叫，Application 停止。**

### 3.6 commit_checkpoint 中的錯誤

**`quixstreams/processing/context.py` (commit_checkpoint)**
```python
def commit_checkpoint(self, force: bool = False):
    """
    Attempts finalizing the current Checkpoint only if the Checkpoint is "expired",
    or `force=True` is passed, otherwise do nothing.

    To finalize: the Checkpoint will be committed if it has any stored offsets,
    else just close it. A new Checkpoint is then created.

    :param force: if `True`, commit the Checkpoint before its expiration deadline.
    """
    if self.checkpoint.expired() or force:
        if self.checkpoint.empty():
            self.checkpoint.close()
        else:
            logger.debug(f"Committing a checkpoint; forced={force}")
            start = time.monotonic()
            self.checkpoint.commit()
            elapsed = round(time.monotonic() - start, 2)
            logger.debug(
                f"Committed a checkpoint; forced={force}, time_elapsed={elapsed}s"
            )
        self.init_checkpoint()
```

Checkpoint.commit() 中可能拋出的例外：

| 步驟 | 例外 | 說明 |
|------|------|------|
| Step 1 Flush sinks | `SinkBackpressureError` | 被內部捕獲，觸發反壓（不崩潰） |
| Step 1 Flush sinks | 其他 Exception | **崩潰** |
| Step 2 Produce changelogs | `StoreTransactionFailed` | **崩潰** |
| Step 3 Flush producer | `CheckpointProducerTimeout` | **崩潰** |
| Step 4 Commit offsets | `CheckpointConsumerCommitError` | **崩潰** |
| Step 5 Flush state | 任何寫入錯誤 | **崩潰** |

**只有 `SinkBackpressureError` 被優雅處理**（暫停 + 重試），其他所有錯誤都會導致 Application 崩潰。

### 3.7 錯誤處理流程圖

```
消息進入 _process_message()
         │
    SDF pipeline 執行
         │
    ┌────▼────┐
    │ 拋出例外？│
    └────┬────┘
         │ Yes
         ▼
  on_processing_error(exc, row, logger)
         │
    ┌────▼──────────┐
    │ 回傳 True?     │
    │ (suppress)     │
    └──┬─────────┬──┘
       │ Yes     │ No
       ▼         ▼
   跳過這筆    raise exc
   繼續下一筆        │
                    ▼
              while 迴圈跳出
                    │
                    ▼
              _exception_handler(fail=True)
                    │
                    ▼
              Application.stop()
                    │
                    ▼
              ProcessingContext.__exit__()
              (exactly_once → abort_transaction)
                    │
                    ▼
              Application 結束
```

### 3.8 如何跳過出錯的訊息並記錄

Quix 沒有內建 dead letter queue，但你可以透過 `on_processing_error` 自行實作：

```python
import json
import logging
from quixstreams import Application

# 方法 1: 寫到日誌檔案
def on_error_log_and_skip(exc, row, logger):
    """記錄錯誤並跳過"""
    logger.error(
        f"SKIPPED message: topic={row.topic} partition={row.partition} "
        f"offset={row.offset} key={row.key} error={exc}",
        exc_info=True,
    )
    # 可選：把失敗的消息寫到一個檔案
    with open("dead_letters.jsonl", "a") as f:
        f.write(json.dumps({
            "topic": row.topic,
            "partition": row.partition,
            "offset": row.offset,
            "key": str(row.key),
            "value": str(row.value),
            "error": str(exc),
        }) + "\n")
    return True  # True = 跳過，不崩潰


app = Application(
    broker_address="localhost:9092",
    consumer_group="my-group",
    on_processing_error=on_error_log_and_skip,  # <-- 傳入自定義 callback
)
```

```python
# 方法 2: 寫到另一個 Kafka topic（Dead Letter Queue）
from confluent_kafka import Producer

dlq_producer = Producer({"bootstrap.servers": "localhost:9092"})

def on_error_dlq(exc, row, logger):
    """把失敗的消息送到 DLQ topic"""
    logger.error(
        f"Sending to DLQ: topic={row.topic} partition={row.partition} "
        f"offset={row.offset} error={exc}"
    )
    dlq_producer.produce(
        topic="my-app-dlq",
        key=row.key if isinstance(row.key, bytes) else str(row.key).encode(),
        value=json.dumps({
            "original_topic": row.topic,
            "partition": row.partition,
            "offset": row.offset,
            "value": row.value,
            "error": str(exc),
        }).encode(),
    )
    dlq_producer.flush()
    return True  # 跳過


app = Application(
    broker_address="localhost:9092",
    consumer_group="my-group",
    on_processing_error=on_error_dlq,
)
```

### 3.9 不同層級的錯誤與行為總結

| 層級 | 觸發時機 | callback 參數 | 預設行為 | 可跳過？ |
|------|---------|-------------|---------|---------|
| Consumer Error | Kafka poll 失敗、反序列化失敗 | `on_consumer_error` | 崩潰 | 是（回傳 True） |
| Processing Error | SDF pipeline 中任何 exception | `on_processing_error` | 崩潰 | 是（回傳 True） |
| Producer Error | 序列化失敗、produce 到 Kafka 失敗 | `on_producer_error` | 崩潰 | 是（回傳 True） |
| Sink Flush Error | Sink.flush() 拋出非 Backpressure 異常 | 無 callback | 崩潰 | 否 |
| Sink Backpressure | Sink.flush() 拋出 `SinkBackpressureError` | 無 callback | 暫停 + 重試 | 自動處理 |
| Checkpoint Error | changelog/offset commit 失敗 | 無 callback | 崩潰 | 否 |

### 3.10 重要注意事項

1. **跳過訊息 ≠ 不 commit offset**：如果 `on_processing_error` 回傳 `True`，`_process_message` 中的 for loop 會繼續下一個 row，但 `store_offset()` 在 for loop 之後才呼叫。這代表**只要同一批 rows 中有一個成功處理，offset 就會被記錄**。

2. **Sink 層級的錯誤無法跳過**：如果 `sink.flush()` 拋出非 `SinkBackpressureError` 的異常，沒有 callback 可以捕獲，Application 會崩潰。你需要**在 Sink 的 `write()` 方法中自行處理 try/except**。

3. **At-least-once 語義**：反壓或崩潰重啟後，上次 checkpoint 之後的所有訊息都會被重新消費。你的 Sink 必須具備冪等性（idempotent），或者能容忍重複資料。

4. **Exactly-once 模式下**：如果 `fail=True`，`ProcessingContext.__exit__()` 會呼叫 `self.producer.abort_transaction(5)` 放棄未完成的事務。
```python
def __exit__(self, exc_type, exc_val, exc_tb):
    if self.exactly_once:
        self.producer.abort_transaction(5)
    self.printer.clear()
```
