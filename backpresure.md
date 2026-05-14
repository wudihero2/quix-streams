# Quix Streams: Backpressure 機制 源碼深度解析

## 目錄

1. [總覽](#1-總覽)
2. [SinkBackpressureError — 反壓信號](#2-sinkbackpressureerror)
3. [Checkpoint.commit() — 反壓處理入口](#3-checkpointcommit)
4. [InternalConsumer — 暫停與恢復 partition](#4-internalconsumer)
5. [Consumer 端 Buffer 反壓 — 時間對齊消費](#5-consumer-端-buffer-反壓)
6. [PartitionBuffer / PartitionBufferGroup — Buffer 管理](#6-partitionbuffer)
7. [InternalConsumerBuffer — 全局 Buffer 管理](#7-internalconsumerbuffer)
8. [BatchingSink.on_paused() — Sink 側丟棄資料](#8-batchingsinkonpaused)
9. [完整流程圖](#9-完整流程圖)

---

## 1. 總覽

Quix Streams 有**兩層**反壓機制：

| 層級 | 觸發條件 | 行為 |
|------|---------|------|
| **Sink 反壓** | Sink.flush() 拋出 `SinkBackpressureError` | 暫停所有 data partition，seek 回 checkpoint 起始 offset，等待 `retry_after` 秒後恢復 |
| **Consumer Buffer 反壓** | 某個 topic-partition 的 buffer 滿了（`max_partition_buffer_size`） | 暫停該 partition 的消費，讓其他 partition 追上，buffer 消耗後恢復 |

---

## 2. SinkBackpressureError

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

**使用方式**：在自定義 Sink 的 `write()` 方法中，當目標系統無法接受更多寫入時拋出：
```python
class MyDatabaseSink(BatchingSink):
    def write(self, batch: SinkBatch):
        try:
            self.db.bulk_insert(batch)
        except DatabaseOverloadedError:
            raise SinkBackpressureError(retry_after=30.0)  # 暫停 30 秒
```

---

## 3. Checkpoint.commit() — 反壓處理入口

**`quixstreams/checkpointing/checkpoint.py` (commit 方法，完整 Step 1)**
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
    for sink in self._sink_manager.sinks:
        if backpressured:
            # Drop the accumulated data for the other sinks
            # if one of them is backpressured to limit the number of duplicates
            # when the data is reprocessed again
            sink.on_paused()
            continue

        try:
            sink.flush()
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

    # Step 2-5 只在沒有反壓時執行...
    # Step 2. Produce the changelogs
    for (
        stream_id,
        partition,
        store_name,
    ), transaction in self._store_transactions.items():
        topics = self._dataframe_registry.get_topics_for_stream_id(
            stream_id=stream_id
        )
        processed_offsets = {
            topic: offset
            for (topic, partition_), offset in self._tp_offsets.items()
            if topic in topics and partition_ == partition
        }
        if transaction.failed:
            raise StoreTransactionFailed(
                f'Detected a failed transaction for store "{store_name}", '
                f"the checkpoint is aborted"
            )
        transaction.prepare(processed_offsets=processed_offsets)

    # Step 3. Flush producer
    logger.debug("Checkpoint: flushing producer")
    unproduced_msg_count = self._producer.flush()
    if unproduced_msg_count > 0:
        raise CheckpointProducerTimeout(
            f"'{unproduced_msg_count}' messages failed to be produced before "
            f"the producer flush timeout"
        )

    # Step 4. Commit offsets to Kafka
    offsets = [
        TopicPartition(topic=topic, partition=partition, offset=offset + 1)
        for (topic, partition), offset in self._tp_offsets.items()
    ]

    if self._exactly_once:
        self._producer.commit_transaction(
            offsets, self._consumer.consumer_group_metadata()
        )
    else:
        logger.debug("Checkpoint: committing consumer")
        try:
            partitions = self._consumer.commit(offsets=offsets, asynchronous=False)
        except KafkaException as e:
            raise CheckpointConsumerCommitError(e.args[0]) from None

        for partition in partitions:
            if partition.error:
                raise CheckpointConsumerCommitError(partition.error)

    # Step 5. Flush state store partitions to disk
    produced_offsets = self._producer.offsets
    for transaction in self._store_transactions.values():
        changelog_tp = transaction.changelog_topic_partition
        changelog_offset = (
            produced_offsets.get(changelog_tp) if changelog_tp is not None else None
        )
        transaction.flush(changelog_offset=changelog_offset)
```

**關鍵邏輯**：
1. 反壓時 `self._consumer.trigger_backpressure()` 暫停所有 data partition
2. `offsets_to_seek=self._starting_tp_offsets.copy()` — seek 回到本次 checkpoint 的**第一個** offset
3. 反壓時 **不 commit offset**，直接 return — Step 2-5 全部跳過
4. 後續的 Sink 呼叫 `on_paused()` 丟棄累積的資料

**Checkpoint 中 `_starting_tp_offsets` 的記錄方式**：
```python
def store_offset(self, topic: str, partition: int, offset: int):
    tp = (topic, partition)
    stored_offset = self._tp_offsets.get(tp, -1)
    if offset <= stored_offset:
        raise InvalidStoredOffset(
            f"Cannot store offset smaller or equal than already processed"
            f" one: {offset} <= {stored_offset}"
        )
    self._tp_offsets[tp] = offset
    # Track the first processed offset in the transaction to rewind back to it
    # in case of sink backpressure
    if tp not in self._starting_tp_offsets:
        self._starting_tp_offsets[tp] = offset
    self._total_offsets_processed += 1
```

---

## 4. InternalConsumer — 暫停與恢復 partition

**`quixstreams/internal_consumer/consumer.py` (完整反壓相關方法)**

### 4.1 初始化

```python
class InternalConsumer(BaseConsumer):
    _backpressure_resume_at: float

    def __init__(
        self,
        broker_address: Union[str, ConnectionConfig],
        consumer_group: str,
        auto_offset_reset: AutoOffsetReset,
        auto_commit_enable: bool = True,
        on_commit: Optional[
            Callable[[Optional[KafkaError], list[TopicPartition]], None]
        ] = None,
        extra_config: Optional[dict] = None,
        on_error: Optional[ConsumerErrorCallback] = None,
        max_partition_buffer_size: int = 10000,
    ):
        super().__init__(
            broker_address=broker_address,
            consumer_group=consumer_group,
            auto_offset_reset=auto_offset_reset,
            auto_commit_enable=auto_commit_enable,
            on_commit=on_commit,
            extra_config=extra_config,
        )
        self._on_error: ConsumerErrorCallback = on_error or default_on_consumer_error
        self._topics: dict[str, Topic] = {}
        self._backpressurred_tps: set[TopicPartition] = set()
        self._max_partition_buffer_size = max_partition_buffer_size
        self._buffer = InternalConsumerBuffer(
            max_partition_buffer_size=self._max_partition_buffer_size
        )
        self.reset_backpressure()
```

### 4.2 trigger_backpressure() — 核心暫停邏輯

```python
def trigger_backpressure(
    self,
    offsets_to_seek: dict[tuple[str, int], int],
    resume_after: float,
):
    """
    Pause all partitions for the certain period of time and seek the partitions
    provided in the `offsets_to_seek` dict.

    This method is supposed to be called in case of backpressure from Sinks.
    """
    resume_at = monotonic() + resume_after
    self._backpressure_resume_at = min(self._backpressure_resume_at, resume_at)

    changelog_topics = {k for k, v in self._topics.items() if v.is_changelog}
    for tp in self.assignment():
        # Pause only data TPs excluding changelog TPs
        if tp.topic in changelog_topics:
            continue

        position, *_ = self.position([tp])
        logger.debug(
            f'Pausing topic partition "{tp.topic}[{tp.partition}]" for {resume_after}s; '
            f"position={position.offset}"
        )
        self.pause(partitions=[tp])                      # <-- 暫停 partition
        self._buffer.clear(topic=tp.topic, partition=tp.partition)  # <-- 清空 buffer

        # Seek the TP back to the "offset_to_seek" to start from it on resume.
        # The "offset_to_seek" is provided by the Checkpoint and is expected to be the
        # first offset processed in the checkpoint.
        # There may be no offset for the TP if no message has been processed yet.
        seek_offset = offsets_to_seek.get((tp.topic, tp.partition))
        if seek_offset is not None:
            logger.debug(
                f'Seek the paused partition "{tp.topic}[{tp.partition}]" back to '
                f"offset {seek_offset}"
            )
            self.seek(                                   # <-- seek 回起始 offset
                partition=TopicPartition(
                    topic=tp.topic, partition=tp.partition, offset=seek_offset
                )
            )

        self._backpressurred_tps.add(tp)
```

**逐步拆解**：
1. 計算 `resume_at` 時間點 = 現在 + `retry_after` 秒
2. 遍歷所有 assigned partition（排除 changelog topic）
3. 對每個 partition：`pause()` 暫停消費
4. 清空該 partition 的 buffer
5. `seek()` 回到 checkpoint 起始 offset（這樣恢復後會重新消費）
6. 記錄到 `_backpressurred_tps` 集合

### 4.3 resume_backpressured() — 等待後恢復

```python
def resume_backpressured(self):
    """
    Resume consuming from assigned data partitions after the wait period has elapsed.
    """
    if self._backpressure_resume_at > monotonic():
        return                                          # <-- 還沒到時間，什麼都不做

    # Resume the previously backpressured TPs
    for tp in self._backpressurred_tps:
        logger.debug(f'Resuming topic partition "{tp.topic}[{tp.partition}]"')
        self.resume(partitions=[tp])                    # <-- 恢復消費
    self.reset_backpressure()
```

### 4.4 reset_backpressure()

```python
def reset_backpressure(self):
    # Reset the timeout back to its initial state
    self._backpressure_resume_at = float("inf")
    self._backpressurred_tps.clear()
```

### 4.5 在主循環中的呼叫位置

**`quixstreams/app.py` (_run_dataframe)**
```python
while run_tracker.running:
    if state_manager.recovery_required:
        state_manager.do_recovery()
        run_tracker.timeout_refresh()
    else:
        process_message(dataframes_composed)
        processing_context.commit_checkpoint()      # <-- 這裡可能觸發 trigger_backpressure
        consumer.resume_backpressured()             # <-- 每次循環檢查是否該恢復
        source_manager.raise_for_error()
```

---

## 5. Consumer 端 Buffer 反壓 — 時間對齊消費

當 Application 使用 join 或 concat（需要時間對齊）時，consumer 使用 buffered 模式消費。Buffer 滿了也會觸發反壓。

### 5.1 poll_row — buffered vs unbuffered

```python
def poll_row(
    self, timeout: Optional[float] = None, buffered: bool = False
) -> Union[Row, list[Row], None]:
    if buffered:
        msg = self._poll_buffered(timeout=timeout)
    else:
        msg = self._poll_unbuffered(timeout=timeout)

    if msg is None:
        return None

    topic_name = msg.topic()
    try:
        topic = self._topics[topic_name]
        row_or_rows = topic.row_deserialize(message=msg)
        return row_or_rows
    except IgnoreMessage:
        return None
    except Exception as exc:
        to_suppress = self._on_error(exc, msg, logger)
        if to_suppress:
            return None
        raise
```

### 5.2 _poll_buffered — 從 buffer 取訊息

```python
def _poll_buffered(
    self, timeout: Optional[float] = None
) -> Optional[SuccessfulConfluentKafkaMessageProto]:
    """
    Poll messages in a buffered way to provide in-order reads across multiple
    topic partitions with the same partition number.
    """
    # Probe the buffer and return immediately if there's data available
    msg = self._buffer.pop()
    if msg is None:
        # If the buffer is empty, feed it and try the probing again
        self._feed_buffer(timeout=timeout)
        msg = self._buffer.pop()

    return msg
```

### 5.3 _feed_buffer — 填充 buffer 並觸發暫停/恢復

```python
def _feed_buffer(self, timeout: Optional[float] = None):
    """
    Feed the internal buffer and pause or resume the assigned partitions for the
    balanced consumption.
    """
    try:
        messages = self.consume(
            num_messages=self._max_partition_buffer_size, timeout=timeout
        )
    except PartitionAssignmentError:
        raise
    except Exception as exc:
        to_suppress = self._on_error(exc, None, logger)
        if not to_suppress:
            raise
        messages = []

    # Get the recent cached high watermarks
    high_watermarks: dict[tuple[str, int], int] = {}
    for tp in self.assignment():
        topic_obj = self._topics[tp.topic]
        if not topic_obj.is_changelog:
            _, high = self.get_watermark_offsets(partition=tp, cached=True)
            high_watermarks[(tp.topic, tp.partition)] = high

    # Create a generator to validate messages
    valid_messages = _validate_message_batch(messages, on_error=self._on_error)

    # Feed the batch and the watermarks to the buffer
    self._buffer.feed(messages=valid_messages, high_watermarks=high_watermarks)

    # Resume partitions with empty buffers        <-- buffer 反壓恢復
    for topic, partition in self._buffer.resume_empty():
        tp = TopicPartition(topic=topic, partition=partition)
        # Make sure we don't resume partitions if they're backpressured
        if not tp in self._backpressurred_tps:     # <-- 不恢復 Sink 反壓的 partition
            self.resume([tp])

    # Pause partitions with full buffers           <-- buffer 反壓觸發
    for topic, partition in self._buffer.pause_full():
        self.pause([TopicPartition(topic=topic, partition=partition)])
```

**關鍵**：Buffer 反壓和 Sink 反壓是獨立的。`resume_empty()` 會檢查 `_backpressurred_tps` 避免恢復被 Sink 反壓暫停的 partition。

---

## 6. PartitionBuffer / PartitionBufferGroup — Buffer 管理

**`quixstreams/internal_consumer/buffering.py`**

### 6.1 PartitionBuffer — 單個 topic-partition 的 buffer

```python
class Idleness(enum.Enum):
    IDLE = 1      # 沒有更多新訊息
    ACTIVE = 2    # 還有更多訊息可消費
    UNKNOWN = 3   # 不確定


class PartitionBuffer:
    def __init__(
        self,
        partition: int,
        topic: str,
        max_size: int,
    ):
        self.partition = partition
        self.topic = topic
        self.next_timestamp = float("inf")
        self._paused = False
        self._max_size = max_size
        self._max_offset = -1
        self._high_watermark = -1001
        self._messages: deque[SuccessfulConfluentKafkaMessageProto] = deque()

    def set_high_watermark(self, offset: int):
        self._high_watermark = offset

    def idleness(self) -> Idleness:
        """
        Check if the partition is idle or has more data to be consumed.
        """
        high_watermark = self._high_watermark
        if high_watermark < 0:
            return Idleness.UNKNOWN
        return (
            Idleness.IDLE if self._max_offset + 1 >= high_watermark else Idleness.ACTIVE
        )

    def append(self, message: SuccessfulConfluentKafkaMessageProto):
        offset = message.offset()
        if offset <= self._max_offset:
            raise ValueError(
                f"Invalid offset {offset} (max offset is {self._max_offset})"
            )
        self._max_offset = offset
        if self.next_timestamp == float("inf"):
            _, self.next_timestamp = message.timestamp()
        self._messages.append(message)

    def popleft(self) -> Optional[SuccessfulConfluentKafkaMessageProto]:
        messages = self._messages
        try:
            item = messages.popleft()
        except IndexError:
            return None

        try:
            self.next_timestamp = messages[0].timestamp()[1]
        except IndexError:
            self.next_timestamp = float("inf")
            self._high_watermark = -1001
        return item

    def empty(self) -> bool:
        return not self._messages

    def full(self) -> bool:
        return len(self._messages) >= self._max_size    # <-- 軟限制

    @property
    def paused(self) -> bool:
        return self._paused

    def pause(self):
        self._paused = True

    def resume(self):
        self._paused = False

    def clear(self):
        self.next_timestamp = float("inf")
        self._max_offset = -1
        self._high_watermark = -1001
        self._messages.clear()
        self._paused = False
```

### 6.2 PartitionBufferGroup — 同一 partition number 下多個 topic 的 buffer 組

```python
class PartitionBufferGroup:
    def __init__(self, partition: int, max_size: int):
        self._partition = partition
        self._max_size = max_size
        self._partition_buffers: dict[str, PartitionBuffer] = {}
        self._partition_buffers_values = self._partition_buffers.values()

    def assign_partition(self, topic: str):
        if topic in self._partition_buffers:
            return
        self._partition_buffers[topic] = PartitionBuffer(
            partition=self._partition, topic=topic, max_size=self._max_size
        )

    def revoke_partition(self, topic: str):
        self._partition_buffers.pop(topic)

    def set_high_watermarks(self, offsets: dict[str, int]):
        for topic, watermark in offsets.items():
            buffer = self._partition_buffers[topic]
            buffer.set_high_watermark(watermark)

    def append(self, message: SuccessfulConfluentKafkaMessageProto):
        partition_buffer = self._partition_buffers[message.topic()]
        partition_buffer.append(message=message)

    def pop(self) -> Optional[SuccessfulConfluentKafkaMessageProto]:
        """
        Pop a message from the partition buffer with the smallest next_timestamp.
        """
        buffers = self._partition_buffers_values
        if len(buffers) == 1:
            buffer = next(iter(buffers))
            return buffer.popleft()
        elif len(buffers) > 1:
            # 有多個 topic buffer 時，等所有非 idle 的 buffer 都有資料
            for buffer in buffers:
                if buffer.empty() and buffer.idleness() != Idleness.IDLE:
                    return None                # <-- 等待慢的 topic 追上

            buffer = min(buffers, key=_next_timestamp_getter)  # <-- 取 timestamp 最小的
            return buffer.popleft()
        else:
            return None

    def pause_full(self) -> list[tuple[str, int]]:
        """
        Pause full PartitionBuffers. Single-partition groups are never paused.
        """
        if len(self._partition_buffers_values) == 1:
            return []                          # <-- 只有一個 topic 時不暫停

        tps = []
        for buffer in self._partition_buffers_values:
            if (
                not buffer.paused
                and buffer.full()                           # <-- buffer 滿了
                and buffer.idleness() == Idleness.ACTIVE    # <-- 且還有更多資料
            ):
                buffer.pause()
                tps.append((buffer.topic, buffer.partition))
        return tps

    def resume_empty(self) -> list[tuple[str, int]]:
        tps = []
        for buffer in self._partition_buffers_values:
            if buffer.paused and (
                buffer.idleness() != Idleness.ACTIVE or not buffer.full()
            ):
                buffer.resume()
                tps.append((buffer.topic, buffer.partition))
        return tps

    def clear(self, topic: str):
        if buffer := self._partition_buffers.get(topic):
            buffer.clear()
```

---

## 7. InternalConsumerBuffer — 全局 Buffer 管理

```python
class InternalConsumerBuffer:
    def __init__(self, max_partition_buffer_size: int = 10000):
        """
        A buffer to align messages across different topics by timestamps.
        Groups buffered messages by partition and provides the message with
        the smallest timestamp across all assigned topics.
        """
        self._partition_groups: dict[int, PartitionBufferGroup] = {}
        self._max_partition_buffer_size = max_partition_buffer_size

    def assign_partitions(self, topic_partitions: list[TopicPartition]):
        topic_partitions = sorted(topic_partitions, key=lambda t: t.partition)
        for tp in topic_partitions:
            partition_group = self._partition_groups.setdefault(
                tp.partition,
                PartitionBufferGroup(
                    partition=tp.partition, max_size=self._max_partition_buffer_size
                ),
            )
            partition_group.assign_partition(topic=tp.topic)

    def revoke_partitions(self, topic_partitions: list[TopicPartition]):
        for tp in topic_partitions:
            partition_group = self._partition_groups.get(tp.partition)
            if partition_group is not None:
                partition_group.revoke_partition(topic=tp.topic)

    def feed(
        self,
        messages: Iterable[SuccessfulConfluentKafkaMessageProto],
        high_watermarks: dict[tuple[str, int], int],
    ):
        for message in messages:
            partition_group = self._partition_groups[message.partition()]
            partition_group.append(message=message)

        for partition_group in self._partition_groups.values():
            group_watermarks = {
                topic: watermark
                for (topic, partition), watermark in high_watermarks.items()
                if partition == partition_group.partition
            }
            partition_group.set_high_watermarks(offsets=group_watermarks)

    def pop(self) -> Optional[SuccessfulConfluentKafkaMessageProto]:
        for partition_group in self._partition_groups.values():
            message = partition_group.pop()
            if message is not None:
                return message
        return None

    def pause_full(self) -> list[tuple[str, int]]:
        tps = []
        for partition_group in self._partition_groups.values():
            tps += partition_group.pause_full()
        return tps

    def resume_empty(self) -> list[tuple[str, int]]:
        tps = []
        for partition_group in self._partition_groups.values():
            tps += partition_group.resume_empty()
        return tps

    def clear(self, topic: str, partition: int):
        partition_group = self._partition_groups.get(partition)
        if partition_group is not None:
            partition_group.clear(topic)

    def close(self):
        self._partition_groups.clear()
```

---

## 8. BatchingSink.on_paused() — Sink 側丟棄資料

**`quixstreams/sinks/base/sink.py` (BatchingSink 相關)**

```python
class BatchingSink(BaseSink):
    _batches: dict[tuple[str, int], SinkBatch]

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
                self.write(batch)           # <-- 這裡可能拋出 SinkBackpressureError
        finally:
            self._batches.clear()           # <-- 無論成敗都清空 buffer

    def on_paused(self):
        """
        When the destination is already backpressured, drop the accumulated batches.
        """
        self._batches.clear()               # <-- 反壓時直接丟棄
```

---

## 9. 完整流程圖

### 9.1 Sink 反壓流程

```
正常處理循環：
  poll() → deserialize → SDF pipeline → sink.add() (累積到記憶體)
                                              │
                                     checkpoint 到期
                                              │
                                     commit() Step 1: flush sinks
                                              │
                                    ┌─────────▼─────────┐
                                    │ sink_A.flush()     │
                                    │ → write() 拋出     │
                                    │ SinkBackpressureError│
                                    │ (retry_after=30s)  │
                                    └─────────┬─────────┘
                                              │
                           ┌──────────────────┼──────────────────┐
                           │                  │                  │
                           ▼                  ▼                  ▼
                  sink_B.on_paused()  sink_C.on_paused()  consumer.trigger_backpressure()
                  (清空 batches)      (清空 batches)              │
                                                                 │
                                              ┌──────────────────┤
                                              │                  │
                                              ▼                  ▼
                                     pause(all data TPs)   seek(起始 offset)
                                              │
                                              ▼
                                     commit() return（不 commit offset）
                                              │
                                              ▼
                                     主循環繼續（但 poll 拿不到資料因為暫停了）
                                              │
                                     每次循環呼叫 resume_backpressured()
                                              │
                                     ┌────────▼────────┐
                                     │ 30秒到了嗎？     │
                                     │ monotonic() >=   │
                                     │ resume_at?       │
                                     └──┬───────────┬──┘
                                        │ No        │ Yes
                                        ▼           ▼
                                     等下次循環   resume(all TPs)
                                                    │
                                                    ▼
                                              從起始 offset 重新消費
                                              所有訊息重新處理
                                              所有 Sink 重新 add() + flush()
```

### 9.2 Consumer Buffer 反壓流程（時間對齊消費）

```
Topic A partition 0: [msg_t=100, msg_t=200, msg_t=300, ...]  ← 快
Topic B partition 0: [msg_t=150]                               ← 慢

_feed_buffer() 被呼叫：
  1. consume() 拿到一批訊息
  2. 放入對應的 PartitionBuffer
  3. 檢查每個 buffer：

     Topic A buffer: [100, 200, 300, ...500 筆]  → full()=True, ACTIVE
     Topic B buffer: [150]                        → full()=False

  4. pause_full(): 暫停 Topic A partition 0（buffer 滿了）
  5. 不暫停 Topic B（buffer 沒滿）

pop() 被呼叫：
  1. PartitionBufferGroup.pop()
  2. 比較所有 buffer 的 next_timestamp
  3. Topic A: 100, Topic B: 150
  4. 返回 Topic A 的 msg_t=100（最小的）

  ... 持續 pop 直到 buffer 空 ...

  5. Topic A buffer 空了
  6. resume_empty(): 恢復 Topic A partition 0
  7. 下次 _feed_buffer() 繼續填充
```

### 9.3 兩層反壓的交互

```
                    ┌─────────────────────┐
                    │  Consumer Buffer    │
                    │  反壓（per TP）     │
                    │                     │
                    │  觸發：buffer 滿    │
                    │  行為：暫停該 TP    │
                    │  恢復：buffer 空    │
                    └──────────┬──────────┘
                               │
                    ┌──────────▼──────────┐
                    │  Sink 反壓          │
                    │  (全局)             │
                    │                     │
                    │  觸發：flush 拋出   │
                    │  SinkBackpressureError│
                    │  行為：暫停所有 TP  │
                    │       + seek 回起始 │
                    │  恢復：等待 N 秒    │
                    └─────────────────────┘

互不干擾：
  resume_empty() 中檢查 _backpressurred_tps：
  if not tp in self._backpressurred_tps:  ← Sink 反壓的 TP 不會被 buffer 恢復
      self.resume([tp])
```
