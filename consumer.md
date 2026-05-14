# Quix Streams Consumer Rebalance & State Recovery 源碼完整解讀

> **情境**：一個 Kafka topic 有 6 個 partition，3 個 pod 使用同一個 consumer group。
> 當一個 pod 掛掉後，partition 會重新分配，state 透過 changelog topic 自動恢復。
>
> 本文件包含所有相關源碼，不需要另外開檔案閱讀。

---

## 目錄

1. [正常狀態下的 Partition 分配](#1-正常狀態下的-partition-分配)
2. [Consumer Subscribe 與 Rebalance Callback 註冊](#2-consumer-subscribe-與-rebalance-callback-註冊)
3. [Pod 死掉：on_assign 觸發 State Recovery](#3-pod-死掉on_assign-觸發-state-recovery)
4. [Recovery 完整流程](#4-recovery-完整流程)
5. [Pod 復活：on_revoke + 再次 on_assign](#5-pod-復活on_revoke--再次-on_assign)
6. [Changelog 的生產端：State 變更如何寫入 Changelog Topic](#6-changelog-的生產端state-變更如何寫入-changelog-topic)
7. [端到端流程圖](#7-端到端流程圖)
8. [關鍵源碼索引](#8-關鍵源碼索引)

---

## 1. 正常狀態下的 Partition 分配

```
Topic: my-topic (6 partitions)

Pod-1: [P0, P1]   Pod-2: [P2, P3]   Pod-3: [P4, P5]
```

Kafka 的 partition assignor（由 `librdkafka` 實作）會將 6 個 partition 平均分配給 3 個 consumer。
每個 consumer 各自持有對應 partition 的 **RocksDB state store**。

每個 partition 都有一個對應的 **changelog topic partition**，記錄所有 state 變更。
這是 recovery 的基礎 — 任何新接管 partition 的 consumer 都可以從 changelog replay state。

---

## 2. Consumer Subscribe 與 Rebalance Callback 註冊

### 2.1 Application 啟動：subscribe + 主迴圈

檔案：`quixstreams/app.py:910-955`

```python
def _run_dataframe(self, sink: Optional[VoidExecutor] = None):
    changelog_topics = self._topic_manager.changelog_topics_list

    # set refs for performance improvements
    state_manager = self._state_manager
    processing_context = self._processing_context
    source_manager = self._source_manager
    process_message = self._process_message
    printer = self._processing_context.printer
    run_tracker = self._run_tracker
    consumer = self._consumer

    # ★ 訂閱 source topic + changelog topic，同時註冊三個 rebalance callback
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

    # ★ 主迴圈：每輪先檢查是否需要 recovery，否則正常處理訊息
    while run_tracker.running:
        if state_manager.recovery_required:
            state_manager.do_recovery()       # ← 進入 recovery 模式
            run_tracker.timeout_refresh()
        else:
            process_message(dataframes_composed)  # ← 正常處理
            processing_context.commit_checkpoint()
            consumer.resume_backpressured()
            source_manager.raise_for_error()
            if self._broker_availability_timeout:
                self._producer.raise_if_broker_unavailable(
                    self._broker_availability_timeout
                )
                self._consumer.raise_if_broker_unavailable(
                    self._broker_availability_timeout
                )
            printer.print()
            run_tracker.update_status()

    logger.info("Stopping the application")
    processing_context.commit_checkpoint(force=True)
```

### 2.2 `StateStoreManager.recovery_required` — 判斷是否需要 recovery

檔案：`quixstreams/state/manager.py:89-95`

```python
@property
def recovery_required(self) -> bool:
    """
    Whether recovery needs to be done.
    """
    if self._recovery_manager:
        return self._recovery_manager.has_assignments
    return False
```

只要 `RecoveryManager` 有任何 `RecoveryPartition` 被追蹤，就需要 recovery。

### 2.3 Consumer 包裝 rebalance callback

檔案：`quixstreams/kafka/consumer.py:303-385`

```python
def _subscribe(
    self,
    topics: List[str],
    on_assign: Optional[RebalancingCallback] = None,
    on_revoke: Optional[RebalancingCallback] = None,
    on_lost: Optional[RebalancingCallback] = None,
):
    # ---- on_assign wrapper ----
    @_wrap_assignment_errors
    def _on_assign_wrapper(consumer: Consumer, partitions: List[TopicPartition]):
        for partition in partitions:
            logger.debug(
                'Assigning topic partition "%s[%s]"',
                partition.topic,
                partition.partition,
            )
            if partition.error:
                raise KafkaPartitionError(
                    f"Kafka partition error: "
                    f'partition="{partition.topic}[{partition.partition}]" '
                    f'error="{partition.error}"'
                )

        if on_assign is not None:
            on_assign(consumer, partitions)

    # ---- on_revoke wrapper ----
    @_wrap_assignment_errors
    def _on_revoke_wrapper(consumer: Consumer, partitions: List[TopicPartition]):
        for partition in partitions:
            logger.debug(
                'Revoking topic partition "%s[%s]"',
                partition.topic,
                partition.partition,
            )
            if partition.error:
                raise KafkaPartitionError(
                    f"Kafka partition error: "
                    f'partition="{partition.topic}[{partition.partition}]" '
                    f'error="{partition.error}"'
                )

        if on_revoke is not None:
            on_revoke(consumer, partitions)

    # ---- on_lost wrapper ----
    @_wrap_assignment_errors
    def _on_lost_wrapper(consumer: Consumer, partitions: List[TopicPartition]):
        for partition in partitions:
            logger.debug(
                'Losing topic partition: topic="%s" partition="%s"',
                partition.topic,
                partition.partition,
            )
            if partition.error:
                raise KafkaPartitionError(
                    f"Kafka partition error: "
                    f'partition="{partition.topic}[{partition.partition}]" '
                    f'error="{partition.error}"'
                )
        if on_lost is not None:
            on_lost(consumer, partitions)

    # ★ 最終把三個 wrapper 傳給 confluent_kafka
    return self._consumer.subscribe(
        topics=topics,
        on_assign=_on_assign_wrapper,
        on_revoke=_on_revoke_wrapper,
        on_lost=_on_lost_wrapper,
    )
```

每個 wrapper 做兩件事：
1. 檢查 partition error → 拋出 `KafkaPartitionError`
2. 呼叫 Application 層的真正 callback

---

## 3. Pod 死掉：on_assign 觸發 State Recovery

假設 Pod-3 掛掉，Kafka 觸發 rebalance，P4、P5 被重新分配：

```
Pod-1: [P0, P1, P4]   Pod-2: [P2, P3, P5]
```

Pod-1 和 Pod-2 收到新的 partition assignment，觸發 `_on_assign()`。

### 3.1 `Application._on_assign()` — Rebalance assign 入口

檔案：`quixstreams/app.py:1040-1094`

```python
def _on_assign(self, _, topic_partitions: List[TopicPartition]):
    """
    Assign new topic partitions to consumer and state.
    """
    # 有時候會收到空的 rebalance callback（更新 consumer epoch），直接跳過
    if not topic_partitions:
        return
    logger.debug("Rebalancing: assigning partitions")

    # ★ Step 0: 確保 consumer 先就位再啟動 source
    # 避免 source 在 consumer 準備好之前就 produce 資料
    self._source_manager.start_sources()

    # ★ Step 1: 手動 assign partition，立即 pause changelog topic
    self._consumer.assign(topic_partitions)
    non_changelog_topics = self._topic_manager.non_changelog_topics
    changelog_tps = [
        tp for tp in topic_partitions if tp.topic not in non_changelog_topics
    ]
    self._consumer.pause(changelog_tps)  # changelog 先暫停，recovery 時才 resume

    # ★ Step 2: 如果有 stateful store，取得每個 partition 的 committed offset
    if self._state_manager.stores:
        non_changelog_tps = [
            tp for tp in topic_partitions if tp.topic in non_changelog_topics
        ]
        committed_tps = self._consumer.committed(
            partitions=non_changelog_tps, timeout=30
        )
        committed_offsets: dict[int, dict[str, int]] = defaultdict(dict)
        for tp in committed_tps:
            if tp.error:
                raise RuntimeError(
                    f"Failed to get committed offsets for "
                    f'"{tp.topic}[{tp.partition}]" from the broker: {tp.error}'
                )
            committed_offsets[tp.partition][tp.topic] = tp.offset

        # ★ Step 3: 對每個 source topic partition，呼叫 state manager 做 assign
        for tp in non_changelog_tps:
            stream_ids = self._dataframe_registry.get_stream_ids(
                topic_name=tp.topic
            )
            for stream_id in stream_ids:
                self._state_manager.on_partition_assign(
                    stream_id=stream_id,
                    partition=tp.partition,
                    committed_offsets=committed_offsets[tp.partition],
                )
    self._run_tracker.timeout_refresh()
```

**重點**：
- Changelog topic 的 partition 會被**立即 pause**，等 recovery 時才 resume
- `committed_offsets` 用於 recovery 時判斷哪些 changelog 訊息該套用（一致性保護）

### 3.2 `StateStoreManager.on_partition_assign()` — 建立 store partition + 觸發 recovery

檔案：`quixstreams/state/manager.py:294-321`

```python
def on_partition_assign(
    self,
    stream_id: Optional[str],
    partition: int,
    committed_offsets: dict[str, int],
) -> Dict[str, StorePartition]:
    """
    Assign store partitions for each registered store for the given stream_id
    and partition number.
    """
    store_partitions = {}
    # ★ 對每個 store（可能有多個：default, windowed 等），建立 StorePartition
    for name, store in self._stores.get(stream_id, {}).items():
        store_partition = store.assign_partition(partition)
        store_partitions[name] = store_partition

    # ★ 如果有 recovery manager 且有 store partition，觸發 recovery 檢查
    if self._recovery_manager and store_partitions:
        self._recovery_manager.assign_partition(
            topic=stream_id,
            partition=partition,
            committed_offsets=committed_offsets,
            store_partitions=store_partitions,
        )
    return store_partitions
```

### 3.3 `Store.assign_partition()` — 建立 RocksDB 實例

檔案：`quixstreams/state/base/store.py:15-85`

```python
class Store(ABC):
    """
    Abstract state store.
    It keeps track of individual store partitions and provides access to the
    partitions' transactions.
    """

    def __init__(self, name: str, stream_id: Optional[str]) -> None:
        super().__init__()
        self._name = name
        self._stream_id = stream_id
        self._partitions: Dict[int, StorePartition] = {}

    @abstractmethod
    def create_new_partition(self, partition: int) -> StorePartition:
        pass

    @property
    def partitions(self) -> Dict[int, StorePartition]:
        return self._partitions

    def assign_partition(self, partition: int) -> StorePartition:
        """
        Assign new store partition
        """
        store_partition = self._partitions.get(partition)
        if store_partition is not None:
            # 已經 assign 過，直接返回（例如 rebalance 時原本就有的 partition）
            logger.debug(
                f'Partition "{partition}" for store "{self._name}" '
                f'(stream "{self._stream_id}") '
                f"is already assigned"
            )
            return store_partition

        # ★ 建立新的 partition（RocksDB 實例），對應到一個 RocksDB 資料夾
        store_partition = self.create_new_partition(partition)
        self._partitions[partition] = store_partition
        logger.debug(
            'Assigned store partition "%s[%s]" (stream "%s")',
            self._name, partition, self._stream_id,
        )
        return store_partition
```

---

## 4. Recovery 完整流程

### 4.1 `RecoveryManager` 類別定義

檔案：`quixstreams/state/recovery.py:337-366`

```python
class RecoveryManager:
    """
    Manages all consumer-related aspects of recovery, including:
        - assigning/revoking, pausing/resuming topic partitions (especially changelogs)
        - consuming changelog messages until state is updated fully.

    Also tracks/manages `RecoveryPartitions`, which are assigned/tracked only if
    recovery for that changelog partition is required.

    Recovery is attempted from the `Application` after any new partition assignment.
    """

    # Maximum number of consecutive invalid offset attempts before failing loudly
    # At 10-second progress logging intervals, 60 attempts = ~10 minutes
    MAX_INVALID_OFFSET_ATTEMPTS = 60

    def __init__(
        self,
        consumer: BaseConsumer,
        topic_manager: TopicManager,
        broker_availability_timeout: float = 0,
    ):
        self._running = False
        self._consumer = consumer
        self._topic_manager = topic_manager
        self._broker_availability_timeout = broker_availability_timeout
        # ★ 核心資料結構：{partition_num: {changelog_name: RecoveryPartition}}
        self._recovery_partitions: Dict[int, Dict[str, RecoveryPartition]] = {}
        self._last_progress_logged_time = time.monotonic()
        self._position_cache: Dict[str, Tuple[float, ConfluentPartition]] = {}
```

### 4.2 `RecoveryPartition` 類別 — 代表一個需要 recovery 的 changelog partition

檔案：`quixstreams/state/recovery.py:40-99`

```python
class RecoveryPartition:
    """
    A changelog topic partition mapped to a respective `StorePartition` with helper
    methods to determine its current recovery status.

    Since `StorePartition`s do recovery directly, it also handles recovery transactions.
    """

    def __init__(
        self,
        changelog_name: str,
        partition_num: int,
        store_partition: StorePartition,
        committed_offsets: dict[str, int],
        lowwater: int,
        highwater: int,
    ):
        self._changelog_name = changelog_name
        self._partition_num = partition_num
        self._store_partition = store_partition
        self._changelog_lowwater = lowwater        # changelog 最早的 offset
        self._changelog_highwater = highwater      # changelog 最新的 offset
        self._committed_offsets = committed_offsets # source topic committed offsets
        self._recovery_consume_position: Optional[int] = None
        self._initial_offset: Optional[int] = None
        self._invalid_offset_count = 0
        self._last_valid_position_time: Optional[float] = None

    @property
    def changelog_name(self) -> str:
        return self._changelog_name

    @property
    def changelog_highwater(self) -> int:
        return self._changelog_highwater

    @property
    def partition_num(self) -> int:
        return self._partition_num

    @property
    def offset(self) -> int:
        """
        Get the changelog offset from the underlying `StorePartition`.
        ★ 如果 RocksDB 裡沒有紀錄過 changelog offset，從頭開始 (OFFSET_BEGINNING)
        """
        offset = self._store_partition.get_changelog_offset()
        if offset is None:
            offset = OFFSET_BEGINNING

        if self._initial_offset is None:
            self._initial_offset = offset
        return offset
```

### 4.3 `needs_recovery_check` — 判斷 state 是否落後

檔案：`quixstreams/state/recovery.py:104-123`

```python
@property
def needs_recovery_check(self) -> bool:
    """
    Determine whether to attempt recovery for underlying `StorePartition`.
    This does NOT mean that anything actually requires recovering.
    """
    # changelog 是否有訊息（lowwater != highwater）
    has_consumable_offsets = self._changelog_lowwater != self._changelog_highwater
    # state 中紀錄的 offset 是否落後於 changelog highwater
    state_potentially_behind = self._changelog_highwater - 1 > self.offset
    return has_consumable_offsets and state_potentially_behind

@property
def has_invalid_offset(self) -> bool:
    """
    Determine if the current changelog offset stored in state is invalid.
    ★ 當 changelog 被刪掉又重建，但 state store 沒有清除，就會出現這種情況
    """
    if self._changelog_highwater == 0:
        return False
    return self._changelog_highwater <= self.offset

@property
def finished_recovery_check(self) -> bool:
    """★ Consumer 已經讀到 changelog highwater → recovery 完成"""
    return self._recovery_consume_position == self._changelog_highwater

@property
def had_recovery_changes(self) -> bool:
    """★ 初始 offset 跟最終 offset 不同 → 確實有恢復資料"""
    return self._initial_offset != self.offset
```

### 4.4 `RecoveryManager.assign_partition()` — 決定是否需要 recovery

檔案：`quixstreams/state/recovery.py:485-541`

```python
def assign_partition(
    self,
    topic: Optional[str],
    partition: int,
    committed_offsets: dict[str, int],
    store_partitions: Dict[str, StorePartition],
):
    """
    Assigns `StorePartition`s (as `RecoveryPartition`s) ONLY IF recovery required.
    Pauses active consumer partitions as needed.
    """
    # ★ Step 1: 為每個 store 產生 RecoveryPartition（會去查 changelog watermark）
    recovery_partitions = self._generate_recovery_partitions(
        topic_name=topic,
        partition_num=partition,
        store_partitions=store_partitions,
        committed_offsets=committed_offsets,
    )

    assigned_tps = set(
        (tp.topic, tp.partition) for tp in self._consumer.assignment()
    )

    for rp in recovery_partitions:
        changelog_name, partition = rp.changelog_name, rp.partition_num

        # ★ Step 2: 驗證 changelog partition 已被 assign 給此 consumer
        if (changelog_name, partition) not in assigned_tps:
            raise ChangelogTopicPartitionNotAssigned(
                f'Changelog topic partition "{changelog_name}[{partition}]" '
                f"must be assigned to recover from it"
            )

        # ★ Step 3: 根據 needs_recovery_check 決定是否加入追蹤
        if rp.needs_recovery_check:
            logger.debug(f"Adding a recovery check for {rp}")
            self._recovery_partitions.setdefault(partition, {})[changelog_name] = rp
        elif rp.has_invalid_offset:
            raise InvalidStoreChangelogOffset(
                "The offset in the state store is greater than or equal to its "
                "respective changelog highwater. This can happen if the changelog "
                "was deleted (and recreated) but the state store was not. The "
                "invalid state store can be deleted by manually calling "
                "Application.clear_state() before running the application again."
            )

    # ★ Step 4: Pause partition 等待 recovery
    if self._recovery_partitions:
        if self._running:
            # 正在 recovery 中，只 pause 新加入的 source partition
            self._consumer.pause(
                [ConfluentPartition(topic=topic, partition=partition)]
            )
        else:
            # Recovery 尚未開始，pause 所有 partition（等 Application 主迴圈啟動 recovery）
            self._consumer.pause(self._consumer.assignment())
```

### 4.5 `_generate_recovery_partitions()` — 取得 changelog watermark 並建立 RecoveryPartition

檔案：`quixstreams/state/recovery.py:452-483`

```python
def _generate_recovery_partitions(
    self,
    topic_name: Optional[str],
    partition_num: int,
    store_partitions: Dict[str, StorePartition],
    committed_offsets: dict[str, int],
) -> List[RecoveryPartition]:
    partitions = []
    for store_name, store_partition in store_partitions.items():
        changelog_topic = self._topic_manager.changelog_topics[topic_name][store_name]

        # ★ 從 Kafka broker 取得 changelog partition 的 low/high watermark
        # lowwater: 最早可讀的 offset
        # highwater: 最新的 offset（下一個要寫入的位置）
        lowwater, highwater = self._consumer.get_watermark_offsets(
            ConfluentPartition(
                topic=changelog_topic.name,
                partition=partition_num,
            ),
            timeout=10,
        )

        partitions.append(
            RecoveryPartition(
                changelog_name=changelog_topic.name,
                partition_num=partition_num,
                store_partition=store_partition,
                committed_offsets=committed_offsets,
                lowwater=lowwater,
                highwater=highwater,
            )
        )
    return partitions
```

### 4.6 `StateStoreManager.do_recovery()` — 委派給 RecoveryManager

檔案：`quixstreams/state/manager.py:110-117`

```python
def do_recovery(self) -> None:
    """
    Perform a state recovery, if necessary.
    """
    if self._recovery_manager is None:
        raise RuntimeError("a recovery manager is needed to do a recovery")

    return self._recovery_manager.do_recovery()
```

### 4.7 `RecoveryManager.do_recovery()` — 執行 recovery 主邏輯

檔案：`quixstreams/state/recovery.py:413-450`

```python
def do_recovery(self):
    """
    If there are any active RecoveryPartitions, do a recovery procedure.
    After, will resume normal `Application` processing.
    """
    logger.info("Beginning recovery check...")
    self._running = True

    # note: technically it should be rp.offset + 1, but to remain backwards
    # compatible with <v2.7 +1 ALOS offsetting, it remains rp.offset.
    # This means we will always re-write the "first" recovery message.

    # ★ Step 1: Seek changelog partition 到上次已知的 offset，並 resume
    for rp in dict_values(self._recovery_partitions):
        tp = ConfluentPartition(
            topic=rp.changelog_name, partition=rp.partition_num, offset=rp.offset
        )
        self._consumer.seek(tp)     # 從 state 中紀錄的位置開始讀
        self._consumer.resume([tp]) # resume 之前被 pause 的 changelog partition

    # ★ Step 2: 進入 recovery loop
    self._recovery_loop()

    if self._running:
        logger.info("Recovery process complete! Resuming normal processing...")
        self._running = False

        # ★ Step 3: Recovery 完成後，resume source topic partition（非 changelog）
        non_changelog_tps = [
            tp
            for tp in self._consumer.assignment()
            if tp.topic in self._topic_manager.non_changelog_topics
        ]
        self._consumer.resume(non_changelog_tps)
    else:
        logger.debug("Recovery process interrupted; stopping.")
```

### 4.8 `_recovery_loop()` — 消費 changelog 訊息逐條寫入 state

檔案：`quixstreams/state/recovery.py:602-621`

```python
def _recovery_loop(self) -> None:
    """
    Perform the recovery loop, which continues updating state with changelog
    messages until recovery is "complete" (i.e. no assigned `RecoveryPartition`s).

    A RecoveryPartition is unassigned immediately once fully updated.
    """
    while self.recovering:
        self._log_recovery_progress()      # 每 10 秒 log 一次進度
        if (msg := self._consumer.poll(1)) is None:
            # ★ 沒收到訊息 → 更新 recovery 狀態（檢查是否已讀到 highwater）
            self._update_recovery_status()
        else:
            msg = raise_for_msg_error(msg)
            # ★ 收到 changelog 訊息 → 找到對應的 RecoveryPartition → 套用
            rp = self._recovery_partitions[msg.partition()][msg.topic()]
            rp.recover_from_changelog_message(changelog_message=msg)
            self._consumer._broker_available()  # noqa: SLF001
        if self._broker_availability_timeout:
            self._consumer.raise_if_broker_unavailable(
                self._broker_availability_timeout
            )
```

### 4.9 `_update_recovery_status()` — 檢查 recovery 是否完成

檔案：`quixstreams/state/recovery.py:581-600`

```python
def _update_recovery_status(self):
    rp_revokes = []
    for rp in dict_values(self._recovery_partitions):
        position = self._get_changelog_offset(rp)
        if position is None:
            # ★ Position 尚不可用（例如 rebalance 中），下次再試
            logger.debug(
                f"Skipping recovery status update for {rp}: position not available"
            )
            continue

        rp.set_recovery_consume_position(position)
        if rp.finished_recovery_check:
            # ★ Consumer position == highwater → 此 partition recovery 完成
            rp_revokes.append(rp)
            if rp.had_recovery_changes:
                logger.info(f"Recovery successful for {rp}")
            else:
                logger.debug(f"No recovery was required for {rp}")
    # ★ 移除已完成 recovery 的 partition
    self._revoke_recovery_partitions(rp_revokes)
```

### 4.10 `_get_changelog_offset()` — 取得 consumer 在 changelog 上的位置（帶容錯）

檔案：`quixstreams/state/recovery.py:677-716`

```python
def _get_changelog_offset(self, rp: RecoveryPartition) -> Optional[int]:
    """
    Get the current offset of the changelog partition.
    Returns None if the position is not yet established.
    Tracks consecutive invalid offset attempts and raises if threshold exceeded.
    """
    # Use cached position to avoid redundant network calls
    position_tp = self._get_position_with_cache(rp)

    # Check for Kafka errors (e.g., during rebalancing)
    if position_tp.error:
        count = rp.increment_invalid_offset_count()
        logger.debug(
            f"Cannot get position for {rp} due to Kafka error: {position_tp.error}. "
            f"This is expected during rebalancing "
            f"(attempt {count}/{self.MAX_INVALID_OFFSET_ATTEMPTS})."
        )
        self._check_invalid_offset_threshold(rp, f"error: {position_tp.error}")
        return None

    # Check for special Kafka offset values (OFFSET_INVALID=-1001, etc.)
    offset = position_tp.offset
    if offset < 0:
        count = rp.increment_invalid_offset_count()
        logger.debug(
            f"Position not yet established for {rp}: offset={offset}. "
            f"This is expected during rebalancing "
            f"(attempt {count}/{self.MAX_INVALID_OFFSET_ATTEMPTS})."
        )
        self._check_invalid_offset_threshold(rp, f"offset={offset}")
        return None

    # ★ Valid offset obtained - reset the counter
    rp.reset_invalid_offset_count()
    return offset
```

### 4.11 `_check_invalid_offset_threshold()` — 超過閾值就 fail loudly

檔案：`quixstreams/state/recovery.py:718-734`

```python
def _check_invalid_offset_threshold(self, rp: RecoveryPartition, reason: str):
    """
    ★ 連續 60 次（約 10 分鐘）拿不到有效 offset → 認為有嚴重問題，拋出異常
    """
    if rp.invalid_offset_count > self.MAX_INVALID_OFFSET_ATTEMPTS:
        error_msg = (
            f"Recovery stuck for {rp}: position has been invalid for "
            f"{rp.invalid_offset_count} consecutive attempts ({reason}). "
            f"This indicates a serious issue with the Kafka consumer or broker. "
            f"Last valid position was at {rp.last_valid_position_time or 'never'}."
        )
        logger.error(error_msg)
        raise RuntimeError(error_msg)
```

### 4.12 `RecoveryPartition.recover_from_changelog_message()` — 套用單條 changelog 訊息

檔案：`quixstreams/state/recovery.py:162-251`

```python
def recover_from_changelog_message(
    self, changelog_message: SuccessfulConfluentKafkaMessageProto
):
    """
    Recover the StorePartition using a message read from its respective changelog.

    The actual update may be skipped when both conditions are met:
    - The changelog message has headers with the processed message offset.
    - This processed offsets are larger than the latest committed offsets
        for the same topic-partitions.

    This way the state does not apply the state changes for not-yet-committed
    messages and improves the state consistency guarantees.
    """
    headers = dict(changelog_message.headers() or ())

    # ★ 從 header 解析 column family 名稱（例如 "default", "__metadata__" 等）
    cf_name = headers.get(CHANGELOG_CF_MESSAGE_HEADER, b"").decode()
    if not cf_name:
        raise ColumnFamilyHeaderMissing(
            f"Header '{CHANGELOG_CF_MESSAGE_HEADER}' missing from changelog message"
        )

    # ★ 從 header 解析這條 state 變更對應的 source topic processed offset
    # 舊版 lib 可能沒有這個 header（為 None）
    processed_offsets = json_loads(
        headers.get(CHANGELOG_PROCESSED_OFFSETS_MESSAGE_HEADER, b"null")
    )

    # ★ 判斷是否要套用這條 changelog
    if processed_offsets is None or self._should_apply_changelog(
        processed_offsets=processed_offsets
    ):
        key = changelog_message.key()
        if not isinstance(key, bytes):
            raise TypeError(
                f'Invalid changelog key type {type(key)}, expected "bytes"'
            )

        value = changelog_message.value()
        if not isinstance(value, (bytes, _NoneType)):
            raise TypeError(
                f'Invalid changelog value type {type(value)}, expected "bytes"'
            )

        # ★ 寫入 RocksDB
        self._store_partition.recover_from_changelog_message(
            cf_name=cf_name,
            key=key,
            value=value,
            offset=changelog_message.offset(),
        )
    else:
        # ★ 跳過套用，但仍推進 changelog offset（避免下次重新讀）
        self._store_partition.write_changelog_offset(
            offset=changelog_message.offset(),
        )


def _should_apply_changelog(self, processed_offsets: dict[str, int]) -> bool:
    """
    ★ 一致性保護的核心邏輯

    每條 changelog 訊息的 header 中記錄了「產生這條 state 變更時，
    source topic 已處理到哪個 offset」。

    如果這個 processed_offset >= committed_offset，
    代表 source 訊息尚未被 commit（可能是上次處理到一半就掛了），
    不應該套用這條 state 變更。
    """
    committed_offsets = self._committed_offsets
    for topic, processed_offset in processed_offsets.items():
        if processed_offset >= committed_offsets[topic]:
            return False
    return True
```

### 4.13 `RocksDBStorePartition.recover_from_changelog_message()` — 實際寫入 RocksDB

檔案：`quixstreams/state/rocksdb/partition.py:71-83`

```python
def recover_from_changelog_message(
    self, key: bytes, value: Optional[bytes], cf_name: str, offset: int
):
    cf_handle = self.get_column_family_handle(cf_name)
    batch = WriteBatch(raw_mode=True)
    if value is None:
        batch.delete(key, cf_handle)     # ★ value=None 代表 tombstone → 刪除 key
    else:
        batch.put(key, value, cf_handle) # ★ 正常寫入 key-value

    # ★ 更新 changelog offset 到 metadata CF，然後 flush
    self._update_changelog_offset(batch=batch, offset=offset)
    self._write(batch)
```

### 4.14 `_update_changelog_offset()` — 在 RocksDB metadata 中記錄 changelog 進度

檔案：`quixstreams/state/rocksdb/partition.py:389-394`

```python
def _update_changelog_offset(self, batch: WriteBatch, offset: int):
    batch.put(
        CHANGELOG_OFFSET_KEY,              # b"__changelog_offset__"
        int_to_bytes(offset),
        self.get_column_family_handle(METADATA_CF_NAME),  # "__metadata__" CF
    )
```

檔案：`quixstreams/state/rocksdb/metadata.py`

```python
PROCESSED_OFFSET_KEY = b"__topic_offset__"
CHANGELOG_OFFSET_KEY = b"__changelog_offset__"
GLOBAL_COUNTER_CF_NAME = "__global-counter__"
GLOBAL_COUNTER_KEY = b"__global_counter__"
```

### 4.15 `get_changelog_offset()` — 從 RocksDB 讀取上次 recovery 的進度

檔案：`quixstreams/state/rocksdb/partition.py:220-230`

```python
def get_changelog_offset(self) -> Optional[int]:
    """
    Get offset that the changelog is up-to-date with.
    ★ Recovery 開始前，用這個 offset 決定從 changelog 的哪裡開始讀
    """
    metadata_cf = self.get_or_create_column_family(METADATA_CF_NAME)
    offset_bytes = metadata_cf.get(CHANGELOG_OFFSET_KEY)
    if offset_bytes is None:
        return None       # ★ 從沒 recovery 過 → 從頭開始

    return int_from_bytes(offset_bytes)
```

### 4.16 `write_changelog_offset()` — 單獨更新 offset（跳過 state 套用時用）

檔案：`quixstreams/state/rocksdb/partition.py:232-243`

```python
def write_changelog_offset(self, offset: int):
    """
    Write a new changelog offset to the db.
    ★ 當 _should_apply_changelog 返回 False 時，只推進 offset 不寫 state
    """
    batch = WriteBatch(raw_mode=True)
    self._update_changelog_offset(batch=batch, offset=offset)
    self._write(batch)
```

### 4.17 `_write()` — 最底層的 RocksDB 寫入

檔案：`quixstreams/state/rocksdb/partition.py:128-133`

```python
def _write(self, batch: WriteBatch):
    """
    Write `WriteBatch` to RocksDB
    """
    self._db.write(batch)
```

### 4.18 `_revoke_recovery_partitions()` — 清理已完成的 RecoveryPartition

檔案：`quixstreams/state/recovery.py:543-568`

```python
def _revoke_recovery_partitions(self, recovery_partitions: List[RecoveryPartition]):
    """
    Pauses all provided RecoveryPartitions and cleans up any remaining
    empty dictionary references.
    """
    partition_nums = {rp.partition_num for rp in recovery_partitions}
    # ★ Pause 已完成 recovery 的 changelog partition
    self._consumer.pause(
        [
            ConfluentPartition(rp.changelog_name, rp.partition_num)
            for rp in recovery_partitions
        ]
    )
    # ★ 從追蹤 dict 中移除
    for rp in recovery_partitions:
        del self._recovery_partitions[rp.partition_num][rp.changelog_name]
        cache_key = f"{rp.changelog_name}:{rp.partition_num}"
        self._position_cache.pop(cache_key, None)
    for partition_num in partition_nums:
        if not self._recovery_partitions[partition_num]:
            del self._recovery_partitions[partition_num]
    if self.recovering:
        logger.debug("Resuming recovery process...")
```

### 4.19 `_log_recovery_progress()` — 每 10 秒打印進度

檔案：`quixstreams/state/recovery.py:647-675`

```python
def _log_recovery_progress(self) -> None:
    if self._last_progress_logged_time < time.monotonic() - 10:
        for rp in dict_values(self._recovery_partitions):
            position_tp = self._get_position_with_cache(rp)

            if position_tp.error:
                count = rp.invalid_offset_count
                log_level = logger.warning if count > 30 else logger.info
                log_level(
                    f"Recovery progress for {rp}: position unavailable "
                    f"(error: {position_tp.error}, attempts: {count})"
                )
            elif position_tp.offset < 0:
                count = rp.invalid_offset_count
                log_level = logger.warning if count > 30 else logger.info
                log_level(
                    f"Recovery progress for {rp}: position not yet established "
                    f"(offset: {position_tp.offset}, attempts: {count})"
                )
            else:
                last_consumed_offset = position_tp.offset - 1
                logger.info(
                    f"Recovery progress for {rp}: "
                    f"{last_consumed_offset} / {rp.changelog_highwater}"
                )
        self._last_progress_logged_time = time.monotonic()
```

---

## 5. Pod 復活：on_revoke + 再次 on_assign

Pod-3 復活加入 consumer group，Kafka 再次觸發 rebalance。

### 5.1 `Application._on_revoke()` — Pod-1/Pod-2 釋放多餘 partition

檔案：`quixstreams/app.py:1096-1114`

```python
def _on_revoke(self, _, topic_partitions: List[TopicPartition]):
    """
    Revoke partitions from consumer and state
    """
    logger.debug("Rebalancing: revoking partitions")

    # ★ 如果應用正在失敗中，不 commit
    # 這樣其他 consumer 會從上次 committed 的 checkpoint 重新處理
    if self._failed:
        logger.warning(
            "Application is stopping due to failure, "
            "latest checkpoint will not be committed."
        )
    else:
        # ★ 正常情況：force commit 當前處理進度，確保不丟資料
        self._processing_context.commit_checkpoint(force=True)

    self._revoke_state_partitions(topic_partitions=topic_partitions)
    self._consumer.reset_backpressure()
```

### 5.2 `Application._on_lost()` — 非預期的 partition 丟失

檔案：`quixstreams/app.py:1116-1123`

```python
def _on_lost(self, _, topic_partitions: List[TopicPartition]):
    """
    Dropping lost partitions from consumer and state
    """
    logger.debug("Rebalancing: dropping lost partitions")

    # ★ 與 on_revoke 的差異：不會 commit checkpoint
    # 因為 partition 已經丟失（可能已被其他 consumer 接管），commit 可能會失敗
    self._revoke_state_partitions(topic_partitions=topic_partitions)
    self._consumer.reset_backpressure()
```

### 5.3 `Application._revoke_state_partitions()` — 清理 state

檔案：`quixstreams/app.py:1125-1138`

```python
def _revoke_state_partitions(self, topic_partitions: List[TopicPartition]):
    non_changelog_topics = self._topic_manager.non_changelog_topics
    # ★ 只處理 source topic（非 changelog）
    non_changelog_tps = [
        tp for tp in topic_partitions if tp.topic in non_changelog_topics
    ]
    for tp in non_changelog_tps:
        if self._state_manager.stores:
            stream_ids = self._dataframe_registry.get_stream_ids(
                topic_name=tp.topic
            )
            for stream_id in stream_ids:
                self._state_manager.on_partition_revoke(
                    stream_id=stream_id, partition=tp.partition
                )
```

### 5.4 `StateStoreManager.on_partition_revoke()` — 回收 store partition

檔案：`quixstreams/state/manager.py:323-339`

```python
def on_partition_revoke(
    self,
    stream_id: str,
    partition: int,
) -> None:
    """
    Revoke store partitions for each registered store
    for the given stream_id and partition number.
    """
    if stores := self._stores.get(stream_id, {}).values():
        # ★ 先通知 recovery manager 停止追蹤這個 partition
        if self._recovery_manager:
            self._recovery_manager.revoke_partition(partition_num=partition)
        # ★ 再關閉每個 store 的 partition（關閉 RocksDB）
        for store in stores:
            store.revoke_partition(partition=partition)
```

### 5.5 `RecoveryManager.revoke_partition()` — 停止 recovery 追蹤

檔案：`quixstreams/state/recovery.py:570-579`

```python
def revoke_partition(self, partition_num: int):
    """
    Revoke ALL StorePartitions (across all Stores) for a given partition number
    """
    if changelogs := self._recovery_partitions.get(partition_num, {}):
        recovery_partitions = list(changelogs.values())
        logger.debug(f"Stopping recovery for {list(map(str, recovery_partitions))}")
        self._revoke_recovery_partitions(recovery_partitions)
```

### 5.6 `Store.revoke_partition()` — 關閉 RocksDB

檔案：`quixstreams/state/base/store.py:87-103`

```python
def revoke_partition(self, partition: int):
    """
    Revoke assigned store partition
    """
    store_partition = self._partitions.pop(partition, None)
    if store_partition is None:
        return

    # ★ 關閉 RocksDB instance
    store_partition.close()
    logger.debug(
        'Revoked store partition "%s[%s]" (stream "%s")',
        self._name, partition, self._stream_id,
    )
```

### 5.7 `RocksDBStorePartition.close()` — 關閉 RocksDB

檔案：`quixstreams/state/rocksdb/partition.py:245-255`

```python
def close(self):
    """
    Close the underlying RocksDB
    """
    logger.debug(f'Closing rocksdb partition on "{self._path}"')
    # ★ 清除 column family cache 才能正確關閉 RocksDB
    self._cf_handle_cache = {}
    self._cf_cache = {}
    self._db.close()
    logger.debug(f'Closed rocksdb partition on "{self._path}"')
```

### 5.8 Pod-3 重新 on_assign

Pod-3 拿到 P4、P5 後，走的流程與 [第 3-4 節](#3-pod-死掉on_assign-觸發-state-recovery) 完全相同：

1. `_on_assign()` → pause changelog、取得 committed offsets
2. `StateStoreManager.on_partition_assign()` → 建立新的 RocksDB partition
3. `RecoveryManager.assign_partition()` → 檢查 changelog watermark
4. `do_recovery()` → 從 changelog topic replay 訊息到 RocksDB
5. Recovery 完成 → resume source partition → 恢復正常處理

---

## 6. Changelog 的生產端：State 變更如何寫入 Changelog Topic

理解 recovery 之前，也需要知道 changelog 訊息是怎麼產生的。

### 6.1 Changelog 相關 Header 常數

檔案：`quixstreams/state/metadata.py`

```python
SEPARATOR = b"|"
SEPARATOR_LENGTH = len(SEPARATOR)

CHANGELOG_CF_MESSAGE_HEADER = "__column_family__"                    # 記錄 column family 名稱
CHANGELOG_PROCESSED_OFFSETS_MESSAGE_HEADER = "__processed_tp_offsets__"  # 記錄 source topic 已處理的 offset
METADATA_CF_NAME = "__metadata__"

DEFAULT_PREFIX = b""
```

### 6.2 `ChangelogProducerFactory` — 為每個 store 建立 producer

檔案：`quixstreams/state/recovery.py:254-280`

```python
class ChangelogProducerFactory:
    """
    Generates ChangelogProducers, which produce changelog messages to a StorePartition.
    """

    def __init__(self, changelog_name: str, producer: InternalProducer):
        self._changelog_name = changelog_name
        self._producer = producer

    def get_partition_producer(self, partition_num) -> "ChangelogProducer":
        """
        Generate a ChangelogProducer for producing to a specific partition number.
        """
        return ChangelogProducer(
            changelog_name=self._changelog_name,
            partition=partition_num,
            producer=self._producer,
        )
```

### 6.3 `ChangelogProducer` — 實際的 changelog 訊息生產者

檔案：`quixstreams/state/recovery.py:283-334`

```python
class ChangelogProducer:
    """
    Generated for a `StorePartition` to produce state changes to its respective
    kafka changelog partition.
    """

    def __init__(
        self,
        changelog_name: str,
        partition: int,
        producer: InternalProducer,
    ):
        self._changelog_name = changelog_name
        self._partition = partition
        self._producer = producer

    @property
    def changelog_name(self) -> str:
        return self._changelog_name

    @property
    def partition(self) -> int:
        return self._partition

    def produce(
        self,
        key: bytes,
        value: Optional[bytes] = None,
        headers: Optional[Headers] = None,
    ):
        """
        Produce a message to a changelog topic partition.

        ★ 每次 state 變更都會呼叫這個方法，將變更寫入 changelog topic
        key: state key（包含 prefix）
        value: state value（None 代表刪除）
        headers: 包含 column family 名稱 + processed offsets
        """
        self._producer.produce(
            key=key,
            value=value,
            headers=headers,
            partition=self._partition,
            topic=self._changelog_name,
        )

    def flush(self, timeout: Optional[float] = None) -> int:
        return self._producer.flush(timeout=timeout)
```

### 6.4 `StorePartition` 抽象基底類別

檔案：`quixstreams/state/base/partition.py:21-116`

```python
class StorePartition(ABC):
    """
    A base class to access state in the underlying storage.
    It represents a single instance of some storage (e.g. a single database for
    the persistent storage).
    """

    def __init__(
        self,
        dumps: DumpsFunc,
        loads: LoadsFunc,
        changelog_producer: Optional["ChangelogProducer"],
    ) -> None:
        super().__init__()
        self._dumps = dumps
        self._loads = loads
        self._changelog_producer = changelog_producer  # ★ 每個 partition 都有對應的 producer

    @abstractmethod
    def close(self): ...

    @abstractmethod
    def get_changelog_offset(self) -> Optional[int]:
        """
        Get the changelog offset that the state is up-to-date with.
        ★ Recovery 時用：從 RocksDB metadata 讀取上次 changelog 寫到哪裡
        """
        ...

    @abstractmethod
    def write_changelog_offset(self, offset: int):
        """
        Write a new changelog offset to the db.
        ★ 跳過 state 套用但仍需推進 offset 時用
        """

    @abstractmethod
    def write(self, cache: PartitionTransactionCache, changelog_offset: Optional[int]):
        """
        Update the state with data from the update cache
        ★ 正常處理時，transaction commit 會呼叫這個方法寫入 RocksDB
        """

    @abstractmethod
    def get(self, key: bytes, cf_name: str = "default") -> Union[bytes, Literal[Marker.UNDEFINED]]:
        """Get a key from the store"""

    @abstractmethod
    def exists(self, key: bytes, cf_name: str = "default") -> bool:
        """Check if a key is present in the store."""

    @abstractmethod
    def recover_from_changelog_message(
        self, key: bytes, value: Optional[bytes], cf_name: str, offset: int
    ):
        """
        Updates state from a given changelog message.
        ★ Recovery 時用：將 changelog 訊息套用到 RocksDB
        """

    @abstractmethod
    def begin(self) -> PartitionTransaction:
        """Start a new `PartitionTransaction`"""
        ...
```

### 6.5 `RocksDBStorePartition` — 完整 RocksDB 實作

檔案：`quixstreams/state/rocksdb/partition.py:35-83, 128-133, 220-255, 389-394`

```python
class RocksDBStorePartition(StorePartition):
    """
    A base class to access state in RocksDB.
    It represents a single RocksDB database.

    Responsibilities:
     1. Managing access to the RocksDB instance
     2. Creating transactions to interact with data
     3. Flushing WriteBatches to the RocksDB
    """

    def __init__(
        self,
        path: str,
        options: Optional[RocksDBOptionsType] = None,
        changelog_producer: Optional[ChangelogProducer] = None,
    ):
        if not options:
            options = RocksDBOptions()

        super().__init__(options.dumps, options.loads, changelog_producer)
        self._path = path
        self._options = options
        self._rocksdb_options = self._options.to_options()
        self._open_max_retries = self._options.open_max_retries
        self._open_retry_backoff = self._options.open_retry_backoff
        self._db = self._init_rocksdb()            # ★ 開啟 RocksDB
        self._cf_cache: Dict[str, Rdict] = {}
        self._cf_handle_cache: Dict[str, ColumnFamily] = {}

    # ---- Recovery 寫入 ----
    def recover_from_changelog_message(
        self, key: bytes, value: Optional[bytes], cf_name: str, offset: int
    ):
        cf_handle = self.get_column_family_handle(cf_name)
        batch = WriteBatch(raw_mode=True)
        if value is None:
            batch.delete(key, cf_handle)
        else:
            batch.put(key, value, cf_handle)
        self._update_changelog_offset(batch=batch, offset=offset)
        self._write(batch)

    # ---- 正常處理寫入 ----
    def write(
        self,
        cache: PartitionTransactionCache,
        changelog_offset: Optional[int],
        batch: Optional[WriteBatch] = None,
    ):
        if batch is None:
            batch = WriteBatch(raw_mode=True)

        column_families = cache.get_column_families()
        for cf_name in column_families:
            cf_handle = self.get_column_family_handle(cf_name)

            updates = cache.get_updates(cf_name=cf_name)
            for prefix_update_cache in updates.values():
                for key, value in prefix_update_cache.items():
                    batch.put(key, value, cf_handle)

            deletes = cache.get_deletes(cf_name=cf_name)
            for key in deletes:
                batch.delete(key, cf_handle)

        if changelog_offset is not None:
            self._update_changelog_offset(batch=batch, offset=changelog_offset)

        self._write(batch)

    # ---- 底層 RocksDB 寫入 ----
    def _write(self, batch: WriteBatch):
        self._db.write(batch)

    # ---- 讀取 changelog offset ----
    def get_changelog_offset(self) -> Optional[int]:
        metadata_cf = self.get_or_create_column_family(METADATA_CF_NAME)
        offset_bytes = metadata_cf.get(CHANGELOG_OFFSET_KEY)  # b"__changelog_offset__"
        if offset_bytes is None:
            return None
        return int_from_bytes(offset_bytes)

    # ---- 單獨更新 changelog offset（跳過 state 套用時用）----
    def write_changelog_offset(self, offset: int):
        batch = WriteBatch(raw_mode=True)
        self._update_changelog_offset(batch=batch, offset=offset)
        self._write(batch)

    # ---- 關閉 RocksDB ----
    def close(self):
        logger.debug(f'Closing rocksdb partition on "{self._path}"')
        self._cf_handle_cache = {}
        self._cf_cache = {}
        self._db.close()
        logger.debug(f'Closed rocksdb partition on "{self._path}"')

    # ---- 在 metadata CF 中記錄 changelog offset ----
    def _update_changelog_offset(self, batch: WriteBatch, offset: int):
        batch.put(
            CHANGELOG_OFFSET_KEY,              # b"__changelog_offset__"
            int_to_bytes(offset),
            self.get_column_family_handle(METADATA_CF_NAME),  # "__metadata__" CF
        )
```

---

## 7. 端到端流程圖

```
┌─────────────────────────────────────────────────────────────────┐
│                        Pod-3 死掉                                │
└────────────────────────────┬────────────────────────────────────┘
                             ▼
                Kafka 觸發 Consumer Group Rebalance
                             │
              ┌──────────────┴──────────────┐
              ▼                             ▼
        Pod-1 on_assign               Pod-2 on_assign
        拿到 [P0,P1,P4]              拿到 [P2,P3,P5]
              │                             │
              ▼                             ▼
     ┌─ consumer.assign(tps) ──────────────────────────────────┐
     │  consumer.pause(changelog_tps)  ← changelog 先暫停       │
     └─────────────────────────────────────────────────────────┘
              │                             │
              ▼                             ▼
     ┌─ consumer.committed(tps, timeout=30) ───────────────────┐
     │  取得每個 source partition 的 committed offset             │
     │  用於後續 _should_apply_changelog() 一致性檢查             │
     └─────────────────────────────────────────────────────────┘
              │                             │
              ▼                             ▼
     ┌─ StateStoreManager.on_partition_assign() ───────────────┐
     │  for store in stores:                                    │
     │    store.assign_partition(partition)                      │
     │    → create_new_partition() → 開啟 RocksDB               │
     │                                                          │
     │  recovery_manager.assign_partition(                       │
     │    topic, partition, committed_offsets, store_partitions  │
     │  )                                                       │
     └─────────────────────────────────────────────────────────┘
              │                             │
              ▼                             ▼
     ┌─ RecoveryManager.assign_partition() ────────────────────┐
     │  _generate_recovery_partitions():                        │
     │    for each store:                                       │
     │      get_watermark_offsets(changelog_tp)                 │
     │      → (lowwater, highwater)                             │
     │      建立 RecoveryPartition(                             │
     │        changelog_name, partition,                         │
     │        store_partition, committed_offsets,                │
     │        lowwater, highwater                                │
     │      )                                                   │
     │                                                          │
     │  for rp in recovery_partitions:                          │
     │    if rp.needs_recovery_check:                           │
     │      ┌───────────────────────────────────────────┐       │
     │      │ lowwater != highwater (有訊息)              │       │
     │      │ AND highwater - 1 > stored_offset (落後)   │       │
     │      └───────────────────────────────────────────┘       │
     │      → 加入 _recovery_partitions 追蹤                    │
     │    elif rp.has_invalid_offset:                           │
     │      → raise InvalidStoreChangelogOffset                 │
     │                                                          │
     │  if _recovery_partitions:                                │
     │    consumer.pause(all_partitions)  ← 全部暫停等 recovery  │
     └─────────────────────────────────────────────────────────┘
              │
              ▼
     ┌─ Application 主迴圈 (app.py:935) ──────────────────────┐
     │  while run_tracker.running:                              │
     │    if state_manager.recovery_required:  ← True!          │
     │      state_manager.do_recovery()                         │
     └─────────────────────────────────────────────────────────┘
              │
              ▼
     ┌─ RecoveryManager.do_recovery() ────────────────────────┐
     │  self._running = True                                    │
     │                                                          │
     │  for rp in recovery_partitions:                          │
     │    consumer.seek(changelog_tp, offset=rp.offset)         │
     │    consumer.resume([changelog_tp])                       │
     │                                                          │
     │  _recovery_loop():                                       │
     │  ┌───────────────────────────────────────────────┐       │
     │  │ while self.recovering:                         │       │
     │  │   _log_recovery_progress()  (每10秒)          │       │
     │  │   msg = consumer.poll(1)                      │       │
     │  │                                               │       │
     │  │   if msg is None:                             │       │
     │  │     _update_recovery_status()                 │       │
     │  │     → position == highwater? → 完成!          │       │
     │  │                                               │       │
     │  │   else:                                       │       │
     │  │     rp = _recovery_partitions[p][topic]       │       │
     │  │     rp.recover_from_changelog_message(msg)    │       │
     │  │     ├─ 解析 header: cf_name, processed_offsets│       │
     │  │     ├─ _should_apply_changelog()?             │       │
     │  │     │  processed_offset < committed_offset?   │       │
     │  │     │  → True: 套用  → False: 只推進 offset   │       │
     │  │     └─ store_partition                        │       │
     │  │        .recover_from_changelog_message()      │       │
     │  │        → WriteBatch.put/delete → RocksDB      │       │
     │  │        → _update_changelog_offset()           │       │
     │  └───────────────────────────────────────────────┘       │
     │                                                          │
     │  Recovery 完成!                                          │
     │  consumer.resume(non_changelog_tps)  ← resume source    │
     └─────────────────────────────────────────────────────────┘
              │
              ▼
     ┌─ 恢復正常處理 ─────────────────────────────────────────┐
     │  while run_tracker.running:                              │
     │    process_message(dataframes_composed)                  │
     │    processing_context.commit_checkpoint()                │
     └─────────────────────────────────────────────────────────┘


┌─────────────────────────────────────────────────────────────────┐
│                        Pod-3 復活                                │
└────────────────────────────┬────────────────────────────────────┘
                             ▼
                Kafka 再次觸發 Rebalance
                             │
         ┌───────────────────┼───────────────────┐
         ▼                   ▼                   ▼
   Pod-1 on_revoke     Pod-2 on_revoke     Pod-3 on_assign
         │                   │                   │
         ▼                   ▼                   ▼
   ┌─────────────────────────────────┐    ┌──────────────────┐
   │ if not self._failed:            │    │ 拿到 [P4, P5]    │
   │   commit_checkpoint(force=True) │    │                  │
   │ _revoke_state_partitions():     │    │ 走完整 recovery  │
   │   recovery_manager              │    │ 流程（同上）     │
   │     .revoke_partition(p)        │    │                  │
   │   store.revoke_partition(p)     │    │ 從 changelog     │
   │     → store_partition.close()   │    │ replay state     │
   │     → RocksDB.close()          │    │ 到 RocksDB       │
   └─────────────────────────────────┘    └──────────────────┘
         │                   │                   │
         ▼                   ▼                   ▼
   Pod-1 on_assign     Pod-2 on_assign     恢復正常處理
   拿到 [P0, P1]      拿到 [P2, P3]
   (原本就有，無需      (原本就有，無需
    recovery)            recovery)
```

---

## 8. 關鍵源碼索引

| 元件 | 檔案 | 行數 | 說明 |
|------|------|------|------|
| `_run_dataframe` | `quixstreams/app.py` | 910-955 | Application 主迴圈 + subscribe |
| `_on_assign` | `quixstreams/app.py` | 1040-1094 | Rebalance assign 入口 |
| `_on_revoke` | `quixstreams/app.py` | 1096-1114 | Rebalance revoke 入口 |
| `_on_lost` | `quixstreams/app.py` | 1116-1123 | Partition lost 處理 |
| `_revoke_state_partitions` | `quixstreams/app.py` | 1125-1138 | 清理 state partition |
| `Consumer._subscribe` | `quixstreams/kafka/consumer.py` | 303-385 | Callback wrapper |
| `StateStoreManager.recovery_required` | `quixstreams/state/manager.py` | 89-95 | 是否需要 recovery |
| `StateStoreManager.do_recovery` | `quixstreams/state/manager.py` | 110-117 | 委派 recovery |
| `StateStoreManager.on_partition_assign` | `quixstreams/state/manager.py` | 294-321 | Store partition 分配 |
| `StateStoreManager.on_partition_revoke` | `quixstreams/state/manager.py` | 323-339 | Store partition 回收 |
| `Store` (abstract) | `quixstreams/state/base/store.py` | 15-103 | Store 抽象類別 |
| `Store.assign_partition` | `quixstreams/state/base/store.py` | 60-85 | 建立 RocksDB partition |
| `Store.revoke_partition` | `quixstreams/state/base/store.py` | 87-103 | 關閉 RocksDB partition |
| `StorePartition` (abstract) | `quixstreams/state/base/partition.py` | 21-116 | StorePartition 抽象類別 |
| `RocksDBStorePartition` | `quixstreams/state/rocksdb/partition.py` | 35-83 | RocksDB 實作 |
| `RocksDBStorePartition.recover_from_changelog_message` | `quixstreams/state/rocksdb/partition.py` | 71-83 | 寫入 RocksDB |
| `RocksDBStorePartition.get_changelog_offset` | `quixstreams/state/rocksdb/partition.py` | 220-230 | 讀取 changelog offset |
| `RocksDBStorePartition.write_changelog_offset` | `quixstreams/state/rocksdb/partition.py` | 232-243 | 更新 changelog offset |
| `RocksDBStorePartition._update_changelog_offset` | `quixstreams/state/rocksdb/partition.py` | 389-394 | 底層 offset 寫入 |
| `RocksDBStorePartition.close` | `quixstreams/state/rocksdb/partition.py` | 245-255 | 關閉 RocksDB |
| Metadata 常數 | `quixstreams/state/metadata.py` | 1-16 | Header key 定義 |
| RocksDB Metadata 常數 | `quixstreams/state/rocksdb/metadata.py` | 1-5 | RocksDB key 定義 |
| `RecoveryPartition` | `quixstreams/state/recovery.py` | 40-251 | Recovery partition 類別 |
| `RecoveryPartition.needs_recovery_check` | `quixstreams/state/recovery.py` | 104-113 | 是否需要 recovery |
| `RecoveryPartition.recover_from_changelog_message` | `quixstreams/state/recovery.py` | 162-220 | 套用單條 changelog |
| `RecoveryPartition._should_apply_changelog` | `quixstreams/state/recovery.py` | 233-251 | 一致性檢查 |
| `ChangelogProducerFactory` | `quixstreams/state/recovery.py` | 254-280 | Changelog producer 工廠 |
| `ChangelogProducer` | `quixstreams/state/recovery.py` | 283-334 | Changelog 訊息生產者 |
| `RecoveryManager` | `quixstreams/state/recovery.py` | 337-738 | Recovery 管理器 |
| `RecoveryManager.assign_partition` | `quixstreams/state/recovery.py` | 485-541 | 判斷是否需要 recovery |
| `RecoveryManager.do_recovery` | `quixstreams/state/recovery.py` | 413-450 | 執行 recovery |
| `RecoveryManager._recovery_loop` | `quixstreams/state/recovery.py` | 602-621 | Recovery 消費迴圈 |
| `RecoveryManager._update_recovery_status` | `quixstreams/state/recovery.py` | 581-600 | 檢查 recovery 完成 |
| `RecoveryManager._get_changelog_offset` | `quixstreams/state/recovery.py` | 677-716 | 取得 changelog offset（帶容錯）|
| `RecoveryManager._check_invalid_offset_threshold` | `quixstreams/state/recovery.py` | 718-734 | 超時 fail 機制 |
| `RecoveryManager._log_recovery_progress` | `quixstreams/state/recovery.py` | 647-675 | Recovery 進度 log |
| `RecoveryManager._revoke_recovery_partitions` | `quixstreams/state/recovery.py` | 543-568 | 清理已完成的 partition |
| `RecoveryManager.revoke_partition` | `quixstreams/state/recovery.py` | 570-579 | 停止 recovery 追蹤 |
| `RecoveryManager.stop_recovery` | `quixstreams/state/recovery.py` | 736-738 | 停止 recovery |
