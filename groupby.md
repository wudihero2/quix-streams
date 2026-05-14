# GroupBy 源碼深度解析

## 1. 概覽

`group_by()` 是 Quix Streams 的 **re-keying（重新分區）** 機制。
它把 message 的 key 改掉，透過一個 **內部 repartition topic** 重新送回 Kafka，再由同一個 Application 消費回來繼續處理。

核心流程：

```
原始 Topic (key=store_id)
    ↓ poll
SDF.group_by("item")
    ↓ produce（新 key = row["item"]）
repartition__mygroup--input_topic--item   ← Kafka 內部 topic
    ↓ poll（Application 自動訂閱）
新 SDF 接手處理（key=item）
    ↓ apply / window / stateful ...
```

---

## 2. group_by 方法完整源碼

檔案：`quixstreams/dataframe/dataframe.py`

### 2.1 overload 簽名

```python
# --- 檔案: quixstreams/dataframe/dataframe.py ---

@overload
def group_by(
    self,
    key: str,                                    # 欄位名稱
    name: Optional[str] = ...,                   # 可選
    value_deserializer: DeserializerType = ...,
    key_deserializer: DeserializerType = ...,
    value_serializer: SerializerType = ...,
    key_serializer: SerializerType = ...,
) -> "StreamingDataFrame": ...

@overload
def group_by(
    self,
    key: Callable[[Any], Any],                   # 自定義函數
    name: str,                                   # 必填！
    value_deserializer: DeserializerType = ...,
    key_deserializer: DeserializerType = ...,
    value_serializer: SerializerType = ...,
    key_serializer: SerializerType = ...,
) -> "StreamingDataFrame": ...
```

**兩種 overload 的差異**：

| 參數    | `key=str` (欄位名)     | `key=Callable` (函數) |
|---------|------------------------|-----------------------|
| `name`  | 可選，預設用欄位名稱   | **必填**              |
| 用途    | 直接取 `row["column"]` | 自定義邏輯如拼接多欄  |

### 2.2 實際實現（逐行解析）

```python
# --- 檔案: quixstreams/dataframe/dataframe.py ---

def group_by(
    self,
    key: Union[str, Callable[[Any], Any]],
    name: Optional[str] = None,
    value_deserializer: DeserializerType = "json",
    key_deserializer: DeserializerType = "json",
    value_serializer: SerializerType = "json",
    key_serializer: SerializerType = "json",
) -> "StreamingDataFrame":
```

#### Step 1：驗證 key 參數

```python
    if not key:
        raise ValueError('Parameter "key" cannot be empty')
```

空字串、`None`、空 callable 都不行。

#### Step 2：決定 operation 名稱

```python
    operation = name
    if not operation and isinstance(key, str):
        operation = key              # 欄位名直接當 operation 名

    if not operation:
        raise ValueError(
            'group_by requires "name" parameter when "key" is a function'
        )
```

- `sdf.group_by("item")` → operation = `"item"`
- `sdf.group_by("item", name="my_gb")` → operation = `"my_gb"`
- `sdf.group_by(lambda r: r["x"], name="custom")` → operation = `"custom"`
- `sdf.group_by(lambda r: r["x"])` → **報錯**，函數必須給 name

**為什麼需要 operation？**
因為它會被用來生成 **repartition topic 名稱**，必須在整個 Application 內唯一。

#### Step 3：衍生 repartition topic 配置

```python
    repartition_config = self._topic_manager.derive_topic_config(self._topics)
```

這裡呼叫 `TopicManager.derive_topic_config()`，完整源碼：

```python
# --- 檔案: quixstreams/models/topics/manager.py ---

@classmethod
def derive_topic_config(cls, topics: Iterable[Topic]) -> TopicConfig:
    """
    Derive a topic config based on one or more input Topic configs.
    To be used for generating the internal changelogs and repartition topics.
    """
    if not topics:
        raise ValueError("At least one Topic must be passed")

    # 取所有來源 topic 中最大的 partition 數
    num_partitions = max(
        t.broker_config.num_partitions
        for t in topics
        if t.broker_config.num_partitions is not None
    )

    # 取最大 replication factor
    replication_factor = max(
        t.broker_config.replication_factor
        for t in topics
        if t.broker_config.replication_factor is not None
    )

    # 取最大 retention.bytes（-1 = 無限）
    retention_bytes_values = [
        int(t.broker_config.extra_config.get("retention.bytes", "-1"))
        for t in topics
    ]
    retention_bytes = (
        -1 if -1 in retention_bytes_values else max(retention_bytes_values)
    )

    # 取最大 retention.ms（-1 = 無限）
    retention_ms_values = [
        int(t.broker_config.extra_config["retention.ms"]) for t in topics
    ]
    retention_ms = -1 if -1 in retention_ms_values else max(retention_ms_values)

    return TopicConfig(
        num_partitions=num_partitions,
        replication_factor=replication_factor,
        extra_config={
            "retention.bytes": str(retention_bytes),
            "retention.ms": str(retention_ms),
        },
    )
```

**關鍵邏輯**：repartition topic 的 partition 數 **繼承原始 topic 的最大值**，保證可以正確分區。

#### Step 4：單 partition 優化捷徑

```python
    # 如果 topic 只有 1 個 partition，不需要建 repartition topic
    # 直接在本地換 key 就好（反正所有資料都在同一個 partition）
    if repartition_config.num_partitions == 1:
        return self._single_partition_groupby(operation, key)
```

**為什麼？** 只有 1 個 partition = 所有消息本來就在同一個地方，換 key 不影響分區路由，省一次 Kafka round-trip。

#### Step 5：建立 repartition topic

```python
    groupby_topic = self._topic_manager.repartition_topic(
        operation=operation,
        stream_id=self.stream_id,
        config=repartition_config,
        key_serializer=key_serializer,
        value_serializer=value_serializer,
        key_deserializer=key_deserializer,
        value_deserializer=value_deserializer,
    )
```

`repartition_topic()` 完整源碼：

```python
# --- 檔案: quixstreams/models/topics/manager.py ---

def repartition_topic(
    self,
    operation: str,
    stream_id: str,
    config: TopicConfig,
    value_deserializer: DeserializerType = "json",
    key_deserializer: DeserializerType = "json",
    value_serializer: SerializerType = "json",
    key_serializer: SerializerType = "json",
) -> Topic:
    topic = Topic(
        name=self._internal_name("repartition", stream_id, operation),
        value_deserializer=value_deserializer,
        key_deserializer=key_deserializer,
        value_serializer=value_serializer,
        key_serializer=key_serializer,
        create_config=config,
        topic_type=TopicType.REPARTITION,
    )
    broker_topic = self._get_or_create_broker_topic(topic)
    topic = self._configure_topic(topic, broker_topic)
    self._repartition_topics[topic.name] = topic
    return topic
```

**Topic 命名**由 `_internal_name()` 產生：

```python
# --- 檔案: quixstreams/models/topics/manager.py ---

def _internal_name(
    self,
    topic_type: Literal["changelog", "repartition"],
    topic_name: Optional[str],
    suffix: str,
) -> str:
    """
    內部格式: <{TYPE}__{GROUP}--{NAME}--{SUFFIX}>
    """
    if topic_name is None:
        parts = [self._consumer_group, suffix]
    else:
        nested_name = self._format_nested_name(topic_name)
        parts = [self._consumer_group, nested_name, suffix]

    return f"{topic_type}__{'--'.join(parts)}"
```

**具體範例**：
- consumer_group = `"my-app"`
- 原始 topic = `"orders"`（所以 stream_id = `"orders"`）
- operation = `"item"`

→ topic name = `repartition__my-app--orders--item`

#### Step 6：produce 到 repartition topic + 終止原始 SDF

```python
    self.to_topic(topic=groupby_topic, key=self._groupby_key(key))
    # 把原始 SDF 的後續輸出全部過濾掉
    self.filter(lambda _: False)
```

**`_groupby_key()`** 生成新 key 的函數：

```python
# --- 檔案: quixstreams/dataframe/dataframe.py ---

def _groupby_key(
    self, key: Union[str, Callable[[Any], Any]]
) -> Callable[[Any], Any]:
    if isinstance(key, str):
        return lambda row: row[key]          # 取欄位值
    elif callable(key):
        return lambda row: key(row)          # 呼叫用戶函數
    else:
        raise TypeError("group_by 'key' must be callable or string (column name)")
```

**`to_topic()`** 的核心邏輯：

```python
# --- 檔案: quixstreams/dataframe/dataframe.py ---

def to_topic(
    self,
    topic: Union[Topic, Callable[[Any, Any, int, Any], Topic]],
    key: Optional[Callable[[Any], Any]] = None,
) -> "StreamingDataFrame":
    if isinstance(topic, Topic):
        topic_callback = lambda value, orig_key, timestamp, headers: topic
    else:
        topic_callback = topic

    return self._add_update(
        lambda value, orig_key, timestamp, headers: self._produce(
            topic=topic_callback(value, orig_key, timestamp, headers),
            value=value,
            key=orig_key if key is None else key(value),  # ← 用新 key
            timestamp=timestamp,
            headers=headers,
        ),
        metadata=True,
    )
```

**`_produce()`**：

```python
# --- 檔案: quixstreams/dataframe/dataframe.py ---

def _produce(
    self,
    topic: Topic,
    value: object,
    key: Any,
    timestamp: int,
    headers: Any,
):
    ctx = message_context()
    row = Row(
        value=value, key=key, timestamp=timestamp, context=ctx, headers=headers
    )
    self._producer.produce_row(row=row, topic=topic, key=key, timestamp=timestamp)
```

**所以 Step 6 做了兩件事**：
1. 把當前 message 的 value 用**新 key** produce 到 repartition topic
2. `filter(lambda _: False)` 確保原始 SDF 不會繼續往下走

#### Step 7：建立新的 SDF 並註冊

```python
    groupby_sdf = self.__dataframe_clone__(groupby_topic)
    self._registry.register_groupby(source_sdf=self, new_sdf=groupby_sdf)
    return groupby_sdf
```

**`__dataframe_clone__()`**：

```python
# --- 檔案: quixstreams/dataframe/dataframe.py ---

def __dataframe_clone__(
    self,
    *topics: Topic,
    stream: Optional[Stream] = None,
    stream_id: Optional[str] = None,
) -> "StreamingDataFrame":
    if topics and stream_id:
        raise ValueError('Cannot pass both "*topics" and "stream_id"')
    elif topics:
        stream_id = None          # 用 topic 自動生成 stream_id
    else:
        topics = self._topics
        stream_id = stream_id or self._stream_id

    clone = self.__class__(
        *topics,
        stream=stream,            # 全新的空 Stream
        stream_id=stream_id,
        processing_context=self._processing_context,
        topic_manager=self._topic_manager,
        registry=self._registry,
    )
    return clone
```

傳入 `groupby_topic` 作為新 SDF 的 topic，所以：
- 新 SDF 的 topic = `repartition__my-app--orders--item`
- 新 SDF 的 stream_id = `repartition__my-app--orders--item`（由 topic name 生成）
- 新 SDF 的 stream = **空 Stream**（從頭開始接 pipeline）

**`register_groupby()`**：

```python
# --- 檔案: quixstreams/dataframe/registry.py ---

def register_groupby(
    self,
    source_sdf: "StreamingDataFrame",
    new_sdf: "StreamingDataFrame",
    register_new_root: bool = True,
):
    # 禁止巢狀 group_by
    if source_sdf.stream_id in self._repartition_origins:
        raise GroupByNestingLimit(
            "Subsequent (nested) `SDF.group_by()` operations are not allowed."
        )

    # 禁止重複的 group_by（同名）
    if new_sdf.stream_id in self._repartition_origins:
        raise GroupByDuplicate(
            "An `SDF.group_by()` operation appears to be the same as another, "
            "either from using the same column or name parameter; "
            "adjust by setting a unique name with `SDF.group_by(name=<NAME>)` "
        )

    self._repartition_origins.add(new_sdf.stream_id)

    if register_new_root:
        try:
            self.register_root(new_sdf)       # 把新 SDF 當作新的 root
        except StreamingDataFrameDuplicate:
            raise GroupByDuplicate(...)
```

**`register_root()` 做了什麼**：

```python
# --- 檔案: quixstreams/dataframe/registry.py ---

def register_root(
    self,
    dataframe: "StreamingDataFrame",
):
    topics = dataframe.topics
    if len(topics) > 1:
        raise ValueError(...)
    topic = topics[0]

    if topic.name in self._registry:
        raise StreamingDataFrameDuplicate(...)

    self._topics.append(topic)                  # ← 加入 consumer_topics
    self._registry[topic.name] = dataframe.stream  # ← 註冊 stream
```

**關鍵**：`self._topics.append(topic)` 把 repartition topic 加入 `consumer_topics`，
這樣 Application 的 consumer 就會**自動訂閱**這個 repartition topic。

---

## 3. 單 partition 優化路徑

當 topic 只有 1 個 partition 時，所有消息都在同一個 partition，換 key 不影響資料分佈，
所以直接在 pipeline 中做 transform 就好，省去 Kafka round-trip：

```python
# --- 檔案: quixstreams/dataframe/dataframe.py ---

def _single_partition_groupby(
    self, operation: str, key: Union[str, Callable[[Any], Any]]
) -> "StreamingDataFrame":
    if isinstance(key, str):

        def _callback(value, _, timestamp, headers):
            return value, value[key], timestamp, headers   # 新 key = value[key]
    else:

        def _callback(value, _, timestamp, headers):
            return value, key(value), timestamp, headers   # 新 key = key(value)

    stream = self.stream.add_transform(_callback, expand=False)

    groupby_sdf = self.__dataframe_clone__(
        stream=stream, stream_id=f"{self.stream_id}--groupby--{operation}"
    )
    self._registry.register_groupby(
        source_sdf=self, new_sdf=groupby_sdf, register_new_root=False
    )

    return groupby_sdf
```

**與多 partition 版本的差異**：

| 面向               | 多 partition                        | 單 partition                     |
|--------------------|-------------------------------------|----------------------------------|
| 是否建 topic       | 建 repartition topic                | **不建**                         |
| 資料路徑           | produce → Kafka → consume           | **直接 transform**               |
| 是否有 Kafka 延遲  | 有（要過 Kafka broker）             | **無**                           |
| register_new_root  | `True`（新增訂閱 topic）            | `False`（不需要新 topic）        |
| stream_id          | repartition topic name              | `{原 stream_id}--groupby--{op}` |

---

## 4. Application 如何消費 repartition topic

### 4.1 訂閱

```python
# --- 檔案: quixstreams/app.py ---

def _run_dataframe(self, sink=None):
    consumer.subscribe(
        topics=self._dataframe_registry.consumer_topics + changelog_topics,
        #       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
        #       consumer_topics 包含原始 topic + repartition topic
        on_assign=self._on_assign,
        on_revoke=self._on_revoke,
        on_lost=self._on_lost,
    )

    dataframes_composed = self._dataframe_registry.compose_all(sink=sink)
    # compose_all 會回傳 {topic_name: executor} 的 dict
    # 其中 repartition topic 也會有自己的 executor
```

### 4.2 為什麼多個 SDF 可以共存

group_by 會產生兩個 SDF（原始 + groupby 後的），但它們**不是同時並行執行**。
整個 Application 只有一個 main loop、一個 consumer、一個 thread。
多個 SDF 能共存，是因為**定義時只是註冊，runtime 才根據 message 來源路由**：

**定義時（`app.dataframe()` + `sdf.group_by()`）**：
```python
# DataFrameRegistry._registry 的最終狀態：
{
    "orders":                                   Stream_A,   # 原始 pipeline
    "repartition__my-app--orders--item":        Stream_B,   # groupby 後的 pipeline
}
```

這只是一個 dict，沒有任何東西在跑。

**啟動時（`app.run()` → `compose_all()`）**：
```python
# compose_all() 把 Stream 編譯成可執行的函數：
dataframe_composed = {
    "orders":                                executor_A,  # to_topic + filter(False)
    "repartition__my-app--orders--item":     executor_B,  # apply(stateful) + sink
}
```

**runtime（main loop）**：
```python
while running:
    row = consumer.poll()        # ← 從 Kafka 拉一條消息
    topic_name = row.topic       # ← 看它來自哪個 topic

    dataframe_composed[topic_name](row.value, row.key, ...)
    #                  ^^^^^^^^^^^
    #                  根據 topic_name 路由到對應的 executor
```

**所以本質是**：一個 main loop 訂閱了多個 topic，每次 poll 一條消息，
根據 `topic_name` 查 dict 找到對應的 executor 來處理。
不是「兩條 pipeline 同時跑」，而是「一條消息走一條路」。

### 4.3 訊息路由源碼

```python
# --- 檔案: quixstreams/app.py ---

def _process_message(self, dataframe_composed):
    self._producer.poll(self._config.producer_poll_timeout)
    rows = self._consumer.poll_row(timeout=self._config.consumer_poll_timeout, ...)

    if rows is None:
        return

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
            context.run(
                dataframe_composed[topic_name],   # ← 根據 topic_name 找 executor
                row.value,
                row.key,
                row.timestamp,
                row.headers,
            )
        except Exception as exc:
            to_suppress = self._on_processing_error(exc, row, logger)
            if not to_suppress:
                raise

    self._processing_context.store_offset(
        topic=topic_name, partition=partition, offset=offset
    )
```

### 4.4 完整資料流向

```
Consumer poll() → message from "orders" (key=store_4)
    ↓
dataframe_composed["orders"](value, key, ts, headers)
    ↓ SDF pipeline: apply → filter → ...
    ↓ to_topic(repartition_topic, key=lambda r: r["item"])
    ↓ _produce(topic=repartition_topic, value=原始value, key="A")
    ↓ filter(lambda _: False)  ← 原始 pipeline 到此結束
    ↓
Producer produce → "repartition__my-app--orders--item" partition=hash("A") % N
    ↓
Consumer poll() → message from "repartition__my-app--orders--item" (key="A")
    ↓
dataframe_composed["repartition__my-app--orders--item"](value, "A", ts, headers)
    ↓ 新 SDF pipeline: apply(stateful) → window → sink → ...
```

---

## 5. DataFrameRegistry 完整源碼

```python
# --- 檔案: quixstreams/dataframe/registry.py ---

from typing import TYPE_CHECKING, Optional

from quixstreams.core.stream import Stream, VoidExecutor
from quixstreams.models import Topic

from .exceptions import (
    GroupByDuplicate,
    GroupByNestingLimit,
    StreamingDataFrameDuplicate,
)

if TYPE_CHECKING:
    from .dataframe import StreamingDataFrame


class DataFrameRegistry:
    """
    Helps manage multiple `StreamingDataFrames` (multi-topic `Applications`)
    and their respective repartitions.
    """

    def __init__(self) -> None:
        self._registry: dict[str, Stream] = {}
        self._topics: list[Topic] = []
        self._repartition_origins: set[str] = set()
        self._topics_to_stream_ids: dict[str, set[str]] = {}
        self._stream_ids_to_topics: dict[str, set[str]] = {}
        self._requires_time_alignment = False

    @property
    def consumer_topics(self) -> list[Topic]:
        """
        :return: 消費者需要訂閱的所有 topics（包含 repartition topics）
        """
        return self._topics

    def register_root(
        self,
        dataframe: "StreamingDataFrame",
    ):
        topics = dataframe.topics
        if len(topics) > 1:
            raise ValueError(
                f"Expected a StreamingDataFrame with one topic, got {len(topics)}"
            )
        topic = topics[0]

        if topic.name in self._registry:
            raise StreamingDataFrameDuplicate(
                f"There is already a StreamingDataFrame using topic {topic.name}"
            )
        self._topics.append(topic)
        self._registry[topic.name] = dataframe.stream

    def register_groupby(
        self,
        source_sdf: "StreamingDataFrame",
        new_sdf: "StreamingDataFrame",
        register_new_root: bool = True,
    ):
        # 禁止巢狀 group_by（A.group_by().group_by() 不允許）
        if source_sdf.stream_id in self._repartition_origins:
            raise GroupByNestingLimit(
                "Subsequent (nested) `SDF.group_by()` operations are not allowed."
            )

        # 禁止重複 group_by（同名）
        if new_sdf.stream_id in self._repartition_origins:
            raise GroupByDuplicate(
                "An `SDF.group_by()` operation appears to be the same as another, "
                "either from using the same column or name parameter; "
                "adjust by setting a unique name with `SDF.group_by(name=<NAME>)` "
            )

        self._repartition_origins.add(new_sdf.stream_id)

        if register_new_root:
            try:
                self.register_root(new_sdf)
            except StreamingDataFrameDuplicate:
                raise GroupByDuplicate(
                    "An `SDF.group_by()` operation appears to be the same as another, "
                    "either from using the same column or name parameter; "
                    "adjust by setting a unique name with `SDF.group_by(name=<NAME>)` "
                )

    def compose_all(
        self, sink: Optional[VoidExecutor] = None
    ) -> dict[str, VoidExecutor]:
        executors = {}
        for topic, root_stream in self._registry.items():
            root_executors = root_stream.compose(sink=sink)
            executors[topic] = root_executors[root_stream]
        return executors

    def register_stream_id(self, stream_id: str, topic_names: list[str]):
        for topic_name in topic_names:
            self._topics_to_stream_ids.setdefault(topic_name, set()).add(stream_id)
            self._stream_ids_to_topics.setdefault(stream_id, set()).add(topic_name)

    def get_stream_ids(self, topic_name: str) -> list[str]:
        return list(self._topics_to_stream_ids[topic_name])

    def get_topics_for_stream_id(self, stream_id: str) -> list[str]:
        return list(self._stream_ids_to_topics[stream_id])
```

**內部資料結構**：

| 欄位                     | 類型                         | 用途                                    |
|--------------------------|------------------------------|-----------------------------------------|
| `_registry`              | `{topic_name: Stream}`       | 每個 topic 對應的 Stream pipeline       |
| `_topics`                | `list[Topic]`                | consumer 要訂閱的所有 topic             |
| `_repartition_origins`   | `set[str]`                   | 已經做過 group_by 的 stream_id 集合     |
| `_topics_to_stream_ids`  | `{topic: set[stream_id]}`    | topic → state store 的映射              |
| `_stream_ids_to_topics`  | `{stream_id: set[topic]}`    | state store → topic 的反向映射          |

---

## 6. GroupBy 例外處理

```python
# --- 檔案: quixstreams/dataframe/exceptions.py ---

class GroupByNestingLimit(QuixException): ...
class GroupByDuplicate(QuixException): ...
class StreamingDataFrameDuplicate(QuixException): ...
```

### 6.1 禁止巢狀 group_by

```python
sdf = app.dataframe(topic)
sdf = sdf.group_by("item")
sdf = sdf.group_by("store_id")   # ← GroupByNestingLimit!
```

**為什麼禁止？**
第二個 `group_by` 的 source SDF 的 stream_id 已經在 `_repartition_origins` 裡了。
如果允許巢狀，每一層 group_by 都會多一個中間 topic，形成：

```
原始 topic → repartition_1 → repartition_2 → ...
```

這會導致：
- 延遲倍增（每層都要 Kafka round-trip）
- topic 爆炸
- checkpoint 的 offset 追蹤變得極複雜

### 6.2 禁止重複 group_by

```python
sdf1 = app.dataframe(topic)
sdf1 = sdf1.group_by("item")

sdf2 = app.dataframe(topic2)
sdf2 = sdf2.group_by("item")     # ← 可能 GroupByDuplicate（如果 topic name 碰撞）
```

解法：給不同的 name

```python
sdf2 = sdf2.group_by("item", name="item_gb_2")
```

---

## 7. stream_id 的生成邏輯

```python
# --- 檔案: quixstreams/utils/stream_id.py ---

def stream_id_from_strings(*strings: str) -> str:
    parts = sorted(set(strings))
    return "--".join(parts)
```

以及 SDF 初始化時：

```python
# --- 檔案: quixstreams/dataframe/dataframe.py ---

def __init__(
    self,
    *topics: Topic,
    ...
    stream_id: Optional[str] = None,
):
    self._stream_id: str = stream_id or topic_manager.stream_id_from_topics(
        self.topics
    )
    # ...
    self._registry.register_stream_id(
        stream_id=self.stream_id, topic_names=[t.name for t in self._topics]
    )
```

**stream_id 對 State 的影響**：

group_by 前：`stream_id = "orders"` → state 存在 `state_dir/default/orders/`
group_by 後：`stream_id = "repartition__my-app--orders--item"` → state 存在 `state_dir/default/repartition__my-app--orders--item/`

**這兩個 state store 完全隔離**。group_by 後的 stateful 操作存取的是新 stream_id 下的 state。

---

## 8. group_by 與 SQL/Pandas groupby 的差異

### 8.1 概念差異

| 面向         | SQL / Pandas `GROUP BY`                | Quix `group_by()`                         |
|--------------|----------------------------------------|-------------------------------------------|
| 本質         | **聚合操作**（collapse rows）          | **re-keying 操作**（換 message key）      |
| 輸入輸出     | N rows → M rows (M ≤ N)               | 1 message → 1 message（key 變了）         |
| 是否聚合     | 必須搭配 SUM/COUNT/AVG 等             | 不一定，單獨用也行（只換 key）            |
| 資料完整性   | 看到所有資料後才輸出                   | **逐筆處理**，每條消息馬上 re-key         |
| 實現機制     | 記憶體中 hash/sort                     | **經過 Kafka repartition topic**          |

### 8.2 為什麼 Kafka 場景需要 re-keying

SQL 的 `GROUP BY item` 是在有完整資料集的前提下做聚合。
但 Kafka 是 **流式的**，消息是一條一條來的，而且 stateful 操作（如求和）依賴 **partition 本地的 state**。

問題：如果原始 key 是 `store_id`，那麼：
- partition 0 可能有 `store_1` 的 item A 和 item B
- partition 1 可能有 `store_2` 的 item A 和 item C

想要 `SUM(quantity) GROUP BY item`，需要同一個 item 的所有消息都在同一個 partition。
**所以必須 re-key + repartition**。

```
Before group_by:
  partition 0: [store_1, item_A, 5] [store_1, item_B, 2]
  partition 1: [store_2, item_A, 3] [store_2, item_C, 8]

After group_by("item"):  ← 經過 repartition topic
  partition 0: [item_A, 5] [item_A, 3]      ← 同一個 item 在同一個 partition
  partition 1: [item_B, 2] [item_C, 8]
```

### 8.3 與 Flink / Kafka Streams 的 groupBy 對比

| 面向              | Flink `keyBy()`             | Kafka Streams `groupBy()`  | Quix `group_by()`          |
|-------------------|-----------------------------|----------------------------|----------------------------|
| 資料傳輸方式      | **網路 shuffle**（記憶體）  | **repartition topic**      | **repartition topic**      |
| 是否經過 Kafka    | 否（task 間直接傳）         | 是                         | 是                         |
| 延遲              | 低（記憶體級）              | 中（Kafka broker 級）      | 中（Kafka broker 級）      |
| 能否巢狀          | 可以                        | 可以                       | **不可以**（限制一次）     |
| State 隔離        | 由 operator 管理            | 由 store name 管理         | 由 stream_id 管理          |

**Quix 不支援巢狀的取捨**：
Flink 可以巢狀 keyBy 是因為它走的是網路 shuffle（記憶體到記憶體），開銷小。
Quix 和 Kafka Streams 每次 group_by 都要經過 Kafka broker，如果允許巢狀，
N 層 group_by = N 次 Kafka round-trip，延遲和 topic 數量都不可控。

---

## 9. 完整例子：從用戶代碼到內部流轉

### 用戶代碼

```python
from quixstreams import Application

app = Application(broker_address="localhost:9092", consumer_group="my-app")
input_topic = app.topic("orders")

sdf = app.dataframe(input_topic)               # stream_id = "orders"
sdf = sdf.group_by("item")                     # stream_id = "repartition__my-app--orders--item"
sdf = sdf.apply(lambda v, state: ..., stateful=True)
sdf = sdf.to_topic(app.topic("output"))
app.run()
```

### 內部發生的事

**1. `app.dataframe(input_topic)` 階段**
- 建立 SDF，stream_id = `"orders"`
- registry 註冊：`_registry["orders"] = stream`
- consumer_topics = `[Topic("orders")]`

**2. `sdf.group_by("item")` 階段**
- operation = `"item"`
- `derive_topic_config(orders_topic)` → `TopicConfig(num_partitions=6, ...)`
- 6 > 1 → 走多 partition 路徑
- `repartition_topic("item", "orders", config)` → 建立 `Topic("repartition__my-app--orders--item")`
- 原始 SDF 加入 `to_topic(repartition_topic, key=lambda r: r["item"])` + `filter(False)`
- `__dataframe_clone__(groupby_topic)` → 新 SDF，stream_id = `"repartition__my-app--orders--item"`
- registry 註冊：`_registry["repartition__my-app--orders--item"] = new_stream`
- consumer_topics = `[Topic("orders"), Topic("repartition__my-app--orders--item")]`
- `_repartition_origins = {"repartition__my-app--orders--item"}`

**3. `app.run()` 階段**
- consumer.subscribe 訂閱 `["orders", "repartition__my-app--orders--item", changelog_topics...]`
- `compose_all()` 產生：
  ```python
  {
      "orders": executor_1,                               # apply→to_topic→filter(False)
      "repartition__my-app--orders--item": executor_2,    # apply(stateful)→to_topic(output)
  }
  ```
- main loop poll 到 `orders` 的消息 → executor_1 → produce 到 repartition topic → filter 掉
- main loop poll 到 repartition topic 的消息 → executor_2 → stateful 處理 → produce 到 output

---

## 10. 總結

```
┌─────────────────────────────────────────────────────────────────────┐
│  group_by() 的本質：                                                │
│                                                                     │
│  1. 不是聚合，是 re-keying                                          │
│  2. 透過 Kafka repartition topic 保證相同 key 落在相同 partition     │
│  3. Application 自動訂閱 repartition topic，對用戶透明               │
│  4. 單 partition 有優化捷徑，不經過 Kafka                            │
│  5. 禁止巢狀（每次 group_by 都是一次 Kafka round-trip）              │
│  6. group_by 後的 state 是獨立的（不同 stream_id）                   │
└─────────────────────────────────────────────────────────────────────┘
```
