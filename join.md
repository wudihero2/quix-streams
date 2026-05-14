# Join 源碼深度解析

## 1. 結論先說：Join 沒有自動 repartition

**Quix Streams 的 join 不會自動 repartition。**
它要求兩邊的 topic **必須已經是 copartitioned（共分區）**，否則直接報錯。

```python
# --- 檔案: quixstreams/models/topics/manager.py ---

@classmethod
def ensure_topics_copartitioned(cls, *topics: Topic):
    partitions_counts = set(t.broker_config.num_partitions for t in topics)
    if len(partitions_counts) > 1:
        msg = ", ".join(
            f'"{t.name}" ({t.broker_config.num_partitions} partitions)'
            for t in topics
        )
        raise TopicPartitionsMismatch(
            f"The underlying topics must have the same number of partitions "
            f"to use State; got {msg}"
        )
```

**只做驗證，不做修復。** partition 數不同 → 直接 `TopicPartitionsMismatch` 例外。

### 1.1 為什麼不自動 repartition？

| 面向 | 自動 repartition（如 Flink） | Quix 做法 |
|------|------------------------------|-----------|
| 資料路徑 | 網路 shuffle 或中間 topic | **無** |
| 延遲 | 增加一次 Kafka round-trip | **零額外延遲** |
| 複雜度 | 需要管理中間 topic 生命週期 | **簡單** |
| 前提條件 | 自動處理 key 不匹配 | 要求用戶確保 copartitioned |

**Quix 的設計哲學**：如果 key 不匹配（比如 left 用 `user_id`、right 用 `order_id`），
用戶應該在 join 之前自己做 `group_by()` 來 re-key，這樣語義更清晰。

### 1.2 Copartitioned 的兩個條件

1. **partition 數量相同**（上面的 `ensure_topics_copartitioned` 檢查）
2. **相同 key 分佈在相同 partition 號**（由 Kafka 預設的 hash partitioner 保證）

### 1.3 Range 分配策略保證

Quix 強制使用 `range` 分配策略，確保同一個 consumer 拿到兩個 topic 的**相同 partition 號**：

```python
# --- 檔案: quixstreams/app.py ---

# Force assignment strategy to be "range" for co-partitioning in internal Consumers
consumer_extra_config_overrides = {"partition.assignment.strategy": "range"}
```

```python
# --- 檔案: quixstreams/kafka/consumer.py ---

self._consumer_config = {
    "enable.auto.offset.store": False,
    "partition.assignment.strategy": "range",    # ← 強制 range
    **extra_config,
    ...
}
```

**Range 策略的效果**：
- topic A 有 partition 0,1,2，topic B 有 partition 0,1,2
- consumer 1 拿到 A-0, B-0
- consumer 2 拿到 A-1, B-1
- consumer 3 拿到 A-2, B-2

這樣 key `"user_123"` 在 A 和 B 都落在 partition 0，就會被同一個 consumer 處理。

---

## 2. Join 類型總覽

Quix Streams 有三種 join：

| 類型 | 方法 | 用途 | 需要 copartition | State 存哪 |
|------|------|------|-------------------|-----------|
| **AsOf Join** | `sdf.join_asof(right)` | 用右側最新的匹配記錄 enrich 左側 | **是** | RocksDB (TimestampedStore) |
| **Interval Join** | `sdf.join_interval(right)` | 時間窗口內雙向匹配 | **是** | RocksDB (TimestampedStore) |
| **Lookup Join** | `sdf.join_lookup(lookup, fields)` | 查外部資料庫 enrich | **否**（不涉及 Kafka） | 無（查外部 DB） |

---

## 3. Join 基類完整源碼

```python
# --- 檔案: quixstreams/dataframe/joins/base.py ---

from abc import ABC, abstractmethod
from datetime import timedelta
from typing import (
    TYPE_CHECKING, Any, Callable, Literal,
    Optional, Union, cast, get_args,
)

from quixstreams.context import message_context
from quixstreams.dataframe.utils import ensure_milliseconds
from quixstreams.models.topics.manager import TopicManager
from quixstreams.state.rocksdb.timestamped import TimestampedPartitionTransaction

from .utils import keep_left_merger, keep_right_merger, raise_merger

if TYPE_CHECKING:
    from quixstreams.dataframe.dataframe import StreamingDataFrame

OnOverlap = Literal["keep-left", "keep-right", "raise"]
OnOverlap_choices = get_args(OnOverlap)


class Join(ABC):
    def __init__(
        self,
        how: str,
        on_merge: Union[OnOverlap, Callable[[Any, Any], Any]],
        grace_ms: Union[int, timedelta],
        store_name: Optional[str] = None,
    ) -> None:
        if callable(on_merge):
            self._merger = on_merge
        elif on_merge == "keep-left":
            self._merger = keep_left_merger
        elif on_merge == "keep-right":
            self._merger = keep_right_merger
        elif on_merge == "raise":
            self._merger = raise_merger
        else:
            raise ValueError(...)

        self._how = how
        self._grace_ms = ensure_milliseconds(grace_ms)
        self._store_name = store_name or "join"

    def join(
        self,
        left: "StreamingDataFrame",
        right: "StreamingDataFrame",
    ) -> "StreamingDataFrame":
        self._validate_dataframes(left, right)       # ← 驗證 copartition
        return self._prepare_join(left, right)        # ← 子類實現

    @abstractmethod
    def _prepare_join(
        self,
        left: "StreamingDataFrame",
        right: "StreamingDataFrame",
    ) -> "StreamingDataFrame": ...

    def _validate_dataframes(
        self,
        left: "StreamingDataFrame",
        right: "StreamingDataFrame",
    ) -> None:
        if left.stream_id == right.stream_id:
            raise ValueError(
                "Joining dataframes originating from "
                "the same topic is not yet supported.",
            )
        # 只驗證，不 repartition
        TopicManager.ensure_topics_copartitioned(*left.topics, *right.topics)

    def _register_store(
        self,
        sdf: "StreamingDataFrame",
        keep_duplicates: bool,
    ) -> None:
        sdf.processing_context.state_manager.register_timestamped_store(
            stream_id=sdf.stream_id,
            store_name=self._store_name,
            grace_ms=self._grace_ms,
            keep_duplicates=keep_duplicates,
            changelog_config=TopicManager.derive_topic_config(sdf.topics),
        )

    def _get_transaction(
        self, sdf: "StreamingDataFrame"
    ) -> TimestampedPartitionTransaction:
        return cast(
            TimestampedPartitionTransaction,
            sdf.processing_context.checkpoint.get_store_transaction(
                stream_id=sdf.stream_id,
                partition=message_context().partition,
                store_name=self._store_name,
            ),
        )
```

### 3.1 基類逐段解析

#### `__init__`：合併策略

```python
on_merge = "raise"     → raise_merger（欄位重複就報錯）
on_merge = "keep-left" → keep_left_merger（左邊優先）
on_merge = "keep-right"→ keep_right_merger（右邊優先）
on_merge = callable    → 用戶自定義的合併函數
```

#### `_validate_dataframes`：兩個檢查

1. **禁止 self-join**：`left.stream_id == right.stream_id` → 報錯
2. **強制 copartition**：`ensure_topics_copartitioned()` → partition 數不同就報錯

#### `_register_store`：註冊 TimestampedStore

join 使用的是 `TimestampedStore`（帶時間戳的 RocksDB store），
不是普通的 `RocksDBStore`。因為 join 需要按時間查詢（`get_latest`、`get_interval`）。

#### `_get_transaction`：取得當前 partition 的 store transaction

注意 `partition=message_context().partition`，
這代表 join 的 state 是 **per-partition** 的。
partition 0 的 left 只能查到 partition 0 的 right state。

---

## 4. AsOf Join 完整源碼

```python
# --- 檔案: quixstreams/dataframe/joins/join_asof.py ---

from datetime import timedelta
from typing import TYPE_CHECKING, Any, Callable, Literal, Optional, Union, get_args

from .base import Join, OnOverlap

if TYPE_CHECKING:
    from quixstreams.dataframe.dataframe import StreamingDataFrame

AsOfJoinHow = Literal["inner", "left"]

DISCARDED = object()
block_all = lambda value: False
block_discarded = lambda value: value is not DISCARDED


class AsOfJoin(Join):
    def __init__(
        self,
        how: AsOfJoinHow,
        on_merge: Union[OnOverlap, Callable[[Any, Any], Any]],
        grace_ms: Union[int, timedelta],
        store_name: Optional[str] = None,
    ) -> None:
        if how not in get_args(AsOfJoinHow):
            raise ValueError(f"Join type not supported: {how}")
        super().__init__(how, on_merge, grace_ms, store_name)

    def _prepare_join(
        self,
        left: "StreamingDataFrame",
        right: "StreamingDataFrame",
    ) -> "StreamingDataFrame":
        # 只為 right 註冊 store（left 不需要存）
        self._register_store(right, keep_duplicates=False)

        tx = self._get_transaction
        is_inner_join = self._how == "inner"
        merger = self._merger

        def left_func(value, key, timestamp, headers):
            # 查 right store：找同 key、timestamp <= 當前的最新一筆
            if right_value := tx(right).get_latest(timestamp=timestamp, prefix=key):
                return merger(value, right_value)
            # inner join 沒找到 → 丟棄；left join 沒找到 → 輸出 (left, None)
            return DISCARDED if is_inner_join else merger(value, None)

        def right_func(value, key, timestamp, headers):
            # right 來的資料只存 state，不往下游發
            tx(right).set_for_timestamp(
                timestamp=timestamp, value=value, prefix=key
            )

        # right: 存 state → 全部過濾掉（block_all）
        right = right.update(right_func, metadata=True).filter(block_all)
        # left: 查 right state → 過濾掉被 DISCARDED 的（inner join 沒匹配的）
        left = left.apply(left_func, metadata=True).filter(block_discarded)
        # 合併兩個 stream
        return left.concat(right)
```

### 4.1 AsOf Join 資料流向圖

```
Topic "measurements" (left)              Topic "metadata" (right)
   key=sensor_1, ts=1000                    key=sensor_1, ts=800
   key=sensor_1, ts=2000                    key=sensor_1, ts=1500
        │                                        │
        ▼                                        ▼
   left_func()                              right_func()
   查 right store:                          存入 right store:
   get_latest(ts=1000, key=sensor_1)        set_for_timestamp(ts=800, sensor_1)
   → 找到 ts=800 的 right 記錄               │
   → merger(left_value, right_value)        filter(block_all) → 不往下游發
        │                                        │
        ▼                                        ▼
   輸出合併結果                              無輸出
        │                                        │
        └──────────────┬─────────────────────────┘
                       │
                  left.concat(right)
                       │
                       ▼
                  後續 pipeline
```

### 4.2 keep_duplicates=False 的含義

AsOf Join 的 right store 設定 `keep_duplicates=False`，意思是：
**同一個 key + 同一個 timestamp 只保留最新的一筆**。
因為 AsOf Join 只需要「最接近的一筆」，不需要保留所有歷史。

### 4.3 grace_ms 的作用

right 記錄按 timestamp 存在 state 裡，但不能無限累積。
`grace_ms` 控制過期：當新的 right 記錄 timestamp=T 進來時，
所有 `timestamp < T - grace_ms` 的同 key 記錄會被清除。

---

## 5. Interval Join 完整源碼

```python
# --- 檔案: quixstreams/dataframe/joins/join_interval.py ---

from datetime import timedelta
from typing import TYPE_CHECKING, Any, Callable, Literal, Optional, Union, get_args

from quixstreams.dataframe.utils import ensure_milliseconds
from quixstreams.models.types import HeadersTuples

from .base import Join, OnOverlap

if TYPE_CHECKING:
    from quixstreams.dataframe.dataframe import StreamingDataFrame

IntervalJoinHow = Literal["inner", "left", "right", "outer"]

drop_headers: Callable[[Any, Any, int, HeadersTuples], HeadersTuples] = lambda *_: []


class IntervalJoin(Join):
    def __init__(
        self,
        how: IntervalJoinHow,
        on_merge: Union[OnOverlap, Callable[[Any, Any], Any]],
        grace_ms: Union[int, timedelta],
        store_name: Optional[str] = None,
        backward_ms: Union[int, timedelta] = 0,
        forward_ms: Union[int, timedelta] = 0,
    ) -> None:
        if how not in get_args(IntervalJoinHow):
            raise ValueError(f"Join type not supported: {how}")

        super().__init__(how, on_merge, grace_ms, store_name)
        self._backward_ms = ensure_milliseconds(backward_ms)
        self._forward_ms = ensure_milliseconds(forward_ms)

        if self._backward_ms > self._grace_ms:
            raise ValueError(
                "The backward_ms must not be greater than the grace_ms "
                "to avoid losing data."
            )

    def _prepare_join(
        self,
        left: "StreamingDataFrame",
        right: "StreamingDataFrame",
    ) -> "StreamingDataFrame":
        # 兩邊都要存 state（雙向查詢）
        self._register_store(left, keep_duplicates=True)
        self._register_store(right, keep_duplicates=True)

        tx = self._get_transaction
        emit_if_no_match_on_the_right = self._how in ["left", "outer"]
        emit_if_no_match_on_the_left = self._how in ["right", "outer"]
        merger = self._merger
        backward_ms = self._backward_ms
        forward_ms = self._forward_ms

        def left_func(value, key, timestamp, headers):
            # 存 left 到 state
            tx(left).set_for_timestamp(
                timestamp=timestamp, value=value, prefix=key
            )

            # 查 right state：找 [timestamp - backward_ms, timestamp + 1) 範圍內的
            if right_values := tx(right).get_interval(
                start=timestamp - backward_ms,
                end=timestamp + 1,    # +1 因為 end 是 exclusive
                prefix=key,
            ):
                return [merger(value, right_value) for right_value in right_values]
            return [merger(value, None)] if emit_if_no_match_on_the_right else []

        def right_func(value, key, timestamp, headers):
            # 存 right 到 state
            tx(right).set_for_timestamp(
                timestamp=timestamp, value=value, prefix=key
            )

            # 查 left state：找 [timestamp - forward_ms, timestamp + 1) 範圍內的
            if left_values := tx(left).get_interval(
                start=timestamp - forward_ms,
                end=timestamp + 1,    # +1 因為 end 是 exclusive
                prefix=key,
            ):
                return [merger(left_value, value) for left_value in left_values]
            return [merger(None, value)] if emit_if_no_match_on_the_left else []

        # 兩邊都清空 headers + apply（expand=True 展開列表）
        right = right.set_headers(drop_headers).apply(
            right_func, expand=True, metadata=True
        )
        left = left.set_headers(drop_headers).apply(
            left_func, expand=True, metadata=True
        )
        return left.concat(right)
```

### 5.1 AsOf vs Interval 核心差異

| 面向 | AsOf Join | Interval Join |
|------|-----------|---------------|
| State 哪邊存 | **只存 right** | **兩邊都存** |
| keep_duplicates | `False`（同 key+ts 只保留一筆） | `True`（同 ts 可以有多筆） |
| 查詢方式 | `get_latest(ts, key)` 找 ≤ ts 的最新一筆 | `get_interval(start, end, key)` 找範圍 |
| 輸出量 | 1 left → 最多 1 輸出 | 1 left → 可能 N 個輸出（expand） |
| 誰觸發輸出 | **只有 left 觸發** | **兩邊都觸發** |
| how 支援 | `inner`, `left` | `inner`, `left`, `right`, `outer` |
| 典型場景 | 用最新 metadata enrich | 時間窗口內的事件關聯 |

### 5.2 Interval Join 時間窗口圖示

設定：`backward_ms=5000, forward_ms=3000`

```
left 記錄  ts=10000
                                backward_ms       forward_ms
                              ◄──── 5000 ────►   ◄── 3000 ──►
時間軸:  ─────┬────────────────┬────────────────┬──────────────┬────
         ts=5000          ts=10000          ts=10001      ts=13000

查 right state 的範圍: [10000 - 5000, 10000 + 1) = [5000, 10001)
→ 找 right 中 ts 在 5000~10000 之間的所有記錄（同 key）
```

right 記錄 ts=10000 到來時：
```
查 left state 的範圍: [10000 - 3000, 10000 + 1) = [7000, 10001)
→ 找 left 中 ts 在 7000~10000 之間的所有記錄（同 key）
```

### 5.3 drop_headers 的作用

```python
drop_headers: Callable[..., HeadersTuples] = lambda *_: []
```

Interval Join 的一筆輸入可能對應多筆輸出（expand=True）。
如果保留 headers，多筆輸出會帶相同的 headers，可能導致下游的冪等性判斷出錯。
所以清空 headers。

### 5.4 backward_ms 不能超過 grace_ms

```python
if self._backward_ms > self._grace_ms:
    raise ValueError(
        "The backward_ms must not be greater than the grace_ms "
        "to avoid losing data."
    )
```

**為什麼？** `grace_ms` 控制 state 裡的記錄多久被清除。
如果 `backward_ms=1小時` 但 `grace_ms=30分鐘`，
那 30 分鐘前的記錄已經被清了，回溯 1 小時永遠找不到資料。

---

## 6. Merge 工具函數

```python
# --- 檔案: quixstreams/dataframe/joins/utils.py ---

def keep_left_merger(left: Optional[Mapping], right: Optional[Mapping]) -> dict:
    left = left if left is not None else {}
    right = right if right is not None else {}
    return {**right, **left}      # left 的 key 覆蓋 right

def keep_right_merger(left: Optional[Mapping], right: Optional[Mapping]) -> dict:
    left = left if left is not None else {}
    right = right if right is not None else {}
    return {**left, **right}      # right 的 key 覆蓋 left

def raise_merger(left: Optional[Mapping], right: Optional[Mapping]) -> dict:
    left = left if left is not None else {}
    right = right if right is not None else {}
    if overlapping_columns := left.keys() & right.keys():
        overlapping_columns_str = ", ".join(sorted(overlapping_columns))
        raise ValueError(
            f"Overlapping columns: {overlapping_columns_str}."
            'You need to provide either an "on_merge" value of '
            "'keep-left' or 'keep-right' or a custom merger function."
        )
    return {**left, **right}
```

具體例子：

```python
left  = {"sensor_id": "s1", "temperature": 25.0}
right = {"sensor_id": "s1", "location": "taipei", "status": "active"}

keep_left_merger(left, right)
# → {"sensor_id": "s1", "location": "taipei", "status": "active", "temperature": 25.0}
#    ↑ sensor_id 用 left 的

keep_right_merger(left, right)
# → {"sensor_id": "s1", "temperature": 25.0, "location": "taipei", "status": "active"}
#    ↑ sensor_id 用 right 的

raise_merger(left, right)
# → ValueError: Overlapping columns: sensor_id
```

---

## 7. concat() — Join 的底層拼接

AsOf Join 和 Interval Join 最後都呼叫 `left.concat(right)`：

```python
# --- 檔案: quixstreams/dataframe/dataframe.py ---

def concat(self, other: "StreamingDataFrame") -> "StreamingDataFrame":
    merged_stream = self.stream.merge(other.stream)

    total_topics = {t.name for t in itertools.chain(self.topics, other.topics)}
    if len(total_topics) > 1:
        # 不同 topic → 啟用時間對齊
        self._registry.require_time_alignment()
        return self.__dataframe_clone__(
            *self.topics, *other.topics, stream=merged_stream
        )
    else:
        # 同一個 topic 的分支合併
        merged_stream_id = stream_id_from_strings(self.stream_id, other.stream_id)
        return self.__dataframe_clone__(
            stream=merged_stream, stream_id=merged_stream_id
        )
```

### 7.1 require_time_alignment 的效果

```python
# --- 檔案: quixstreams/dataframe/registry.py ---

def require_time_alignment(self):
    self._requires_time_alignment = True
```

設定後，Application 的 consumer 會進入 **buffered 模式**：

```python
# --- 檔案: quixstreams/app.py ---

rows = self._consumer.poll_row(
    timeout=self._config.consumer_poll_timeout,
    buffered=self._dataframe_registry.requires_time_alignment,
    #        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    #        join 時為 True → 按 timestamp 排序消費
)
```

**buffered 模式的作用**：
確保 partition 0 的 left 和 partition 0 的 right 消息按 **timestamp 順序** 交替處理。
否則可能 left 的消息全部先處理完，去查 right store 時 right 還沒到。

---

## 8. Lookup Join 完整源碼

Lookup Join 和 AsOf/Interval 完全不同。它查的是**外部資料庫**，不涉及 Kafka topic 間的 join。

### 8.1 SDF 的 join_lookup 方法

```python
# --- 檔案: quixstreams/dataframe/dataframe.py ---

def join_lookup(
    self,
    lookup: BaseLookup,
    fields: dict[str, BaseField],
    on: Optional[Union[str, Callable[[dict[str, Any], Any], str]]] = None,
) -> "StreamingDataFrame":
    if callable(on):
        def _on(value: dict[str, Any], key: Any) -> str:
            return on(value, key)
    elif isinstance(on, str):
        def _on(value: dict[str, Any], key: Any) -> str:
            return value[on]               # 用 value 的某個欄位當 lookup key
    else:
        def _on(value: dict[str, Any], key: Any) -> str:
            return key                     # 預設用 message key

    def _join(
        value: dict[str, Any], key: Any, timestamp: int, headers: HeadersMapping
    ):
        on_key = _on(value, key)
        lookup.join(fields, on_key, value, key, timestamp, headers)

    return self.update(_join, metadata=True)
```

**就是一個 `update()`**，在 pipeline 中 in-place 修改 value。
沒有 repartition、沒有 state store、沒有 copartition 要求。

### 8.2 BaseLookup 抽象類

```python
# --- 檔案: quixstreams/dataframe/joins/lookups/base.py ---

class BaseLookup(abc.ABC, Generic[F]):
    @abc.abstractmethod
    def join(
        self,
        fields: Mapping[str, F],
        on: str,
        value: dict[str, Any],
        key: Any,
        timestamp: int,
        headers: HeadersMapping,
    ) -> None:
        """
        在 value dict 上 in-place 加入 enrichment 資料。
        """
        pass


@dataclasses.dataclass(frozen=True)
class BaseField(abc.ABC):
    pass
```

### 8.3 SQLiteLookup 實現

```python
# --- 檔案: quixstreams/dataframe/joins/lookups/sqlite.py ---

class SQLiteLookup(BaseLookup[Union[SQLiteLookupField, SQLiteLookupQueryField]]):

    def __init__(self, path: str, cache_size: int = 1000):
        self.db_path = path
        self._conn = sqlite3.connect(
            f"file:{self.db_path}?mode=ro", uri=True, check_same_thread=False
        )
        self._cache: OrderedDict[tuple[str, Any], tuple[float, Any]] = OrderedDict()
        self._cache_size = cache_size

    def join(
        self,
        fields: Mapping[str, Union[SQLiteLookupField, SQLiteLookupQueryField]],
        on: str,
        value: dict[str, Any],
        key: Any,
        timestamp: int,
        headers: Any,
    ) -> None:
        now = time.time()
        for field_name, field in fields.items():
            if field.ttl <= 0:
                # TTL=0 → 不快取，每次都查 DB
                value[field_name] = self._process_field(field, on, value)
                continue

            cache_key = (field_name, on)
            result = self._get_from_cache(cache_key, field.ttl, now)
            if result is MISSING:
                self._cache_misses += 1
                result = self._process_field(field, on, value)
                self._set_cache(cache_key, result, now)

            value[field_name] = result   # in-place 更新

    def _process_field(self, field, on, value):
        query, parameters = field.build_query(on, value)
        cur = self._conn.execute(query, parameters)
        return field.result(cur)
```

### 8.4 SQLiteLookupField 查詢構建

```python
# --- 檔案: quixstreams/dataframe/joins/lookups/sqlite.py ---

@dataclasses.dataclass(frozen=True)
class SQLiteLookupField(BaseSQLiteLookupField):
    table: str
    columns: list[str]
    on: str                                          # WHERE 欄位名
    order_by: str = ""
    order_by_direction: Literal["ASC", "DESC"] = "ASC"
    ttl: float = 60.0                                # 快取 TTL（秒）
    default: Any = None
    first_match_only: bool = True                    # 只取第一筆

    def build_query(self, on, value):
        query = (
            f"SELECT {', '.join(self.columns)} "
            f"FROM {self.table} WHERE {self.on} = ?"
        )
        if self.order_by:
            query += f" ORDER BY {self.order_by} {self.order_by_direction}"
        if self.first_match_only:
            query += " LIMIT 1"
        return query, (on,)

    def result(self, cursor):
        if self.first_match_only:
            row = cursor.fetchone()
            return {k: v for k, v in zip(self.columns, row)} if row else self.default
        else:
            return [{k: v for k, v in zip(self.columns, row)} for row in cursor]
```

使用範例：

```python
lookup = SQLiteLookup(path="/data/users.db")

fields = {
    "user_info": SQLiteLookupField(
        table="users",
        columns=["name", "email"],
        on="user_id",          # WHERE user_id = ?
        ttl=300.0,             # 快取 5 分鐘
    )
}

sdf = sdf.join_lookup(lookup, fields, on="user_id")
# 每筆 message 會被加上 "user_info": {"name": "...", "email": "..."}
```

---

## 9. 三種 Join 的完整對比

| 面向 | AsOf Join | Interval Join | Lookup Join |
|------|-----------|---------------|-------------|
| **觸發方式** | left 觸發查 right store | 兩邊都觸發查對方 store | 每筆消息查外部 DB |
| **State** | 只存 right（RocksDB） | 兩邊都存（RocksDB） | 無（LRU cache 在記憶體） |
| **需要 copartition** | 是 | 是 | 否 |
| **需要 time alignment** | 是（concat 啟用） | 是（concat 啟用） | 否 |
| **how 類型** | inner, left | inner, left, right, outer | N/A |
| **一對多** | 否（一對一） | 是（expand=True） | 否 |
| **grace_ms** | 控制 right state 過期 | 控制兩邊 state 過期 | N/A（用 ttl 控制快取） |
| **自動 repartition** | **否** | **否** | **不涉及** |
| **用途** | 用最新 metadata enrich | 時間窗口事件關聯 | 查靜態/外部資料 enrich |

---

## 10. 與 Flink / Kafka Streams 的 Join 對比

### 10.1 自動 repartition 對比

| 框架 | 自動 repartition | 機制 |
|------|------------------|------|
| **Flink** | **是** | 如果兩邊 key 不同，自動 network shuffle（hash partition） |
| **Kafka Streams** | **否** | 要求 copartitioned，否則用戶要先 `repartition()` |
| **Quix Streams** | **否** | 要求 copartitioned，否則用戶要先 `group_by()` |

### 10.2 Flink 自動 repartition 示意

```java
// Flink：兩個 stream key 不同也能 join
stream_A.keyBy(a -> a.userId)
        .connect(stream_B.keyBy(b -> b.userId))
        .process(new JoinFunction());
// Flink 會自動做 network shuffle，確保同 userId 到同 task
```

```python
# Quix：必須確保兩個 topic 的 key 已經是同一個欄位
# 如果 left key=store_id 而 right key=user_id，必須先 re-key
sdf_left = sdf_left.group_by("user_id")    # 先 repartition
sdf_right = sdf_right                       # 假設已經用 user_id 當 key
result = sdf_left.join_asof(sdf_right)      # 現在才能 join
```

### 10.3 State 管理對比

| 框架 | Join State 存哪 | 跨 partition 查詢 |
|------|----------------|-------------------|
| **Flink** | TaskManager heap / RocksDB | 可以（經 network） |
| **Kafka Streams** | RocksDB（本地） | 不行（per-partition） |
| **Quix Streams** | RocksDB（TimestampedStore） | 不行（per-partition） |

### 10.4 為什麼 Quix/Kafka Streams 不做自動 repartition

1. **Kafka 的設計哲學**：資料移動靠 topic，不靠記憶體 shuffle
2. **repartition 成本高**：每次 repartition 都是一次完整的 produce → consume cycle
3. **語義清晰**：用戶顯式控制 re-key，知道自己在做什麼
4. **避免隱式 topic 爆炸**：自動 repartition 會暗中建 topic，難以管理

---

## 11. 完整例子

### 11.1 AsOf Join：感測器 + 設備元數據

```python
from datetime import timedelta
from quixstreams import Application

app = Application(broker_address="localhost:9092", consumer_group="my-app")

# 兩個 topic 都用 sensor_id 當 key，partition 數相同
measurements = app.topic("measurements")   # 6 partitions
metadata = app.topic("metadata")           # 6 partitions

sdf_m = app.dataframe(measurements)
sdf_meta = app.dataframe(metadata)

# join: 用 metadata 中 timestamp ≤ measurement timestamp 的最新記錄 enrich
sdf_joined = sdf_m.join_asof(
    sdf_meta,
    how="left",                  # 即使沒有 metadata 也輸出
    on_merge="keep-left",        # measurement 欄位優先
    grace_ms=timedelta(days=30), # 保留 30 天的 metadata
)
```

內部流程：
1. `_validate_dataframes` → 檢查 measurements 和 metadata 都是 6 partition → OK
2. `_register_store(right=sdf_meta, keep_duplicates=False)` → 註冊 TimestampedStore
3. consumer 訂閱兩個 topic + 啟用 buffered 模式（time alignment）
4. 收到 metadata 消息 → `right_func` → 存入 RocksDB → filter 掉（不輸出）
5. 收到 measurement 消息 → `left_func` → 查 RocksDB → merger → 輸出

### 11.2 如果 key 不匹配怎麼辦

```python
# orders topic: key = order_id
# users topic: key = user_id
# 想要 join orders 和 users

sdf_orders = app.dataframe(app.topic("orders"))
sdf_users = app.dataframe(app.topic("users"))

# 直接 join → 錯！key 不同，相同 user_id 可能在不同 partition
# sdf_orders.join_asof(sdf_users)  # 結果不正確

# 正確做法：先把 orders re-key 成 user_id
sdf_orders = sdf_orders.group_by("user_id")  # repartition by user_id
sdf_joined = sdf_orders.join_asof(sdf_users)  # 現在 key 匹配了
```

---

## 12. 總結

```
┌─────────────────────────────────────────────────────────────────────┐
│  Join 的核心設計：                                                   │
│                                                                     │
│  1. 不自動 repartition，要求用戶確保 copartitioned                   │
│  2. 強制 range 分配策略，保證同 partition 號分給同 consumer           │
│  3. AsOf Join：右側存 state，左側查最新匹配                          │
│  4. Interval Join：兩側互存互查，支援時間窗口                        │
│  5. Lookup Join：查外部 DB，不涉及 Kafka copartition                 │
│  6. buffered 模式 (time alignment) 確保跨 topic 按時間序消費         │
│  7. 如果 key 不匹配，先用 group_by() re-key 再 join                 │
└─────────────────────────────────────────────────────────────────────┘
```
