# Quix Streams: DAG 提取與視覺化 源碼深度解析

## 目錄

1. [總覽：Pipeline 是一個 DAG](#1-總覽)
2. [Stream 類 — DAG 的核心數據結構](#2-stream-類)
3. [StreamFunction 基類 — 節點的函數包裝](#3-streamfunction-基類)
4. [四種 StreamFunction 類型](#4-四種-streamfunction)
5. [StreamingDataFrame — 用戶 API 如何轉成 Stream 節點](#5-streamingdataframe)
6. [DataFrameRegistry — 多 topic 管理](#6-dataframeregistry)
7. [compose() — DAG 編譯為可執行 closure](#7-compose)
8. [如何提取 DAG 給前端顯示](#8-如何提取-dag)

---

## 1. 總覽

Quix Streams 的 pipeline 本質上是一個 **DAG (Directed Acyclic Graph)**：

```
Source Topic A ──→ filter ──→ apply ──→ sink (DB)
                                │
                                └──→ apply ──→ sink (S3)

Source Topic B ──→ apply ──→ ─┐
                              ├──→ concat ──→ filter ──→ sink
Source Topic A (filtered) ────┘
```

內部用 `Stream` 類實現，每個 `Stream` 節點有：
- `func`: 一個 `StreamFunction`（Apply/Filter/Update/Transform）
- `parents`: 上游節點列表
- `children`: 下游節點列表

---

## 2. Stream 類 — DAG 的核心數據結構

**`quixstreams/core/stream/stream.py` (完整源碼)**

```python
import collections
import copy
from collections import deque
from graphlib import TopologicalSorter
from typing import (
    Any,
    Deque,
    List,
    Literal,
    Optional,
    Union,
    cast,
    overload,
)

from .exceptions import InvalidOperation, InvalidTopology
from .functions import (
    ApplyCallback,
    ApplyExpandedCallback,
    ApplyFunction,
    ApplyWithMetadataCallback,
    ApplyWithMetadataExpandedCallback,
    ApplyWithMetadataFunction,
    FilterCallback,
    FilterFunction,
    FilterWithMetadataCallback,
    FilterWithMetadataFunction,
    ReturningExecutor,
    StreamFunction,
    TransformCallback,
    TransformExpandedCallback,
    TransformFunction,
    UpdateCallback,
    UpdateFunction,
    UpdateWithMetadataCallback,
    UpdateWithMetadataFunction,
    VoidExecutor,
)

__all__ = ("Stream",)


class Stream:
    def __init__(
        self,
        func: Optional[StreamFunction] = None,
        parents: Optional[List["Stream"]] = None,
    ):
        """
        A base class for all streaming operations.

        `Stream` is an abstraction of a function pipeline.
        Each Stream has a function and an optional list of parents (None by default).
        When adding new function to the stream, it creates a new `Stream` object and
        sets "parent" to the previous `Stream` to maintain an order of execution.

        Streams supports four types of functions:

        - "Apply" - generate new values based on a previous one.
        - "Update" - update values in-place.
        - "Filter" - to filter values from the Stream.
        - "Transform" - to transform keys and timestamps along with the values.

        To execute the functions on the `Stream`, call `.compose()` method, and
        it will return a closure to execute all the functions accumulated in the Stream
        and its parents.
        """
        if func is not None and not isinstance(func, StreamFunction):
            raise ValueError("Provided function must be a subclass of StreamFunction")

        self.func = func if func is not None else ApplyFunction(lambda value: value)
        self.parents = parents or []
        self.children: list["Stream"] = []
        self.pruned = False

    def __repr__(self) -> str:
        func_repr = f"<{self.func.__class__.__name__}: {self.func.func.__qualname__}>"
        return (
            f"<{self.__class__.__name__} "
            f"(parents={len(self.parents)} children={len(self.children)}): "
            f"{func_repr}>"
        )

    def add_filter(
        self,
        func: Union[FilterCallback, FilterWithMetadataCallback],
        *,
        metadata: bool = False,
    ) -> "Stream":
        if metadata:
            filter_func: StreamFunction = FilterWithMetadataFunction(
                cast(FilterWithMetadataCallback, func)
            )
        else:
            filter_func = FilterFunction(cast(FilterCallback, func))
        return self._add(filter_func)

    def add_apply(
        self,
        func,
        *,
        expand: bool = False,
        metadata: bool = False,
    ) -> "Stream":
        if metadata:
            apply_func: StreamFunction = ApplyWithMetadataFunction(func, expand=expand)
        else:
            apply_func = ApplyFunction(func, expand=expand)
        return self._add(apply_func)

    def add_update(
        self,
        func: Union[UpdateCallback, UpdateWithMetadataCallback],
        *,
        metadata: bool = False,
    ) -> "Stream":
        if metadata:
            update_func: StreamFunction = UpdateWithMetadataFunction(
                cast(UpdateWithMetadataCallback, func)
            )
        else:
            update_func = UpdateFunction(cast(UpdateCallback, func))
        return self._add(update_func)

    def add_transform(
        self,
        func: Union[TransformCallback, TransformExpandedCallback],
        *,
        expand: bool = False,
    ) -> "Stream":
        return self._add(TransformFunction(func, expand=expand))

    def merge(self, other: "Stream") -> "Stream":
        """
        Merge two Streams together and return a new Stream with two parents.
        """
        if other is self:
            raise InvalidOperation("Cannot merge a SDF with itself")
        elif other in self.root_path() or self in other.root_path():
            raise InvalidOperation("The target SDF is already present in the topology")

        merged_stream = self.__class__(parents=[self, other])
        self.children.append(merged_stream)
        other.children.append(merged_stream)
        return merged_stream

    def full_tree(self) -> List["Stream"]:
        """
        Find every related Stream in the tree and return them
        in topologically sorted order.
        """
        sorter: TopologicalSorter = TopologicalSorter()
        visited: set["Stream"] = set()

        to_traverse: Deque["Stream"] = collections.deque()
        to_traverse.append(self)
        while to_traverse:
            node = to_traverse.popleft()
            if node in visited:
                continue
            sorter.add(node, *node.parents)
            visited.add(node)
            to_traverse += node.parents + node.children

        nodes = list(sorter.static_order())
        return nodes

    def compose(
        self,
        allow_filters=True,
        allow_expands=True,
        allow_updates=True,
        allow_transforms=True,
        sink: Optional[VoidExecutor] = None,
    ) -> dict["Stream", VoidExecutor]:
        """
        Generate an "executor" closure by composing all functions together.
        """
        sink = sink or self._default_sink
        executors: dict["Stream", VoidExecutor] = {}

        for stream in reversed(self.full_tree()):
            func = stream.func

            if not allow_updates and isinstance(
                func, (UpdateFunction, UpdateWithMetadataFunction)
            ):
                raise ValueError("Update functions are not allowed")
            elif not allow_filters and isinstance(
                func, (FilterFunction, FilterWithMetadataFunction)
            ):
                raise ValueError("Filter functions are not allowed")
            elif not allow_transforms and isinstance(func, TransformFunction):
                raise ValueError("Transform functions are not allowed")
            elif not allow_expands and func.expand:
                raise ValueError("Expand functions are not allowed")

            if stream.children:
                child_executors = [executors[child] for child in stream.children]
            else:
                child_executors = [sink]

            executor = func.get_executor(*child_executors)
            executors[stream] = executor

        root_executors = {s: e for s, e in executors.items() if not s.parents}
        return root_executors

    def is_merged(self) -> bool:
        return len(self.parents) > 1

    def is_branched(self) -> bool:
        return len(self.children) > 1

    def root_path(self) -> List["Stream"]:
        """Start from self and collect all parents until reaching the root nodes"""
        nodes = [self]
        to_traverse: Deque = collections.deque([self])
        while to_traverse:
            node = to_traverse.popleft()
            for parent in node.parents:
                nodes.append(parent)
                to_traverse.append(parent)
        return nodes

    def _add(self, func: StreamFunction) -> "Stream":
        new_node = self.__class__(func=func, parents=[self])
        self.children.append(new_node)
        return new_node

    def _default_sink(
        self, value: Any, key: Any, timestamp: int, headers: Any
    ) -> None: ...
```

**核心數據結構**：
- `parents` / `children` 構成了 DAG 的邊
- `func` 是節點上的操作
- `full_tree()` 用 `graphlib.TopologicalSorter` 做拓撲排序
- `compose()` 從葉子往根遍歷，把每個節點的函數串成一個大 closure

---

## 3. StreamFunction 基類 — 節點的函數包裝

**`quixstreams/core/stream/functions/base.py` (完整源碼)**

```python
import abc
from typing import Any

from quixstreams.utils.pickle import pickle_copier

from .types import StreamCallback, VoidExecutor

__all__ = ("StreamFunction",)


class StreamFunction(abc.ABC):
    """
    A base class for all the streaming operations in Quix Streams.
    """

    expand: bool = False

    def __init__(self, func: StreamCallback):
        self.func = func

    @abc.abstractmethod
    def get_executor(self, *child_executors: VoidExecutor) -> VoidExecutor:
        """
        Returns a wrapper to be called on a value, key, timestamp and headers.
        """

    def _resolve_branching(self, *child_executors: VoidExecutor) -> VoidExecutor:
        """
        Handle branching: if multiple children, copy value for each branch.
        """
        if not child_executors:
            raise ValueError("At least one executor is required")

        if len(child_executors) > 1:
            def wrapper(
                value: Any, key: Any, timestamp: int, headers: Any,
            ):
                first_branch_executor, *branch_executors = child_executors
                copier = pickle_copier(value)
                # Pass original to first branch, copy for rest
                first_branch_executor(value, key, timestamp, headers)
                for branch_executor in branch_executors:
                    branch_executor(copier(), key, timestamp, headers)
            return wrapper
        else:
            return child_executors[0]
```

**分支處理**：當一個節點有多個 children 時，`_resolve_branching` 會用 `pickle_copier` 深拷貝 value，確保各分支互不影響。

---

## 4. 四種 StreamFunction

### 4.1 ApplyFunction — 轉換值

```python
class ApplyFunction(StreamFunction):
    def __init__(self, func, expand=False):
        super().__init__(func)
        self.expand = expand

    def get_executor(self, *child_executors: VoidExecutor) -> VoidExecutor:
        child_executor = self._resolve_branching(*child_executors)
        func = self.func

        if self.expand:
            def wrapper(value, key, timestamp, headers):
                result = func(value)
                for item in result:
                    child_executor(item, key, timestamp, headers)
        else:
            def wrapper(value, key, timestamp, headers):
                result = func(value)
                child_executor(result, key, timestamp, headers)
        return wrapper
```

### 4.2 FilterFunction — 過濾

```python
class FilterFunction(StreamFunction):
    def get_executor(self, *child_executors: VoidExecutor) -> VoidExecutor:
        child_executor = self._resolve_branching(*child_executors)
        func = self.func

        def wrapper(value, key, timestamp, headers):
            if func(value):                     # <-- 回傳 True 才繼續
                child_executor(value, key, timestamp, headers)
        return wrapper
```

### 4.3 UpdateFunction — 就地修改

```python
class UpdateFunction(StreamFunction):
    def get_executor(self, *child_executors: VoidExecutor) -> VoidExecutor:
        child_executor = self._resolve_branching(*child_executors)
        func = self.func

        def wrapper(value, key, timestamp, headers):
            func(value)                          # <-- 忽略回傳值
            child_executor(value, key, timestamp, headers)  # <-- 傳原始 value
        return wrapper
```

### 4.4 TransformFunction — 轉換值、key、timestamp

```python
class TransformFunction(StreamFunction):
    def get_executor(self, *child_executors: VoidExecutor) -> VoidExecutor:
        child_executor = self._resolve_branching(*child_executors)

        if self.expand:
            expanded_func = cast(TransformExpandedCallback, self.func)
            def wrapper(value, key, timestamp, headers):
                result = expanded_func(value, key, timestamp, headers)
                for new_value, new_key, new_timestamp, new_headers in result:
                    child_executor(new_value, new_key, new_timestamp, new_headers)
        else:
            func = cast(TransformCallback, self.func)
            def wrapper(value, key, timestamp, headers):
                new_value, new_key, new_timestamp, new_headers = func(
                    value, key, timestamp, headers
                )
                child_executor(new_value, new_key, new_timestamp, new_headers)
        return wrapper
```

---

## 5. StreamingDataFrame — 用戶 API 如何轉成 Stream 節點

每個 SDF 操作都會在內部呼叫 `Stream.add_*()` 方法，建立新的 Stream 節點。

### 5.1 apply / filter / update

```python
# quixstreams/dataframe/dataframe.py (簡化)
class StreamingDataFrame:
    def __init__(self, ...):
        self._stream = Stream()  # 根 Stream 節點

    @property
    def stream(self) -> Stream:
        return self._stream

    def apply(self, func, *, expand=False, metadata=False) -> "StreamingDataFrame":
        stream = self.stream.add_apply(func, expand=expand, metadata=metadata)
        return self.__dataframe_clone__(stream=stream)

    def filter(self, func, *, metadata=False) -> "StreamingDataFrame":
        stream = self.stream.add_filter(func, metadata=metadata)
        return self.__dataframe_clone__(stream=stream)

    def update(self, func, *, metadata=False) -> "StreamingDataFrame":
        stream = self.stream.add_update(func, metadata=metadata)
        return self.__dataframe_clone__(stream=stream)
```

每次操作都 **建立新的 Stream 節點**，並 clone 出新的 SDF，形成鏈式呼叫。

### 5.2 sink — 終端節點

```python
def sink(self, sink: BaseSink):
    self._processing_context.sink_manager.register(sink)

    def _sink_callback(value, key, timestamp, headers):
        ctx = message_context()
        sink.add(
            value=value, key=key, timestamp=timestamp, headers=headers,
            partition=ctx.partition, topic=ctx.topic, offset=ctx.offset,
        )

    self.apply(_sink_callback, metadata=True)
    # sink() 是 terminal operation，不回傳新 SDF
```

### 5.3 group_by — 建立 repartition 節點

```python
def group_by(self, key, name=None, ...):
    # ...
    repartition_config = self._topic_manager.derive_topic_config(self._topics)

    if repartition_config.num_partitions == 1:
        return self._single_partition_groupby(operation, key)

    groupby_topic = self._topic_manager.repartition_topic(...)
    self.to_topic(topic=groupby_topic, key=self._groupby_key(key))
    self.filter(lambda _: False)  # 過濾掉原始 SDF 的輸出

    groupby_sdf = self.__dataframe_clone__(groupby_topic)  # 新的 SDF
    self._registry.register_groupby(source_sdf=self, new_sdf=groupby_sdf)
    return groupby_sdf
```

### 5.4 concat — 合併兩個 Stream

```python
def concat(self, other: "StreamingDataFrame") -> "StreamingDataFrame":
    merged_stream = self.stream.merge(other.stream)  # Stream.merge()
    # ...
    return self.__dataframe_clone__(stream=merged_stream)
```

---

## 6. DataFrameRegistry — 多 topic 管理

**`quixstreams/dataframe/registry.py` (完整源碼)**

```python
class DataFrameRegistry:
    def __init__(self) -> None:
        self._registry: dict[str, Stream] = {}     # topic_name → root Stream
        self._topics: list[Topic] = []
        self._repartition_origins: set[str] = set()
        self._topics_to_stream_ids: dict[str, set[str]] = {}
        self._stream_ids_to_topics: dict[str, set[str]] = {}
        self._requires_time_alignment = False

    def register_root(self, dataframe: "StreamingDataFrame"):
        topics = dataframe.topics
        topic = topics[0]
        if topic.name in self._registry:
            raise StreamingDataFrameDuplicate(...)
        self._topics.append(topic)
        self._registry[topic.name] = dataframe.stream   # <-- 記錄 root Stream

    def register_groupby(self, source_sdf, new_sdf, register_new_root=True):
        self._repartition_origins.add(new_sdf.stream_id)
        if register_new_root:
            self.register_root(new_sdf)

    def compose_all(
        self, sink: Optional[VoidExecutor] = None
    ) -> dict[str, VoidExecutor]:
        """
        Composes all Streams and returns {topic_name: executor}.
        """
        executors = {}
        for topic, root_stream in self._registry.items():
            root_executors = root_stream.compose(sink=sink)
            executors[topic] = root_executors[root_stream]
        return executors
```

---

## 7. compose() — DAG 編譯為可執行 closure

`Stream.compose()` 是把 DAG 編譯為可執行函數的核心：

```
DAG:
  root(identity) → filter(is_valid) → apply(transform) → update(log)
                                                │
                                                └→ apply(to_json) → sink(S3)

compose() 步驟：
  1. full_tree() — 拓撲排序所有節點
  2. reversed() — 從葉子開始往根遍歷
  3. 每個節點：
     - 如果有 children → 取 children 的 executor 作為下游
     - 如果沒有 children → 用 sink 作為下游
     - func.get_executor(*child_executors) → 返回包含下游呼叫的 closure
  4. 最終 root 節點的 executor 就是完整 pipeline 的入口

結果：一個 closure，呼叫 executor(value, key, timestamp, headers)
      就會依序執行所有節點的函數
```

---

## 8. 如何提取 DAG 給前端顯示

Quix Streams **沒有內建** DAG 視覺化功能，但 `Stream` 類的數據結構天然就是一個 DAG，可以輕鬆提取。

### 8.1 方法：遍歷 Stream 樹

```python
def extract_dag(registry: DataFrameRegistry) -> dict:
    """
    從 DataFrameRegistry 提取 DAG 結構。
    返回 {nodes: [...], edges: [...]} 給前端渲染。
    """
    nodes = []
    edges = []
    visited = set()

    for topic_name, root_stream in registry._registry.items():
        # full_tree() 返回拓撲排序的所有節點
        for stream in root_stream.full_tree():
            node_id = id(stream)
            if node_id in visited:
                continue
            visited.add(node_id)

            # 提取節點資訊
            func = stream.func
            node_type = func.__class__.__name__  # ApplyFunction, FilterFunction, etc.
            func_name = func.func.__qualname__   # 函數名稱

            nodes.append({
                "id": node_id,
                "type": node_type,
                "label": func_name,
                "is_root": len(stream.parents) == 0,
                "is_leaf": len(stream.children) == 0,
                "is_branched": stream.is_branched(),
                "is_merged": stream.is_merged(),
                "expand": func.expand,
            })

            # 提取邊（parent → child）
            for child in stream.children:
                edges.append({
                    "from": node_id,
                    "to": id(child),
                })

    return {"nodes": nodes, "edges": edges}
```

### 8.2 使用 Stream.__repr__() 快速查看

每個 Stream 節點都有 `__repr__`：

```python
def __repr__(self) -> str:
    func_repr = f"<{self.func.__class__.__name__}: {self.func.func.__qualname__}>"
    return (
        f"<{self.__class__.__name__} "
        f"(parents={len(self.parents)} children={len(self.children)}): "
        f"{func_repr}>"
    )
```

例如：`<Stream (parents=1 children=2): <FilterFunction: <lambda>>>`

### 8.3 完整範例：提取為 JSON 給前端

```python
import json
from quixstreams import Application

app = Application(broker_address="localhost:9092")
topic = app.topic("input")

sdf = app.dataframe(topic)
sdf = sdf.filter(lambda v: v["status"] == "active")
sdf = sdf.apply(lambda v: {**v, "processed": True})

# 分支 1: 寫到 DB
sdf.sink(PostgresSink(...))

# 分支 2: 寫到 S3
sdf.apply(lambda v: json.dumps(v)).sink(S3Sink(...))


# ---- 提取 DAG ----
def stream_to_dag(stream, visited=None, nodes=None, edges=None):
    if visited is None:
        visited, nodes, edges = set(), [], []

    node_id = id(stream)
    if node_id in visited:
        return nodes, edges
    visited.add(node_id)

    func = stream.func
    nodes.append({
        "id": str(node_id),
        "type": type(func).__name__.replace("Function", "").replace("WithMetadata", ""),
        "name": getattr(func.func, '__qualname__', str(func.func)),
        "parents": len(stream.parents),
        "children": len(stream.children),
    })

    for child in stream.children:
        edges.append({"from": str(node_id), "to": str(id(child))})
        stream_to_dag(child, visited, nodes, edges)

    for parent in stream.parents:
        stream_to_dag(parent, visited, nodes, edges)

    return nodes, edges

# 從 registry 取出 root stream
root_stream = app._dataframe_registry._registry["input"]
nodes, edges = stream_to_dag(root_stream)

dag = {"nodes": nodes, "edges": edges}
print(json.dumps(dag, indent=2))
```

### 8.4 輸出範例（給前端渲染）

```json
{
  "nodes": [
    {"id": "140234567890", "type": "Apply", "name": "<lambda>", "parents": 0, "children": 1},
    {"id": "140234567891", "type": "Filter", "name": "<lambda>", "parents": 1, "children": 1},
    {"id": "140234567892", "type": "Apply", "name": "<lambda>", "parents": 1, "children": 2},
    {"id": "140234567893", "type": "Apply", "name": "_sink_callback", "parents": 1, "children": 0},
    {"id": "140234567894", "type": "Apply", "name": "<lambda>", "parents": 1, "children": 1},
    {"id": "140234567895", "type": "Apply", "name": "_sink_callback", "parents": 1, "children": 0}
  ],
  "edges": [
    {"from": "140234567890", "to": "140234567891"},
    {"from": "140234567891", "to": "140234567892"},
    {"from": "140234567892", "to": "140234567893"},
    {"from": "140234567892", "to": "140234567894"},
    {"from": "140234567894", "to": "140234567895"}
  ]
}
```

前端可以用 [React Flow](https://reactflow.dev/) 或 [D3.js](https://d3js.org/) 渲染這個 JSON。

### 8.5 關鍵觀察

| Stream 特徵 | DAG 意義 | 代碼判斷 |
|-------------|---------|---------|
| `parents == []` | 根節點（Source topic） | `not stream.parents` |
| `children == []` | 葉子節點（Sink 或 terminal） | `not stream.children` |
| `len(children) > 1` | 分支點 | `stream.is_branched()` |
| `len(parents) > 1` | 合併點（concat） | `stream.is_merged()` |
| `func` 是 `_sink_callback` | Sink 節點 | 檢查 func 名稱 |
| repartition topic | group_by 節點 | 在 registry 中檢查 `_repartition_origins` |

### 8.6 group_by 在 DAG 中的表現

```
Source Topic A
     │
     ▼
  filter(is_valid)
     │
     ▼
  to_topic(repartition__groupby__customer_id)  ← 產出到 Kafka
     │
     ▼
  filter(lambda _: False)  ← 阻斷原始流

                    ┌─────────────────┐
                    │ Kafka Topic:    │
                    │ repartition__   │
                    │ groupby__       │
                    │ customer_id     │
                    └────────┬────────┘
                             │
                             ▼
                   New SDF (new root)
                             │
                             ▼
                   apply(stateful_func)
                             │
                             ▼
                         sink(DB)
```

在 `DataFrameRegistry` 中，repartition SDF 被當作新的 root 註冊，所以在 DAG 中它是一個**獨立的子圖**，通過 Kafka topic 名稱與原始 SDF 關聯。
