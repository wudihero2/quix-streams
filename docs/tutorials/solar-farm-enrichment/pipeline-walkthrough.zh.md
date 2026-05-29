# Solar Farm Tutorial 內部走訪

以 `tutorial_app.py` 為主軸,沿著
**`Application` → `Source` → `app.dataframe()` → 各種算子 → `Sink`** 的執行路線,
逐段對照 `quixstreams` 套件的原始碼,並用示意圖串起整條 pipeline。

> 程式碼引用皆使用 `檔案:行號` 的格式。
> 路徑相對於 repo 根目錄 `/Users/stanhsu/projects/quix-streams`。

---

## 0. 整體鳥瞰圖

```
                                   ┌──────────────────────────────────┐
                                   │   Application (主程序)            │
                                   │   - InternalConsumer              │
                                   │   - InternalProducer              │
                                   │   - StateStoreManager (RocksDB)   │
                                   │   - ProcessingContext / Checkpoint│
                                   └──────────▲────────────▲──────────┘
                                              │ poll       │ produce
                                              │            │
   ┌──────────────────────┐                   │            │           ┌──────────────────────┐
   │ Source 子程序 1       │ ──produce──►  Kafka topic ──poll──►       │ StreamingDataFrame    │
   │ BatteryTelemetryGen  │               source__telemetry            │ telemetry_sdf         │
   └──────────────────────┘                                            └──────────┬───────────┘
                                                                                  │ join_asof
   ┌──────────────────────┐                                                       │ (state 在主程序)
   │ Source 子程序 2       │ ──produce──►  Kafka topic ──poll──►       ┌──────────▼───────────┐
   │ WeatherForecastGen   │               source__forecast             │ StreamingDataFrame    │
   └──────────────────────┘                                            │ forecast_sdf          │
                                                                       └──────────┬───────────┘
                                                                                  │
                                                                       ┌──────────▼───────────┐
                                                                       │ enriched_sdf          │
                                                                       │ apply / print_table   │
                                                                       │ to_topic              │
                                                                       └──────────┬───────────┘
                                                                                  │ produce
                                                                                  ▼
                                                                       Kafka topic
                                                                       telemetry-with-forecast
```

幾個要記住的事實:

- **Source 跑在獨立的子程序**(`multiprocessing.Process`),
  透過 `InternalProducer` 把資料寫進一個「source topic」。
- 主程序的 `Application` 不直接讀 Source 物件,
  它讀的是 Kafka 上那個 source topic,跟讀其他 topic 沒兩樣。
- `StreamingDataFrame`(以下簡稱 SDF)在 **建構期** 只是把算子組成 DAG;
  真正要等到 `app.run()` 開始 poll Kafka 才會逐筆執行。

---

## 1. `Application(...)` — 整個應用的容器

`tutorial_app.py:110-116`:

```python
app = Application(
    broker_address=os.getenv("BROKER_ADDRESS", "localhost:9092"),
    consumer_group="solar-farm",
    auto_offset_reset="earliest",
    use_changelog_topics=False,
)
```

對應原始碼:`quixstreams/app.py:126-401`。

### 1.1 `__init__` 在做什麼

`app.py:247-401` 大致可以分幾段:

1. **設定 broker / consumer group**(`app.py:258-315`)
   - 從參數或環境變數補齊 `broker_address`、`consumer_group`。
   - 如果走 Quix Cloud,改建 `QuixTopicManager`、抓 SDK token 配置。
2. **產生 `ApplicationConfig`**(`app.py:322-349`)
   - 是個 `frozen=True` 的 Pydantic settings(`app.py:1156-1224`),
     把 producer/consumer 的 extra config 補上 idempotence、fetch backoff 等預設值。
3. **建立內部 Kafka 客戶端**(`app.py:360-364`)
   - `InternalConsumer`:`auto_commit_enable=False`,offset 由 `ProcessingContext` 用 checkpoint 控制。
   - `InternalProducer`:預設啟用 idempotence。
4. **狀態 / 子模組**(`app.py:368-401`)
   - `TopicManager`:管理 input/output/changelog/repartition topic。
   - `StateStoreManager`:管理 RocksDB(以及 changelog 復原)。
     範例傳了 `use_changelog_topics=False`,因此 `RecoveryManager` 與 changelog producer **不會**建立(`app.py:372-378`)。
   - `SourceManager`、`SinkManager`、`DataFrameRegistry`、`ProcessingContext`、`RunTracker`。

示意圖:

```
Application.__init__
 ├── ApplicationConfig (frozen)
 ├── InternalConsumer  ◄── 之後 subscribe + poll
 ├── InternalProducer  ◄── 之後 produce + commit
 ├── TopicManager
 ├── StateStoreManager (RocksDB)
 ├── SourceManager     ◄── 子程序生命週期
 ├── SinkManager
 ├── DataFrameRegistry ◄── 多個 SDF 的根節點
 └── ProcessingContext ◄── checkpoint / sink / state 連接點
```

### 1.2 `ApplicationConfig`

- 定義在 `app.py:1156-1224`,
  用 `SettingsConfigDict(frozen=True, revalidate_instances="always")`,
  代表「設定一旦建好就不可變」。
- `flush_timeout`(`app.py:1213-1220`)= `max.poll.interval.ms / 1000`,
  是 producer flush 的安全上限。

---

## 2. `Source` — 在子程序中產生資料

範例裡的兩個 Source 都繼承自 `quixstreams.sources.Source`(`tutorial_app.py:17, 70`)。

```python
class BatteryTelemetryGenerator(Source):
    def generate_telemetry_event(self, panel_id): ...
    def run(self):
        while self.running:
            for panel_id in PANELS_IDS:
                event = self.generate_telemetry_event(panel_id=panel_id)
                message = self.serialize(
                    key=LOCATION_ID,
                    value=event,
                    timestamp_ms=int(event["timestamp"] * 1000),
                )
                self.produce(
                    key=message.key, value=message.value, timestamp=message.timestamp
                )
            time.sleep(1)
```

對應原始碼:`quixstreams/sources/base/source.py`。

### 2.1 介面層次

```
BaseSource (ABC)               ← sources/base/source.py:28-169
   ↑
Source                         ← sources/base/source.py:172-378
   ↑
BatteryTelemetryGenerator      ← tutorial_app.py:70-106
WeatherForecastGenerator       ← tutorial_app.py:17-67
```

`BaseSource` 規範三個必須實作的方法(`source.py:144-169`):

- `start()`:子程序的入口。
- `stop()`:graceful shutdown。
- `default_topic()`:當使用者沒指定 topic 時要回傳一個預設 `Topic`。

`Source` 把這些都實作好,改成只要使用者覆寫 `run()`(`source.py:294-301`):

| Source 方法 | 用途 | 程式碼位置 |
|---|---|---|
| `start()` | 標記 running、呼叫 `_init_client()`、跑 `self.run()`,最後 `cleanup()` | `source.py:278-292` |
| `stop()`  | 把 `self._running` 設為 False,`run()` 自己負責跳離迴圈 | `source.py:270-276` |
| `serialize(...)` | 呼叫 producer topic 上的 serializer,輸出 `KafkaMessage` | `source.py:303-317` |
| `produce(...)` | 透過 `InternalProducer.produce()` 把訊息寫入 source topic | `source.py:319-342` |
| `flush(...)` | 等待所有訊息送出;送不出去就 raise `CheckpointProducerTimeout` | `source.py:344-360` |
| `default_topic()` | 預設用 `name` 當 topic 名稱、JSON 序列化 | `source.py:362-375` |

### 2.2 子程序怎麼啟動

當 `app.dataframe(source=...)` 被呼叫(`app.py:564-565`),
會接著呼叫 `Application.add_source()`:

`app.py:748-780`:

```python
def add_source(self, source: BaseSource, topic: Optional[Topic] = None) -> Topic:
    if not topic:
        default_topic = source.default_topic()
        default_topic = default_topic.__clone__(name=f"source__{default_topic.name}")
        topic = self._topic_manager.register(default_topic)

    self._source_manager.register(
        source,
        topic,
        self._get_internal_producer(transactional=False),
        self._get_internal_consumer(extra_config_overrides=...),
        self._get_topic_manager(),
        broker_availability_timeout=self._broker_availability_timeout,
    )
    return topic
```

重點:

- 若沒有提供 `topic`,Application 會把 `source.default_topic()` 的名稱加上 `source__` 前綴
  (例如本例就會建出 `source__telemetry`、`source__forecast`)。
- 每個 source 都會拿到 **自己的** producer / consumer / topic manager,因為它要跑在另一個程序。

接著 `SourceManager.register()`(`sources/base/manager.py:263-291`)會建立 `SourceProcess` 物件:

```
SourceManager.register(source, topic, producer, consumer, topic_manager)
        │
        └── SourceProcess(SpawnProcess)
                │   __init__: 把 source / topic / producer / consumer 存起來
                │             用 multiprocessing.Pipe 建立子→父的 error channel
                └── start():  fork/spawn 子程序,執行 SourceProcess.run()
```

`SourceProcess.run()`(`sources/base/manager.py:83-124`)是子程序的入口:

```python
def run(self) -> None:
    self._started = True
    self._setup_signal_handlers()
    configure_logging(...)
    configuration = {}
    if isinstance(self.source, StatefulSource):
        configuration["store_partition"] = self._recover_state(self.source)
    self.source.configure(topic=self.topic, producer=self._producer, **configuration)
    try:
        self.source.start()
    except BaseException as err:
        self._report_exception(err)
        return
```

也就是說:**主程序負責建立 producer / consumer 物件,然後 fork/spawn 子程序;
子程序拿到這些物件後才呼叫 `configure()` 與 `start()`**。
主程序之後跟 source 之間只剩兩條通道:Kafka topic(資料)、`multiprocessing.Pipe`(錯誤)。

### 2.3 範例的 `run()` 細節

`BatteryTelemetryGenerator.run()`(`tutorial_app.py:90-106`):

```
while self.running:
    for panel_id in PANELS_IDS:
        event   = self.generate_telemetry_event(panel_id=panel_id)
        message = self.serialize(key=LOCATION_ID, value=event,
                                 timestamp_ms=int(event["timestamp"] * 1000))
        self.produce(key=message.key, value=message.value, timestamp=message.timestamp)
    time.sleep(1)
```

對應流程:

```
Source.run() ──► self.serialize() ──► topic.serialize()
            │       (KafkaMessage)
            └──► self.produce() ──► InternalProducer.produce()
                                            │
                                            └──► Kafka 「source__telemetry」topic
```

`WeatherForecastGenerator.run()`(`tutorial_app.py:35-67`)行為相同,
只是把資料寫到 `source__forecast`,且每 30 秒才發一次。

**重要**:兩個 Source 都使用 `key=LOCATION_ID = "location-1"`(`tutorial_app.py:9, 52, 95`)。
這是後面 `join_asof` 能對到資料的關鍵——
join 是 per-key 的查詢,沒有相同 key 就永遠對不到。

---

## 3. `app.topic()` 與 `app.dataframe()`

### 3.1 `app.topic()` — 註冊輸出 topic

`tutorial_app.py:117`:

```python
output_topic = app.topic(name="telemetry-with-forecast")
```

對應 `app.py:444-522`:

- 預設 value 是 JSON 序列化、key 是 bytes。
- 實際上委派給 `TopicManager.topic()`,把 `Topic` 物件登記起來。
- 程式啟動時(`auto_create_topics=True` 預設值),
  缺少的 Kafka topic 會自動補建。

### 3.2 `app.dataframe()` — 把 Source 串成 SDF

`tutorial_app.py:119-120`:

```python
telemetry_sdf = app.dataframe(source=BatteryTelemetryGenerator(name="telemetry"))
forecast_sdf  = app.dataframe(source=WeatherForecastGenerator(name="forecast"))
```

對應 `app.py:524-578`:

```python
def dataframe(self, topic=None, source=None) -> StreamingDataFrame:
    if source is not None:
        topic = self.add_source(source, topic)   # 註冊子程序、回傳 source topic
    if topic is None:
        raise ValueError("one of `source` or `topic` is required")
    sdf = StreamingDataFrame(
        topic,
        topic_manager=self._topic_manager,
        processing_context=self._processing_context,
        registry=self._dataframe_registry,
    )
    self._dataframe_registry.register_root(sdf)
    return sdf
```

實際上呼叫 `app.dataframe(source=...)` 後發生的事是:

```
app.dataframe(source=X)
   │
   ├── app.add_source(X, None)
   │       ├── X.default_topic()                ──► Topic("telemetry", json/json)
   │       ├── __clone__(name="source__telemetry")
   │       ├── TopicManager.register(topic)
   │       └── SourceManager.register(X, topic, ...) ──► SourceProcess(尚未啟動)
   │       回傳: topic
   │
   ├── StreamingDataFrame(topic=topic, registry, ctx, topic_manager)
   │
   └── DataFrameRegistry.register_root(sdf)   ──► registry 把 topic 名稱 → root Stream 記下
```

`SDF.__init__`(`dataframe/dataframe.py:141-168`)做兩件主要的事:

1. 從 topics 排序去重,建立 `_topics`、`_stream_id`
   (`stream_id` 在沒有 join/group_by 時等於 topic 名稱組合)。
2. 建一個空的 `Stream()`,後面所有算子都附加在這個 stream 上。

`DataFrameRegistry.register_root()`(`dataframe/registry.py:46-70`):

- 一個 topic 只能有一個 root SDF。
- registry 是日後 `Application._run_dataframe()` 用來 `compose_all()` 的入口。

---

## 4. 算子(operators)— 在 SDF 上組 DAG

關鍵概念:**SDF 上的每個算子方法只是在 `Stream` 物件上追加節點,並非立即執行**。
`Stream` 才是執行 DAG 的本體,定義在 `quixstreams/core/stream/stream.py:43-`。

### 4.1 `Stream` 是什麼

`stream.py:43-93`:

```
class Stream:
    def __init__(self, func: StreamFunction | None = None, parents: list[Stream] | None = None):
        self.func     = func or ApplyFunction(lambda v: v)
        self.parents  = parents or []
        self.children = []
        self.pruned   = False
```

Stream 支援 4 種函式型別(`stream.py:57-75`):

| 型別 | 行為 |
|---|---|
| **Apply** | 回傳新值,往下游傳 |
| **Update** | 原地修改,下游拿到的還是原值 |
| **Filter** | 回傳 truthy 才繼續往下游 |
| **Transform** | 同時改 value / key / timestamp / headers |

新增節點時(`stream.py:555-558`),會 fork 出新節點並把自己加到 children:

```
self.children.append(new_node)
return new_node
```

`compose()`(`stream.py:404-461`)在 `app.run()` 時被呼叫,
**從葉節點往回**把每個 function 串成一個 closure,
最終回傳「root → executor」的字典。

### 4.2 `join_asof()` — 整個 pipeline 最複雜的算子

`tutorial_app.py:131-136`:

```python
enriched_sdf = telemetry_sdf.join_asof(
    forecast_sdf,
    how="inner",
    on_merge=merge_events,
    grace_ms=timedelta(days=7),
)
```

對應 `dataframe/dataframe.py:1739-1813`:

```python
def join_asof(self, right, how="inner", on_merge="raise",
              grace_ms=timedelta(days=7), name=None):
    return AsOfJoin(
        how=how, on_merge=on_merge, grace_ms=grace_ms, store_name=name
    ).join(self, right)
```

#### `AsOfJoin._prepare_join()` 的魔法

`dataframe/joins/join_asof.py:30-51`:

```python
def _prepare_join(self, left, right):
    self._register_store(right, keep_duplicates=False)  # 替 right 註冊一個 timestamped store

    tx = self._get_transaction
    is_inner_join = self._how == "inner"
    merger = self._merger

    def left_func(value, key, timestamp, headers):
        if right_value := tx(right).get_latest(timestamp=timestamp, prefix=key):
            return merger(value, right_value)
        return DISCARDED if is_inner_join else merger(value, None)

    def right_func(value, key, timestamp, headers):
        tx(right).set_for_timestamp(timestamp=timestamp, value=value, prefix=key)

    right = right.update(right_func, metadata=True).filter(block_all)
    left  = left.apply(left_func, metadata=True).filter(block_discarded)
    return left.concat(right)
```

讀法:

```
                            ┌─────────────────────────────┐
                            │  Timestamped State Store    │
                            │  store_name="join"          │
                            │  key = msg key (location-1) │
                            │  value 依 timestamp 排序     │
                            └──────────▲────────────┬─────┘
                                       │            │
                              (1) set_for_timestamp │
                                       │            │ (2) get_latest(ts ≤ telemetry.ts)
   forecast_sdf  ──update(right_func)─►│            │
                 .filter(block_all)    │            │
                 (永遠不再往下游)        │            │
                                       │            ▼
   telemetry_sdf ──apply(left_func) ──┴── merger(value, right_value) → 新 dict
                 .filter(block_discarded)
                                       ▼
                                  enriched_sdf (= left.concat(right))
```

對應的副作用:

- `right.update(right_func)` — **每筆 forecast 進來只更新 store,不發送到下游**。
- `filter(block_all)` — `block_all = lambda value: False`(`join_asof.py:14`),
  所以 forecast 走完 update 後一律被丟掉。
- `left.apply(left_func)` — 每筆 telemetry 查 store 拿到 ≤ 自身 timestamp 的最新 forecast。
  - **inner**:沒對到就回傳 `DISCARDED` 哨兵。
  - **left**:沒對到就 `merger(value, None)`。
- `filter(block_discarded)` — `block_discarded = lambda v: v is not DISCARDED`(`join_asof.py:15`),把哨兵濾掉。
- `left.concat(right)` — 把兩個 SDF 合併成一個 stream,共用 stream id。
  - 對應 `dataframe.py:1703-1737` 與 `stream.merge()`。
  - 同時呼叫 `registry.require_time_alignment()`(`registry.py:158-166`),
    告訴 `InternalConsumer` 之後 poll 必須做 timestamp 對齊。

#### 前置檢查 `Join.join()`

`dataframe/joins/base.py:57-95`:

```python
def join(self, left, right):
    self._validate_dataframes(left, right)
    return self._prepare_join(left, right)

def _validate_dataframes(self, left, right):
    if left.stream_id == right.stream_id:
        raise ValueError("Joining dataframes originating from the same topic is not yet supported.")
    TopicManager.ensure_topics_copartitioned(*left.topics, *right.topics)

def _register_store(self, sdf, keep_duplicates):
    sdf.processing_context.state_manager.register_timestamped_store(
        stream_id=sdf.stream_id,
        store_name=self._store_name,
        grace_ms=self._grace_ms,
        keep_duplicates=keep_duplicates,
        changelog_config=TopicManager.derive_topic_config(sdf.topics),
    )
```

> 兩個 source topic 都只有 1 個 partition(`tutorial.md` 開頭也提到),
> `ensure_topics_copartitioned` 就過了。
> `grace_ms=timedelta(days=7)` 會被 `ensure_milliseconds()` 轉成毫秒,
> store 在主程序的 RocksDB 內保留 7 天的 forecast。

#### `merge_events` 的角色

`tutorial_app.py:122-127`:

```python
def merge_events(telemetry: dict, forecast: dict) -> dict:
    forecast = {"forecast." + k: v for k, v in forecast.items()}
    return {**telemetry, **forecast}
```

`Join.__init__`(`base.py:31-55`)會根據 `on_merge` 型別決定 `self._merger`:

- `"raise"` / `"keep-left"` / `"keep-right"` 是內建。
- 傳函式進來就直接用 `merge_events` 作為 merger。

範例之所以要寫自訂 merger,是因為兩邊都有 `timestamp` 欄位,
否則 `raise_merger` 會在欄位重疊時拋例外。

### 4.3 `sdf[column]` 與 `sdf[column] = ...`

`tutorial_app.py:139-142`:

```python
enriched_sdf["timestamp"] = enriched_sdf["timestamp"].apply(timestamp_to_str)
enriched_sdf["forecast.timestamp"] = enriched_sdf["forecast.timestamp"].apply(
    timestamp_to_str
)
```

#### `__getitem__("timestamp")` → `StreamingSeries`

`dataframe/dataframe.py:2114-2142`:

```python
elif isinstance(item, str):
    return StreamingSeries(name=item, sdf_id=id(self))
```

`StreamingSeries.__init__`(`dataframe/series.py:109-122`)在 stream 上掛一個取欄位的 `ApplyFunction`:

```python
self._stream = Stream(func=ApplyFunction(lambda v: _getitem(v, name)))
```

`.apply(timestamp_to_str)`(`series.py:152-184`)再加一個 Apply 節點。
此時 **series 內部維持自己的 stream**,還沒有合併進 SDF。

#### `__setitem__` — 把 series 的結果寫回欄位

`dataframe.py:2074-2104`:

```python
def __setitem__(self, item_key, item):
    ...
    elif isinstance(item, StreamingSeries):
        if id(self) != item.sdf_id:
            raise InvalidOperation(...)
        series_composed = item.compose_returning()
        self._add_update(
            lambda value, key, timestamp, headers: operator.setitem(
                value, item_key, series_composed(value, key, timestamp, headers)[0]
            ),
            metadata=True,
        )
```

`compose_returning()`(`stream.py:463-499`)把整段 series stream 壓成一個 closure,
然後用 `_add_update` 在 SDF 主 stream 上加一個 update 節點,
每進來一筆 message 就「把計算結果原地寫進 value[item_key]」。

```
enriched_sdf  ─── add_update(write timestamp str) ─── ...
                          │
                          └── 內部呼叫 ── series_composed(value, key, ts, headers)
                                          ↓
                                          ApplyFunction("timestamp" getter)
                                          ApplyFunction(timestamp_to_str)
```

### 4.4 `print_table(live=False)` — 觀測算子

`tutorial_app.py:145`:

```python
enriched_sdf.print_table(live=False)
```

對應 `dataframe.py:897-...`。它的特性:

- 是 **in-place** 算子,內部把 printer 註冊到 `ProcessingContext.printer`
  (主程序的單一收集器,看 `app.py:918, 951`)。
- 因為 `live=False`,當 stdout 不是 tty 時會等表格滿(`size=5`)或 timeout 5 秒才一次印出。
- 不影響資料,純粹觀測。

### 4.5 `to_topic(output_topic)` — Sink:寫回 Kafka

`tutorial_app.py:148`:

```python
enriched_sdf.to_topic(output_topic)
```

對應 `dataframe.py:675-755`:

```python
def to_topic(self, topic, key=None):
    if isinstance(topic, Topic):
        topic_callback = lambda value, orig_key, timestamp, headers: topic
    else:
        topic_callback = topic
    return self._add_update(
        lambda value, orig_key, timestamp, headers: self._produce(
            topic=topic_callback(value, orig_key, timestamp, headers),
            value=value,
            key=orig_key if key is None else key(value),
            timestamp=timestamp,
            headers=headers,
        ),
        metadata=True,
    )
```

底層 `_produce`(`dataframe.py:1987-1999`)會包成 `Row` 並呼叫
`InternalProducer.produce_row()`,實際把訊息送進 Kafka。

> 注意 `to_topic` 也是 `_add_update`,因此**不需要再賦值給 `enriched_sdf`**。

至此整個 SDF 的「邏輯 DAG」如下:

```
[telemetry source topic]                       [forecast source topic]
        │ root Stream                                  │ root Stream
        │ (telemetry_sdf)                              │ (forecast_sdf)
        │                                              │
        │ apply(left_func, metadata=True)              │ update(right_func, metadata=True)
        │ filter(block_discarded)                      │ filter(block_all)
        └────────────────► concat ◄────────────────────┘
                            │ stream.merge()
                            ▼
                     enriched_sdf root
                            │
                            │ update(set value["timestamp"] = str(ts))
                            │ update(set value["forecast.timestamp"] = str(ts))
                            │ update(print_table 收集)
                            │ update(produce 到 output_topic)
                            ▼
                          (sink)
```

---

## 5. `app.run()` — 真正開始跑

`tutorial_app.py:151`:

```python
app.run()
```

入口 `app.py:782-898`,核心可以拆成兩條路:

```
run()
 ├── 沒有 SDF (純 Source)            ──► _run_sources()  (app.py:957-974)
 └── 有 SDF (本範例)                  ──► _run_dataframe() (app.py:910-955)
```

### 5.1 啟動順序

`app.py:881-898`:

```python
exit_stack = contextlib.ExitStack()
exit_stack.enter_context(self._processing_context)
exit_stack.enter_context(self._state_manager)
exit_stack.enter_context(self._consumer)
exit_stack.enter_context(self._source_manager)
exit_stack.push(self._exception_handler)

with exit_stack:
    if self._dataframe_registry.consumer_topics:
        ...
        self._run_dataframe(sink=collector)
    else:
        self._run_sources()
```

注意 **`_source_manager` 的 `__enter__` 不直接啟動子程序**;
真正啟動點在 `_on_assign()` 內,等到 consumer 拿到 partition 之後才啟動,
這樣可以避免 source 比 consumer 先寫資料、且 `auto_offset_reset=latest` 時的丟資料問題
(`app.py:1040-1094`):

```python
def _on_assign(self, _, topic_partitions):
    ...
    self._source_manager.start_sources()
    self._consumer.assign(topic_partitions)
    self._consumer.pause(changelog_tps)
    ...
```

### 5.2 主迴圈 `_run_dataframe()`

`app.py:910-955`:

```python
consumer.subscribe(topics=registry.consumer_topics + changelog_topics,
                   on_assign=self._on_assign, on_revoke=..., on_lost=...)

dataframes_composed = self._dataframe_registry.compose_all(sink=sink)
processing_context.init_checkpoint()
run_tracker.set_as_running()

while run_tracker.running:
    if state_manager.recovery_required:
        state_manager.do_recovery()
    else:
        process_message(dataframes_composed)
        processing_context.commit_checkpoint()
        consumer.resume_backpressured()
        source_manager.raise_for_error()
        ...
```

幾個值得注意的點:

- `compose_all`(`registry.py:108-124`)會把每個 root stream 串成最終的 closure。
  每個 topic 都對應一個 executor。
- `process_message`(`app.py:986-1038`)做的事:
  1. `producer.poll(...)` — 推進 producer callback。
  2. `consumer.poll_row(buffered=registry.requires_time_alignment)`
     — 因為前面 `join_asof` 觸發 `require_time_alignment()`,這裡 `buffered=True`,
     代表 consumer 會在 buffer 中做 timestamp 對齊後才往下發。
  3. 對每個 row 設好 message context,呼叫 `dataframe_composed[topic_name]`。
  4. 把 offset 存進 `ProcessingContext`(尚未真正 commit 到 broker)。
- `commit_checkpoint()` 才會真的把 offset / state 寫回。

### 5.3 子程序與主程序的協作示意

```
       Source 子程序                           主程序
─────────────────────────             ────────────────────────────────
 SourceProcess.run                    Application.run / _run_dataframe
   │ self.source.start()               while running:
   │   while running:                    process_message(...)
   │      generate event                   InternalConsumer.poll_row
   │      serialize()                       (buffered time alignment)
   │      InternalProducer.produce ───►  ► subscribe 的 source__ topic
   │                                      │
   │                                      ▼
   │                                  dataframe_composed[topic](value,...)
   │                                      │  順著 Stream DAG 走
   │                                      ▼
   │                                  apply / update / filter / to_topic
   │                                      │
   │                                      ▼
   │                                  InternalProducer.produce
   │                                  → "telemetry-with-forecast"
```

### 5.4 終止流程

- SIGINT / SIGTERM → `self.stop()`(`app.py:580-601, 1140-1153`)。
- `RunTracker.stop()` 把 `running = False`,主迴圈跳出。
- `commit_checkpoint(force=True)` 把最後 offset/state 寫回。
- `exit_stack` 觸發各個 `__exit__`:
  - `SourceManager.__exit__` 會對所有子程序送 SIGTERM,
    超過 `shutdown_timeout` 就 SIGKILL(`sources/base/manager.py:230-250, 306-314`)。
  - Consumer 與 ProcessingContext 也會關閉。

---

## 6. 串成一張總圖

```
┌────────────────────────────────────────────────────────────────────────────┐
│                                  Kafka                                     │
│   ┌────────────────────────┐  ┌────────────────────────┐                  │
│   │ source__telemetry      │  │ source__forecast       │  output topic    │
│   │ (1 partition)          │  │ (1 partition)          │  telemetry-with- │
│   └──────────▲─────────────┘  └──────────▲─────────────┘  forecast        │
│              │ produce                   │ produce         ▲              │
└──────────────┼──────────────────────────┬┼─────────────────┼──────────────┘
               │                          ││                 │
   ┌───────────┴───────────┐  ┌───────────┴┴──────────┐      │
   │ Source 子程序 1        │  │ Source 子程序 2       │      │
   │ BatteryTelemetryGen   │  │ WeatherForecastGen    │      │ produce
   │ .run() 每秒 3 筆       │  │ .run() 每 30s 1 筆     │      │
   └───────────────────────┘  └───────────────────────┘      │
                                                              │
   ┌──────────────────────────────────────────────────────────┴───────────┐
   │ 主程序  Application                                                   │
   │                                                                       │
   │  InternalConsumer.subscribe([source__telemetry, source__forecast])   │
   │            │ poll_row (buffered=True, 因為 join 需要 time alignment)  │
   │            ▼                                                          │
   │  dataframe_composed[topic](value, key, ts, headers)                  │
   │            │                                                          │
   │            ▼                                                          │
   │  +----------------- telemetry root stream -----------------+         │
   │  | apply(left_func)  ──► store.get_latest(ts ≤ telemetry) |         │
   │  | filter(block_discarded)                                |         │
   │  +-----------------+--------------------------------------+         │
   │                    │                                                 │
   │  +-----------------▼----------- merged stream -------------+         │
   │  |   concat (forecast 那邊在 update 後 filter block_all)    |         │
   │  |   update(value["timestamp"] = str(ts))                  |         │
   │  |   update(value["forecast.timestamp"] = str(ts))         |         │
   │  |   update(print_table 收集)                              |         │
   │  |   update(to_topic → InternalProducer.produce) ──────────┼─────►   │
   │  +---------------------------------------------------------+         │
   │                                                                       │
   │  ProcessingContext.commit_checkpoint() (定期 / 訊息數量觸發)          │
   │     ├── State store (RocksDB)  - 儲存最新 forecast (key=location-1)  │
   │     └── Offset commit          - 回寫到 broker                       │
   └──────────────────────────────────────────────────────────────────────┘
```

---

## 7. 連結對照表

| `tutorial_app.py` 行 | 行為 | 主要原始碼位置 |
|---|---|---|
| 110-116 | `Application(...)` 建構 | `quixstreams/app.py:126-401` |
| 117 | `app.topic(...)` 註冊輸出 topic | `app.py:444-522` |
| 119-120 | `app.dataframe(source=...)` | `app.py:524-578` + `app.py:748-780` + `sources/base/manager.py:253-291` |
| 131-136 | `telemetry_sdf.join_asof(forecast_sdf, ...)` | `dataframe/dataframe.py:1739-1813` → `dataframe/joins/join_asof.py` + `joins/base.py` |
| 139-142 | `sdf["x"] = sdf["x"].apply(f)` | `dataframe.py:2074-2104, 2114-2142` + `dataframe/series.py:109-184` + `core/stream/stream.py:463-499` |
| 145 | `enriched_sdf.print_table(live=False)` | `dataframe.py:897-` + `processing` printer |
| 148 | `enriched_sdf.to_topic(output_topic)` | `dataframe.py:675-755, 1987-1999` |
| 151 | `app.run()` | `app.py:782-1153` |
| Source 子類別 | 子程序執行模型 | `sources/base/source.py` + `sources/base/manager.py` |

---

## 8. 額外可挖的小細節(留作自學)

- **`StateStoreManager.register_timestamped_store`** 為 join 註冊一個帶時間索引的 RocksDB column family,
  搭配 `TimestampedPartitionTransaction` 提供 `get_latest`、`set_for_timestamp`。
- **`InternalConsumer.poll_row(buffered=True)`** 會啟用 partition buffering,
  把多個 input topic 的訊息以 timestamp 升序送進 pipeline,
  讓 `join_asof` 在 forecast/telemetry 不同來源時也能正確對齊。
- **Checkpoint** 是把「未 commit 的 offset + state 變更 + producer flush」一起做的最小單位,
  由 `ApplicationConfig.commit_interval / commit_every` 控制節奏。

---

## 9. 深入:`Stream` 怎麼從節點被串成一個 closure

這節要回答的問題:
**為什麼我們可以一直 `.apply().filter().update()` 串下去,執行的時候資料卻能順順往下流?**

關鍵在兩個東西:

1. 每個算子在 SDF 上其實只是在 `Stream` 樹上「掛一個節點」。
2. 真正執行時(`app.run()` 內),`Stream.compose()` 會**從葉子往回**把每一個節點
   包成一個 closure,讓每一層拿到「下游 closure」當回呼,
   形成一個遞迴呼叫鏈。

### 9.1 節點本身 — `StreamFunction.get_executor()`

每個算子在 `dataframe.py` 裡都是這個樣子(例:`.apply()`,`dataframe.py:286-304`):

```python
stream = self.stream.add_apply(func, expand=expand, metadata=metadata)
return self.__dataframe_clone__(stream=stream)
```

而 `Stream.add_apply()`(`core/stream/stream.py:184-215`)的本體只是:

```python
apply_func = ApplyFunction(func, expand=expand)   # 把 callback 包進 StreamFunction
return self._add(apply_func)                       # 在 DAG 上掛一個新節點
```

`_add` 就更簡單(`stream.py:555-558`):

```python
def _add(self, func: StreamFunction) -> "Stream":
    new_node = self.__class__(func=func, parents=[self])
    self.children.append(new_node)
    return new_node
```

**所以「呼叫一個算子」的副作用只有兩個:在父節點的 `children` 加一筆、新建一個 `Stream` 節點。**
裡面什麼資料都還沒有流。

`StreamFunction` 是抽象基底(`core/stream/functions/base.py:11-65`):

```python
class StreamFunction(abc.ABC):
    expand: bool = False
    def __init__(self, func): self.func = func

    @abc.abstractmethod
    def get_executor(self, *child_executors): ...
```

每個具體型別(`ApplyFunction`、`UpdateFunction`、`FilterFunction`、`TransformFunction`)
都實作 `get_executor`,回傳一個 closure。
這個 closure **接受 `(value, key, timestamp, headers)`,然後內部呼叫 `child_executor(...)`**。

最容易讀的是 `ApplyFunction`(`functions/apply.py:39-69`):

```python
def get_executor(self, *child_executors: VoidExecutor) -> VoidExecutor:
    child_executor = self._resolve_branching(*child_executors)
    func = self.func

    def wrapper(value, key, timestamp, headers):
        result = func(value)                                  # 跑使用者的 callback
        child_executor(result, key, timestamp, headers)       # 把 result 往下游送
    return wrapper
```

對比 `UpdateFunction`(`functions/update.py:25-34`):

```python
def get_executor(self, *child_executors):
    child_executor = self._resolve_branching(*child_executors)
    func = self.func

    def wrapper(value, key, timestamp, headers):
        func(value)                                            # 跑 callback(原地改 value)
        child_executor(value, key, timestamp, headers)        # 注意:把原本的 value 往下傳
    return wrapper
```

`FilterFunction`(`functions/filter.py:22-36`):

```python
def get_executor(self, *child_executors):
    child_executor = self._resolve_branching(*child_executors)
    func = self.func

    def wrapper(value, key, timestamp, headers):
        if func(value):                                       # 條件成立才繼續呼叫下游
            child_executor(value, key, timestamp, headers)
    return wrapper
```

可以看到,每個 wrapper 的「資料流」都是同一個套路:

```
   wrapper(value, key, ts, headers)
       │
       │  根據型別決定要不要改 value、要不要往下發
       ▼
   child_executor(下一層的 value, key, ts, headers)
```

**所以「下一個函式」這件事,就是 wrapper 裡固定會呼叫的 `child_executor`。**
而 `child_executor` 從哪來?就是 `compose()` 從下游遞迴包好之後傳進來的。

### 9.2 `compose()` — 從葉子往回包

`stream.py:404-461`:

```python
def compose(self, ..., sink=None) -> dict["Stream", VoidExecutor]:
    sink = sink or self._default_sink                      # 沒指定 sink 就用 noop
    executors: dict["Stream", VoidExecutor] = {}

    for stream in reversed(self.full_tree()):              # ★ 反向拓樸:先處理葉子,再處理父節點
        func = stream.func
        ...
        if stream.children:
            child_executors = [executors[child] for child in stream.children]
        else:
            child_executors = [sink]                       # 葉節點的下游就是 sink

        executor = func.get_executor(*child_executors)     # ★ 父節點拿子節點的 executor 包起來
        executors[stream] = executor

    root_executors = {s: e for s, e in executors.items() if not s.parents}
    return root_executors                                  # 只回傳根節點 (root) 的 executor
```

幾個重點:

- **`reversed(full_tree())`** 是「拓樸排序的反向」,
  代表迴圈處理一個節點時,它的所有子節點都已經先在 `executors` 字典裡了。
- **`func.get_executor(*child_executors)`** 把當前節點的 `wrapper`
  套上「下游的 wrapper」——這就是 closure 形成的時刻。
- 最後只回傳沒有 parents 的節點(root)的 executor,
  因為呼叫端只需要從 root 開始觸發。

### 9.3 用一個極簡 pipeline 從建構期追到執行期

直接用 tutorial 範例會太多算子,先用最小可重現的 pipeline 走一遍,
**每一步都標出實際會跑的原始碼位置**。
看懂這個,再回頭看 `join_asof` 的 `apply(left_func).filter(block_discarded)` 就只是同一套機制重複幾次。

範例:

```python
sdf = app.dataframe(topic)
sdf = sdf.apply(lambda v: v + 1)     # 算子 A
sdf = sdf.apply(lambda v: v * 10)    # 算子 B
sdf.update(lambda v: print(v))       # 算子 C
```

#### 9.3.1 建構期 — 在樹上掛 4 個節點

| 動作 | 實際跑哪段程式碼 | 結果 |
|---|---|---|
| `app.dataframe(topic)` | `app.py:524-578` 建立 `StreamingDataFrame` | `sdf._stream = Stream()` |
| `Stream()` 預設建構 | `core/stream/stream.py:87-93` | `self.func = ApplyFunction(λ v: v)`(identity);`parents=[]`、`children=[]` |
| `sdf.apply(λ v: v+1)` | `dataframe.py:238-304`,走 `else` 分支進 `self.stream.add_apply(...)`(`dataframe.py:297-303`) | 回傳一個新的 SDF,共用同一個 stream-id |
| `Stream.add_apply()` | `stream.py:184-215`,呼叫 `_add(ApplyFunction(...))`(`stream.py:212-215`) | 新節點 `n_A`,`func=ApplyFunction(λ v: v+1)` |
| `Stream._add()` | `stream.py:555-558`:`self.children.append(new_node)` | `root.children = [n_A]`、`n_A.parents = [root]` |
| `.apply(λ v: v*10)` | 同上 | `n_A.children = [n_B]`、`n_B.parents = [n_A]` |
| `.update(λ v: print(v))` | `dataframe.py:_add_update`(`dataframe.py:2001-2007`)→ `Stream.add_update`(`stream.py:225-249`)→ `_add(UpdateFunction(...))` | `n_B.children = [n_C]`、`n_C.parents = [n_B]` |

建構結束後的 DAG:

```
 root  (func = ApplyFunction(identity))
   │
   └─ n_A  (func = ApplyFunction(λ v: v+1))
        │
        └─ n_B  (func = ApplyFunction(λ v: v*10))
             │
             └─ n_C  (func = UpdateFunction(λ v: print(v)))
```

**注意這時候還沒有任何「下一個函式」的概念,純粹是個樹。**

#### 9.3.2 組裝期 — `compose()` 從葉子往回包

`app.run()` 內部會呼叫 `compose_all()`(`registry.py:108-124`),
最終進到 `Stream.compose()`(`stream.py:404-461`)。

迴圈順序:

```
full_tree()            # stream.py:378-402,用 TopologicalSorter 保證父在前
   ↓
[root, n_A, n_B, n_C]

reversed(...)
   ↓
[n_C, n_B, n_A, root]   # ← 從葉子先處理
```

進迴圈前的初始狀態(`stream.py:433-434`):

```
sink       = self._default_sink   # stream.py:560-561,簽名 (v, k, t, h) → None,什麼都不做
executors  = {}
```

接著一輪一輪跑,每輪都對應到 `stream.py:436-458` 那 23 行:

##### Iter 1:`stream = n_C`(葉節點)

```python
# stream.py:452-455
stream.children == []                # True
child_executors = [sink]

# stream.py:457
executor = func.get_executor(*child_executors)
#          ↑ func = UpdateFunction(λ v: print(v))
#          進到 functions/update.py:25-34
```

`UpdateFunction.get_executor`(`functions/update.py:25-34`)實際回傳:

```python
def E_C(value, key, ts, h):
    print(value)              # ← functions/update.py:31,跑使用者 callback
    sink(value, key, ts, h)   # ← functions/update.py:32,把同一個 value 往下傳
```

```
executors = { n_C: E_C }
```

##### Iter 2:`stream = n_B`

```python
# stream.py:452-453
stream.children == [n_C]             # True
child_executors = [ executors[n_C] ] # = [E_C]   ← 上一輪剛塞進來

# stream.py:457
executor = ApplyFunction(λ v: v*10).get_executor(E_C)
#                                  ↑ functions/apply.py:39-69
```

`ApplyFunction.get_executor`(`functions/apply.py:39-69`,`expand=False` 分支)會先過 `_resolve_branching(E_C)`(`functions/base.py:30-65`),因為只有一個 child 直接回傳 `E_C`(`base.py:64-65`),然後組合出:

```python
def E_B(value, key, ts, h):
    result = (λ v: v*10)(value)   # ← functions/apply.py:66
    E_C(result, key, ts, h)       # ← functions/apply.py:67
```

```
executors = { n_C: E_C, n_B: E_B }
```

##### Iter 3:`stream = n_A`

完全一樣的流程:

```python
child_executors = [ executors[n_B] ] = [E_B]
executor = ApplyFunction(λ v: v+1).get_executor(E_B)
```

```python
def E_A(value, key, ts, h):
    result = (λ v: v+1)(value)    # ← functions/apply.py:66
    E_B(result, key, ts, h)       # ← functions/apply.py:67
```

```
executors = { n_C: E_C, n_B: E_B, n_A: E_A }
```

##### Iter 4:`stream = root`

```python
child_executors = [ executors[n_A] ] = [E_A]
executor = ApplyFunction(identity).get_executor(E_A)
```

```python
def E_root(value, key, ts, h):
    result = func(value)           # ← functions/apply.py:66,func 是下面講的 identity lambda
    E_A(result, key, ts, h)        # ← functions/apply.py:67
```

> **`root` 的 `func` 為什麼是 identity?**
> 看 `stream.py:90`:
> ```python
> self.func = func if func is not None else ApplyFunction(lambda value: value)
> ```
> `app.dataframe(topic)` 時(`dataframe.py:158`)做的是 `self._stream = stream or Stream()`,
> 沒有指定 `func`,所以 `Stream()` 走 else 分支,把 root 的 `func` 設成 identity 的 `ApplyFunction`。
> **identity 語義是寫死在 `Stream()` 的預設 `func`,不是 `compose()` 才加的。**
> 因此 Iter 4 的 `result = func(value)` 實際就是 `result = (lambda v: v)(value) == value`。

```
executors = { n_C: E_C, n_B: E_B, n_A: E_A, root: E_root }
```

##### 最後一步 — 只回根節點

```python
# stream.py:460-461
root_executors = { s: e for s, e in executors.items() if not s.parents }
#               = { root: E_root }
return root_executors
```

`compose_all()`(`registry.py:118-123`)拿到這個字典後,把 `{ topic.name: E_root }` 回給 `Application._run_dataframe`(`app.py:929`),存進 `dataframes_composed`。

> **結論:`compose()` 跑完之後,只剩一個 `E_root` closure 是真的會被呼叫的入口。
> 其他 `executors[...]` 都只是「父節點組裝時的中間產物」,組完就被 closure 鎖在 `E_root` 內部、字典本體被丟掉。**

#### 9.3.3 執行期 — 用 `value=5` 走一遍

每來一筆 row,`Application._process_message`(`app.py:986-1038`)會跑:

```python
# app.py:1010-1021(節錄)
for row in rows:
    context = copy_context()                          # contextvars 快照
    context.run(set_message_context, row.context)     # context.py:22-50
    try:
        context.run(
            dataframe_composed[topic_name],           # ← 這就是上面那個 E_root
            row.value, row.key, row.timestamp, row.headers,
        )
    ...
```

假設 `row.value = 5`,實際的呼叫鏈:

```
E_root(5, "k", 0, [])                  # 在 contextvars Context 內執行
  │  result = 5                        # identity
  └─► E_A(5, "k", 0, [])               # ← apply.py:67 那行呼叫的
         │  result = 5 + 1 = 6         # ← apply.py:66
         └─► E_B(6, "k", 0, [])        # ← apply.py:67
                │  result = 6 * 10 = 60 # ← apply.py:66
                └─► E_C(60, "k", 0, []) # ← apply.py:67
                       │  print(60)              # ← update.py:31  → stdout: 60
                       └─► sink(60, "k", 0, []) # ← update.py:32  → noop
```

#### 9.3.4 三個非看不可的觀察

1. **「下一個函式」是 closure 變數,不是 return**
   - `E_B` 怎麼知道下一個要呼叫 `E_C`?
     因為 `ApplyFunction.get_executor(E_C)`(`apply.py:39-69`)被呼叫時,
     Python 把 `E_C` 綁進了 `child_executor` 這個 local,
     而回傳的 `wrapper` 把 `child_executor` 變成 closure 變數,永遠抓得到 `E_C`。
2. **反向迴圈是為了「子要先存在」**
   - 處理 `n_B` 那一輪要做 `executors[n_C]` 的查表,
     所以 `n_C` 必須**已經處理過**才行 → 葉子先處理。
3. **只回 root 是因為只需要一個入口**
   - 中間節點的 closure 不會被外部直接呼叫;
     它們的「使命」在父節點 `get_executor(*child_executors)` 那一刻就完成了。

#### 9.3.5 把這個範例對到 `join_asof`

回到 tutorial 的真實情況(`join_asof.py:49-50`):

```python
right = right.update(right_func, metadata=True).filter(block_all)
left  = left.apply(left_func, metadata=True).filter(block_discarded)
```

兩條路線各自掛了兩個節點:

| Stream 上的節點 | 對應的 `StreamFunction` 子類 | `get_executor` 程式碼 |
|---|---|---|
| right `.update(right_func)` | `UpdateWithMetadataFunction` | `functions/update.py:53-62` |
| right `.filter(block_all)` | `FilterFunction` | `functions/filter.py:22-36` |
| left `.apply(left_func)` | `ApplyWithMetadataFunction` | `functions/apply.py:101-131` |
| left `.filter(block_discarded)` | `FilterFunction` | `functions/filter.py:22-36` |

`compose()` 那段反向迴圈跑到這四個節點時,
做的事情**跟上面 §9.3.2 一字不差**:
從葉子先包,把下游 closure 塞進父節點的 `wrapper` 變數裡。
唯一的差別是 `Filter*` 的 wrapper 多了一個 `if func(value): ...` 條件,
所以 `block_discarded` 回 `False` 那筆就「不繼續呼叫下游」——
在 closure 鏈裡這就是「過濾掉」的意思,沒有什麼魔法。

### 9.4 分岔與合流

兩個會讓圖變不單純的事:

- **分岔(branching)**:同一個父節點有多個 children(例如同一個 SDF 又被 filter 又被 to_topic)。
  `_resolve_branching`(`functions/base.py:30-65`)會在這時包一層 wrapper,
  把 value 用 `pickle_copier` 拷貝給「除了第一個 child 之外」的分支,
  避免下游 mutate 互相污染。
- **合流(merge / concat)**:`Stream.merge()`(`stream.py:283-302`)建一個 `parents=[self, other]` 的新節點。
  - 但每個 root 的 executor 是各自獨立的,
    `compose_all`(`registry.py:108-124`)會替每個 topic 的 root 回一個 executor。
  - 兩個 root 透過 merge 點之後共用同一段下游 closure,
    所以無論訊息從哪個 topic 進來,跑到 merge 點之後執行路徑是同一條。

示意:

```
 telemetry root ─► apply_wrapper ─► filter_wrapper ─┐
                                                    ├─► merged_node_wrapper ─► … ─► sink
 forecast  root ─► update_wrapper ─► filter_wrapper ─┘
                                     (block_all)
```

### 9.5 `compose_returning()` — 給 series 用的小變形

`__setitem__` 與 `series.apply` 會用到 `compose_returning()`(`stream.py:463-499`):

```python
buffer = collections.deque(maxlen=1)
executor = self.compose_single(
    allow_filters=False, allow_expands=False,
    allow_updates=False, allow_transforms=False,
    sink=lambda value, key, ts, headers: buffer.appendleft((value, key, ts, headers)),
)

def wrapper(value, key, timestamp, headers):
    try:
        executor(value, key, timestamp, headers)
        return buffer.popleft()
    finally:
        buffer.clear()

return wrapper
```

關鍵在 **sink 變成「把結果塞到 buffer」**,
外層 wrapper 跑完後從 buffer 拿值返回。
這樣 series 雖然底層也是同一套 Stream 機制,
卻能被當成「一個會回傳值的純函式」使用,
所以 `__setitem__` 才能用 `series_composed(...)` 拿到 series 計算結果再寫回 dict。

### 9.6 一張腦海中固定下來的圖

```
   建構期 (一次):
   sdf.apply(f)            ─►  Stream._add(ApplyFunction(f))
   sdf.filter(g)           ─►  Stream._add(FilterFunction(g))
   sdf.update(h)           ─►  Stream._add(UpdateFunction(h))

   ┌────────────┐   parents/children   ┌────────────┐
   │  root      │ ◄──────────────────► │ apply_node │ ◄────► filter_node ◄────► update_node
   └────────────┘                      └────────────┘                          (children=[])

   app.run() 啟動時(一次):
   compose()  ──► 反向拓樸 ──► get_executor 由葉子往上層包
                  update_wrapper  (child=sink)
                  filter_wrapper  (child=update_wrapper)
                  apply_wrapper   (child=filter_wrapper)
                  root_wrapper    (child=apply_wrapper)

   每來一筆訊息(N 次):
   root_wrapper(value, key, ts, headers)
       └─ apply_wrapper(...)
            └─ filter_wrapper(...)
                 └─ update_wrapper(...)
                      └─ sink(...)
```

---

## 10. 深入:`context.run` 怎麼讓這些函式跑起來

接著回答另一個問題:
**主程序的 `_process_message()` 那段為什麼要先 `copy_context()` 再 `context.run(...)`?
這跟「函式串起來跑、把資料丟給下一個」有什麼關係?**

### 10.1 `_process_message` 裡那段詭異的程式碼

`app.py:986-1038`:

```python
def _process_message(self, dataframe_composed):
    self._producer.poll(self._config.producer_poll_timeout)
    rows = self._consumer.poll_row(timeout=..., buffered=...)
    if rows is None: ...
    rows = rows if isinstance(rows, list) else [rows]
    ...
    for row in rows:
        context = copy_context()                          # ① 抓一份目前的 contextvars 快照
        context.run(set_message_context, row.context)     # ② 在新 context 裡呼叫 set_message_context
        try:
            context.run(                                  # ③ 在「同一份」 context 裡跑 executor
                dataframe_composed[topic_name],
                row.value, row.key, row.timestamp, row.headers,
            )
        except Exception as exc:
            ...
```

這裡有三件事要拆開講:

#### ① `copy_context()` 是什麼

Python 的 `contextvars.Context` 是個「快照」物件,
裡面記錄了所有 `ContextVar` 此刻的值。
`copy_context()` 會抓當前 thread 的 contextvars 副本回來,
之後你**在這個 Context 物件上做的修改不會影響原本的 context**,
而是只活在這個 Context 裡。

#### ② `set_message_context` 是怎麼把 metadata 傳給算子的

`quixstreams/context.py:14-50`:

```python
_current_message_context: ContextVar[Optional[MessageContext]] = ContextVar(
    "current_message_context"
)

def set_message_context(context: Optional[MessageContext]):
    _current_message_context.set(context)

def message_context() -> MessageContext:
    ctx = _current_message_context.get()
    if ctx is None:
        raise MessageContextNotSetError("Message context is not set")
    return ctx
```

執行時很多地方會用到 `message_context()` 拿 partition / offset / key:

- `dataframe.py:_produce`(`dataframe.py:1995`):`ctx = message_context()` 把 partition 寫進 row。
- `dataframe.py:_as_stateful`(`dataframe.py:2179-2191`):用 `message_context().partition` 找對應的 store transaction。
- `joins/base.py:_get_transaction`(`base.py:97-107`):一樣用 `message_context().partition`。

也就是說,**算子內部不需要把 partition 一路當參數傳下去**,
只要在 pipeline 開頭 `set_message_context(row.context)`,
之後不論 closure 鏈跑得多深、跨多少層,
任何一層 callback 呼叫 `message_context()` 都拿得到對的訊息 metadata。

#### ③ `context.run(executor, value, key, ts, headers)` 才是真正執行

`Context.run(fn, *args)` 的語意是:

> 在這個 Context 的環境下執行 `fn(*args)`,執行期間對 `ContextVar` 的修改只活在這個 Context 裡。

換句話說:

- 在這個 Context 裡呼叫 `_current_message_context.get()` 會拿到 `row.context`(因為步驟 ② 設過了)。
- 在這個 Context 裡呼叫的任何深層 callback,呼叫 `message_context()` 都會拿到 `row.context`。
- 等 `context.run` 回來之後,**主 thread 的 `_current_message_context` 還是空的**——
  彼此完全隔離。

### 10.2 為什麼非用 contextvars 不可

直接用 module-global 變數理論上也行,但有兩個壞處:

1. **巢狀執行**:`__setitem__` 內部會呼叫 `series_composed(value, key, ts, headers)`,
   那段 series 又是一個 stream pipeline;若沒有 contextvars 隔離,
   只要中途有人改了 global,在巢狀執行裡就會看到「中間狀態」。
   contextvars + `Context.run` 保證每次外層執行是獨立的環境。
2. **錯誤處理 / commit**:主迴圈在 `try/except` 裡跑 executor,
   如果其中拋例外,我們希望「沒處理完的訊息」不會留下汙染的 message context。
   contextvars 的範圍只在 `context.run()` 那一行,離開那行就消失,
   讓主迴圈的狀態保持乾淨。

### 10.3 整段執行的時序圖

把 §9 跟 §10 合起來,主程序處理一筆訊息的完整時序如下:

```
 ┌─────────────────────────────────────────────────────────────────────────────┐
 │  Application._process_message                                              │
 │                                                                            │
 │    rows = consumer.poll_row(...)                                           │
 │    for row in rows:                                                        │
 │        context = copy_context()           ← 拷貝目前 ContextVar 狀態        │
 │        context.run(set_message_context, row.context)                       │
 │                                            └ 在新 context 裡設定           │
 │                                              _current_message_context      │
 │        context.run(executor, row.value, row.key, row.timestamp, ...)       │
 │                                            └ 在同一份 context 裡執行       │
 │                                              預先 compose 好的 closure 鏈   │
 │                                                                            │
 │                ┌──────────────────────────────────────────┐                │
 │                │ executor = root_wrapper                  │                │
 │                │   └► apply_wrapper(value, key, ts, h)    │                │
 │                │       result = left_func(value, k, t, h) │                │
 │                │       ─ left_func 內部呼叫               │                │
 │                │         message_context() → row.context │                │
 │                │         拿到 partition 去查 state store   │                │
 │                │       └► filter_wrapper(result, k, t, h) │                │
 │                │           if block_discarded(result):    │                │
 │                │               └► merged_wrapper(...)     │                │
 │                │                    └► update_wrapper     │                │
 │                │                       (set timestamp_str)│                │
 │                │                       └► update_wrapper  │                │
 │                │                          (print_table)   │                │
 │                │                          └► update_wr.   │                │
 │                │                              (to_topic)  │                │
 │                │                              ─ to_topic  │                │
 │                │                                內部用    │                │
 │                │                                message_  │                │
 │                │                                context() │                │
 │                │                                建 Row 再 │                │
 │                │                                produce   │                │
 │                │                              └► sink ─► collector/noop   │
 │                └──────────────────────────────────────────┘                │
 │                                                                            │
 │    processing_context.store_offset(topic, partition, offset)               │
 │    (此時還沒 commit,等 commit_checkpoint 統一寫回)                          │
 └─────────────────────────────────────────────────────────────────────────────┘
```

### 10.4 三個容易誤會的小點

1. **`context.run` 的回傳值**:`Context.run(fn, *args)` 會回 `fn(*args)` 的回傳值,
   但這裡所有 wrapper 都沒回值(void executor),所以實際上忽略。
   pipeline 的「資料流向」走的是 **closure 內部呼叫 `child_executor(...)`**,
   不是靠 return 一路把值回傳到主程序。
2. **`copy_context()` 後再呼叫兩次 `context.run`**:會用同一個 `context` 變數,
   是因為 `Context` 物件本身會把每次 `run()` 的 ContextVar 變更累積進去。
   所以「先 set 再 run executor」可以看成兩次 `run` 共享同一個環境。
3. **subprocess 不需要 contextvars**:Source 子程序根本不會走這條 pipeline,
   它只負責把資料 produce 到 Kafka topic,**走 Kafka 才回到主程序的 SDF**。
   所以 contextvars 跟「跨程序」沒關係,只用來解決「同一程序內、巢狀執行、不污染外層」。

### 10.5 帶回前面的疑問

> 「context.run 那邊怎麼讓這些函數跑並丟回傳給下一個函數?」

完整回答:

- 「讓這些函數跑」的部分是 **`compose()` 已經把整條呼叫鏈包成一個 closure 樹**,
  `context.run(executor, value, key, ts, headers)` 只是觸發呼叫鏈的入口。
- 「丟回傳給下一個函數」**不是用 Python `return`**,
  而是每一層 wrapper 在自己內部主動呼叫 `child_executor(處理過的值, key, ts, headers)`。
  return 留給 `compose_returning()`(series 用)那種特例。
- `context.run` 額外保證的是:整條 closure 鏈共享同一份 `MessageContext`,
  任何一層深處呼叫 `message_context()` 都拿得到正確的 partition/offset/key/headers,
  而且離開這次 `run` 之後立刻乾淨,不會污染下一筆訊息的處理。

---

## 11. `sink` 到底是誰?有被呼叫嗎?

§9 在 trace closure 鏈時不斷出現 `sink(value, key, ts, headers)`,
但它**在 production 跑的時候其實什麼都沒做**。下面把來龍去脈說清楚。

#### `sink` 的來源

`Application._run_dataframe()`(`app.py:891-894`):

```python
if self._dataframe_registry.consumer_topics:
    collector = self._run_tracker.get_collector(collect=collect, metadata=metadata)
    self._run_dataframe(sink=collector)
```

`RunTracker.get_collector()`(`runtracker.py:205-217`):

```python
def get_collector(self, collect, metadata) -> Optional[VoidExecutor]:
    if not self._has_stop_condition:
        return None                              # ★ production 沒設 timeout/count → 回 None
    elif not collect:
        return self.increment_count              # 設了 stop 條件但不收資料 → 只計數
    elif not metadata:
        return self.collect_values               # 收資料(只 value)
    else:
        return self.collect_values_and_metadata  # 收資料 + metadata
```

`Application._run_dataframe()`(`app.py:929`):

```python
dataframes_composed = self._dataframe_registry.compose_all(sink=sink)
```

進到 `Stream.compose()`(`stream.py:433`):

```python
sink = sink or self._default_sink     # ← collector 是 None 就用 _default_sink
```

`_default_sink`(`stream.py:560-562`):

```python
def _default_sink(self, value, key, timestamp, headers) -> None:
    ...                               # 函式體只有 `...`,就是 no-op
```

#### 所以結論是

| 情境 | `sink` 是什麼 | 被呼叫時做什麼 |
|---|---|---|
| `app.run()`(production) | `_default_sink`(no-op) | 什麼都不做 |
| `app.run(timeout=5)` | `increment_count` | `_collector.count += 1`,用來觸發停止 |
| `app.run(count=10, collect=True)` | `collect_values` | 把 value 累積到 list,給 `app.run()` 回傳 |

`sink` **每筆訊息都會被呼叫一次**(由葉節點 wrapper 內部呼叫,見 §9.3.2 Iter 1),
但在 production 路徑上它就是個空殼。

#### 為什麼「真正的副作用」不在 sink

關鍵:`to_topic`、`print_table`、state 寫入 **都不是 sink**,
它們是**鏈條中間的節點**,在自己 wrapper 內部就做完事再呼叫下游。

對照 `dataframe.py:746-755`(`to_topic` 的本體):

```python
return self._add_update(
    lambda value, orig_key, timestamp, headers: self._produce(
        topic=...,
        value=value,
        ...
    ),
    metadata=True,
)
```

`_add_update` → `Stream.add_update` → `UpdateWithMetadataFunction` 節點。
所以 `to_topic` 在 closure 鏈裡長這樣(對照 `functions/update.py:57-60`):

```python
def E_to_topic(value, key, ts, h):
    self._produce(topic=..., value=value, ...)   # ← 真正 produce 到 Kafka(buffered)
    child_executor(value, key, ts, h)            # ← 然後仍然呼叫下游 (= 下一個 update,或 sink)
```

也就是說,**生效的是 `self._produce` 那行**,
不是後面的 `child_executor(...)`。`sink` 只是「整條鏈最尾端那個用來收尾的空函式」。

---

## 12. Checkpoint 怎麼跟 closure 鏈串起來

回到你的第二個問題:「checkpoint 後會跟上面的串起來嗎?」
答案是 **closure 鏈跟 checkpoint 是兩條獨立但前後啣接的軌道**,
中間靠 `ProcessingContext` 與 `InternalProducer` 的 buffer 來串。

### 12.1 一筆訊息的完整生命週期

`Application._run_dataframe()` 的主迴圈(`app.py:935-952`):

```python
while run_tracker.running:
    if state_manager.recovery_required:
        state_manager.do_recovery()
    else:
        process_message(dataframes_composed)        # ① 跑 closure 鏈
        processing_context.commit_checkpoint()      # ② 視情況才 commit
        consumer.resume_backpressured()
        source_manager.raise_for_error()
        ...
```

#### ① `_process_message` — 跑 closure 鏈

`app.py:1010-1038`(節錄):

```python
for row in rows:
    context = copy_context()
    context.run(set_message_context, row.context)
    try:
        context.run(
            dataframe_composed[topic_name],         # ← §9 那個 E_root
            row.value, row.key, row.timestamp, row.headers,
        )
    except Exception as exc:
        ...
self._processing_context.store_offset(             # ← 記錄 offset 到目前的 Checkpoint
    topic=topic_name, partition=partition, offset=offset
)
```

closure 鏈跑完當下,**真正寫到外面的事還沒落地**:

| 副作用 | 在 closure 鏈裡發生什麼 | 真正落地的時機 |
|---|---|---|
| `to_topic` produce 訊息 | `InternalProducer.produce_row()` 把訊息塞進 librdkafka 內部佇列(非同步) | checkpoint commit 時 `producer.flush()` |
| `apply(stateful=True)` 寫 state | 寫進 `PartitionTransaction`(記憶體中) | checkpoint commit 時 `transaction.prepare()` + `flush()` |
| `join_asof` 寫 forecast 進 store | 同上,寫進 timestamped `PartitionTransaction` | 同上 |
| offset 進度 | `ProcessingContext.store_offset()` 寫到 `Checkpoint._tp_offsets` 字典 | checkpoint commit 時送 `consumer.commit(offsets)` |

`ProcessingContext.store_offset`(`processing/context.py:50-58`):

```python
def store_offset(self, topic, partition, offset):
    self.checkpoint.store_offset(topic=topic, partition=partition, offset=offset)
```

最後寫進的是 `Checkpoint._tp_offsets`(`checkpoint.py:46`)那個 dict。**到這裡為止,broker 還不知道有任何事發生。**

#### ② `commit_checkpoint()` — 把上面累積的東西真正落地

`ProcessingContext.commit_checkpoint`(`processing/context.py:75-96`):

```python
def commit_checkpoint(self, force=False):
    if self.checkpoint.expired() or force:
        if self.checkpoint.empty():
            self.checkpoint.close()
        else:
            self.checkpoint.commit()
        self.init_checkpoint()                # ← 重置,開新的 Checkpoint
```

`expired()` 看的是 `commit_interval` 秒數到了沒,或處理量達到 `commit_every`。
也就是說 **大多數迴圈跑 commit_checkpoint() 時其實什麼都不做**,
直到 5 秒(預設)或 N 筆訊息累積到才會真正 commit。

真正 commit 的 `Checkpoint.commit()`(`checkpointing/checkpoint.py:181-`)五個步驟:

```
Step 1. flush 各個 SinkManager.sinks
        (這裡的 "sink" 指 BaseSink 子類,例如 file sink、ClickHouse sink,
         跟 §10.5 那個 closure 鏈尾端的 sink 是完全不同的東西!)
        ─ checkpoint.py:194-223

Step 2. 對每個 store transaction 呼叫 transaction.prepare(processed_offsets=...)
        把要寫的 changelog 推給 InternalProducer(也是 buffered)
        ─ checkpoint.py:226-244

Step 3. self._producer.flush()
        ★ 等所有 buffered 訊息真正送進 broker 且收到 ack
        這就是 to_topic 寫的訊息「真的進 Kafka」的時刻
        ─ checkpoint.py:248-254

Step 4. self._consumer.commit(offsets=...)  (或 commit_transaction 走 EOS)
        ★ offset 真的回到 broker,broker 才知道哪些訊息已處理
        ─ checkpoint.py:256-269

Step 5. 各 store partition transaction.flush() 寫 RocksDB 到磁碟
```

> ⚠️ **「sink」這個詞在這個 codebase 有兩個意思,別搞混**:
> 1. `Stream.compose(sink=...)` 那個 — 是 closure 鏈最末端的 callback,production 是 no-op。
> 2. `SinkManager.sinks` 那些 — 是使用者透過 `sdf.sink(MySink())` 註冊的 `BaseSink` 子類(寫檔、寫資料庫等),由 checkpoint 統一 flush。

### 12.2 「checkpoint 後會跟上面串起來嗎?」

「串起來」分成兩種:

#### A. 同一個 closure 鏈會跑很多次,每跑一次都會被 checkpoint 收尾

```
closure 鏈執行(訊息 1)   ─►  store_offset(1)  ─┐
closure 鏈執行(訊息 2)   ─►  store_offset(2)  ─┤  累積在 _tp_offsets
closure 鏈執行(訊息 3)   ─►  store_offset(3)  ─┘
...
commit_interval 到了 ─► Checkpoint.commit()
                          ├ producer.flush()
                          ├ consumer.commit(offsets={...到 3 為止})
                          └ state stores flush

closure 鏈執行(訊息 4)   ─►  store_offset(4)  ─┐  累積到下一個 Checkpoint
...
```

每次 `commit_checkpoint()` 之後立刻 `init_checkpoint()` 開新的 `Checkpoint`,
所以 closure 鏈的下一輪訊息**寫到新的 Checkpoint**。
從這個角度看,closure 鏈跟 checkpoint **是同一個故事**——
closure 鏈跑出來的副作用累積在 Checkpoint,Checkpoint 定期把它們同步出去。

#### B. `Stream.compose()` 本身只在啟動時跑一次,checkpoint 不會重 compose

`compose_all()` 是在 `_run_dataframe()` 啟動時呼叫(`app.py:929`),
之後**整個 application 跑期間都用同一個 `dataframes_composed` 字典**。
Checkpoint 不會重新組裝 closure 鏈,
所以 `E_root` 始終是同一個 closure。

可以把整體想成兩條時間軸:

```
時間軸 1(每訊息):
  poll → set_message_context → E_root(...) → store_offset
       └─ 在 closure 鏈內把:
           to_topic 訊息推進 producer buffer
           state 變更推進 PartitionTransaction
           offset 推進 Checkpoint._tp_offsets

時間軸 2(每 commit_interval):
  Checkpoint.commit() 把上面三個 buffer 一次性對外落地
                     並開一個新 Checkpoint
```

### 12.3 怎麼跟 `_process_message` 對到實際程式碼

把 §9 的 closure 鏈與 §11 的 checkpoint 用實際呼叫位置串一次:

```
app.py:935  while run_tracker.running:
app.py:940     process_message(dataframes_composed)
                 │
                 ├ app.py:989  rows = consumer.poll_row(...)
                 ├ app.py:1011 for row in rows: context = copy_context()
                 ├ app.py:1012 context.run(set_message_context, row.context)
                 ├ app.py:1015 context.run(dataframe_composed[topic], value, key, ts, h)
                 │                │
                 │                └─► §9 整條 closure 鏈
                 │                       ├ to_topic        → producer 內部 buffer
                 │                       └ stateful apply  → PartitionTransaction
                 │
                 └ app.py:1030 processing_context.store_offset(topic, partition, offset)
                                  → checkpoint._tp_offsets[(topic, part)] = offset

app.py:941    processing_context.commit_checkpoint()
                 │
                 ├ processing/context.py:85  if checkpoint.expired() or force:
                 ├ processing/context.py:91  self.checkpoint.commit()
                 │                              ├ checkpoint.py:194  flush BaseSinks
                 │                              ├ checkpoint.py:244  transaction.prepare()
                 │                              ├ checkpoint.py:249  producer.flush()  ★ 訊息真的送出
                 │                              └ checkpoint.py:269  consumer.commit() ★ offset 真的提交
                 └ processing/context.py:96  self.init_checkpoint()  → 新一輪
```

這就是「closure 鏈的副作用怎麼跟 checkpoint 接上」的全部答案:
**closure 鏈把工作塞進三個 buffer(producer / state transaction / offset dict),
checkpoint 負責把這三個 buffer 一次性對外同步。
兩者不是「串」在一條呼叫鏈上,而是「同一個 ProcessingContext 物件的兩個生命週期事件」。**

---

## 13. 真實場景:`sdf.sink(PostgreSQLSink(...))` 怎麼運作

§11 的 `sink` 是 closure 模板的 no-op 終點;
但實際專案常會這樣寫:

```python
from quixstreams.sinks.community.postgresql import PostgreSQLSink

pg_sink = PostgreSQLSink(host="...", dbname="...", table_name="enriched")
enriched_sdf.sink(pg_sink)
```

這裡的 `sink` 是 **`StreamingDataFrame.sink()` 方法**,
是使用者 API,**跟 `Stream.compose(sink=...)` 那個參數完全不是同個東西**。
本節把這條路徑跟前面 §11–§12 接起來。

### 13.1 `sdf.sink(pg_sink)` 內部其實是一個 apply 算子

看 `dataframe.py:1665-1701`:

```python
def sink(self, sink: BaseSink):
    self._processing_context.sink_manager.register(sink)   # ① 註冊到 SinkManager

    def _sink_callback(value, key, timestamp, headers):
        ctx = message_context()
        sink.add(                                          # ② 每筆訊息呼叫 sink.add()
            value=value, key=key, timestamp=timestamp,
            headers=headers,
            partition=ctx.partition, topic=ctx.topic, offset=ctx.offset,
        )

    self.apply(_sink_callback, metadata=True)              # ③ 在 closure 鏈掛一個 apply 節點
```

所以呼叫 `sdf.sink(pg_sink)` 真正做了兩件事:

1. 把 `pg_sink` 註冊到 `SinkManager`(checkpoint 之後會找它)。
2. **在 closure 鏈尾巴掛一個普通的 `ApplyWithMetadataFunction` 節點**,callback 是 `_sink_callback`。

**對 `Stream` 而言,這個節點跟 `apply(lambda v: v+1)` 沒有任何差別。**
框架不知道、也不關心它叫「sink」。

### 13.2 closure 鏈長相

假設整條 pipeline 是 `apply → to_topic → sink(pg_sink)`,
`compose()` 跑完後鏈條最尾端兩層是:

```python
# 倒數第二棒 — pg_sink 節點的 wrapper
def E_pg_sink(value, key, ts, h):
    _sink_callback(value, key, ts, h)   # ← 內部呼叫 pg_sink.add(...) 進記憶體 batch
    no_op_sink(value, key, ts, h)       # ← child_executor 就是 §11 的 _default_sink

# 最後一棒 — Stream._default_sink (stream.py:560-562)
def _default_sink(value, key, ts, h):
    ...                                  # 空函式
```

也就是:**`sdf.sink(pg_sink)` 不會取代 closure 鏈尾的 no-op,它只是在 no-op 前面再加一棒。**

對照圖:

```
            建構期(在 Stream 樹上掛節點)        執行期 closure 鏈

 root  ┐                                  E_root(v, k, t, h)
       │ ApplyFunction(identity)            └ result = v
       │                                    └ E_to_topic(v, ...)
 ...   ┤ (前面各種算子)
       │                                          ...
       │
 n_to_topic                               E_to_topic(v, k, t, h)
       │ UpdateWithMetadataFunction         └ producer.produce_row(v)  ← 寫進 producer buffer
       │                                    └ E_pg_sink(v, ...)
       │
 n_pg_sink                                E_pg_sink(v, k, t, h)
       │ ApplyWithMetadataFunction          └ pg_sink.add(v, ...)      ← 寫進 SinkBatch
       │ (因為 sdf.sink 內部用 apply)        └ _default_sink(v, ...)
       │                                          └ pass               ← 真正的「葉」
       └ (沒有 child)
```

### 13.3 各層在記憶體做什麼 vs 何時落地

| 鏈中的層 | 是哪種 `StreamFunction` | 在記憶體做什麼 | 何時真正對外落地 |
|---|---|---|---|
| `to_topic` 節點 | `UpdateWithMetadataFunction` | `InternalProducer.produce_row()` 進 librdkafka buffer | checkpoint `producer.flush()` |
| `sdf.sink(pg)` 節點 | `ApplyWithMetadataFunction` | `pg_sink.add()` 進 `SinkBatch` 字典 | checkpoint `pg_sink.flush()` → `pg_sink.write(batch)` |
| `_default_sink` | (compose sink 參數) | 什麼都沒做 | 永遠不落地(它就是 no-op) |

`PostgreSQLSink` 繼承 `BatchingSink`(`sinks/community/postgresql.py:68`),
`add` 跟 `flush` 直接用基類版本(`sinks/base/sink.py:159-197`):

```python
# BatchingSink.add — 純記憶體
def add(self, value, key, timestamp, headers, topic, partition, offset):
    tp = (topic, partition)
    batch = self._batches.get(tp)
    if batch is None:
        batch = SinkBatch(topic=topic, partition=partition)
        self._batches[tp] = batch
    batch.append(value=value, key=key, timestamp=timestamp, ...)

# BatchingSink.flush — 才真的對外
def flush(self):
    try:
        for (topic, partition), batch in self._batches.items():
            self.write(batch)        # PostgreSQLSink.write(...) 在 postgresql.py:160
    finally:
        self._batches.clear()
```

### 13.4 完整時序 — 跟 §12 接起來

```
每筆訊息(closure 鏈執行):
   ... → to_topic 算子 → sdf.sink(pg) 算子 → _default_sink
              │                  │                 │
              │                  │                 └ pass (no-op)
              │                  └ pg_sink.add → in-memory SinkBatch
              └ producer.produce_row → in-memory librdkafka buffer
   _process_message 結束 → store_offset → 進入下一筆

每 5 秒 (commit_interval)或 commit_every N 筆:
   ProcessingContext.commit_checkpoint()
         │
         └─► Checkpoint.commit()      (checkpointing/checkpoint.py:181-)
                ├─► Step 1. for sink in sink_manager.sinks: sink.flush()
                │              └ pg_sink.flush() → pg_sink.write(batch)
                │                                      └ ★ INSERT INTO ...  ← 此刻 PG 才真的看到資料
                ├─► Step 2. state transaction.prepare()
                ├─► Step 3. producer.flush()    ← to_topic 訊息此刻才真的送到 Kafka broker
                ├─► Step 4. consumer.commit()   ← offset 此刻才真的提交
                └─► Step 5. state stores 落盤
```

### 13.5 三個關鍵領悟

1. **「sink」這個詞在 codebase 有兩個層級,別搞混**
   - `Stream.compose(sink=...)` — closure 鏈的終點 no-op(框架層,使用者看不到)。
   - `sdf.sink(BaseSink)` — 使用者 API,本質上是個 apply 節點 + 註冊 `SinkManager`(應用層)。
2. **`sdf.sink()` 不是特殊節點,框架不認得它**
   - 對 `Stream` 而言它就是 `ApplyWithMetadataFunction`。
   - 所有「sink 特性」(batch、flush、backpressure)都在 `BaseSink` / `BatchingSink` 子類自己處理。
   - 框架只負責「在 closure 鏈呼叫 `add()`」與「在 checkpoint 呼叫 `flush()`」兩件事。
3. **真正寫 PG 的時機跟 Kafka offset commit 是同一個 checkpoint 事務的兩個步驟**
   - 順序:flush sink(寫 PG)→ flush producer → commit offset。
   - 任何一步失敗都會中斷,offset 不會提交 → 下次重啟從上一次成功 commit 的 offset 重跑(at-least-once)。
   - 如果 PG 寫成功但 offset commit 失敗,重啟後會重寫一次 → 所以 PG 端常需要 idempotent insert 或 upsert。
   - sink 也可以拋 `SinkBackpressureError` 讓應用 pause partition 等一下再 resume(`checkpoint.py:206-219`)。
