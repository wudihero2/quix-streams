# quix-metrics — End-to-End 程式碼導讀

這份文件帶你**從頭到尾讀懂 `dashboard/collector`(套件名 `quix_metrics`)的每一支程式**:它做什麼、怎麼用、程式碼怎麼讀、跟 quix-streams 怎麼接、有哪些要注意的耦合點。

> 想要「怎麼安裝 / API 參數 / 輸出格式」的速查,看 `README.md`。
> 這份是「**讀 code**」導向 —— 解釋設計、資料流、每個模組的職責與閱讀順序。

---

## 1. 它到底在做什麼(一句話)

`MetricsAgent` 是一個**外掛式的監控探針**:你把它套在一個正在跑的 quix-streams `Application` 上,它會在背景**週期性**地把 pipeline 的五種指標(DAG 拓撲、吞吐、錯誤、資源、broker 健康)以 JSON 寫進 Kafka 的 `__quix_metrics` topic,給 dashboard 後端消費。

核心設計哲學有三點,讀 code 前先記住:

1. **外掛、零侵入**:不改 quix-streams 原始碼,而是用 **monkey-patch** 把 hook 掛到 `Application` 的內部 callback 上。使用者只要多寫一行 `MetricsAgent(app)`。
2. **熱路徑便宜、匯出在背景**:每筆訊息只做「計數器 +1」這種極輕的事;貴的事(JSON 序列化、發 Kafka)交給**背景 thread 每 10/30 秒做一次**。這是 metrics collector 的標準正確姿勢。
3. **collector / exporter 分離**:每種指標各自有一個「收集器」class,只負責累積資料;「怎麼送出去」由 `KafkaMetricsExporter` 統一處理。靠 `MetricsCollector` Protocol 解耦。

---

## 2. 怎麼用(最小例子)

```python
from quixstreams import Application
from quix_metrics import MetricsAgent

app = Application(broker_address="localhost:9092", consumer_group="my-pipeline")
sdf = app.dataframe(app.topic("input"))
sdf = sdf.apply(lambda row: row).to_topic(app.topic("output"))

agent = MetricsAgent(app)   # ← 掛上探針,就這樣
app.run()                   # 照常跑
```

`MetricsAgent(app)` 一被 new 出來,就立刻:安裝 hook、起兩條背景 daemon thread。之後 `app.run()` 照跑即可。要顯式停止用 `agent.stop()`(不停也行,daemon thread 會隨 process 結束)。

---

## 3. 整體資料流(先看這張圖)

```
                          你的 quix-streams Application
                                     │
   ┌─────────────────────────────────┼──────────────────────────────────┐
   │ (hook,每筆/每錯誤即時觸發)        │ (patch,啟動時一次)                  │
   ▼                                 ▼                                    ▼
 _on_message_processed          _on_processing_error                  app.run()
   │  每筆訊息                      │  每次處理錯誤                        │ 啟動瞬間
   ▼                              ▼                                     ▼
 ThroughputTracker             ErrorInterceptor                    extract_dag()
 (counts/bytes +1,有鎖)        (count + 最多3個樣本)                (走 registry 建拓撲)
   │                              │                                     │
   └──────────────┬───────────────┘                                     │
                  │  累積在記憶體                                          │
   ┌──────────────┴───────────────────────────────────────┐            │
   │ flush thread(每 10s)                                   │            │
   │   collect_and_reset() → envelope → KafkaMetricsExporter│◀───────────┘ (DAG 立即 emit)
   │   也順手收 broker_health(讀 consumer._broker_states)    │
   ├────────────────────────────────────────────────────────┤
   │ resource thread(每 30s)                                 │
   │   ResourceCollector.collect() → envelope → Exporter     │
   └────────────────────────────────────────┬───────────────┘
                                             ▼
                              Kafka topic  __quix_metrics  (JSON envelopes)
                                             ▼
                                     dashboard backend / frontend
```

---

## 4. 建議的閱讀順序

從「最單純、無依賴」往「把大家串起來」讀,最不會卡:

1. `protocol.py` — 一個 Protocol,30 秒看完,理解 collector/exporter 的契約。
2. `kafka_exporter.py` — 唯一真正對外送資料的人。
3. `throughput.py` → `errors.py` → `resources.py` → `broker_health.py` — 四個獨立收集器(模式都一樣:累積 + `collect_and_reset`/`collect`)。
4. `dag_extractor.py` — 一個純函式,把 quix-streams 的內部 registry 轉成 nodes/edges。
5. `agent.py` — **總指揮**,把上面全部黏起來、裝 hook、開背景 thread。最後讀它,前面都懂了它就好懂。
6. `__init__.py` — 對外公開的三個名字。

---

## 5. 逐檔導讀

### 5.1 `protocol.py` — 解耦契約

```python
@runtime_checkable
class MetricsCollector(Protocol):
    def emit(self, envelope: dict) -> None: ...
    def flush(self) -> None: ...
```

- 定義「一個 exporter 長什麼樣」:有 `emit`(送一筆)和 `flush`(把 buffer 清空)。
- `Protocol` = 結構型別(duck typing 的型別版),任何有這兩個 method 的物件都算數,**不需要繼承**。
- 用途:`MetricsAgent` 只依賴這個介面,所以你可以塞一個「寫檔案的 exporter」或測試用的 fake,不一定要 Kafka。`@runtime_checkable` 讓你能用 `isinstance(x, MetricsCollector)` 檢查。
- **怎麼讀**:這是全套件的「插槽形狀」。看完它,後面所有收集器/匯出器都是在填這個形狀。

### 5.2 `kafka_exporter.py` — 唯一的出口

```python
METRICS_TOPIC = "__quix_metrics"

class KafkaMetricsExporter:
    def __init__(self, broker_address, topic=METRICS_TOPIC, producer=None):
        self._producer = producer or Producer({"bootstrap.servers": broker_address})

    def emit(self, envelope: dict) -> None:
        key = f"{envelope.get('consumer_group','unknown')}.{envelope.get('type','unknown')}"
        try:
            self._producer.produce(topic=self._topic,
                                   key=key.encode(), value=json.dumps(envelope).encode(),
                                   on_delivery=self._on_delivery)
            self._producer.poll(0)
        except Exception:
            logger.debug(...)   # 吞掉:監控壞掉不該拖垮主程式
```

讀這支要抓住的點:

- **直接用 `confluent_kafka.Producer`**,不走 quix-streams 的 producer —— metrics 是獨立 pipeline,不想跟業務資料糾纏。
- **message key = `{consumer_group}.{type}`**(例:`my-pipeline.throughput`)。同一 key 進同一 partition,方便 dashboard 依 consumer group / 指標型別分流。
- **`poll(0)`**:非阻塞地觸發 librdkafka 的事件回呼(送出 delivery callback),不等待。
- **`emit` 把所有例外吞掉只 debug log**:這是刻意的 —— **監控失敗絕不能讓主程式爆掉**。同理 `flush()`(process 結束前把殘留訊息送完,timeout 5s)。
- **`producer=None` 可注入**:測試時塞 `MagicMock`(見 `test_kafka_exporter.py`),不用真的連 Kafka。

### 5.3 `throughput.py` — 每筆計數(熱路徑)

```python
class ThroughputTracker:
    def __init__(self):
        self._lock = threading.Lock()
        self._counts: dict[tuple[str,int], int] = {}   # (topic,partition) -> 筆數
        self._bytes:  dict[tuple[str,int], int] = {}    # (topic,partition) -> bytes

    def on_message_processed(self, topic, partition, offset):   # ← 每筆訊息呼叫
        key = (topic, partition)
        with self._lock:
            self._counts[key] = self._counts.get(key, 0) + 1

    def collect_and_reset(self) -> dict:   # ← flush thread 每 10s 呼叫
        with self._lock:
            counts = dict(self._counts); bytes_map = dict(self._bytes)
            self._counts.clear(); self._bytes.clear()
        # ...組成 {total_messages, total_bytes, partitions{...}}
```

- **唯一的熱路徑**:`on_message_processed` 每筆訊息都被呼叫(透過下面 agent 的 hook)。所以它只做「拿鎖 → dict +1 → 放鎖」,刻意極簡。
- **鎖在保護什麼**:處理執行緒一直 `+1`,flush thread 同時要 `dict(...)` 複製 + `clear()`。兩邊若不互斥,clear 可能漏算或讀到半更新。
- **`collect_and_reset` 是「讀完即歸零」**:讀走當前累積值並清空,所以每個 envelope 代表「**這 10 秒內**的增量」,dashboard 端不必自己做差分。
- **注意**:還有一個 `on_message_processed_with_size(..., size)` 會同時累積 bytes,但 **agent 目前沒接它**(見 5.7),所以實際輸出的 `total_bytes` 永遠是 0。要量 bytes 得自己改 agent 的 hook 去呼叫 size 版。
- 效能:這支的 per-message 成本實測約 165ns(含 agent 的 wrapper),等效上限約 6M msg/s,遠高於實際吞吐,所以不是瓶頸。鎖佔其中 ~50ns,單執行緒處理下幾乎無競爭。

### 5.4 `errors.py` — 攔截處理錯誤

```python
MAX_SAMPLES = 3

class ErrorInterceptor:
    def wrap_processing_error(self, original_callback):
        def wrapper(exc, row, logger_inst):
            self._record_error("processing", exc, context={
                "topic": getattr(row,"topic",None),
                "partition": getattr(row,"partition",None),
                "offset": getattr(row,"offset",None)})
            if original_callback is not None:
                return original_callback(exc, row, logger_inst)   # 原行為照舊
            return True
        return wrapper
```

- **裝飾器模式**:`wrap_processing_error` 收下 quix-streams 原本的錯誤 callback,回傳一個「先記錄、再轉呼叫原本」的新 callback。
- **記什麼**:用 `processing:<例外類別名>` 當 key,累積 **count** + 最多 `MAX_SAMPLES=3` 個樣本(型別、訊息、完整 traceback、topic/partition/offset、時間)。樣本上限避免高頻錯誤把記憶體灌爆。
- **`collect_and_reset`**:同 throughput,讀走 + 歸零。
- **⚠️ 行為保留性**:wrapper 會 `return original_callback(...)`,所以「錯誤要不要 suppress」沿用原本的決定。quix-streams 的預設 callback(`default_on_processing_error`)`return False` = **不 suppress、照樣 raise**(`app.py:1025` `if not to_suppress: raise`)。agent 在裝 hook 時,`app._on_processing_error` 早已被 `Application.__init__` 設成這個預設值(非 None),所以 wrapper 一定走 `return original_callback(...)` 這條 —— **不會改變你原本的錯誤行為**。只有當有人把它設成 `None` 時,fallback 的 `return True` 才會把錯誤吃掉(實務上不會發生)。

### 5.5 `resources.py` — CPU / 記憶體 / 磁碟

```python
class ResourceCollector:
    def __init__(self, state_dir=None):
        self._process = psutil.Process(os.getpid())
        self._process.cpu_percent()   # 第一次呼叫是「定錨」,之後才有意義

    def collect(self) -> dict:
        cpu = self._process.cpu_percent()           # 距上次 collect 的平均 CPU%
        mem = self._process.memory_info()           # rss / vms
        # 若有 state_dir:shutil.disk_usage() 拿整顆磁碟 total/used/free
        # state_dir 實際佔用(rglob 加總檔案大小)很貴 → 快取 STATE_DIR_CACHE_TTL=300s
```

- **`psutil.Process(pid)`**:量的是「**這支程式自己**」的 CPU/記憶體,不是整台機器。
- **`cpu_percent()` 的眉角**:它回的是「距離上一次呼叫到現在」的平均 CPU%。所以 `__init__` 裡先空呼叫一次定錨,否則第一筆會是 0 或不準。
- **state_dir 大小很貴所以快取**:`rglob("*")` 遞迴加總所有檔案 size,在大狀態目錄會很慢,因此 5 分鐘才算一次(`STATE_DIR_CACHE_TTL`),其餘時間回快取值。`disk_usage`(整顆磁碟)很便宜每次都算。
- 這支由 **resource thread 每 30s** 呼叫(比其他指標慢,因為資源變化沒那麼快、且較貴)。

### 5.6 `broker_health.py` — broker 連線狀態

```python
class BrokerHealthCollector:
    def collect(self, consumer) -> dict:
        broker_states = getattr(consumer, "_broker_states", {})           # {broker: "UP"/"DOWN"}
        unavailable_since = getattr(consumer, "_broker_unavailable_since", None)
        brokers = {name: {"state": s, "is_up": s == "UP"} for name, s in broker_states.items()}
        return {"brokers": brokers,
                "all_brokers_up": all(...) if brokers else False,
                "any_broker_unavailable_since": unavailable_since,
                "broker_count": len(brokers)}
```

- **資料來源是 quix-streams consumer 的內部狀態**:`consumer._broker_states` 與 `_broker_unavailable_since`(定義在 `quixstreams/kafka/consumer.py:131,135`,由 librdkafka 的事件更新)。
- collector 自己不主動探測 broker,只是**讀現成的狀態**,所以幾乎零成本。
- `getattr(..., default)`:萬一 quix-streams 版本沒這屬性也不會炸,回空集合(並 `logger.warning` 一次,`self._warned` 防洗版)。
- 由 flush thread 每 10s 收(在 `_flush_broker_health`)。

### 5.7 `dag_extractor.py` — 把 pipeline 拓撲挖出來

```python
def extract_dag(registry) -> dict:
    for topic_name, root_stream in registry._registry.items():     # 每個輸入 topic 一棵樹
        # 1) 建一個 topic 節點 "topic:<name>"
        # 2) root_stream.full_tree() 取得這條 SDF 的所有 Stream 節點
        # 3) 逐個 stream 建節點 "stream:<topic>:<i>",label = "<FuncType>: <func 名>"
        #    並把前一個節點連到這個節點(edge)
    return {"nodes": [...], "edges": [...], "topics": [...]}
```

- 把 quix-streams 的 `DataFrameRegistry` 翻譯成前端畫得出來的 **有向圖**:`nodes`(topic 節點 + 每個運算子節點)、`edges`(資料流向)、`topics`。
- **怎麼挖**:`registry._registry` 是 `{topic_name: root_stream}`;對每條 `root_stream.full_tree()` 攤平成節點序列,線性串起來。節點 label 取運算子類別名(如 `ApplyFunction`)+ 使用者函式的 `__qualname__`(如 `<lambda>`、`tokenize_and_count`)。
- **防呆**:`full_tree()` 失敗就退化成只有 root 一個節點(`try/except`);用 `seen_nodes`/`seen_edges` 去重(多個 topic 共用節點時不重複)。
- 由 agent 在 **`app.run()` 啟動瞬間**呼叫一次(拓撲是靜態的,不需週期送)。

### 5.8 `agent.py` — 總指揮(最後讀)

`MetricsAgent` 把上面全部黏起來。三件事:**解析設定 → 裝 hook → 開背景 thread**。

**(a) `__init__` — 組裝**
- 從 `app._config` 推導 `consumer_group` 與 `broker_address`(`broker_address.as_librdkafka_dict()["bootstrap.servers"]`)。
- new 出 exporter + 四個收集器;`ResourceCollector` 需要 state 目錄,從 `app._state_manager._state_dir` 拿(拿不到就降級、warning)。
- 呼叫 `_install_hooks()`,然後起兩條 `daemon=True` thread:`_flush_loop`(10s)、`_resource_loop`(30s)。

**(b) `_install_hooks` — monkey-patch 三個接點**
```python
# 1) 每筆訊息:包住原本的 _on_message_processed
original = app._on_message_processed                    # 預設是 None
def on_message_processed(topic, partition, offset):
    self._throughput.on_message_processed(topic, partition, offset)
    if original is not None: original(topic, partition, offset)
app._on_message_processed = on_message_processed

# 2) 處理錯誤:用 ErrorInterceptor 包住
app._on_processing_error = self._errors.wrap_processing_error(app._on_processing_error)

# 3) 啟動瞬間:patch app.run,先 emit DAG 再跑原本的 run
original_run = app.run
def patched_run(*a, **k):
    self._emit_dag(); return original_run(*a, **k)
app.run = patched_run
```
重點:全部都是「**保存原本的 → 換成包了一層的 → 包裡面再呼叫原本的**」,所以不破壞 quix-streams 既有行為,只是「搭便車」。

**(c) 背景 flush — 真正送資料的地方**
- `_flush_loop` 用 `self._stop_event.wait(interval)` 當計時器:回傳 `False` 代表 timeout(時間到該做事),回傳 `True` 代表被 `stop()` 設定了(該收工)。**這個寫法讓 `stop()` 能立刻喚醒、不用空等整個 interval。**
- 每輪呼叫 `_flush_throughput / _flush_errors / _flush_broker_health`;各自 `collect_and_reset()`(或 `collect()`)→ 包成 envelope `{type, consumer_group, timestamp, data}` → `exporter.emit()`。throughput 與 errors **沒資料就不送**(省流量)。
- `stop()`:set event → join 兩條 thread(各 timeout 5s)→ `exporter.flush()` 把殘留訊息送完。

**(d) envelope 統一長相**
```json
{ "type": "throughput", "consumer_group": "my-pipeline", "timestamp": 1700000000.1, "data": { ... } }
```
五種 `type`:`dag` / `throughput` / `errors` / `resources` / `broker_health`(各自的 `data` 結構見 README)。

### 5.9 `__init__.py` — 對外 API

```python
from .agent import MetricsAgent
from .kafka_exporter import KafkaMetricsExporter
from .protocol import MetricsCollector
__all__ = ["MetricsAgent", "KafkaMetricsExporter", "MetricsCollector"]
```
使用者只需要 `MetricsAgent`;另外兩個是給「想換 exporter / 做型別檢查」的進階用途。

---

## 6. 它跟 quix-streams 的耦合點(維護時最該盯的)

這套件靠**讀 / 改 quix-streams 的私有屬性**運作。這是它「零侵入」的代價 —— 升級 quix-streams 時這些是最可能壞的地方:

| 用到的私有 API | 在哪 | 用途 | 風險 |
|---|---|---|---|
| `app._on_message_processed` | app.py:351 設定 / :1037 呼叫 | 每筆訊息 hook | 預設 `None`;被 rename 就失效 |
| `app._on_processing_error` | app.py:352 / :1025 | 錯誤 hook | 預設 `default_on_processing_error`(return False=照 raise) |
| `app._config.consumer_group` / `.broker_address` | — | 推導設定 | config 結構若變 |
| `app._state_manager._state_dir` | — | state 磁碟用量 | 拿不到會降級 |
| `app._dataframe_registry` → `registry._registry` | — | 建 DAG | dict 結構 `{topic: root_stream}` |
| `stream.full_tree()` / `stream.func.func` | — | 攤平運算子、取函式名 | Stream 內部結構 |
| `app._consumer._broker_states` / `_broker_unavailable_since` | consumer.py:131,135 | broker 健康 | 有 `getattr` 防呆 |

> 讀 code 時若看到 `getattr(x, "...", default)`,通常就是作者知道「這是私有、可能不在」而加的防呆。

---

## 7. 併發模型(三條 thread)

- **主處理執行緒**(quix-streams 的)：呼叫 `on_message_processed` / 錯誤 wrapper —— 只做累積。
- **flush thread**(daemon,10s)：throughput / errors / broker_health。
- **resource thread**(daemon,30s)：CPU/mem/disk。

共享狀態(`ThroughputTracker`、`ErrorInterceptor` 的 dict)用 `threading.Lock` 保護;`collect_and_reset` 的「複製+清空」在鎖內完成,確保不漏算。daemon thread 讓你忘了 `stop()` 也不會卡住程式結束。

---

## 8. 測試怎麼讀 / 怎麼跑

`tests/` 三支,都是**單元測試、不需要真 Kafka**:

- `test_throughput.py` — 計數、bytes 版、reset 後歸零。
- `test_errors.py` — 攔截後仍轉呼叫原 callback、`MAX_SAMPLES=3` 上限、reset。
- `test_kafka_exporter.py` — 用 `MagicMock` 當 producer,驗 topic/key/JSON;驗「producer 爆了也不 raise」;驗 flush。

```bash
cd dashboard/collector
pip install -e ".[dev]"
pytest tests/ -v
```

讀測試是理解「**每個元件的契約**」最快的方法 —— 例如看 `test_emit_produces_message` 就懂 key 格式與 envelope 結構,不用通讀 exporter。

---

## 9. 一眼帶走

- **進入點**:`MetricsAgent(app)` → 裝 3 個 hook + 開 2 條背景 thread。
- **熱路徑只計數**,序列化/送 Kafka 都在背景週期做 → 對主程式幾乎無感。
- **5 種 envelope** 寫進 `__quix_metrics`,key=`{group}.{type}`。
- **收集器都長一樣**:累積 → `collect_and_reset()` 讀走歸零。
- **最脆的地方**是對 quix-streams 私有屬性的耦合(第 6 節),升級時優先回歸這裡。
