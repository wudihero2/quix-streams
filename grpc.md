# Quix Streams: 分散式架構與 gRPC 節點間傳輸 深度分析

## 目錄

1. [現有架構：單進程模型](#1-現有架構)
2. [group_by — 現有的「分散式」機制（透過 Kafka）](#2-group_by)
3. [哪些地方可以用 gRPC 替代 Kafka](#3-grpc-替代點)
4. [架構方案：gRPC 作為節點間傳輸層](#4-架構方案)
5. [State 管理的挑戰](#5-state-管理的挑戰)
6. [需要修改的核心抽象層](#6-需要修改的核心抽象層)
7. [參考：其他框架如何做分散式](#7-其他框架)
8. [結論與建議](#8-結論與建議)

---

## 1. 現有架構：單進程模型

### 1.1 Quix Streams 的處理模型

```
┌─────────────────────────────────────────────────┐
│                    Pod / Process                 │
│                                                  │
│   Consumer ──→ SDF Pipeline ──→ Sink             │
│      │              │                            │
│      │         State (RocksDB)                   │
│      │              │                            │
│      └── Checkpoint ┘                            │
│                                                  │
│   全部在同一個線程，同步執行                       │
└─────────────────────────────────────────────────┘
```

**`quixstreams/app.py` (_run_dataframe — 主循環)**
```python
def _run_dataframe(self, sink=None):
    # ...
    while run_tracker.running:
        if state_manager.recovery_required:
            state_manager.do_recovery()
        else:
            process_message(dataframes_composed)      # <-- 單線程同步
            processing_context.commit_checkpoint()
            consumer.resume_backpressured()
            source_manager.raise_for_error()
            printer.print()
            run_tracker.update_status()
```

### 1.2 多 Pod 擴展方式

目前 Quix 的「分散式」完全依賴 **Kafka Consumer Group**：

```
Pod 1 (consumer group: my-app)          Pod 2                   Pod 3
┌──────────────────────┐         ┌──────────────────┐    ┌──────────────────┐
│ partition 0, 1       │         │ partition 2, 3   │    │ partition 4, 5   │
│ Consumer → SDF → Sink│         │ Consumer → SDF   │    │ Consumer → SDF   │
│ State (RocksDB)      │         │ State (RocksDB)  │    │ State (RocksDB)  │
└──────────────────────┘         └──────────────────┘    └──────────────────┘
         │                                │                        │
         └────────────── Kafka ───────────┘────────────────────────┘
```

**Pod 之間不直接通訊**。所有資料流轉都經過 Kafka。

---

## 2. group_by — 現有的「分散式」機制

`group_by()` 是目前唯一涉及跨 Pod 資料轉移的操作。它透過 Kafka repartition topic 實現：

**`quixstreams/dataframe/dataframe.py` (group_by)**
```python
def group_by(self, key, name=None, ...):
    repartition_config = self._topic_manager.derive_topic_config(self._topics)

    if repartition_config.num_partitions == 1:
        return self._single_partition_groupby(operation, key)

    # 建立 repartition topic
    groupby_topic = self._topic_manager.repartition_topic(
        operation=operation,
        stream_id=self.stream_id,
        config=repartition_config,
        key_serializer=key_serializer,
        value_serializer=value_serializer,
        key_deserializer=key_deserializer,
        value_deserializer=value_deserializer,
    )

    # 把資料 produce 到 repartition topic（用新的 key）
    self.to_topic(topic=groupby_topic, key=self._groupby_key(key))
    # 過濾掉原始 SDF 的輸出
    self.filter(lambda _: False)

    # 建立新的 SDF 消費 repartition topic
    groupby_sdf = self.__dataframe_clone__(groupby_topic)
    self._registry.register_groupby(source_sdf=self, new_sdf=groupby_sdf)
    return groupby_sdf
```

**流程**：
```
Pod 1: partition 0              Pod 2: partition 1
  │                               │
  ▼                               ▼
filter(...)                    filter(...)
  │                               │
  ▼                               ▼
to_topic(repartition_topic,    to_topic(repartition_topic,
  key=new_key)                   key=new_key)
  │                               │
  └──────── Kafka ────────────────┘
           repartition_topic
  ┌──────────────┬────────────────┐
  │              │                │
  ▼              ▼                ▼
Pod 1: p0     Pod 2: p1       Pod 3: p2
consume from  consume from    consume from
repartition   repartition     repartition
```

### 2.1 Kafka 作為中間層的代價

| 優點 | 缺點 |
|------|------|
| 天然持久化、容錯 | 延遲高（寫 Kafka → 讀 Kafka） |
| 自動 rebalance | 需要額外 topic（磁碟空間） |
| exactly-once 語義 | 吞吐量受 Kafka broker 限制 |
| 解耦生產者和消費者 | 對 real-time 應用延遲不友善 |

---

## 3. 哪些地方可以用 gRPC 替代 Kafka

### 3.1 可替代的位置

```
                          可以用 gRPC
                              │
Source Topic ──→ [filter ──→ apply] ──→ <<<< gRPC >>>> ──→ [apply ──→ sink]
                   Pod 1                                      Pod 2
                                        ↑
                              取代 repartition topic
```

| 場景 | 現有方式 | gRPC 替代 |
|------|---------|-----------|
| group_by repartition | Kafka repartition topic | gRPC streaming，按 new key hash 路由到目標 Pod |
| 跨 Pod 資料傳輸 | 不支援 | gRPC bidirectional streaming |
| Source 注入 | Kafka Source topic | gRPC server 接收外部資料 |
| Sink 輸出 | 各種 Sink 實作 | gRPC client 推送到下游服務 |

### 3.2 不可替代的位置

| 場景 | 為什麼不能替代 |
|------|---------------|
| 原始 Source topic | 需要 Kafka 的持久化、replay、consumer group 語義 |
| Changelog topics | State recovery 依賴 Kafka 的持久化保證 |
| Offset commit | Kafka consumer group protocol |
| Exactly-once | 依賴 Kafka transactions |

---

## 4. 架構方案：gRPC 作為節點間傳輸層

### 4.1 方案 A：gRPC 替代 Repartition Topic

```
                   ┌─────── gRPC ──────┐
                   │                   │
Pod 1 (partition 0)│   Pod 2 (partition 1)
┌──────────────────┤   ├──────────────────┐
│ consume topic A  │   │ consume topic A  │
│      │           │   │      │           │
│      ▼           │   │      ▼           │
│  filter(...)     │   │  filter(...)     │
│      │           │   │      │           │
│      ▼           │   │      ▼           │
│  [gRPC Router]───┼───┤──[gRPC Router]  │
│      │           │   │      │           │
│      ▼           │   │      ▼           │
│  [gRPC Server]◄──┼───┤──[gRPC Server]  │
│      │           │   │      │           │
│      ▼           │   │      ▼           │
│  apply(stateful) │   │  apply(stateful) │
│      │           │   │      │           │
│      ▼           │   │      ▼           │
│  sink(DB)        │   │  sink(DB)        │
└──────────────────┘   └──────────────────┘
```

**gRPC Proto 定義**：
```protobuf
syntax = "proto3";

service StreamTransfer {
    // 雙向 streaming
    rpc Transfer(stream TransferRequest) returns (stream TransferResponse);
}

message TransferRequest {
    bytes key = 1;
    bytes value = 2;
    int64 timestamp = 3;
    map<string, bytes> headers = 4;
    int32 target_partition = 5;
}

message TransferResponse {
    bool ack = 1;
}
```

**gRPC Router 的邏輯**（替代 `to_topic`）：
```python
import grpc
import hashlib

class GrpcRouter:
    def __init__(self, pod_addresses: list[str], num_partitions: int):
        """
        pod_addresses: ["pod-0:50051", "pod-1:50051", "pod-2:50051"]
        """
        self._channels = {
            i: grpc.insecure_channel(addr)
            for i, addr in enumerate(pod_addresses)
        }
        self._stubs = {
            i: StreamTransferStub(ch)
            for i, ch in self._channels.items()
        }
        self._num_partitions = num_partitions

    def route(self, key: bytes, value: bytes, timestamp: int):
        """按 key 的 hash 路由到目標 Pod"""
        target_partition = int(hashlib.md5(key).hexdigest(), 16) % self._num_partitions
        target_pod = target_partition  # 假設 partition:pod = 1:1

        self._stubs[target_pod].Transfer(
            TransferRequest(
                key=key,
                value=value,
                timestamp=timestamp,
                target_partition=target_partition,
            )
        )
```

### 4.2 方案 B：gRPC 作為通用 Source/Sink

```python
# gRPC Source — 接收其他 Pod 推送的資料
class GrpcSource(BaseSource):
    def __init__(self, port: int):
        super().__init__()
        self._port = port

    def run(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
        add_StreamTransferServicer_to_server(
            GrpcReceiver(self), server
        )
        server.add_insecure_port(f"[::]:{self._port}")
        server.start()
        server.wait_for_termination()

class GrpcReceiver(StreamTransferServicer):
    def __init__(self, source: GrpcSource):
        self._source = source

    def Transfer(self, request_iterator, context):
        for request in request_iterator:
            # 把收到的資料注入到 Quix pipeline
            self._source.produce(
                key=request.key,
                value=request.value,
                timestamp=request.timestamp,
            )
            yield TransferResponse(ack=True)
```

```python
# gRPC Sink — 把資料推送到另一個 Pod
class GrpcSink(BaseSink):
    def __init__(self, target_address: str):
        super().__init__()
        self._target = target_address
        self._buffer = []

    def add(self, value, key, timestamp, headers, topic, partition, offset):
        self._buffer.append((key, value, timestamp))

    def flush(self):
        channel = grpc.insecure_channel(self._target)
        stub = StreamTransferStub(channel)

        def request_generator():
            for key, value, timestamp in self._buffer:
                yield TransferRequest(
                    key=key, value=value, timestamp=timestamp
                )

        responses = stub.Transfer(request_generator())
        for resp in responses:
            if not resp.ack:
                raise SinkBackpressureError(retry_after=5.0)

        self._buffer.clear()
```

### 4.x 方案 A vs 方案 B 的本質差異

**一句話**：方案 A 是改 Quix 內部（替換 repartition 機制），方案 B 是用 Quix 外部介面（Source/Sink）串接兩個獨立 Application。

---

**方案 A：gRPC 替代 Repartition Topic**

```
       同一個 Application 內部
┌──────────────────────────────────────────────────┐
│                                                  │
│  Kafka ──▶ filter ──▶ [gRPC Router] ──▶ [gRPC Server] ──▶ aggregate ──▶ sink │
│                          │                    ▲                                │
│                          │    gRPC（替代原本   │                                │
│                          └──  Kafka repartition ┘                              │
│                               topic 的角色）                                    │
│                                                                                │
│  ★ group_by() 背後的實作從「寫 Kafka topic → 讀 Kafka topic」                    │
│    變成「gRPC 直連目標 Pod」                                                     │
│  ★ 需要修改 Quix 內部的 group_by 機制、checkpoint、state recovery                │
└──────────────────────────────────────────────────┘
```

對應現有 Quix 的修改點：

```python
# 現在的 group_by（透過 Kafka）：
sdf = sdf.group_by("user_id")
# 底層：sdf.to_topic(repartition_topic) → consumer 再從 repartition_topic 讀

# 方案 A（透過 gRPC）：
sdf = sdf.group_by("user_id")
# 底層：sdf → GrpcRouter.route(key) → 目標 Pod 的 GrpcServer 接收 → 繼續 pipeline
# ★ 不經過 Kafka，延遲更低
# ★ 但資料「在途」時沒有持久化（TCP 記憶體 buffer）
```

**方案 B：gRPC 作為通用 Source/Sink**

```
    Application 1（獨立 process）              Application 2（獨立 process）
┌─────────────────────────────────┐       ┌─────────────────────────────────┐
│                                 │       │                                 │
│ Kafka ──▶ filter ──▶ GrpcSink ─┼──gRPC──▶ GrpcSource ──▶ aggregate ──▶ DB│
│                                 │       │                                 │
│ 自己的 checkpoint               │       │ 自己的 checkpoint               │
│ 自己的 consumer group           │       │ 自己的 consumer group           │
│ 自己的 state                    │       │ 自己的 state                    │
└─────────────────────────────────┘       └─────────────────────────────────┘

★ 兩個完全獨立的 Quix Application
★ 不需要修改 Quix 內部任何東西
★ GrpcSink 和 GrpcSource 只是使用者自定義的 Sink/Source
```

---

**核心差異對比**：

```
                          方案 A                               方案 B
                     (替代 Repartition)                    (通用 Source/Sink)
                   ──────────────────────              ──────────────────────
gRPC 扮演的角色     Quix 內部的 shuffle 通道              兩個獨立 App 間的連接線

需要改 Quix 源碼？  ★ 需要，改 group_by、                 ★ 不需要
                   checkpoint、state recovery             純用 BaseSink/BaseSource API

幾個 Application？  1 個（內部用 gRPC shuffle）            2 個（各自獨立）

Pipeline 結構       Source → filter → [gRPC] → agg → Sink  App1: Source → filter → GrpcSink
                   （一條完整 pipeline）                     App2: GrpcSource → agg → Sink
                                                            （兩條獨立 pipeline）

資料流              Pod 之間 gRPC 直連                      App1 flush 時 → gRPC → App2
                   （record-at-a-time）                     （批次或逐筆，取決於 Sink 模式）

Checkpoint          ★ 很複雜：                             ★ 各自獨立，簡單
                   gRPC 在途資料沒有持久化                  App1 commit offset 時確認 gRPC 送達
                   需要類似 barrier 的機制                  App2 有自己的 offset 追蹤
                   來保證一致性                             （但 GrpcSource 不是 Kafka，
                                                            沒有 offset，失敗時如何 replay？）

State recovery      ★ 很複雜：                             各自獨立，跟現在一樣
                   repartition 後的 state
                   要從哪裡 recovery？
                   Kafka changelog 還在嗎？

延遲                ★ 更低：gRPC 直連，                    較高：要經過 Sink flush
                   省掉 Kafka broker 的                    （BatchingSink = commit_interval
                   produce → persist → consume              StreamingSink = 幾十毫秒）

持久化保證          ★ 無：gRPC TCP buffer 掉了就掉了         取決於實作
                   需要自己做重試或 WAL                     （GrpcSink flush 失敗 → backpressure
                                                            → seek back → 重新處理）

適合場景            想把 Quix 改造成類似 Flink              想把多個 Quix App 串接起來
                   的分散式框架                            （微服務風格）
```

---

**具體例子：計算每個 user 的消費總額**

```python
# 方案 A：一個 Application，gRPC 替代 repartition（需改源碼）
app = Application(broker_address="...", consumer_group="my-app")
sdf = app.dataframe(topic)
sdf = sdf.group_by("user_id")          # ★ 底層走 gRPC 而不是 Kafka topic
sdf = sdf.apply(lambda v, state: ..., stateful=True)
sdf.sink(PostgreSQLSink(...))
app.run()
# 寫起來跟現在一模一樣，但 group_by 內部用 gRPC
```

```python
# 方案 B：兩個獨立 Application，gRPC 串接（不改源碼）

# App 1: 過濾 + 按 user_id 路由
app1 = Application(broker_address="...", consumer_group="app1")
sdf = app1.dataframe(topic)
sdf = sdf.filter(lambda v: v["amount"] > 0)
sdf.sink(GrpcSink(target="app2-pod:50051"))   # ★ 自定義 Sink
app1.run()

# App 2: 接收 + 聚合
app2 = Application(broker_address="...", consumer_group="app2")
source = GrpcSource(port=50051)                 # ★ 自定義 Source
sdf = app2.dataframe(source)
sdf = sdf.apply(lambda v, state: ..., stateful=True)
sdf.sink(PostgreSQLSink(...))
app2.run()
```

**方案 B 的問題**：App2 的 `GrpcSource` 收到的資料來自 gRPC 而不是 Kafka。
如果 App2 crash，它無法從 Kafka offset seek back 重新取得那些資料。
你需要自己在 GrpcSource 中實作某種 buffer/replay 機制，或者接受資料遺失風險。

**方案 A 的問題**：需要大幅修改 Quix 內部，而且 gRPC 在途資料沒有 Kafka 的持久化保證。
本質上是在重新發明 Flink 的 shuffle layer。

---

### 4.3 方案 C：完整分散式架構（類似 Flink）

```
                          Coordinator (Python)
                          ┌─────────────────────┐
                          │ - 任務分配            │
                          │ - 節點健康檢查        │
                          │ - Checkpoint 協調     │
                          │ - gRPC 控制面板       │
                          └──────────┬──────────┘
                                     │ gRPC
                  ┌──────────────────┼──────────────────┐
                  │                  │                   │
           Worker 1            Worker 2            Worker 3
     ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
     │ Task: filter     │  │ Task: filter     │  │ Task: aggregate  │
     │ partition: 0,1   │  │ partition: 2,3   │  │ partition: 0-3   │
     │                  │  │                  │  │                  │
     │ gRPC Server ◄────┼──┼── gRPC Client ──►│──│ gRPC Server      │
     │ gRPC Client ─────┼──┼── gRPC Server    │  │                  │
     │                  │  │                  │  │                  │
     │ State (RocksDB)  │  │ State (RocksDB)  │  │ State (RocksDB)  │
     └─────────────────┘  └─────────────────┘  └─────────────────┘
                  │                  │                   │
                  └──────── Kafka (source/sink only) ────┘
```

**Coordinator 的 gRPC 服務**：
```protobuf
service Coordinator {
    rpc RegisterWorker(WorkerInfo) returns (Assignment);
    rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);
    rpc InitiateCheckpoint(CheckpointRequest) returns (CheckpointResponse);
    rpc ReportCheckpointComplete(CheckpointComplete) returns (Ack);
}

service Worker {
    rpc TransferData(stream DataRecord) returns (stream Ack);
    rpc TriggerCheckpoint(CheckpointBarrier) returns (CheckpointAck);
    rpc RestoreState(StateSnapshot) returns (Ack);
}
```

### 4.y 方案 C 的挑戰：不只是 State 管理

方案 C 是把 Quix 改造成一個完整的分散式框架（類似 Flink）。State 管理只是冰山一角，
它要面對的是**分散式系統的所有經典問題**。

回顧方案 C 的架構：

```
Worker 1 (filter, partition 0,1)
Worker 2 (filter, partition 2,3)        gRPC shuffle
Worker 3 (aggregate, partition 0-3) ◄────────────────
     ↑
     └── State (RocksDB): {Alice→150, Bob→200, ...}
```

Worker 3 的 state 會收到來自 Worker 1 和 Worker 2 的資料（跟 Flink 的 keyBy 一樣）。

---

**方案 A、B、C 面對的問題數量比較**：

```
                        方案 A          方案 B           方案 C
                     (替代 repartition)  (Source/Sink)   (完整分散式)
                     ────────────────   ─────────────   ─────────────
1. 在途資料遺失        ★ 有             ★ 有            ★ 有
2. State 歸屬          簡單             各自獨立         ★ 複雜
3. Checkpoint 一致性   ★ 複雜           各自獨立         ★ 非常複雜
4. Rebalance          靠 Kafka         不需要           ★ 自己實作
5. 服務發現            ★ 需要           簡單(固定 addr)  ★ 需要
6. Exactly-once       靠 Kafka txn     各自獨立         ★ 自己實作 2PC
7. Failure detection  靠 Kafka         各自獨立         ★ 自己實作
8. Task scheduling    不需要           不需要           ★ 自己實作
9. Barrier 機制        不需要           不需要           ★ 自己實作
```

方案 C 除了 state 管理，還多了 5 個方案 A、B 沒有的問題（4, 7, 8, 9，以及 6 的難度升級）。
逐一說明：

---

**問題 1：Checkpoint 一致性 — 需要自建 Barrier**

方案 A、B 的每個 pod/app 還是單 thread，checkpoint 天然一致（如前面分析的）。

方案 C 打破了這個前提：Worker 3 (aggregate) 的 state 依賴 Worker 1 和 Worker 2 的資料，
跟 Flink 一模一樣 — **需要 barrier 來對齊 checkpoint**。

```
Coordinator: "做 checkpoint cp=5"
     │
     ▼
Worker 1 (filter):  ... [data] [data] |barrier_cp5| [data] ...
Worker 2 (filter):  ... [data] |barrier_cp5| [data] [data] ...
     │                               │
     └─── gRPC ──────────────────────┘
                    │
                    ▼
Worker 3 (aggregate):
  收到 Worker 1 的 barrier → 暫停處理 Worker 1 的資料
  收到 Worker 2 的 barrier → 兩邊 barrier 都到了 → snapshot state
  ★ 跟 Flink 的 Chandy-Lamport 完全一樣
```

你需要自己實作：
- Coordinator 觸發 checkpoint
- Source worker 注入 barrier 到 gRPC 資料流
- Downstream worker 做 barrier alignment
- 全部 worker 回報 snapshot 完成
- Coordinator 確認 checkpoint 成功

這就是 Flink 的 CheckpointCoordinator，花了多年才穩定。

---

**問題 2：Rebalance — Worker 掛了怎麼辦？**

現在 Quix 靠 Kafka consumer group 自動 rebalance：一個 pod 掛了，
Kafka 把它的 partition 分給其他 pod，新 pod 從 changelog 恢復 state。

方案 C 的 Worker 不只消費 Kafka，還互相 gRPC 連接：

```
Worker 1 掛了：
  - Worker 3 跟 Worker 1 的 gRPC 連線斷了
  - Worker 3 收不到 Worker 1 負責的 partition 0,1 的資料了
  - 誰來接手？

需要自己實作：
  1. Coordinator 偵測 Worker 1 心跳超時
  2. 把 Worker 1 的 task (filter partition 0,1) 重新分配給 Worker 4（或 Worker 2）
  3. Worker 4 需要：
     a. subscribe Kafka partition 0,1
     b. 恢復 Worker 1 的 state（如果 filter 有 state）
     c. 跟 Worker 3 建立新的 gRPC 連線
     d. Worker 3 要知道「Worker 1 的資料現在從 Worker 4 來了」
  4. 正在進行的 checkpoint 可能要 abort 重來
```

Kafka consumer group 只需要一個 `group.coordinator`，自動處理 join/leave/rebalance。
方案 C 要自己把這整套搬到 gRPC 上。

---

**問題 3：Exactly-once — 跨 Worker 的原子性**

現在 Quix 的 exactly-once 靠 Kafka transaction：
`produce changelog + commit offset` 在同一個 transaction 裡。

方案 C 的資料流經 gRPC，不經過 Kafka：

```
Worker 1 → gRPC → Worker 3 → state update → checkpoint

問題：Worker 3 checkpoint 時要 commit 什麼？
  - Worker 3 沒有 Kafka offset（資料來自 gRPC）
  - Worker 1 有 Kafka offset，但 Worker 1 和 Worker 3 的 checkpoint 要原子性
  - 需要 2PC (Two-Phase Commit)：
    Phase 1: Worker 1 和 Worker 3 都 pre-commit
    Phase 2: Coordinator 確認都 OK → 都 commit
    如果 Worker 3 pre-commit 失敗 → 都 abort
```

Kafka transaction 是 Kafka broker 幫你做的 2PC，免費的。
方案 C 要自己在 gRPC 上實作 2PC，或者用一個外部的 transaction coordinator。

---

**問題 4：Task Scheduling — 哪個 Worker 跑哪個 Task？**

方案 A、B 不需要 task scheduling — 每個 pod 就是一個完整的 Application。

方案 C 把 pipeline 拆成多個 task 分配到不同 Worker：

```
Pipeline: Source → filter → group_by → aggregate → sink

需要決定：
  - filter 要幾個 parallelism？跑在哪些 Worker？
  - aggregate 要幾個 parallelism？跑在哪些 Worker？
  - 一個 Worker 能同時跑 filter 和 aggregate 嗎？
  - Worker 的 CPU/memory 夠嗎？
  - 如果 aggregate 是瓶頸，能不能只 scale aggregate？
```

這就是 Flink 的 JobManager + ResourceManager 的職責。

---

**問題 5：Failure Detection**

```
Coordinator 怎麼知道 Worker 掛了？

方案：心跳機制
  Worker → Coordinator: Heartbeat(worker_id, timestamp) 每 N 秒
  Coordinator: 如果 M 秒沒收到 → 認定 Worker 掛了 → 觸發 rebalance

但是：
  - 網路抖動 ≠ Worker 掛了（false positive → 不必要的 rebalance）
  - Worker 在做 GC pause（Python 不常見但 JVM 很常見）
  - Split brain：Worker 還活著但 Coordinator 認為它掛了
    → 兩個 Worker 同時處理同一個 partition → 資料重複或 state 衝突
```

---

**總結：方案 C 要自己建的東西**

| 元件 | 對應 Flink | Quix 現在有嗎 | 難度 |
|---|---|---|---|
| CheckpointCoordinator | CheckpointCoordinator | ❌（靠 periodic + Kafka txn） | 非常高 |
| Barrier alignment | Chandy-Lamport variant | ❌（單 thread 不需要） | 非常高 |
| Task scheduling | JobManager + Scheduler | ❌（一個 pod 一個 pipeline） | 高 |
| Failure detection | HeartbeatManager | ❌（靠 Kafka consumer group） | 中 |
| Rebalance/Failover | TaskRestart + State Recovery | ❌（靠 Kafka rebalance + changelog） | 非常高 |
| 2PC / Exactly-once | Kafka Transaction + Epoch fencing | ❌（靠 Kafka 原生） | 非常高 |
| Network shuffle | Netty-based ShuffleEnvironment | ❌（靠 Kafka repartition topic） | 高 |
| Backpressure 跨節點 | Credit-based flow control | ❌（靠 Kafka consumer pause） | 高 |

**State 管理只是其中一項。** 方案 C 本質上是要從零打造一個分散式計算框架，
相當於重寫 Flink 的核心。Flink 有上百人的團隊花了 10 年才做到穩定。

這也是為什麼第 8 節結論建議：如果需要完整分散式，直接用 Flink/Bytewax，不要在 Quix 上造輪子。

---

## 5. State 管理的挑戰

### 5.1 現有模型：State 與 Partition 綁定

**場景**：計算每個 user 的累計消費金額。

```python
app = Application(broker_address="...", consumer_group="my-app")
topic = app.topic("orders")
sdf = app.dataframe(topic)

def accumulate(value, state: State):
    total = state.get("total", 0)
    total += value["amount"]
    state.set("total", total)
    return {**value, "running_total": total}

sdf = sdf.apply(accumulate, stateful=True)
```

Quix 的 state 是嚴格按 `(stream_id, partition)` 隔離的。從源碼看：

```python
# quixstreams/state/base/store.py（完整源碼）
class Store(ABC):
    def __init__(self, name: str, stream_id: Optional[str]) -> None:
        self._name = name
        self._stream_id = stream_id
        self._partitions: Dict[int, StorePartition] = {}   # ★ 按 partition number 索引

    def assign_partition(self, partition: int) -> StorePartition:
        """Assign new store partition"""
        store_partition = self._partitions.get(partition)
        if store_partition is not None:
            return store_partition                          # ★ 已存在就直接返回
        store_partition = self.create_new_partition(partition)
        self._partitions[partition] = store_partition       # ★ 一個 partition 一個 StorePartition
        return store_partition

    def start_partition_transaction(self, partition: int) -> PartitionTransaction:
        store_partition = self._partitions.get(partition)
        if store_partition is None:
            raise PartitionNotAssignedError(...)            # ★ 只能操作已 assign 的 partition
        return store_partition.begin()

    def revoke_partition(self, partition: int):
        store_partition = self._partitions.pop(partition, None)
        if store_partition is None:
            return
        store_partition.close()                             # ★ revoke 時關閉 RocksDB
```

```python
# quixstreams/state/rocksdb/store.py（完整源碼）
class RocksDBStore(Store):
    def __init__(self, name, stream_id, base_dir, ...):
        partitions_dir = Path(base_dir).absolute() / self._name
        if self._stream_id:
            partitions_dir = partitions_dir / self._stream_id
        self._partitions_dir = partitions_dir
        # ★ 磁碟路徑：{state_dir}/{store_name}/{stream_id}/{partition}/
        # 例如：/data/state/default/orders/0/  ← partition 0 的 RocksDB

    def create_new_partition(self, partition: int) -> RocksDBStorePartition:
        path = str((self._partitions_dir / str(partition)).absolute())
        # ★ 每個 partition 有自己的 RocksDB 目錄
        return RocksDBStorePartition(path=path, options=self._options, ...)
```

```python
# quixstreams/state/manager.py — StateStoreManager（關鍵方法）
class StateStoreManager:
    # ★ _stores 結構：{stream_id: {store_name: Store}}
    _stores: Dict[Optional[str], Dict[str, Store]] = {}

    def on_partition_assign(self, stream_id, partition, committed_offsets):
        """Kafka rebalance assign callback 觸發"""
        store_partitions = {}
        for name, store in self._stores.get(stream_id, {}).items():
            store_partition = store.assign_partition(partition)  # ★ 開啟 RocksDB
            store_partitions[name] = store_partition
        if self._recovery_manager and store_partitions:
            self._recovery_manager.assign_partition(            # ★ 觸發 changelog recovery
                topic=stream_id,
                partition=partition,
                committed_offsets=committed_offsets,
                store_partitions=store_partitions,
            )
        return store_partitions

    def on_partition_revoke(self, stream_id, partition):
        """Kafka rebalance revoke callback 觸發"""
        if self._recovery_manager:
            self._recovery_manager.revoke_partition(partition_num=partition)
        for store in self._stores.get(stream_id, {}).values():
            store.revoke_partition(partition=partition)          # ★ 關閉 RocksDB
```

**現在的狀態**：state 完全綁定 Kafka partition，lifecycle 跟著 Kafka rebalance 走。

用具體例子畫出來：

```
topic "orders" 有 4 個 partition，3 個 Pod

Pod A (partition 0, 1):
  磁碟：
    /data/state/default/orders/0/    ← RocksDB: {Alice→150, Carol→30}
    /data/state/default/orders/1/    ← RocksDB: {Bob→200, Dave→50}

Pod B (partition 2):
  磁碟：
    /data/state/default/orders/2/    ← RocksDB: {Eve→100}

Pod C (partition 3):
  磁碟：
    /data/state/default/orders/3/    ← RocksDB: {Frank→80}

★ 每個 Pod 只能讀寫自己被 assign 的 partition 的 state
★ Pod A 不可能讀到 Pod B 的 Eve 的資料
★ Alice 在哪個 Pod？取決於她的訊息在哪個 Kafka partition
```

---

### 5.2 挑戰一：gRPC 替代 Repartition 後，State 歸誰？

現在用 Kafka repartition 的 `group_by` 流程：

```python
sdf = app.dataframe(topic)           # topic "orders", 4 partitions
sdf = sdf.group_by("user_id")        # 產生 repartition topic "repartition__user_id"
sdf = sdf.apply(accumulate, stateful=True)
```

```
Kafka 處理：
  orders partition 0 的訊息 {user: "Alice", amount: 100}
    → group_by → produce 到 repartition topic
    → hash("Alice") % 4 = 2 → repartition partition 2
    → Pod B 消費 repartition partition 2
    → Pod B 的 state store (stream_id="repartition__user_id", partition=2)
    → RocksDB: /data/state/default/repartition__user_id/2/

★ State 歸屬很清楚：
  repartition topic 的 partition 決定 state 在哪個 Pod
  Kafka consumer group 管理 assign/revoke
  Changelog topic 用來 recovery
```

**如果改用 gRPC 替代 repartition**：

```
orders partition 0 的訊息 {user: "Alice", amount: 100}
  → group_by → gRPC Router → hash("Alice") → 送到 Pod B
  → Pod B 的 gRPC Server 接收
  → accumulate(value, state) → state.set("total", 250)

問題：這個 state 的 key 是什麼？
  → 沒有 repartition topic，所以沒有 repartition partition number
  → state 不知道自己屬於哪個 stream_id 和 partition
  → StateStoreManager.on_partition_assign() 不會被呼叫
     （因為 gRPC 不經過 Kafka consumer group）
  → RocksDB 路徑怎麼決定？
  → Changelog topic 叫什麼名字？
  → Recovery 時從哪個 changelog 恢復？
```

**具體問題**：

```python
# 現有流程：Kafka rebalance 觸發 state assign
def _on_assign(self, _, topic_partitions):
    for tp in topic_partitions:
        self._state_manager.on_partition_assign(
            stream_id=tp.topic,           # ★ repartition topic name
            partition=tp.partition,        # ★ repartition partition number
            committed_offsets=...,
        )

# gRPC 流程：沒有 Kafka rebalance！
# 誰來觸發 on_partition_assign？
# stream_id 是什麼？gRPC 沒有 topic 的概念
# partition 是什麼？gRPC 沒有 partition 的概念
```

---

### 5.3 挑戰二：State Recovery — 從哪裡恢復？

現在的 recovery 流程：

```
Pod B crash → 重啟 → Kafka rebalance → Pod B 被 assign partition 2
  → StateStoreManager.on_partition_assign(stream_id="repartition__user_id", partition=2)
  → RecoveryManager.assign_partition(...)
  → 消費 changelog topic "changelog__repartition__user_id--default" partition 2
  → 逐筆 apply changelog 訊息到 RocksDB
  → 恢復完成 → 繼續處理
```

**如果用 gRPC，changelog 怎麼辦？**

```
方案 1：gRPC 傳輸，但仍然 produce changelog 到 Kafka

  Pod A → gRPC → Pod B
                   │
                   ├─ 更新本地 RocksDB
                   └─ produce changelog 到 Kafka topic  ← ★ 仍然需要 Kafka

  Pod B crash → 重啟 → 從 changelog 恢復
  ★ 可行，但 gRPC 只省了 repartition topic 的 produce/consume
    changelog topic 還是要走 Kafka
    省的延遲有限

方案 2：gRPC 傳輸，不用 changelog

  Pod B crash → 重啟 → state 怎麼恢復？
    → 讓 Pod A 重新把資料 gRPC 送一遍？
    → Pod A 的資料早就處理過了，offset 已經 commit
    → 需要 Pod A 從 Kafka source topic 重新消費 → 全量重新處理
    → ❌ 太慢了

方案 3：gRPC 傳輸 + State 定期 snapshot 到 S3/HDFS

  每次 checkpoint 把 RocksDB snapshot 上傳到 S3
  Pod B crash → 重啟 → 從 S3 下載最近的 snapshot → 從 snapshot 之後繼續
  ★ 這就是 Flink 的做法（Flink 用 HDFS/S3 存 state snapshot）
  ★ 但需要自建 snapshot 上傳/下載 + 版本管理
```

---

### 5.4 挑戰三：Rebalance — State 搬家

```
場景：3 Pod 變 4 Pod（scale up）

之前：
  Pod A: partition 0, 1 → state: {Alice→150, Bob→200}
  Pod B: partition 2    → state: {Eve→100}
  Pod C: partition 3    → state: {Frank→80}

Kafka rebalance 後：
  Pod A: partition 0    → state: {Alice→150}     ← 只保留 partition 0 的
  Pod B: partition 1    → state: {Bob→200}       ← 從哪來的？
  Pod C: partition 2    → state: {Eve→100}       ← 之前是 Pod B 的
  Pod D: partition 3    → state: {Frank→80}      ← 之前是 Pod C 的
```

**現在 Quix 靠 Kafka 解決**：

```
1. Pod A 被 revoke partition 1
   → StateStoreManager.on_partition_revoke("orders", 1)
   → store.revoke_partition(1)  → 關閉 partition 1 的 RocksDB

2. Pod B 被 assign partition 1
   → StateStoreManager.on_partition_assign("orders", 1, committed_offsets)
   → store.assign_partition(1)  → 建新的 RocksDB
   → RecoveryManager 消費 changelog topic partition 1
   → 逐筆 apply → 恢復 {Bob→200}

★ State 不需要「搬家」— 新 Pod 從 changelog 重建
★ 代價是 recovery 時間（取決於 changelog 大小）
```

**gRPC 方案的問題**：

```
如果 gRPC 替代了 repartition topic，changelog 可能不存在或不完整。

方案 A（仍有 changelog）：
  跟現在一樣，revoke → assign → 從 changelog 恢復
  ★ 可行，但 changelog 是必須的（不能省掉）

方案 B（沒有 changelog，用 state transfer）：
  Pod A 被 revoke partition 1
    → 把 partition 1 的 RocksDB snapshot（SST files）打包
    → gRPC 傳送給 Pod B
    → Pod B 直接載入 snapshot
  ★ 比 changelog replay 快（直接傳檔案 vs 逐筆 apply）
  ★ 但需要自建 state snapshot 傳輸協議
  ★ 傳輸期間 partition 1 的訊息怎麼處理？暫停？丟棄？

方案 C（Flink 的做法）：
  Checkpoint 時把 state snapshot 上傳 S3
  Rebalance 後新 Pod 從 S3 下載 snapshot
  ★ 不需要 Pod-to-Pod 傳輸
  ★ 但需要分散式檔案系統 + snapshot 管理
```

---

### 5.5 挑戰四：Exactly-once 與 State

```
場景：Pod B 處理 Alice 的訊息，更新 state，然後 crash

時間線：
T1: Pod A → gRPC → Pod B: {user: "Alice", amount: 100}
T2: Pod B: state.set("total", 250)    ← 寫入 RocksDB cache
T3: Pod B: crash!

問題：T2 的 state 更新有沒有持久化？
```

**現在 Quix 的 exactly-once**：

```
T1: 消費 Kafka → 處理 → state 寫入 cache
T2: checkpoint:
    → produce changelog（在 Kafka transaction 內）
    → commit offset（在同一個 Kafka transaction 內）
    → flush state to RocksDB
    ★ 三者要嘛全部成功，要嘛全部 abort

T3: crash after T1 but before T2:
    → transaction 沒有 commit → offset 沒有推進
    → 重啟後從上次 committed offset 重新消費
    → state 從 changelog 恢復到上次 checkpoint 的狀態
    ★ 一致的
```

**gRPC 方案的問題**：

```
T1: Pod A 消費 Kafka offset=100 → gRPC 送給 Pod B
T2: Pod B 收到 → state.set("total", 250)
T3: Pod A checkpoint → commit offset=100 到 Kafka

問題：Pod A 已經 commit offset=100，
      但 Pod B 的 state 還沒 checkpoint（Pod B 有自己的 checkpoint 節奏）
      → Pod B crash → state 丟失
      → Pod A 不會重新消費 offset=100（已 commit）
      → Alice 的 100 元消失了 ❌

需要的解法：
  Pod A 和 Pod B 的 checkpoint 必須是原子性的：
    要嘛 Pod A commit offset + Pod B flush state 一起成功
    要嘛都 abort

  ★ 這就是分散式 2PC（Two-Phase Commit）
  ★ 或者用 barrier 機制確保 Pod A 和 Pod B 在同一個邏輯點 checkpoint
```

---

### 5.6 總結：State 管理的四個挑戰

| 挑戰 | 現在（Kafka repartition） | gRPC 替代後 |
|---|---|---|
| **State 歸屬** | Kafka partition 決定 | 需要自定義「虛擬 partition」 |
| **Recovery** | Changelog topic replay | 仍需 changelog 或自建 snapshot |
| **Rebalance** | Kafka assign/revoke + changelog | 自建 state transfer 或仍靠 changelog |
| **Exactly-once** | Kafka transaction 原子性 | 需自建分散式 2PC 或 barrier |

**核心矛盾**：Quix 的整個 state 系統（`Store` → `StorePartition` → `PartitionTransaction` → `ChangelogProducer` → `RecoveryManager`）
都是圍繞 Kafka 設計的。`partition` 這個概念貫穿了所有層：

```
StateStoreManager._stores = {
    "repartition__user_id": {            ← stream_id = Kafka topic name
        "default": RocksDBStore(
            _partitions = {
                0: RocksDBStorePartition,  ← partition = Kafka partition number
                1: RocksDBStorePartition,
            }
        )
    }
}
```

用 gRPC 替代 Kafka repartition 不是簡單地「換一個傳輸層」，
而是要**重新定義 state 的 identity（stream_id + partition）、lifecycle（assign/revoke）、
recovery（changelog）和一致性保證（exactly-once）**。

### 5.7 最務實的解法：混合架構

**保留 Kafka 做 state recovery 和 exactly-once，用 gRPC 只做熱路徑加速**：

```
正常處理（熱路徑）：
  Pod A ──gRPC──→ Pod B (low latency，不經過 Kafka broker)

同時：
  Pod A ──produce──→ repartition topic (仍然寫 Kafka，但 Pod B 不需要消費它)
  ★ repartition topic 只用於 recovery，不用於正常處理

Checkpoint：
  Pod A: commit Kafka offset + produce changelog
  Pod B: flush state to RocksDB
  ★ 用 Kafka transaction 保證原子性（Pod A 的 offset + changelog）
  ★ Pod B 的 state flush 在自己的 checkpoint 中

State Recovery：
  Pod B crash → 重啟 → 從 changelog topic 恢復（不是從 gRPC replay）
  ★ 跟現在完全一樣

好處：
  - 正常處理走 gRPC → 低延遲
  - Recovery 走 Kafka changelog → 已驗證的機制
  - Exactly-once 靠 Kafka transaction → 不需要自建 2PC

代價：
  - repartition topic 還是要寫（雙寫：gRPC + Kafka）
  - 多了 Kafka produce 的開銷（但不影響熱路徑延遲，因為可以 async produce）
  - 複雜度增加（要管理 gRPC 連線 + Kafka repartition topic 的一致性）
```

### 5.8 gRPC Checkpoint 的兩種方式：2PC vs Barrier 完整走過一遍

**場景**：

```
Pipeline: Kafka → Pod A (filter) ──gRPC──→ Pod B (aggregate, stateful) → Sink (DB)

Kafka topic "orders" partition 0:
  offset 100: {user: "Alice", amount: 100}
  offset 101: {user: "Bob",   amount: 200}
  offset 102: {user: "Alice", amount: 50}
  offset 103: {user: "Carol", amount: 300}
  ...

Pod A: 消費 Kafka, filter 後透過 gRPC 送給 Pod B
Pod B: 收到 gRPC 資料, aggregate (stateful), 寫 DB

Pod B 的 state:
  {Alice → 500, Bob → 200, Carol → 300, ...}
```

---

#### 方式一：2PC（Two-Phase Commit）

需要一個 **Coordinator**（可以是獨立 process，也可以是 Pod A 兼任）。

```
角色：
  Coordinator: 協調 checkpoint
  Pod A: 消費 Kafka → filter → gRPC 送 Pod B
  Pod B: gRPC 接收 → aggregate(state) → 寫 DB
```

**完整流程**：

```
正常處理中...
  Pod A: 消費 offset 100 → gRPC 送 Pod B
  Pod A: 消費 offset 101 → gRPC 送 Pod B
  Pod A: 消費 offset 102 → gRPC 送 Pod B
  Pod B: 收到 Alice +100 → state: {Alice→600}
  Pod B: 收到 Bob   +200 → state: {Bob→400}
  Pod B: 收到 Alice  +50 → state: {Alice→650}

═══════════════════════════════════════════════════
Coordinator: 「5 秒到了，做 checkpoint cp=7」
═══════════════════════════════════════════════════

──── Phase 1: Prepare（預備）────

Coordinator ──→ Pod A:  「prepare cp=7」
Coordinator ──→ Pod B:  「prepare cp=7」

Pod A 收到 prepare:
  1. 暫停消費 Kafka（不再 poll 新訊息）
  2. 等 gRPC 的 in-flight 資料都送到 Pod B 且 Pod B 確認收到
     ★ 這一步很重要：確保 Pod A 已送出的資料 Pod B 都收到了
  3. 記錄：「我處理到 offset 102」
  4. 回報 Coordinator：「prepared, offset=102」

Pod B 收到 prepare:
  1. 暫停處理新的 gRPC 資料（放到 buffer）
  2. 把目前的 state 寫到「暫存區」（不是正式 commit）
     例如：RocksDB snapshot 或 WAL
     state snapshot: {Alice→650, Bob→400, Carol→300}
  3. 如果有 Sink (DB)：DB transaction prepare（不 commit）
  4. 回報 Coordinator：「prepared, state_snapshot_id=xxx」

Coordinator 收到兩個 prepared:
  ★ 檢查：Pod A prepared ✓, Pod B prepared ✓
  ★ 如果任一個 prepare 失敗 → 全部 abort（見下方失敗處理）

──── Phase 2: Commit（正式提交）────

Coordinator ──→ Pod A:  「commit cp=7」
Coordinator ──→ Pod B:  「commit cp=7」

Pod A 收到 commit:
  1. commit offset=102 到 Kafka（consumer.commit）
  2. 回報 Coordinator：「committed」
  3. 恢復消費 Kafka

Pod B 收到 commit:
  1. 把暫存區的 state snapshot 正式寫入 RocksDB
  2. DB transaction commit（Sink 的資料正式可見）
  3. 回報 Coordinator：「committed」
  4. 恢復處理 gRPC 資料（消化 buffer）

Coordinator:
  ★ 收到兩個 committed → cp=7 完成 ✓
  ★ 記錄：checkpoint 7 = {Pod A: offset 102, Pod B: state_snapshot_xxx}

═══════════════════════════════════════════════════
繼續正常處理...
═══════════════════════════════════════════════════
```

**失敗場景**：

```
場景 1: Pod B prepare 失敗（例如 RocksDB 寫入失敗）

  Coordinator 收到 Pod A: prepared ✓, Pod B: prepare failed ✗
  → Coordinator 發送 abort 給所有人
  → Pod A: 不 commit offset，恢復消費（從 offset 100 重新來）
  → Pod B: 丟棄暫存的 snapshot，恢復處理
  ★ 一致：offset 沒推進，state 沒變，下次重來

場景 2: Pod B committed，但 Pod A commit offset 失敗

  → Pod A 重啟後從上次 committed offset 重新消費
  → Pod B 的 state 已經 commit 了（Alice→650）
  → Pod A 重新送 offset 100-102 給 Pod B
  → Pod B 會收到重複資料！Alice 又加了 100+50
  → ❌ state 變成 Alice→800（多了 150）

  解法：Pod B 要記錄「cp=7 已經 commit 了，offset 102 之前的資料要丟棄」
  → 這就是 Flink 的「已 commit checkpoint 的 offset fence」
  → 每個 operator 要記住「我的 state 對應到 source 的哪個 offset」
```

**2PC 的問題**：

```
1. 阻塞時間長
   Phase 1 期間所有人都暫停 → 整個 pipeline 停滯
   prepare 的時間 = max(Pod A prepare, Pod B prepare)
   如果 Pod B 的 state 很大（GB 級），snapshot 很慢

2. Coordinator 是單點故障
   Coordinator crash 在 Phase 1 和 Phase 2 之間：
   → Pod A、B 都 prepared 了，但沒人告訴它們 commit 還是 abort
   → 卡住（blocking）
   → 經典的 2PC 無法解決的問題（需要 3PC 或 Paxos）

3. 網路分區
   Pod A 收到 commit，Pod B 沒收到：
   → Pod A commit offset，Pod B 沒 commit state
   → 不一致
```

---

#### 方式二：Barrier（Chandy-Lamport 變體，Flink 的做法）

不需要暫停整個 pipeline，barrier 像普通資料一樣在 gRPC 中流動。

```
角色：
  Coordinator: 只負責「觸發 checkpoint」和「收集結果」
  Pod A: 消費 Kafka → filter → gRPC 送 Pod B（在資料流中注入 barrier）
  Pod B: gRPC 接收 → aggregate(state) → 寫 DB
```

**完整流程**：

```
正常處理中...
  Pod A: offset 100 → gRPC → Pod B
  Pod A: offset 101 → gRPC → Pod B
  Pod A: offset 102 → gRPC → Pod B

═══════════════════════════════════════════════════
Coordinator: 「5 秒到了，做 checkpoint cp=7」
Coordinator ──→ Pod A:  「注入 barrier cp=7」
★ 只通知 Source（Pod A），不通知下游
═══════════════════════════════════════════════════

──── Step 1: Pod A 注入 barrier ────

Pod A:
  1. 收到 Coordinator 的指令
  2. 記錄當前 offset: 「cp=7 對應 offset 102」
  3. 在 gRPC stream 中注入一個特殊的 barrier 訊息：

     gRPC 資料流:
     ... [offset 101 的資料] [offset 102 的資料] [★ BARRIER cp=7 ★] [offset 103 的資料] ...

  4. ★ Pod A 不停！繼續消費 offset 103, 104, ...
  5. Pod A 自己做 snapshot：「我處理到 offset 102」
  6. 回報 Coordinator：「Pod A snapshot done, offset=102」

──── Step 2: Barrier 流到 Pod B ────

Pod B 的 gRPC 接收端看到的資料流:
  ... [Alice +100] [Bob +200] [Alice +50] [★ BARRIER cp=7 ★] [Carol +300] ...
                                            ↑
                                     barrier 到了！

Pod B 收到 barrier:
  1. ★ barrier 之前的資料都已處理完（Alice→650, Bob→400）
     ★ barrier 之後的資料（Carol +300）還沒處理
     ★ 此刻的 state 精確反映了 Pod A offset 0~102 的所有資料

  2. Snapshot 當前 state: {Alice→650, Bob→400, Carol→300, ...}
     ★ 這個 snapshot 是非同步的（async）！
     ★ Pod B 可以繼續處理 barrier 之後的資料（Carol +300）
     ★ snapshot 在背景執行（fork RocksDB snapshot 或 copy-on-write）

  3. 如果 Pod B 有多個上游（例如 Pod A 和 Pod C 都送資料）：
     → 等所有上游的 barrier 都到了才 snapshot（barrier alignment）
     → 先到的那一路暫停，繼續處理還沒到 barrier 的那一路

  4. 回報 Coordinator：「Pod B snapshot done, state=xxx」

──── Step 3: Coordinator 確認 ────

Coordinator 收集到所有回報：
  Pod A: snapshot done (offset=102) ✓
  Pod B: snapshot done (state=xxx) ✓
  → 宣布 cp=7 完成 ✓

★ 此時 Pod A 和 Pod B 都已經在處理 barrier 之後的新資料了
★ 整個 checkpoint 過程中 pipeline 沒有停止！

──── Step 4: 如果有 Sink（兩階段提交的 Phase 2）────

如果 Pod B 的 Sink 是 PostgreSQL（需要 exactly-once）：

  Step 2 時（收到 barrier）：
    Pod B: DB transaction prepare (不 commit)
    → barrier 之前的資料在 DB 的 prepared transaction 中

  Step 3 後（Coordinator 確認 cp=7 完成）：
    Coordinator ──→ Pod B: 「cp=7 confirmed」
    Pod B: DB transaction commit
    → barrier 之前的資料正式在 DB 中可見 ✓

  ★ Phase 1 = 收到 barrier → prepare
  ★ Phase 2 = 收到 confirmed → commit
  ★ 中間 Pod B 可以繼續處理新資料（寫入下一個 DB transaction）

═══════════════════════════════════════════════════
繼續正常處理...直到下一次 checkpoint
═══════════════════════════════════════════════════
```

**失敗場景**：

```
場景 1: Pod B snapshot 失敗

  Coordinator: Pod A ✓, Pod B ✗
  → cp=7 失敗 → 不影響任何東西（沒有人 commit 任何東西）
  → 下次 checkpoint cp=8 再試
  → ★ 不需要 abort！跟 2PC 不同，barrier 模式的 prepare 是無副作用的 snapshot

場景 2: Pod B crash（在 barrier 之後，confirmed 之前）

  → cp=7 失敗（Pod B 沒有回報 snapshot done）
  → Pod B 重啟 → 從最近成功的 checkpoint cp=6 恢復 state
  → Pod A 從 cp=6 的 offset 重新消費
  → 重新處理，重新注入 barrier
  ★ 一致：回到 cp=6 的狀態，重新來

場景 3: Coordinator crash

  → 沒人收集 snapshot 回報 → cp=7 超時 → 自動放棄
  → Coordinator 重啟 → 觸發新的 cp=8
  → ★ 不會卡住！（2PC 會卡在 Phase 1 和 Phase 2 之間）
```

---

**2PC vs Barrier 完整對比**：

```
                              2PC                          Barrier
                         ─────────────                ─────────────────
觸發方式                  Coordinator → 所有人          Coordinator → 只有 Source

Pipeline 暫停嗎？         ★ 是！Phase 1 全部暫停        ★ 否！只在 barrier alignment
                                                       時暫停部分 input channel

Prepare 做什麼？          暫停 + snapshot + 報告         收到 barrier → async snapshot
                                                       → 繼續處理新資料

Commit 做什麼？           Coordinator 確認 → 正式寫入    Coordinator confirmed
                                                       → Sink 做 Phase 2 commit

阻塞時間                  整個 Phase 1                   幾乎為零
                         （最慢的那個 Pod 決定）          （barrier alignment 可能短暫阻塞）

Coordinator crash         ★ 致命：卡在 prepare/commit    ★ 不致命：超時放棄，重新來
                         之間，需要 recovery log

Prepare 失敗              全部 abort                     不影響，下次再試

Sink exactly-once         2PC: prepare → commit          也是 2PC！但 prepare 在 barrier
                         （跟 checkpoint 的 2PC 混在一起） 時做，commit 在 confirmed 後做
                                                        （跟 checkpoint 的 barrier 分開）

網路開銷                  Coordinator ↔ 所有 Pod          Barrier 跟資料一起流
                         多次 RPC 來回                   不需要額外 RPC

適合的拓撲                簡單（2-3 個 Pod）              複雜（多層 DAG，幾十個 operator）
```

---

**為什麼 Flink 選 Barrier 不選 2PC？**

```
Flink 的典型 pipeline:

  Source (parallelism=4)
    ↓ shuffle
  Filter (parallelism=8)
    ↓ shuffle
  Aggregate (parallelism=4)
    ↓
  Sink (parallelism=4)

  = 20 個 operator

2PC 的做法：
  Coordinator → 20 個 operator 發 prepare
  等 20 個都 prepared（最慢的決定等待時間）
  Coordinator → 20 個 operator 發 commit
  = 40 次 RPC + 20 個 operator 全部暫停
  ★ 延遲和吞吐量影響巨大

Barrier 的做法：
  Coordinator → 4 個 Source 發 inject barrier
  Barrier 自動隨資料流向下游
  每個 operator 收到 barrier → async snapshot → 繼續處理
  = 4 次 RPC + snapshot 在背景執行
  ★ pipeline 幾乎不停
```

**一句話**：2PC 是「停下來拍照」，Barrier 是「邊走邊拍」。
Pipeline 越長、parallelism 越大，Barrier 的優勢越明顯。

---

## 6. 需要修改的核心抽象層

### 6.1 替換 group_by 的傳輸層

```python
# 現有：quixstreams/dataframe/dataframe.py
def group_by(self, key, ...):
    # 現有：produce to Kafka repartition topic
    self.to_topic(topic=groupby_topic, key=self._groupby_key(key))
    self.filter(lambda _: False)
    groupby_sdf = self.__dataframe_clone__(groupby_topic)
    return groupby_sdf

# 改造後：
def group_by(self, key, ..., transport="kafka"):
    if transport == "grpc":
        # 用 gRPC router 取代 to_topic
        router = GrpcRouter(pod_addresses, num_partitions)
        self.update(
            lambda v, k, ts, h: router.route(
                key=new_key(v), value=serialize(v), timestamp=ts
            ),
            metadata=True,
        )
        self.filter(lambda _: False)
        # 新的 SDF 從 gRPC source 消費
        grpc_source = GrpcSource(port=50051)
        groupby_sdf = app.dataframe(source=grpc_source)
        return groupby_sdf
    else:
        # 原有 Kafka 方式
        ...
```

### 6.2 需要新增的元件

| 元件 | 功能 | 複雜度 |
|------|------|--------|
| `GrpcRouter` | 按 key hash 路由到目標 Pod | 中 |
| `GrpcSource` | gRPC server，接收資料注入 pipeline | 中 |
| `GrpcSink` | gRPC client，推送資料到其他 Pod | 低 |
| `ServiceDiscovery` | 節點發現（哪個 Pod 負責哪個 partition） | 高 |
| `CheckpointBarrier` | 分散式 checkpoint（類似 Flink barrier） | 非常高 |
| `StateTransfer` | 節點間 state 遷移（rebalance 時） | 非常高 |

---

## 7. 其他框架如何做分散式

### 7.1 Apache Flink

- **TaskManager** 之間用 **Netty TCP** 傳輸（不是 gRPC）
- **Barrier-based checkpoint**：checkpoint barrier 跟隨資料流傳播
- State 用 RocksDB，通過 **state snapshot** 持久化到分散式檔案系統（HDFS/S3）
- JobManager 負責協調任務分配和 checkpoint

### 7.2 Bytewax (Python)

- 用 **Timely Dataflow** 的 Rust 核心做跨 worker 通訊
- Worker 之間用 **TCP** 直連
- Python API 類似 Quix：`flow.map()`, `flow.filter()`, `flow.reduce()`
- 自動處理 state 分區和恢復

### 7.3 Ray (Python)

- **Ray Actor** 之間用 **gRPC** 通訊
- 自動處理序列化、反序列化
- 有 **Ray Serve** 做 streaming 推理
- State 管理靠 **Actor state** + checkpoint

### 7.4 Apache Beam (Dataflow)

- Runner（如 Flink Runner, Dataflow Runner）負責分散式執行
- SDK 只定義 pipeline graph
- 傳輸層由 Runner 決定（Flink 用 Netty，Dataflow 用 Google 內部 RPC）

---

## 8. 結論與建議

### 8.1 可行性評估

| 改造目標 | 可行性 | 工作量 | 建議 |
|---------|--------|--------|------|
| gRPC 替代 repartition topic（group_by） | 可行 | 中等 | 最有價值的改造點 |
| gRPC Source/Sink | 容易 | 低 | 可以當作自定義 Source/Sink 實作 |
| 完整分散式架構（類 Flink） | 理論可行 | 巨大 | 不建議，不如直接用 Flink/Bytewax |
| 分散式 checkpoint | 非常困難 | 巨大 | Flink 花了多年才完善 |

### 8.2 推薦路線

**Phase 1：gRPC Source + Sink（低風險）**
```python
# 不改框架，只是實作新的 Source 和 Sink
app = Application(...)
grpc_source = GrpcSource(port=50051)
sdf = app.dataframe(source=grpc_source)
sdf.sink(GrpcSink(target="pod-2:50051"))
```

**Phase 2：gRPC Repartition（中風險）**
```python
# 替代 group_by 的 Kafka repartition
# 需要服務發現 + 路由邏輯
sdf = sdf.group_by("customer_id", transport="grpc")
```

**Phase 3（不建議）：完整分散式**
- 需要自建 Coordinator、checkpoint barrier、state transfer
- 工程量相當於重寫一個簡化版 Flink
- 建議直接用 Flink + PyFlink 或 Bytewax

### 8.3 gRPC vs Kafka Repartition 取捨

| 維度 | Kafka Repartition | gRPC |
|------|-------------------|------|
| 延遲 | 高（寫盤 + 讀取） | 低（記憶體直傳） |
| 容錯 | 高（持久化） | 低（需自建重試） |
| Exactly-once | 有（Kafka transactions） | 需自建 |
| 運維複雜度 | 低（Kafka 已有） | 高（服務發現、健康檢查） |
| 適用場景 | 生產環境、資料不能丟 | 低延遲需求、可容忍重新處理 |
