# Quix Streams vs Arroyo — 串流引擎核心設計比較

## 基本資訊

| | Quix Streams | Arroyo |
|---|---|---|
| 語言 | Python | Rust |
| 資料模型 | Python dict (JSON) | Apache Arrow RecordBatch |
| 部署模型 | 單 process（embedded library） | 分散式叢集（Controller + Worker） |
| 訊息來源 | Kafka | Kafka、Kinesis、WebSocket、HTTP、檔案等 |
| SQL 支援 | 無 | 有（SQL + UDF） |
| 定位 | 輕量 Python streaming library | 完整的分散式串流引擎 |

---

## 1. 每筆資料的處理方式

這是兩個引擎最根本的設計差異。

### Quix：逐筆處理 + 函數鏈同步推進

```
consumer.poll()
    │
    ▼
Row (1 筆 Kafka message)
    │
    ▼
dataframe_composed[topic_name](value, key, timestamp, headers)
    │
    ├── ApplyFunction(transform_value)    ← 同步呼叫，return 新 value
    │       │
    │       ▼
    ├── FilterFunction(check_valid)       ← 同步呼叫，return True/False
    │       │
    │       ▼
    └── UpdateFunction(enrich)            ← 同步呼叫，mutate in-place
            │
            ▼
        sink / 下一個 Stream 節點
```

**核心程式碼**（`quixstreams/app.py:986-1038`）：
```python
def _process_message(self, dataframe_composed):
    rows = self._consumer.poll_row(timeout=...)     # 拉 1 條訊息
    for row in rows:
        context = copy_context()
        context.run(set_message_context, row.context)
        context.run(
            dataframe_composed[topic_name],          # 執行整條函數鏈
            row.value, row.key, row.timestamp, row.headers,
        )
    self._processing_context.store_offset(...)       # 記錄 offset
```

**函數鏈的組合**（`quixstreams/core/stream/stream.py:404-461`）：

就是把函數一層層包起來，每個 `get_executor()` 回傳一個 wrapper function，裡面呼叫自己的 func 再呼叫下一個 executor。沒有什麼特別的技術，就是嵌套 function call：

```python
# ApplyFunction.get_executor() 做的事（apply.py:59-67）：
def wrapper(value, key, timestamp, headers):
    result = func(value)                          # 跑自己的函數
    child_executor(result, key, timestamp, headers)  # 呼叫下一個

# 最終組合出來就是普通的嵌套呼叫：
# wrapper_1 裡面呼叫 wrapper_2，wrapper_2 裡面呼叫 wrapper_3...
# 跟手寫 func_a(func_b(func_c(value))) 本質一樣
```

唯一值得注意的是 branching 處理（`base.py:47-62`）：如果一個節點有多個 children（DAG 分叉），會用 `pickle_copier` 複製 value 給各分支，避免 mutation 互相影響。

**特點**：
- 一次處理一筆，同步執行完整條函數鏈
- 沒有 batch、沒有 buffer — `poll()` → 執行 → `store_offset()` → 下一個 `poll()`
- Python GIL 限制，單 thread 處理

### Arroyo：批次處理 + Operator 間 async 推進

```
Source Operator
    │
    ▼
RecordBatch (N 筆 rows，Arrow 格式)
    │
    ▼
Operator A ──[BatchSender queue]──▶ Operator B ──[BatchSender queue]──▶ Operator C
    │                                    │                                   │
    │  tokio::select! 主迴圈             │  tokio::select! 主迴圈            │
    │  收 batch → process_batch()        │  收 batch → process_batch()       │
    │  收 signal → handle_signal()       │  收 signal → handle_signal()      │
    │  收 control → handle_control()     │                                   │
```

**核心程式碼**（`arroyo-operator/src/operator.rs:982-1061`）：

每個 operator 就是一個 tokio task，跑一個無限迴圈，`select!` 同時等四種事件：

```rust
loop {
    tokio::select! {
        // ① controller 送來的控制指令
        Some(control_message) = control_rx.recv() => {
            this.handle_controller_message(&control_message, ...).await?;
        }

        // ② 上游 operator 送來的資料或訊號
        Some(((idx, message), s)) = sel.next() => {
            match message {
                ArrowMessage::Data(record) => {
                    // 資料 batch — 跑 operator 邏輯
                    this.process_batch_index(idx, in_partitions, record, collector).await?;
                }
                ArrowMessage::Signal(signal) => {
                    // 控制訊號 — checkpoint barrier / watermark / stop
                    this.handle_control_message(idx, &signal, &mut counter, ...).await?;
                }
            }
        }

        // ③ operator 自己的 async future（有些 operator 有背景工作）
        Some(val) = operator_future => {
            this.handle_future_result(val.0, val.1, collector).await?;
        }

        // ④ 定時 tick（有些 operator 需要週期性觸發，如 window flush）
        _ = interval.tick() => {
            this.handle_tick(ticks, collector).await?;
        }
    }
}
```

**四個 handler 分別做什麼**：

**② `process_batch_index`** — 跑 operator 的實際處理邏輯（`operator.rs:608-629`）：

就是呼叫 operator 的 `process_batch()`，不同 operator 有不同實作。
看兩個具體例子就懂了：

```rust
// ProjectionOperator（投影/map）— arroyo-worker/src/arrow/mod.rs:156-178
// 對 batch 的每個 column 執行 expression，產生新 batch，丟給 collector
async fn process_batch(&mut self, batch: RecordBatch, _, collector: &mut dyn Collector) {
    let outputs = self.exprs.iter()
        .map(|e| e.evaluate(&batch).and_then(|f| f.into_array(batch.num_rows())))
        .try_collect()?;
    collector.collect(RecordBatch::try_new(self.output_schema.schema.clone(), outputs)?).await
}

// ValueExecutionOperator（通用 filter/transform）— arroyo-worker/src/arrow/mod.rs:84-96
// 執行一個完整的 DataFusion physical plan，可能產生 0~N 個 output batch
async fn process_batch(&mut self, batch: RecordBatch, _, collector: &mut dyn Collector) {
    let mut records = self.executor.process_batch(batch).await;
    while let Some(batch) = records.next().await {
        collector.collect(batch?).await?;
    }
    Ok(())
}
```

所有 operator 都是同一個模式：收 `RecordBatch` → 做事 → `collector.collect(output_batch)` 送下游。

`collector.collect()` 做的事取決於是否有 chain（`operator.rs:419-451`）：
- 有下一個 chained operator → 直接呼叫下一個的 `process_batch_index()`（同 task 內，零 queue 開銷）
- 沒有 → 送到 `ArrowCollector`，repartition 後塞進 `BatchSender` queue 送給下游 task

**② `handle_control_message`** — 處理 in-band 控制訊號（`operator.rs:661-731`）：

跟資料混在同一個 queue 裡的訊號（`SignalMessage`），有四種：

```
SignalMessage::Barrier(epoch)   → checkpoint 觸發
    第一個 barrier 到 → 回報 StartedAlignment
    所有 input 的 barrier 都到齊 → run_checkpoint() → 向下游 broadcast barrier

SignalMessage::Watermark(time)  → event time 推進
    更新 operator 內部的 watermark 追蹤
    當所有 input 的 watermark 都推進時，才向下游傳播
    （window operator 靠這個決定何時 flush 視窗）

SignalMessage::Stop             → 優雅關機
    所有 input 都 Stop 後，向下游傳播 Stop

SignalMessage::EndOfData        → 資料結束（有限流）
    所有 input 都結束後，通知下游
```

**① `handle_controller_message`** — 處理 out-of-band 控制指令（`operator.rs:533-573`）：

跟資料在不同 channel（`control_rx`）的指令，由 Job Controller 直接送來：

```
ControlMessage::Commit { epoch }      → checkpoint 第二階段：告訴 sink operator 可以 commit 外部系統了
ControlMessage::LoadCompacted { .. }  → 載入壓縮後的 state（recovery 用）
ControlMessage::NoOp                  → 心跳
```

**③ `operator_future`** — 有些 operator 會起背景 async 工作（如 async I/O），完成後從這裡拿結果。大多數 operator 回傳 `None`（不用）。

**④ `handle_tick`** — 定時觸發，用於 window operator 定期 flush、session timeout 檢查等。

**特點**：
- 一次處理一個 RecordBatch（幾十到幾千筆 rows）
- Operator 之間是 async queue（`BatchSender`/`BatchReceiver`），非同步推進
- 每個 Operator 是獨立的 tokio task，有自己的 `select!` 迴圈
- 同一 task 內可以 chain 多個 operator（直接呼叫，不經過 queue）
- **向量化處理** — Arrow columnar format，對整個 column 做 evaluate

### 差異深度分析

| | Quix | Arroyo |
|---|---|---|
| 處理單位 | 1 筆 (Row) | 1 batch (RecordBatch, N rows) |
| 函數呼叫 | 同步閉包鏈，`f(g(h(value)))` | async operator pipeline，batch 在 queue 間流轉 |
| 資料格式 | Python dict → 每筆都要 serialize/deserialize | Arrow RecordBatch → 零拷貝 columnar 操作 |
| Operator 間通訊 | 無（同一閉包內） | 有（bounded async queue） |
| 執行緒模型 | 單 thread（GIL） | 多 thread（tokio runtime） |
| 延遲 | 極低（poll 到處理完 ~μs 級） | 略高（batch 填滿才送，queue 延遲） |
| 吞吐量 | 受 Python 限制（~10K-100K msg/s） | 高（向量化 + Rust，~1M+ msg/s） |

**其他串流引擎的做法**（供參考）：

| 引擎 | 處理模型 |
|---|---|
| Apache Flink | Record-by-record，但在 network shuffle 時打包成 buffer（類似 micro-batch in transit） |
| Kafka Streams | Record-by-record，和 Quix 最像（同步函數鏈） |
| Apache Spark Structured Streaming | Micro-batch（整個 batch 一起處理，延遲高） |
| RisingWave | 批次（Arrow batch），和 Arroyo 類似 |
| Bytewax | Record-by-record（Python，和 Quix 類似） |

Quix 和 Kafka Streams 的設計最接近 — 都是 embedded library、逐筆處理、同步函數鏈。
Arroyo 和 Flink/RisingWave 更接近 — 分散式、批次處理、operator pipeline。

---

## 2. Checkpoint 做法

### Quix：定時檢查 + 計數器觸發

```
主迴圈（單 thread）：

while running:
    _process_message()          ← poll 1 條、跑完函數鏈、store_offset()
    commit_checkpoint()         ← 每次迴圈都「檢查」是否該 commit
        │
        ├── checkpoint.expired()?
        │     ├── 時間到了？(commit_interval, 預設 5s)
        │     └── 處理夠多筆了？(commit_every)
        │
        ├── 如果沒過期 → return（不做事）
        │
        └── 如果過期 → commit():
              1. flush sinks
              2. produce changelogs (state)
              3. flush producer
              4. consumer.commit(offsets)
              5. flush state store to disk
```

**核心程式碼**（`quixstreams/checkpointing/checkpoint.py:58-66`）：
```python
def expired(self) -> bool:
    return (time.monotonic() - self._commit_interval) >= self._created_at or (
        0 < self._commit_every <= self._total_offsets_processed
    )
```

**特點**：
- **不是每筆都 commit** — 每筆都 `store_offset()` 記錄到記憶體，但只在 `expired()` 時才真正 commit
- **檢查頻率 = 主迴圈頻率** — 每處理完一筆就檢查一次 `expired()`
- `commit_interval` 預設 5 秒，`commit_every` 預設 0（不用）
- Commit 是同步的：flush producer → commit offsets → flush state → 才繼續 poll
- 失敗處理：整個 checkpoint 放棄，重新處理從上次 committed offset 開始

### Arroyo：Barrier 對齊 + 分散式兩階段

```
Controller                        Operator A            Operator B            Operator C
    │                                 │                     │                     │
    │── Barrier(epoch=5) ──────────▶  │                     │                     │
    │                                 │                     │                     │
    │                            StartedAlignment          │                     │
    │                                 │                     │                     │
    │                            (等待所有 input            │                     │
    │                             的 barrier 到齊)          │                     │
    │                                 │                     │                     │
    │                            所有 input 到齊             │                     │
    │                            StartedCheckpointing       │                     │
    │                                 │                     │                     │
    │                            handle_checkpoint()        │                     │
    │                            (存 state)                 │                     │
    │                                 │                     │                     │
    │                            FinishedOperatorSetup      │                     │
    │                                 │                     │                     │
    │                            table_manager.checkpoint() │                     │
    │                                 │                     │                     │
    │                            FinishedSync               │                     │
    │                                 │                     │                     │
    │                            broadcast(Barrier) ──────▶ │                     │
    │                                                  (同樣的流程)               │
    │                                                       │                     │
    │                                                  broadcast(Barrier) ──────▶ │
    │                                                                        (同樣的流程)
    │                                                                             │
    │◀──────────────────────── FinishedSync ──────────────────────────────────────│
    │                                                                             │
    │── Commit(epoch=5) ──────────────────────────────────────────────────────────▶│
    │                                                  (只有 committing operator   │
    │                                                   如 sink 需要)             │
```

**核心程式碼**（`arroyo-operator/src/operator.rs:673-708`）：
```rust
SignalMessage::Barrier(t) => {
    if counter.all_clear() {
        // 第一個 barrier 到達，開始 alignment
        send(CheckpointEvent { event_type: StartedAlignment });
    }
    if counter.mark(idx, t) {
        // 所有 input 的 barrier 都到齊了
        self.run_checkpoint(t, control_tx, collector).await?;
        collector.broadcast(SignalMessage::Barrier(*t)).await;  // 向下游傳播
    }
}
```

**特點**：
- **Chandy-Lamport 演算法變體** — 和 Apache Flink 相同的 barrier alignment 機制
- Controller 定期注入 Barrier 訊號到 source operators
- 每個 operator 等所有 input stream 的同一個 epoch barrier 到齊才 checkpoint
- Barrier 沿著 DAG 流向下游，形成全域一致快照
- **非同步** — checkpoint 不阻塞其他 partition 的處理（只暫停已收到 barrier 的 input）
- 兩階段：Sync（存 state）+ Commit（sink 確認外部寫入）

### 差異深度分析

| | Quix | Arroyo |
|---|---|---|
| 觸發方式 | 定時器 + 計數器 | Controller 注入 Barrier |
| 一致性範圍 | 單一 consumer process | 全叢集所有 operator |
| 對齊機制 | 不需要（單 process） | Barrier alignment（等所有 input 到齊） |
| 處理阻塞 | Commit 時阻塞主迴圈 | Barrier 之間不阻塞（只暫停已收到 barrier 的 input） |
| State 存儲 | RocksDB + changelog topic | 可插拔（S3、本地磁碟等） |
| 失敗恢復 | 重新從 last committed offset 消費 | 從最近的全域 checkpoint 恢復所有 operator state |
| Exactly-once | Kafka transaction（producer + consumer offset 原子提交） | Barrier alignment 保證全域一致 |

**Quix 的設計取捨**：單 process 不需要分散式協調，所以 checkpoint 非常簡單 — 就是「累積一段時間，一次性 flush + commit」。代價是 commit 時會短暫阻塞主迴圈。

**Arroyo 的設計取捨**：分散式系統需要全域一致的 checkpoint，所以用 Flink 式的 barrier alignment。代價是 barrier 對齊期間某些 input 可能被暫停（等其他 input 的 barrier），影響延遲。

### Arroyo Checkpoint 失敗場景：A、B 做完但 C 掛了

**不會 rollback A 和 B。整個 job 直接重啟，從上一個成功的 checkpoint 恢復。**

流程：

```
1. Barrier(epoch=5) 從 source 流入

2. Operator A checkpoint 完成 → operators_checkpointed = 1
   Operator B checkpoint 完成 → operators_checkpointed = 2
   Operator C checkpoint 過程中 crash

3. Worker 偵測到 TaskFailed
   → job_controller/controller.rs:544 的 progress() 回傳 TaskFailed

4. Controller 收到 TaskFailed
   → states/running.rs:111 呼叫 handle_task_error()
   → states/mod.rs:602 判斷 RetryHint:
      - WithBackoff → 轉入 Recovering 狀態
      - NoRetry → 直接報 FatalError，job 結束

5. Recovering 狀態（states/recovering.rs:16-228）:
   → 檢查重啟次數是否超過上限（config.pipeline.allowed_restarts）
   → 套用 backoff 等待
   → cleanup(): 殺掉所有現有 worker
   → 轉入 Compiling → Scheduling → Running

6. 重新啟動時：
   → controller.rs:142-233 取得 parent_checkpoint_ref
     （這是 epoch=4 的 checkpoint，也就是上一個完整成功的）
   → 所有 operator 從 epoch=4 的 state 恢復
   → 如果有未完成的 commit（如 sink 寫了一半），做 commit replay
```

**為什麼不需要 rollback**：

checkpoint 的 state 是「寫完所有 operator 才算一個完整的 checkpoint」。
A 和 B 雖然個別完成了 epoch=5 的 checkpoint，但 epoch=5 從未被標記為「完成」
（`checkpoint_state.rs` 裡的 `done()` 需要 `operators_checkpointed == operators` 才 return true）。

所以 epoch=5 根本不存在一個完整的 checkpoint。
重啟時系統只會找到 epoch=4（上一個完整的），從那邊恢復。
A 和 B 在 epoch=5 寫的 partial state 就像垃圾一樣被忽略。

```
epoch=1  ✓ 完整 checkpoint
epoch=2  ✓ 完整 checkpoint
epoch=3  ✓ 完整 checkpoint（已清理，超過保留數量）
epoch=4  ✓ 完整 checkpoint ← 從這裡恢復
epoch=5  ✗ A、B 寫了，C 沒寫 ← 不完整，直接丟棄
```

### A、B 寫的 epoch=5 partial state 怎麼辦？

**短期：留在磁碟上不管。長期：被垃圾回收刪掉。**

Arroyo 的 checkpoint state 寫在遠端儲存（S3 / GCS / 本地檔案系統），路徑結構：
```
{job_id}/checkpoints/
├── checkpoint-0000004/          ← 完整（上一個成功的）
│   ├── metadata
│   ├── operator-A/
│   │   └── table-0.parquet
│   ├── operator-B/
│   │   └── table-0.parquet
│   └── operator-C/
│       └── table-0.parquet
│
├── checkpoint-0000005/          ← 不完整（C 沒寫完）
│   ├── operator-A/
│   │   └── table-0.parquet      ← 孤兒檔案，沒有對應的完整 metadata
│   └── operator-B/
│       └── table-0.parquet      ← 孤兒檔案
│   （沒有 operator-C，沒有頂層 metadata）
```

**重啟時不會讀到 partial state**：
- 重啟時 `scheduling.rs:275-308` 呼叫 `execute_mark_failed()` 把 epoch > last_successful 的 checkpoint 標記為 FAILED
- Recovery 只看 `parent_checkpoint_ref`，指向 epoch=4
- epoch=5 的 partial 檔案就躺在那裡，沒人理它

**什麼時候清掉**：
- 靠 `cleanup_needed()`（`model.rs:750-758`）觸發垃圾回收
- 條件：`epoch - min_epoch > CHECKPOINTS_TO_KEEP(4)` 且 `epoch % COMPACT_EVERY(2) == 0`
- 假設 job 恢復後繼續跑到 epoch=10，此時 `10 - 4 > 4`，觸發清理
- `cleanup_checkpoint()`（`parquet.rs:110-167`）刪掉 `[old_min_epoch, new_min_epoch)` 範圍內的所有檔案
- epoch=5 的孤兒 parquet 檔案在這時被刪掉

```
時間線：
epoch=4  ✓ 完整 checkpoint
epoch=5  ✗ partial（A、B 寫了，C 沒寫）  ← 留在磁碟
              ↓ job crash，重啟
epoch=5  重新開始（從 epoch=4 恢復）
epoch=6  ✓
epoch=7  ✓
epoch=8  ✓
epoch=9  ✓
epoch=10 ✓  ← 這時 cleanup_needed() 觸發，清掉 epoch 4~5 的舊檔案
```

**簡單說就是「懶清理」** — 不完整的 checkpoint 不會被主動刪除，
而是等正常的垃圾回收（每隔幾個 epoch）一起清掉。
中間這段時間 partial 檔案佔的磁碟空間就是浪費的，但通常不大（state 快照而已）。

### 本地 state 怎麼辦？Arroyo vs Quix 根本不同

這個問題的關鍵在於：**Arroyo 根本沒有本地 state**。

#### Arroyo：state 全在記憶體，checkpoint 直接寫遠端

```
處理中                            Checkpoint 時
┌───────────────┐                ┌───────────────┐
│  HashMap<K,V> │  ── 序列化 ──▶ │  Parquet 檔   │ ──寫──▶ S3 / GCS
│  (純記憶體)    │                │  (bytes)      │
└───────────────┘                └───────────────┘
      ▲                                              沒有 RocksDB
      │                                              沒有本地磁碟 state
 operator 讀寫
```

Arroyo 的 operator state 存在 `HashMap<K, V>`（純記憶體）：
```rust
// global_keyed_map.rs:445
pub struct GlobalKeyedView<K: Key, V: Data> {
    data: HashMap<K, V>,   // ← 就是 HashMap，不是 RocksDB
}
```

Checkpoint 時，背景 thread 把 HashMap 序列化成 Parquet，**直接寫 S3**，
不經過本地磁碟（`table_manager.rs:368-386`）。

**所以 epoch=5 的 partial state 根本不存在於本地**：
- A、B 在 epoch=5 checkpoint 時把 state 寫到了 S3（遠端 parquet 檔案）
- 本地 HashMap 繼續被後續的訊息修改
- Job crash → process 死掉 → 記憶體中的 HashMap 直接消失
- 重啟 → 從 S3 讀 epoch=4 的 parquet → 重建 HashMap
- S3 上 epoch=5 的 partial 檔案等 GC 清掉（如前述）

**沒有 rollback 問題** — 記憶體 state 隨 process 死亡自動消失，
不像 RocksDB 會殘留在磁碟上。

#### Quix：state 在 RocksDB（本地磁碟），靠 changelog topic 恢復

```
處理中                               Checkpoint commit 時（5 步驟）
┌──────────────────┐
│ PartitionTransaction │
│ (記憶體 cache)        │
└──────────┬───────────┘
           │
           │  Step 2: produce changelog
           ├──────────────────────────────▶ Kafka changelog topic
           │
           │  Step 3: flush producer
           │
           │  Step 4: commit consumer offsets
           │
           │  Step 5: WriteBatch 寫入 RocksDB  ← 最後一步！
           ▼
┌──────────────────┐
│    RocksDB       │  本地磁碟
│    (WAL + SST)   │
└──────────────────┘
```

**關鍵設計：RocksDB 寫入是 commit 的最後一步**（`checkpoint.py:277-290`）。

```python
# checkpoint.py commit() 順序：
# Step 1: sink.flush()
# Step 2: produce changelogs         ← state 變更寫入 Kafka changelog topic
# Step 3: producer.flush()           ← 確保 changelog 送到 Kafka
# Step 4: consumer.commit(offsets)   ← commit consumer offset
# Step 5: transaction.flush()        ← WriteBatch 寫入 RocksDB（最後！）
```

**各種失敗場景**：

| 失敗時機 | RocksDB 狀態 | 恢復方式 |
|----------|-------------|----------|
| Step 1-4 任一步失敗 | **未寫入**（還在記憶體 cache） | 不需恢復，RocksDB 仍是上一個 checkpoint 的狀態 |
| Step 5 之前 crash | **未寫入** | 同上 |
| Step 5 途中 crash | WriteBatch 是**原子的**，要嘛全寫要嘛沒寫 | 沒寫完 = 沒寫 |
| Step 5 完成後 crash | **已寫入**，且是一致的 | 正常，下次啟動直接用 |

```python
# RocksDB WriteBatch 原子性（partition.py:85-134）
batch = WriteBatch(raw_mode=True)
for key, value in updates:
    batch.put(key, value, cf_handle)
for key in deletes:
    batch.delete(key, cf_handle)
# changelog offset 也在同一個 batch 裡！
self._update_changelog_offset(batch=batch, offset=changelog_offset)
# 原子寫入 — 全部成功或全部不寫
self._db.write(batch)
```

**如果 RocksDB 損壞或不一致**：從 changelog topic 重建

```
重啟時的 recovery 流程：

1. 開啟 RocksDB → 讀出 changelog_offset（上次寫到哪）
2. 從 changelog topic 的該 offset 開始 replay
3. 每條 changelog message → WriteBatch(put/delete + offset) → 原子寫入
4. replay 完畢 → RocksDB state 恢復到最新
5. 恢復正常處理
```

核心程式碼（`recovery.py:162-220`）：每條 changelog message 都帶有原始訊息的 offset header，
recovery 時會檢查：如果這條 changelog 對應的原始訊息還沒被 commit（offset > committed），
就跳過不 apply — 避免把「未 commit 的中間狀態」寫入 RocksDB。

#### 兩者對比

| | Arroyo | Quix |
|---|---|---|
| 本地 state 存儲 | **無**（純記憶體 HashMap） | **RocksDB**（本地磁碟） |
| State 大小限制 | 受記憶體限制 | 受磁碟限制（可以很大） |
| Checkpoint 寫哪裡 | S3 / GCS（Parquet） | Kafka changelog topic |
| Crash 後本地 state | 記憶體消失，從 S3 重建 | RocksDB 檔案留在磁碟，從 changelog 補齊 |
| Partial checkpoint 清理 | S3 孤兒檔案等 GC | 不存在（RocksDB 寫入是最後一步，寫不完就沒寫） |
| State 超過記憶體 | OOM crash | 正常（RocksDB 用 mmap，按需載入） |

#### 假設 Arroyo 用本地 RocksDB，通常會怎麼處理？

Arroyo 選了純記憶體，但如果分散式引擎要用本地 RocksDB + barrier alignment checkpoint，
標準做法是什麼？看 Flink — 它就是這個組合的典型代表。

**核心原則：不 rollback，丟掉重建。**

```
Flink 的做法（RocksDB state backend + barrier alignment）：

正常 checkpoint：
┌──────────┐     snapshot      ┌──────────────┐    upload     ┌─────┐
│ RocksDB  │ ──────────────▶  │ SST 檔案快照  │ ──────────▶  │ S3  │
│ (本地)   │   硬連結/複製     │ (本地暫存)    │              │     │
└──────────┘                   └──────────────┘              └─────┘
     │
     │ checkpoint 完成後
     │ 繼續正常讀寫
     ▼

失敗恢復：
┌─────┐    download    ┌──────────────┐    載入     ┌──────────┐
│ S3  │ ──────────▶   │ SST 檔案     │ ────────▶  │ 新的     │
│     │                │ (下載到本地)  │            │ RocksDB  │
└─────┘                └──────────────┘            └──────────┘
                                                        │
舊的 RocksDB 目錄？直接刪除。                              │
不 rollback、不 replay、不修復。                           ▼
                                                   從新 RocksDB 繼續處理
```

**具體步驟**：

**1. Checkpoint 時（正常路徑）**：
```
Barrier 到齊
    │
    ▼
RocksDB.checkpoint()    ← RocksDB 原生 API，建立硬連結快照
    │                      幾乎零成本（硬連結，不複製資料）
    ▼
背景 thread 把 SST 檔案 upload 到 S3
    │                      增量上傳：只傳新增的 SST 檔案（增量 checkpoint）
    ▼
回報 controller：checkpoint 完成
```

RocksDB 原生的 `CreateCheckpoint()` 用硬連結建快照，
幾乎不影響正常讀寫。這就是為什麼 Flink 選 RocksDB — 它天生支援快照。

**2. Checkpoint 失敗（A、B 完成，C 沒完成）**：
```
epoch=5 checkpoint 失敗
    │
    ▼
A、B 的本地 RocksDB 怎麼辦？
    │
    ▼
什麼都不做。它們繼續被後續的訊息修改。
epoch=5 的 state 已經「過去了」，沒有人在乎它。
    │
    ▼
如果 job crash → 整個 task 重啟
    │
    ▼
TaskManager 啟動新的 task 實例
    │
    ▼
刪除舊的 RocksDB 目錄（rm -rf）
    │
    ▼
從 S3 下載 epoch=4 的 SST 檔案
    │
    ▼
用下載的 SST 檔案開一個全新的 RocksDB
    │
    ▼
從 epoch=4 繼續處理
```

**3. 為什麼不 rollback？**

RocksDB 沒有 rollback 到任意時間點的能力。它只有：
- `WriteBatch`：單次原子寫入（但不能「回到 N 分鐘前的狀態」）
- `Checkpoint()`：建立某一刻的快照（唯讀，不能寫回去）
- `Backup()`：冷備份

所以即使你想 rollback，RocksDB 也做不到。
唯一的辦法就是**從上一個快照重建**。

```
❌ 不可能：RocksDB.rollback_to(epoch=4)     ← 不存在這個 API

✅ 實際做法：
   1. 刪除整個 RocksDB 目錄
   2. 從 S3 下載 epoch=4 的快照
   3. 用快照開新的 RocksDB 實例
```

**4. 三種引擎的 local state 恢復對比**：

| | Flink | Arroyo（假設用 RocksDB） | Quix |
|---|---|---|---|
| 本地 state | RocksDB | （假設）RocksDB | RocksDB |
| 遠端備份 | S3（SST 檔案） | S3（Parquet） | Kafka changelog topic |
| Checkpoint 方式 | `RocksDB.checkpoint()` 硬連結快照 + 上傳 S3 | （假設）同 Flink | WriteBatch 原子寫 + produce changelog |
| 失敗恢復 | 刪本地 → 從 S3 下載快照 → 開新 RocksDB | （假設）同 Flink | RocksDB 還在 → 從 changelog 補齊差異 |
| 恢復速度 | 慢（要從 S3 下載，可能 GB 級） | （假設）同 Flink | 快（本地 RocksDB 大部分已經是新的，只補差異） |
| 本地 state 存活 | 否（刪掉重建） | 否（刪掉重建） | 是（保留，增量恢復） |

**Quix 的設計其實很聰明**：因為是單 process、本地 RocksDB 不會被其他 worker 搶走，
所以可以**保留本地 state + 只從 changelog 補差異**。
恢復速度遠快於「從 S3 下載整個 state」。

Flink/分散式引擎做不到這一點，因為 task 可能被排程到不同機器，
本地 RocksDB 目錄可能根本不在了。所以只能從遠端完整重建。

**設計哲學**：
- Arroyo 把遠端儲存（S3）當 source of truth，本地只是 cache（記憶體），crash 後從遠端重建
- Flink 也把遠端儲存（S3）當 source of truth，本地 RocksDB 是 cache，crash 後刪掉重建
- Quix 把本地 RocksDB 當 source of truth，changelog topic 是備份/同步機制

**和 Quix 的對比**：

Quix 是單 process，所以不存在「A 做完 B 沒做完」的情況。
整個 commit 要嘛成功要嘛失敗：

```python
# checkpoint.py — commit 是一連串同步操作，任何一步失敗就整個放棄
1. sink.flush()           ← 失敗就 backpressure，放棄本次 checkpoint
2. produce changelogs     ← 失敗就 raise
3. producer.flush()       ← 失敗就 raise
4. consumer.commit()      ← 失敗就 raise
5. state.flush()          ← 失敗就 raise
```

失敗後 Quix 也是從上一個 committed offset 重新消費，邏輯上和 Arroyo 一樣 —
都是「丟棄不完整的，從上一個完整的重來」。只是 Quix 不需要分散式協調。

---

## 3. Backpressure

### Quix：Sink 回壓 → 暫停 Consumer → 回退重來

Quix 的 backpressure 只在 **Sink 層** 實現，pipeline 中間沒有背壓機制（因為同步函數鏈不需要）。

```
正常流程：
consumer.poll() → 函數鏈 → sink.add() → ... → checkpoint.commit() → sink.flush()

背壓流程：
checkpoint.commit()
    └── sink.flush()
            └── raise SinkBackpressureError(retry_after=30)
                    │
                    ▼
            consumer.trigger_backpressure(
                resume_after=30,
                offsets_to_seek=starting_offsets   ← 回退到本次 checkpoint 的起始 offset
            )
                    │
                    ▼
            暫停所有 partition 30 秒
                    │
                    ▼
            恢復後從 starting_offsets 重新消費
            （重新處理本次 checkpoint 的所有訊息）
```

**核心程式碼**（`quixstreams/checkpointing/checkpoint.py:195-223`）：
```python
try:
    sink.flush()
except SinkBackpressureError as exc:
    self._consumer.trigger_backpressure(
        resume_after=exc.retry_after,
        offsets_to_seek=self._starting_tp_offsets.copy(),
    )
    backpressured = True
```

**特點**：
- Backpressure 是 **checkpoint-level** 的 — sink flush 失敗就整個 checkpoint 放棄
- 機制是 **暫停 + 回退** — 暫停消費 N 秒，然後從起始 offset 重新處理
- Pipeline 中間不存在背壓 — 因為是同步函數鏈，一條龍跑完，沒有 buffer 可以滿
- 只有 Sink 能觸發背壓（`SinkBackpressureError`）

### Arroyo：每條 Edge 都有 Bounded Queue

Arroyo 的 backpressure 是 **自然的** — 每個 operator 之間的 queue 有容量上限。

```
Operator A ──[BatchSender(size=1000 rows)]──▶ Operator B ──[BatchSender(size=1000 rows)]──▶ Operator C

A 處理太快？
    │
    ▼
BatchSender.send() 發現 queued_messages >= size
    │
    ▼
A 的 tokio task 被 await 暫停（不是 thread block）
    │
    ▼
等 B 從 queue 消費 → BatchReceiver.recv() → notify.notify_waiters()
    │
    ▼
A 被喚醒，繼續送
```

**核心程式碼**（`arroyo-operator/src/context.rs:116-157`）：
```rust
pub async fn send(&self, item: QueueItem) -> Result<(), SendError<QueueItem>> {
    let count = message_count(&item, self.size);
    loop {
        let cur = self.queued_messages.load(Ordering::Acquire);
        if cur as usize + count as usize <= self.size as usize {
            // 有空間，CAS 搶佔容量
            match self.queued_messages.compare_exchange(cur, cur + count, ...) {
                Ok(_) => return self.tx.send(item),
                Err(_) => continue,  // CAS 失敗，重試
            }
        } else {
            // 沒空間 — backpressure！
            let notified = self.notify.notified();
            // double-check
            let cur = self.queued_messages.load(Ordering::Acquire);
            if cur as usize + count as usize <= self.size as usize {
                continue;
            }
            notified.await;  // 暫停，等 receiver 消費後喚醒
        }
    }
}
```

**特點**：
- Backpressure 是 **edge-level** 的 — 每條 operator 之間的連線都有獨立的背壓
- 機制是 **async 暫停** — 上游 task 被 tokio runtime 暫停，零 CPU 消耗
- Queue 容量以 **row count** 計（不是 batch count），更精確
- 背壓會自然向上游傳播 — A→B 滿了，A 暫停，A 的 input queue 也會滿，再往上傳
- 不需要回退重處理 — 資料還在 queue 裡，只是暫停送入

### 差異深度分析

| | Quix | Arroyo |
|---|---|---|
| 觸發點 | Sink flush 失敗 | 任何 operator 間的 queue 滿 |
| 粒度 | 整個 pipeline（暫停所有 partition） | 單條 edge（只暫停對應的上游 operator） |
| 機制 | 暫停 + 回退 + 重處理 | async await（零成本暫停） |
| 資料重處理 | 是（回退到 starting offset） | 否（資料在 queue 中等待） |
| 中間節點背壓 | 不存在（同步鏈） | 每層都有 |
| 實作複雜度 | 簡單 | 複雜（CAS + notify + double-check） |

**Quix 為什麼不需要中間背壓**：因為整條函數鏈是同步執行的閉包 `f(g(h(value)))`。
沒有 buffer、沒有 queue — consumer poll 一條、同步跑完、再 poll 下一條。
Consumer 本身的 `fetch.queue.backoff.ms` 和 Kafka 的 `max.poll.interval.ms` 就是唯一的流量控制。

---

## 4. Shuffle（重分區）

### Quix：透過 Kafka Topic 重分區

```
sdf.group_by("customer_id")
         │
         ▼
    自動建立 repartition topic
    "repartition__customer_id"
         │
         ▼
    produce(key=customer_id, value=original_value)
    到 repartition topic
         │
         ▼
    同一個 Application 的 consumer
    重新從 repartition topic 消費
    （Kafka 保證相同 key 去相同 partition）
```

**核心程式碼**（`quixstreams/dataframe/dataframe.py:530-538`）：
```python
def group_by(self, key, name=None, ...):
    """
    "Groups" messages by re-keying them via the provided group_by operation.
    group_by generates a new topic with the "repartition__" prefix
    that copies the settings of original topics.
    """
```

**特點**：
- Shuffle = **寫入新 Kafka topic + 重新消費**
- 利用 Kafka 原生的 key-based partitioning（hash(key) % num_partitions）
- **穿越網路**：即使在同一台機器，也要經過 Kafka broker
- 延遲高：produce → broker 持久化 → consumer poll → 至少幾十 ms
- 優點：可靠、簡單、利用 Kafka 原生保證

### Arroyo：記憶體內 Hash Shuffle + 跨 Worker 網路傳輸

```
同一 Worker 內：
Operator A
    │
    ▼
repartition(record_batch, routing_keys, num_partitions)
    │
    ├── hash(routing_keys) → partition_id
    ├── sort by partition_id
    └── slice batch → 每個 partition 一個 sub-batch
         │
         ├── partition 0 ──▶ BatchSender[0] ──▶ Operator B (subtask 0)
         ├── partition 1 ──▶ BatchSender[1] ──▶ Operator B (subtask 1)
         └── partition 2 ──▶ BatchSender[2] ──▶ Operator B (subtask 2)

跨 Worker：
Worker 1                              Worker 2
Operator A                            Operator B
    │                                      ▲
    ▼                                      │
BatchSender ──▶ OutNetworkLink ──TCP──▶ InNetworkLink ──▶ BatchSender
                (Arrow IPC encode)      (Arrow IPC decode)
                (100ms flush interval)
```

**核心程式碼**（`arroyo-operator/src/context.rs:506-559`）：
```rust
fn repartition(record, keys, qs) -> impl Iterator<Item = (usize, RecordBatch)> {
    if let Some(keys) = keys {
        // Key-based: hash → partition → sort → slice
        hash_utils::create_hashes(&keys[..], &get_hasher(), &mut buf);
        let servers = server_for_hash_array(&buf_array, qs);
        let indices = sort_to_indices(&servers, None, None);
        // ... slice by partition ranges
    } else {
        // Random round-robin
        let range_size = record.num_rows() / qs + 1;
        let rotation = rand::rng().random_range(0..qs);
    }
}
```

**特點**：
- 同 Worker 內：純記憶體 queue，零網路開銷
- 跨 Worker：TCP 直連，Arrow IPC 編碼，100ms flush interval
- **不經過 Kafka** — 零持久化延遲
- 批次化 shuffle — 整個 batch 一起 hash/sort/slice，向量化操作
- 代價：shuffle 途中的資料在故障時會丟（靠 checkpoint 恢復）

### 差異深度分析

| | Quix | Arroyo |
|---|---|---|
| 媒介 | Kafka topic | 記憶體 queue / TCP 直連 |
| 延遲 | 高（經過 broker，幾十 ms） | 低（記憶體 <1ms，跨 Worker 幾 ms） |
| 持久性 | 有（Kafka 持久化） | 無（故障靠 checkpoint 恢復） |
| Hash 算法 | Kafka default partitioner | Arrow hash_utils（可能是 xxhash） |
| 批次化 | 逐筆 produce | 整個 batch 向量化 hash + sort |
| 跨機器傳輸 | Kafka protocol | Arrow IPC over TCP（自訂 header + 100ms flush） |
| Exactly-once shuffle | Kafka transaction 保證 | Barrier alignment 保證 |

**設計哲學差異**：
- Quix 把 Kafka 當作「萬用膠水」— 任何需要持久化、分區、容錯的地方都用 Kafka topic
- Arroyo 把 Kafka 只當「入口和出口」— 中間處理全部在記憶體/網路完成，靠 checkpoint 容錯

---

## 5. 每筆資料的可觀測性

### Quix：最小化，callback-based

| 指標 | 來源 | 粒度 |
|------|------|------|
| 訊息處理完成 | `on_message_processed(topic, partition, offset)` callback | 每筆 |
| 訊息大小 | `MessageContext.size` (bytes) | 每筆可取，但沒有自動收集 |
| 處理錯誤 | `on_processing_error(exc, row, logger)` callback | 每筆（出錯時） |
| Consumer 錯誤 | `on_consumer_error(exc, message, logger)` callback | 每筆（出錯時） |
| Producer 錯誤 | `on_producer_error(exc, row, logger)` callback | 每筆（出錯時） |
| Offset 追蹤 | `Checkpoint._tp_offsets` | 每筆（記錄到記憶體） |
| Broker 狀態 | `consumer._broker_states`, `_stats_cb()` | 30s 定時 |

**沒有**：
- 沒有內建 metrics 系統（沒有 counter、gauge、histogram）
- 沒有 per-operator 指標（整條函數鏈是一個閉包，看不到中間步驟）
- 沒有 throughput 自動統計（要自己在 `on_message_processed` 裡算）
- 沒有 latency 追蹤（進入函數鏈到離開的時間）

### Arroyo：內建 Prometheus metrics，per-operator per-batch

| 指標 | 來源 | 粒度 |
|------|------|------|
| `MessagesReceived` | `operator.rs:1001` | per-operator, per-batch（inc_by num_rows） |
| `MessagesSent` | `context.rs:564` | per-operator, per-batch |
| `BatchesReceived` | `operator.rs:1001` | per-operator, per-batch |
| `BatchesSent` | `context.rs:566` | per-operator, per-batch |
| `BytesReceived` | `operator.rs:1003` | per-operator, per-batch |
| `BytesSent` | `context.rs:567` | per-operator, per-batch |
| `DeserializationErrors` | per-connection | 每筆（出錯時） |
| Queue 深度 | `context.rs:593-603` | 每次 send 後更新 |
| Queue 剩餘容量 | `tx_queue_rem_gauges` | 每次 send 後更新 |
| Queue bytes | `tx_queue_bytes_gauges` | 每次 send 後更新 |
| Checkpoint 時間 | `checkpoint_state.rs:196-208` | 每次 checkpoint |
| Checkpoint alignment 時間 | `checkpoint_state.rs:204` | 每次 checkpoint |

**Metrics label**：
```
node_id, subtask_idx, operator_name, connection_id
```

每個 metric 都帶 operator 粒度的 label，可以在 Grafana 中看到每個 operator 的 throughput、每條 edge 的 queue 深度。

### 差異深度分析

| | Quix | Arroyo |
|---|---|---|
| Metrics 系統 | 無（用戶自行 callback） | Prometheus（IntCounter, Gauge） |
| 粒度 | Pipeline 級（看不到中間步驟） | Operator 級（每個 operator 獨立指標） |
| Throughput | 手動算（`on_message_processed`） | 自動（`MessagesReceived/Sent` counter） |
| Bytes | 有（`MessageContext.size`），但沒自動收集 | 自動（`BytesReceived/Sent` counter） |
| Queue 監控 | 不適用（無 queue） | 自動（capacity, remaining, bytes） |
| Checkpoint 指標 | 無 | alignment_time, sync_time, commit_time, size_bytes |
| 接入方式 | 自己寫 exporter（見 frontend.md 方案） | 原生 Prometheus endpoint |
| 成本 | 零（沒收集就沒成本） | 極低（per-batch counter increment + label cache） |

**Arroyo 的 metrics cache 設計值得學習**（`arroyo-metrics/src/lib.rs:144-172`）：
```rust
// 用 OnceLock + RwLock + HashMap 做 counter 快取
// 避免每次都 with_label_values() 查表
static CACHE: OnceLock<Arc<RwLock<HashMap<(TaskCounters, Arc<ChainInfo>), (IntCounter, bool)>>>>;
```
第一次查表後快取，後續只做 `counter.inc()`，開銷 < 10ns。

---

## 總結：設計哲學對比

| 維度 | Quix Streams | Arroyo |
|---|---|---|
| **核心哲學** | 「Kafka Streams for Python」— 簡單、嵌入式、逐筆處理 | 「Flink in Rust」— 分散式、批次處理、完整引擎 |
| **資料流** | 同步閉包鏈 `f(g(h(x)))` | async operator pipeline + bounded queue |
| **Checkpoint** | 定時 flush（簡單有效） | Barrier alignment（分散式一致） |
| **Backpressure** | Sink 層回退重處理 | 每條 edge bounded queue + async await |
| **Shuffle** | Kafka topic（可靠但慢） | 記憶體/TCP（快但依賴 checkpoint 容錯） |
| **可觀測性** | 最小化 callback | 內建 Prometheus per-operator metrics |
| **適合場景** | Python 生態、中等吞吐量、快速開發 | 高吞吐量、低延遲、需要分散式擴展 |

Quix 的最大優勢是 **簡單** — 沒有分散式協調、沒有 barrier alignment、沒有 operator queue。
一個 Python process 跑完所有事情，debug 容易、部署簡單。

Arroyo 的最大優勢是 **效能和可擴展性** — Arrow 向量化、Rust 原生效能、分散式 operator pipeline。
但代價是系統複雜度高得多（checkpoint alignment、network shuffle、queue 管理）。
