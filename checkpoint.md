# Quix Streams Checkpoint 機制深度源碼解讀

> Checkpoint 是 Quix Streams 中確保**訊息處理 + state 更新 + offset commit** 三者一致性的核心機制。
> 本文件包含所有相關源碼，不需要另外開檔案閱讀。

---

## 目錄

1. [Checkpoint 概觀](#1-checkpoint-概觀)
2. [ProcessingContext — Checkpoint 的管理者](#2-processingcontext--checkpoint-的管理者)
3. [BaseCheckpoint — 基底類別](#3-basecheckpoint--基底類別)
4. [Checkpoint — 完整實作](#4-checkpoint--完整實作)
5. [Commit 五步驟詳解](#5-commit-五步驟詳解)
6. [PartitionTransaction — State 變更的 Changelog 生產](#6-partitiontransaction--state-變更的-changelog-生產)
7. [Exactly-Once vs At-Least-Once](#7-exactly-once-vs-at-least-once)
8. [端到端流程圖](#8-端到端流程圖)
9. [關鍵源碼索引](#9-關鍵源碼索引)

---

## 1. Checkpoint 概觀

### 1.1 Checkpoint 模型：Continuous + Periodic Checkpoint

Quix Streams 的 checkpoint **既不是 micro-batch，也不是 barrier injection，也不是 epoch**。
它是 **Continuous record-at-a-time processing with periodic checkpoint**（逐筆處理 + 週期性 checkpoint）。

**運作方式**：
1. 主迴圈**每次只 poll 一筆訊息**，立即處理（非 batch）
2. 處理完後 state 變更累積在 `PartitionTransactionCache`，offset 累積在 `_tp_offsets`
3. 每次處理完都檢查「到期了嗎？」（時間 or 訊息數）
4. 到期時，執行一次性的 5-step atomic commit

```python
# quixstreams/app.py:931-941 — 主迴圈
processing_context.init_checkpoint()       # 建立第一個 checkpoint

while run_tracker.running:
    process_message(dataframes_composed)    # ★ poll 一筆，處理一筆（非 batch）
    processing_context.commit_checkpoint()  # ★ 檢查到期 → 到期就 commit
```

```python
# quixstreams/app.py:986-1034 — _process_message 內部
rows = self._consumer.poll_row(...)        # ★ poll 一筆訊息
# ...
for row in rows:                           # ★ 逐 row 執行 SDF pipeline
    context.run(dataframe_composed[topic_name], row.value, row.key, ...)
# ...
self._processing_context.store_offset(     # ★ 記錄 offset 到 checkpoint
    topic=topic_name, partition=partition, offset=offset
)
```

```python
# quixstreams/checkpointing/checkpoint.py:58-66 — 到期判斷
def expired(self) -> bool:
    """
    ★ 兩種到期條件（OR 關係）：
      1. 時間到了：monotonic clock 超過 commit_interval（預設 5 秒）
      2. 訊息數到了：0 < commit_every <= total_offsets_processed
    """
    return (time.monotonic() - self._commit_interval) >= self._created_at or (
        0 < self._commit_every <= self._total_offsets_processed
    )
```

### 1.2 為什麼不需要 Barrier / Epoch？

因為 Quix Streams 是**單 process、單 consumer thread** 的架構：

| 模型 | 代表系統 | 機制 | 為什麼需要 | Quix Streams |
|---|---|---|---|---|
| **Micro-batch** | Spark Structured Streaming | 累積一批資料再處理 | 將 stream 切成 batch 來簡化處理 | ❌ 逐筆處理，延遲更低 |
| **Barrier injection** | Flink (Chandy-Lamport) | Coordinator 注入 barrier 到資料流，所有 operator 的 barrier 對齊才 snapshot | **多節點間需要一致的 snapshot 切點** | ❌ 單 process，不需要跨節點對齊 |
| **Epoch** | Kafka Streams (EOS) | Transaction 綁定 epoch ID，coordinator 分配 | 多 instance 間的 exactly-once 協調 | ❌ 單 process，用 Kafka transaction 就夠 |
| **Periodic checkpoint** | **Quix Streams** | 逐筆處理，定時/定量觸發 5-step commit | 單 process 只需要定期 flush 即可 | ✅ |

---

#### 具體範例：為什麼分散式需要這些機制，而單節點不需要

---

**❌ 問題一：為什麼 Flink 需要 Barrier，而 Quix Streams 不需要？**

先搞清楚 Flink 的架構。Flink 的一個 pipeline 會**平行化**：

```
場景：計算每個 user 的累計消費金額
Kafka topic "orders" 有 2 個 partition

Flink pipeline: Source → keyBy(user) → sum(amount) → Sink
                        ↑ 這步會 shuffle：同一個 user 的資料要送到同一個 subtask
```

Flink 會展開成這樣的**物理執行圖**（每個框是獨立的 thread/process）：

```
┌──────────────────┐       ┌──────────────────────────┐
│ Source subtask 0 │──┬──▶ │ Aggregate subtask 0      │
│ (讀 partition 0) │  │    │ (負責 key: Alice, Carol)  │
└──────────────────┘  │    │ state: {Alice→?, Carol→?} │
                      │    └──────────────────────────┘
                      │              ▲
                      │    ┌─────────┘  (shuffle: 按 key hash 分配)
                      │    │
┌──────────────────┐  │    │  ┌──────────────────────────┐
│ Source subtask 1 │──┴──▶─┴▶ │ Aggregate subtask 1      │
│ (讀 partition 1) │          │ (負責 key: Bob, Dave)     │
└──────────────────┘          │ state: {Bob→?, Dave→?}    │
                              └──────────────────────────┘
```

**重點**：Aggregate subtask 0 會同時收到來自 **Source 0 和 Source 1** 的資料。
因為 Alice 的訂單可能在 partition 0 也可能在 partition 1，但 keyBy 後都要送到同一個 subtask。

現在，假設 Kafka 裡的資料是這樣：

```
partition 0: [Alice +100] [Carol +50] [Alice +30] [Carol +20] [Alice +10] ...
              offset 0     offset 1    offset 2    offset 3    offset 4

partition 1: [Bob +200] [Alice +80] [Dave +40] ...
              offset 0   offset 1    offset 2
```

**問題來了**：某個時刻 JobManager 想做 checkpoint。

```
Source subtask 0 已經讀到 offset=4，把 5 筆資料都 shuffle 出去了
Source subtask 1 比較慢，才讀到 offset=1，只 shuffle 出 2 筆

此刻 Aggregate subtask 0 的 state 是什麼？
  - 收到了 Source 0 送來的 Alice +100, +30, +10 和 Carol +50, +20
  - 收到了 Source 1 送來的 Alice +80
  - state = {Alice: 220, Carol: 70}    ← 包含 Source 0 的 offset 0~4 + Source 1 的 offset 0~1
```

如果就這樣 snapshot，紀錄：
- Source 0: offset=4
- Source 1: offset=1
- Aggregate 0 state: {Alice: 220, Carol: 70}

**看起來沒問題？但問題出在 Source 1 的 offset=2 (Dave +40) 還在網路傳輸中**。
Source subtask 1 已經讀了 offset=2 並送出去了，但 Aggregate subtask 1 還沒收到。

如果此刻 crash 然後 recovery：
- Source 1 從 offset=1 重新讀（因為 checkpoint 紀錄 offset=1）
- Source 1 重新送出 offset=1 的 Alice +80 → Aggregate 0 收到 → **Alice 被加了兩次！**

```
Recovery 後 Alice = 220 + 80 = 300 ← ❌ 正確應該是 220
```

**根本原因**：Source subtask 1「已讀但下游還沒處理完」的資料，在 snapshot 中無處安放。
snapshot 紀錄 offset=1，但有些 offset=1 之後的資料已經影響了下游的 state。

**Flink Barrier 的解法**：

```
Step 1: JobManager 說「做 checkpoint」
        ↓
Step 2: 每個 Source subtask 在當前位置注入一個特殊的 barrier 訊息

Source 0: [Alice+100] [Carol+50] [Alice+30] [Carol+20] [Alice+10] |barrier(cp=1)| ...
          offset 0    offset 1   offset 2   offset 3   offset 4   ↑ 在 offset 4 後面

Source 1: [Bob+200] [Alice+80] |barrier(cp=1)| [Dave+40] ...
          offset 0   offset 1  ↑ 在 offset 1 後面    offset 2
        ↓
Step 3: barrier 像普通資料一樣流過 shuffle 網路

Aggregate subtask 0 的輸入 channel：
  來自 Source 0: ... [Alice+10] [barrier] ...
  來自 Source 1: ... [Alice+80] [barrier] ...
        ↓
Step 4: ★ Barrier 對齊（Barrier Alignment）

Aggregate subtask 0：
  - 先收到 Source 0 的 barrier → 暫停處理 Source 0 的資料，繼續處理 Source 1
  - 再收到 Source 1 的 barrier → ★ 兩邊 barrier 都到了！
  - 此刻 snapshot state: {Alice: 220, Carol: 70}
  - 這個 state 精確反映了「Source 0 offset 0~4 + Source 1 offset 0~1」的所有資料
  - Source 1 offset=2 (Dave+40) 一定在 barrier 後面，還沒被處理 → 不會污染 snapshot
```

**barrier 保證的是**：當所有上游 barrier 到齊時，下游 state 精確反映 barrier 之前的所有資料，barrier 之後的資料**一定**還沒被處理。

**Quix Streams 為什麼不需要？**

```python
# 單 thread，沒有 shuffle 網路，沒有「資料在傳輸中」的問題：
while running:
    msg = consumer.poll()          # offset=1
    state["Alice"] += 100          # ★ 立即更新，沒有網路延遲
    store_offset("orders", 0, 1)   # ★ offset 和 state 永遠同步

    msg = consumer.poll()          # offset=2
    state["Bob"] += 200
    store_offset("orders", 0, 2)

    # 5 秒到了，checkpoint:
    commit()
    # state = {Alice: 100, Bob: 200}, offset = 2
    # ★ 不可能有「已讀但還沒處理」的資料 — 讀一筆處理一筆，沒有中間狀態
```

單 thread 不存在「資料在 shuffle 網路中傳輸」的問題。讀一筆 → 處理一筆 → 下一筆。
checkpoint 時所有已讀的資料都已處理完畢，不需要 barrier 來標記邊界。

---

**❌ 問題二：雙流 Join 場景 — Barrier 最不可或缺的地方**

```
場景：訂單流 join 付款流，找出「已付款的訂單」
```

**Flink 雙流 Join**：

```
┌────────────────────┐
│ Source: orders      │──┐
│ (讀 orders topic)   │  │    ┌──────────────────────────────────────┐
└────────────────────┘  ├──▶ │ Join operator                         │
                        │    │ state:                                │
┌────────────────────┐  │    │   left_buffer:  {order_id → order}    │
│ Source: payments    │──┘    │   right_buffer: {order_id → payment}  │
│ (讀 payments topic) │       └──────────────────────────────────────┘
└────────────────────┘
```

Join operator 同時消費兩個流，維護兩邊的 buffer state。

假設：
```
orders topic:   [order_1, $100] [order_2, $200] [order_3, $50]
                 offset 0       offset 1        offset 2

payments topic: [pay_1, order_1] [pay_2, order_2]
                 offset 0        offset 1
```

**沒有 barrier 會怎樣？**

```
時間線：
T1: Join 收到 orders offset=0 (order_1) → 存入 left_buffer
T2: Join 收到 payments offset=0 (pay_1, match order_1) → ★ join 成功，輸出結果
T3: Join 收到 orders offset=1 (order_2) → 存入 left_buffer
T4: ← 此刻想 checkpoint →
T5: Join 收到 payments offset=1 (pay_2, match order_2) → join 成功

如果 T4 直接 snapshot（沒有 barrier）：
  orders offset = 1（已處理到 offset 1）
  payments offset = 0（已處理到 offset 0）
  ← 但等等！payments offset=1 可能已經從 Source 讀出來了，
    正在網路中傳輸，還沒到 Join operator

兩種情況：
A) payments offset=1 還沒到 Join → snapshot 是一致的 → OK
B) payments offset=1 已經到了但還在 operator 的 input buffer 中
   → 這筆資料的狀態是什麼？算「已處理」還是「未處理」？
   → 如果 offset 記為 0 但資料已經影響了 state → ❌ 不一致
```

**Barrier 的解法**：

```
JobManager: "做 checkpoint cp=7"

orders Source:   ... [order_2] |barrier_cp7| [order_3] ...
                    offset 1                 offset 2
                    ↑ barrier 後面的不能被 Join 處理

payments Source: ... [pay_1] |barrier_cp7| [pay_2] ...
                   offset 0               offset 1
                   ↑ barrier 後面的不能被 Join 處理

Join operator 的 barrier alignment：
  1. 先收到 orders 的 barrier → 暫停處理 orders channel
     繼續處理 payments channel
  2. 收到 payments 的 barrier → ★ 兩邊 barrier 都到了
  3. Snapshot:
     - orders offset = 1
     - payments offset = 0
     - left_buffer = {order_1, order_2}  ← 精確反映 orders 0~1
     - right_buffer = {pay_1}            ← 精確反映 payments 0~0
     - 已輸出: (order_1, pay_1)
     ★ 三者完全一致，recovery 後可以從這個點精確恢復
```

**Quix Streams 的雙流 Join 為什麼不需要 barrier？**

Quix Streams 是單 thread，兩個 topic 的訊息由同一個 consumer poll：

```python
# quixstreams/app.py 主迴圈（簡化）
while running:
    msg = consumer.poll()  # ★ 可能拿到 orders 的訊息，也可能拿到 payments 的

    if msg.topic == "orders":
        state["left_buffer"][msg.order_id] = msg
        store_offset("orders", partition, msg.offset)
    elif msg.topic == "payments":
        state["right_buffer"][msg.order_id] = msg
        if msg.order_id in state["left_buffer"]:
            emit_join_result(...)
        store_offset("payments", partition, msg.offset)

    commit_checkpoint()  # 到期就 commit
```

```
時間線（單 thread）：
T1: poll() → orders offset=0  → 處理 → store_offset("orders", 0, 0)
T2: poll() → payments offset=0 → 處理 → store_offset("payments", 0, 0)
T3: poll() → orders offset=1  → 處理 → store_offset("orders", 0, 1)
T4: commit_checkpoint() → expired! → commit:
    offsets = {orders: 1, payments: 0}
    state = {left_buffer: {order_1, order_2}, right_buffer: {pay_1}}
    ★ 完全一致 — 因為 T3 處理完才到 T4，不存在「處理到一半」的資料
T5: poll() → payments offset=1 → 處理 → ...
```

**關鍵差異**：

```
Flink:
  Source 0 ──(網路)──▶ ┐
                        ├─ Join ──▶ ...
  Source 1 ──(網路)──▶ ┘

  三個獨立的 thread/process，中間有網路。
  「Source 已讀」不代表「Join 已處理」→ 需要 barrier 來標記精確邊界。

Quix Streams:
  ┌─────────────────────────────────────────┐
  │ 單一 thread:                             │
  │   poll() → 處理 → store_offset          │
  │   poll() → 處理 → store_offset          │
  │   commit()                              │
  │                                         │
  │ ★ 「poll 出來」=「立即處理」=「offset 更新」│
  │   三者在同一個 thread 同步完成             │
  │   不存在「已讀未處理」的中間狀態            │
  └─────────────────────────────────────────┘
```

---

**❌ 問題三：為什麼 Spark 需要 Micro-batch，而 Quix Streams 不需要？**

Spark Structured Streaming 用 micro-batch 把 stream 切成小的 DataFrame：

```
時間 T1: 累積 offset 0-99   → 組成一個 DataFrame → 分散到 100 個 executor 平行處理 → 寫出結果
時間 T2: 累積 offset 100-199 → 組成一個 DataFrame → 分散到 100 個 executor 平行處理 → 寫出結果
```

為什麼要 batch？因為 Spark 的計算模型是 **MapReduce**：
- 需要知道「這一批有哪些資料」才能 shuffle、aggregate
- 需要一個明確的「批次邊界」來觸發 reduce 階段
- 需要等所有 executor 都處理完這一批，才能 commit offset

**Quix Streams 為什麼不需要？**

```python
# Quix Streams 的 aggregate 不需要等一批處理完：
msg = consumer.poll()  # {user: "Alice", amount: 100}
# 直接更新 RocksDB cache，不需要等其他訊息
state["Alice"] = state.get("Alice", 0) + 100  # 立即可讀（read-your-own-writes cache）

msg = consumer.poll()  # {user: "Alice", amount: 50}
state["Alice"] = state.get("Alice", 0) + 50   # 150，基於上一筆的結果
```

單 thread + RocksDB state store 允許逐筆累加，不需要把資料切成 batch 再 shuffle。

---

**❌ 問題四：為什麼 Kafka Streams 需要 Epoch，而 Quix Streams 不需要？**

Kafka Streams 允許多個 instance（process）消費同一個 consumer group：

```
Instance 1 (處理 partition 0-2)  ←── 同一個 consumer group ──→  Instance 2 (處理 partition 3-5)
```

問題：Rebalance 時，partition 從 Instance 1 轉移到 Instance 2。
如果 Instance 1 的 transaction 還沒 commit 完，Instance 2 就開始處理，會導致 **zombie transaction**：

```
Instance 1: begin_transaction() → produce(changelog) → ...（掛了或 rebalance）
Instance 2: 接手 partition → begin_transaction() → produce(changelog)
                                                      ↑
                        Instance 1 的 zombie transaction 可能也 commit → 重複！
```

Kafka Streams 的解法：**Epoch（fencing）**
```
Coordinator 分配 epoch_id=1 給 Instance 1
Rebalance 後分配 epoch_id=2 給 Instance 2
Broker 看到 epoch_id=1 的 transaction → 拒絕（因為已有 epoch_id=2）→ zombie 被 fence 掉
```

**Quix Streams 為什麼不需要？**

```python
# 只有一個 process，不存在 zombie：
#
# Quix Streams 的 exactly-once：
init_checkpoint()
  → producer.begin_transaction()   # 只有這一個 process 會開 transaction

commit_checkpoint()
  → producer.commit_transaction(offsets, consumer_group_metadata)
  # ★ 只有一個 process，不可能有另一個 process 的 zombie transaction
  # consumer_group_metadata 裡的 generation_id 就足以 fence（Kafka 原生機制）
```

單 process 不會有 zombie 問題，Kafka consumer 原生的 `generation_id` 就足以處理 rebalance 後的 fencing，不需要額外的 epoch 機制。

---

**一句話總結**：
> 這三種機制的本質都是在解決同一個問題：**多個獨立執行者之間有網路延遲，「已發送」不等於「已處理」，需要一個機制來對齊一致性切點。**
> 單 thread 沒有網路、沒有平行，「讀到」就是「處理完」，天然一致，不需要任何對齊機制。

---

**關鍵洞察**：
- 因為只有一個 thread 在處理，所以「checkpoint 到期時累積的所有 state + offset」天然就是一個一致的 snapshot
- 不需要 barrier 來「對齊」多個 operator 的進度 — 只有一個 thread，進度天然一致
- Exactly-once 靠的是 Kafka Transaction（`producer.begin_transaction()` / `producer.commit_transaction()`），不需要分散式 epoch 協調

---

#### 多 Pod 水平擴展（6 partition / 3 pod）為什麼也不需要 Barrier？

```
場景：topic "orders" 有 6 個 partition，部署 3 個 pod
consumer_group = "my-app"

Kafka consumer group 自動分配：
  Pod A → partition 0, 1
  Pod B → partition 2, 3
  Pod C → partition 4, 5
```

每個 pod 是一個**完全獨立的** `Application` process，各自有自己的一切：

```
Pod A 的世界：                     Pod B 的世界：                     Pod C 的世界：
┌──────────────────────┐          ┌──────────────────────┐          ┌──────────────────────┐
│ while running:       │          │ while running:       │          │ while running:       │
│   poll() → p0 or p1  │          │   poll() → p2 or p3  │          │   poll() → p4 or p5  │
│   process()          │          │   process()          │          │   process()          │
│   store_offset()     │          │   store_offset()     │          │   store_offset()     │
│   commit_checkpoint()│          │   commit_checkpoint()│          │   commit_checkpoint()│
│                      │          │                      │          │                      │
│ Checkpoint:          │          │ Checkpoint:          │          │ Checkpoint:          │
│  _tp_offsets:        │          │  _tp_offsets:        │          │  _tp_offsets:        │
│   (orders,0) → 150  │          │   (orders,2) → 200  │          │   (orders,4) → 180  │
│   (orders,1) → 145  │          │   (orders,3) → 190  │          │   (orders,5) → 175  │
│                      │          │                      │          │                      │
│ RocksDB (本地磁碟):   │          │ RocksDB (本地磁碟):   │          │ RocksDB (本地磁碟):   │
│  partition 0 state   │          │  partition 2 state   │          │  partition 4 state   │
│  partition 1 state   │          │  partition 3 state   │          │  partition 5 state   │
│                      │          │                      │          │                      │
│ Kafka Transaction:   │          │ Kafka Transaction:   │          │ Kafka Transaction:   │
│  獨立的 transactional │          │  獨立的 transactional │          │  獨立的 transactional │
│  .id                 │          │  .id                 │          │  .id                 │
└──────────────────────┘          └──────────────────────┘          └──────────────────────┘
        ↕ 零互動                          ↕ 零互動                          ↕ 零互動
```

**為什麼不需要 barrier？因為 pod 之間沒有任何資料流動。**

從源碼看，`Checkpoint._tp_offsets` 是 `Dict[Tuple[str, int], int]`：

```python
# quixstreams/checkpointing/checkpoint.py:46-49
self._tp_offsets: Dict[Tuple[str, int], int] = {}
self._starting_tp_offsets: Dict[Tuple[str, int], int] = {}
```

每個 pod 的 Checkpoint 只紀錄自己 poll 到的 partition 的 offset。
Pod A commit 時只 commit `(orders,0)` 和 `(orders,1)` 的 offset，跟 Pod B、C 無關。

同樣地，`_store_transactions` 也只包含自己處理的 partition 的 state transaction：

```python
# quixstreams/checkpointing/checkpoint.py:51
self._store_transactions: Dict[Tuple[str, int, str], PartitionTransaction] = {}
```

Pod A 的 RocksDB 只有 partition 0, 1 的 state。Pod B crash 不影響 Pod A 的任何東西。

**跟 Flink 的根本差異**：

```
Flink（需要 barrier）：

  Kafka partition 0 ──▶ Source subtask 0 ──┐
                                            ├──(keyBy shuffle)──▶ Aggregate subtask 0
  Kafka partition 1 ──▶ Source subtask 1 ──┘                     state: {Alice→?, Bob→?}

  ★ Aggregate subtask 0 的 state 受「兩個 Source」影響
  ★ 兩個 Source 速度不同 → state 混合了不同進度的資料 → 需要 barrier 對齊


Quix Streams 3 pod（不需要 barrier）：

  Kafka partition 0 ──▶ ┐
                        ├── Pod A (獨立處理，獨立 state，獨立 checkpoint)
  Kafka partition 1 ──▶ ┘

  Kafka partition 2 ──▶ ┐
                        ├── Pod B (獨立處理，獨立 state，獨立 checkpoint)
  Kafka partition 3 ──▶ ┘

  Kafka partition 4 ──▶ ┐
                        ├── Pod C (獨立處理，獨立 state，獨立 checkpoint)
  Kafka partition 5 ──▶ ┘

  ★ 每個 pod 的 state 只來自自己的 partition，不受其他 pod 影響
  ★ 每個 pod 就是一個獨立的單 thread 處理器，各自 checkpoint 就好
```

**核心區別**：Flink 的 `keyBy` 會做跨節點 shuffle，讓一個 operator 的 state 依賴多個來源。
Quix Streams 的 Kafka consumer group 按 partition 分配，每個 pod 的 state 只來自自己的 partition，沒有跨 pod 資料流動。

---

**那 `group_by()` repartition 呢？跨 pod 的資料會不會有問題？**

`group_by()` 是 Quix Streams 中唯一會產生「跨 pod 資料流動」的操作：

```python
# quixstreams/dataframe/dataframe.py:602-619
sdf = app.dataframe(topic)
sdf = sdf.group_by("user_id")   # ★ 產生 repartition topic
sdf = sdf.apply(aggregate)
```

`group_by` 的源碼：

```python
# quixstreams/dataframe/dataframe.py:594-619
# 產生一個新的 repartition topic（partition 數跟原始 topic 一樣）
repartition_config = self._topic_manager.derive_topic_config(self._topics)
groupby_topic = self._topic_manager.repartition_topic(
    operation=operation,
    stream_id=self.stream_id,
    config=repartition_config,
    key_serializer=key_serializer,
    ...
)

# ★ 把資料寫到 repartition topic（按新 key hash 分 partition）
self.to_topic(topic=groupby_topic, key=self._groupby_key(key))
# ★ 原始 SDF 到此為止，後面的訊息丟掉（已經寫到 repartition topic 了）
self.filter(lambda _: False)

# ★ 建一個新的 SDF 來消費 repartition topic
groupby_sdf = self.__dataframe_clone__(groupby_topic)
self._registry.register_groupby(source_sdf=self, new_sdf=groupby_sdf)
```

資料流向：

```
Pod A 被分配: source partition 0,1 + repartition partition 0,1

Pod A 的處理流程（單 thread）：

  poll() → 收到 source topic partition 0 的訊息
    → 執行 SDF pipeline 直到 group_by
    → produce 到 repartition topic（按 user_id hash 分 partition）
      → 可能寫到 repartition partition 0（自己），也可能寫到 partition 2（Pod B 的）
    → filter(lambda _: False) → 丟掉，不繼續

  poll() → 收到 repartition topic partition 0 的訊息（可能是自己或 Pod B 寫來的）
    → 執行 group_by 之後的 SDF pipeline
    → state 更新

  commit_checkpoint()
    → _tp_offsets 同時包含 source topic 和 repartition topic 的 offset
    → 一次性 commit 兩個 topic 的 offset + state
```

**為什麼不會有問題？**

```
                    寫入 repartition topic
                   ┌─────────────────────────────────────────────────────┐
                   │                      Kafka Broker                   │
                   │  repartition topic:                                 │
Pod A ── produce ─▶│    partition 0: [msg_a1] [msg_b2] [msg_a3] ...     │──▶ Pod A 消費
Pod B ── produce ─▶│    partition 1: [msg_b1] [msg_a2] ...              │──▶ Pod A 消費
Pod C ── produce ─▶│    partition 2: [msg_c1] [msg_a4] ...              │──▶ Pod B 消費
                   │    ...                                             │
                   └─────────────────────────────────────────────────────┘

★ 關鍵：repartition topic 就是一個普通的 Kafka topic
  - Pod A produce 到 repartition topic → 訊息先落地到 Kafka broker
  - Pod A（或其他 pod）再從 repartition topic 消費
  - 中間經過 Kafka broker，有持久化保證
  - 每個 pod 消費 repartition topic 時，跟消費 source topic 完全一樣
    → 單 thread poll → 處理 → store_offset → commit
```

跟 Flink barrier 的情境不同：

```
Flink 的 keyBy shuffle：
  Source 0 ──(TCP 直連)──▶ Aggregate 0
  ★ 資料在「記憶體 → 網路 → 記憶體」之間流動
  ★ Source 已發送但 Aggregate 還沒收到 → 「在途資料」→ snapshot 不一致

Quix Streams 的 group_by repartition：
  Pod A ──(produce)──▶ Kafka broker ──(consume)──▶ Pod A
  ★ 資料流經 Kafka broker，有持久化
  ★ Pod A 的 checkpoint 同時記錄 source topic 和 repartition topic 的 offset
  ★ 如果 crash：
    - source topic 未 commit 的訊息 → 重新消費 → 重新 produce 到 repartition topic
    - repartition topic 未 commit 的訊息 → 重新消費
    - 可能有重複 produce（at-least-once），但不會丟失
    - exactly-once 模式下，source → repartition 的 produce 在同一個 Kafka transaction
```

**一句話**：`group_by` 把「跨 pod shuffle」轉化成了「寫 Kafka → 讀 Kafka」，
不是記憶體直連，所以不存在「在途資料」的問題。
每個 pod 各自 checkpoint 自己消費的所有 topic（source + repartition）的 offset，一致性由 Kafka 保證。

---

**多 Pod 場景總結**：

| 場景 | Pod 之間有資料流動？ | 需要跨 pod 協調 checkpoint？ |
|---|---|---|
| 純處理（無 group_by） | ❌ 無 | ❌ 各自 checkpoint |
| 有 group_by | ✅ 有（經過 Kafka broker） | ❌ 各自 checkpoint（Kafka 做中間持久化）|
| Flink keyBy | ✅ 有（TCP 直連） | ✅ 需要 barrier 對齊 |

### 1.3 Checkpoint 的職責

**Checkpoint 的職責**：
- 追蹤每個 topic-partition 已處理到哪個 offset
- 管理所有 state store 的 `PartitionTransaction`
- 在到期時（或被 force）觸發一個**原子性的 commit 流程**

**Commit 的五步驟**（順序極為重要）：
1. Flush sinks（外部輸出）
2. Produce changelogs（state 變更寫入 changelog topic）
3. Flush producer（確保訊息全部送達 broker）
4. Commit offsets（Kafka consumer offset commit）
5. Flush state to disk（RocksDB WriteBatch 寫入磁碟）

**為什麼這個順序？**
- Step 2 在 Step 4 之前 → 確保 changelog 已經寫完才 commit offset
- Step 4 在 Step 5 之前 → 如果 Step 5 失敗，offset 已 commit，但 state 可以從 changelog recovery
- Step 1 在 Step 4 之前 → 如果 sink flush 失敗（backpressure），不 commit offset，下次重新處理

---

## 2. ProcessingContext — Checkpoint 的管理者

檔案：`quixstreams/processing/context.py`

```python
@dataclasses.dataclass
class ProcessingContext:
    """
    A class to share processing-related objects
    between `Application` and `StreamingDataFrame` instances.
    """

    commit_interval: float
    producer: InternalProducer
    consumer: InternalConsumer
    state_manager: StateStoreManager
    sink_manager: SinkManager
    dataframe_registry: DataFrameRegistry
    commit_every: int = 0
    exactly_once: bool = False
    printer: Printer = Printer()

    _checkpoint: Optional[Checkpoint] = dataclasses.field(
        init=False, repr=False, default=None
    )

    @property
    def checkpoint(self) -> Checkpoint:
        if self._checkpoint is None:
            raise CheckpointNotInitialized("Checkpoint has not been initialized yet")
        return self._checkpoint

    def store_offset(self, topic: str, partition: int, offset: int):
        """
        Store the offset of the processed message to the checkpoint.
        ★ 每處理完一條訊息就呼叫
        """
        self.checkpoint.store_offset(topic=topic, partition=partition, offset=offset)

    def init_checkpoint(self):
        """
        Initialize a new checkpoint
        ★ 在 Application 啟動時呼叫一次，之後每次 commit 完也會呼叫
        """
        self._checkpoint = Checkpoint(
            commit_interval=self.commit_interval,
            commit_every=self.commit_every,
            state_manager=self.state_manager,
            producer=self.producer,
            consumer=self.consumer,
            sink_manager=self.sink_manager,
            dataframe_registry=self.dataframe_registry,
            exactly_once=self.exactly_once,
        )

    def commit_checkpoint(self, force: bool = False):
        """
        Attempts finalizing the current Checkpoint only if the Checkpoint is "expired",
        or `force=True` is passed, otherwise do nothing.

        ★ 核心決策邏輯：
           - checkpoint 到期了嗎？（時間 or 訊息數）
           - 如果到期且有 offset → commit
           - 如果到期但沒 offset → close（清理 exactly-once transaction）
           - commit 或 close 完後 → 建新的 checkpoint
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

    def __enter__(self):
        self.sink_manager.start_sinks()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.exactly_once:
            self.producer.abort_transaction(5)
        self.printer.clear()
```

### 在 Application 主迴圈中的呼叫

檔案：`quixstreams/app.py:928-955`

```python
processing_context.init_checkpoint()       # ★ 建立第一個 checkpoint
run_tracker.set_as_running()

while run_tracker.running:
    if state_manager.recovery_required:
        state_manager.do_recovery()
        run_tracker.timeout_refresh()
    else:
        process_message(dataframes_composed)            # 處理訊息（內部會呼叫 state 操作）
        processing_context.commit_checkpoint()          # ★ 檢查是否需要 commit
        consumer.resume_backpressured()
        source_manager.raise_for_error()
        # ...

logger.info("Stopping the application")
processing_context.commit_checkpoint(force=True)  # ★ 關閉時 force commit
```

`store_offset` 的呼叫點：

檔案：`quixstreams/app.py:1030-1034`

```python
self._processing_context.store_offset(
    topic=topic_name, partition=partition, offset=offset
)
```

---

## 3. BaseCheckpoint — 基底類別

檔案：`quixstreams/checkpointing/checkpoint.py:29-114`

```python
class BaseCheckpoint:
    """
    Base class to keep track of state updates and consumer offsets and to checkpoint these
    updates on schedule.

    Two implementations exist:
        * one for checkpointing the Application in quixstreams/checkpoint/checkpoint.py
        * one for checkpointing the kafka source in quixstreams/sources/kafka/checkpoint.py
    """

    def __init__(
        self,
        commit_interval: float,
        commit_every: int = 0,
    ):
        self._created_at = time.monotonic()
        # ★ {(topic, partition): processed_offset} — 記錄每個 tp 處理到哪
        self._tp_offsets: Dict[Tuple[str, int], int] = {}
        # ★ {(topic, partition): starting_offset} — 記錄 checkpoint 內第一筆 offset
        #   用於 sink backpressure 時 seek 回去重新處理
        self._starting_tp_offsets: Dict[Tuple[str, int], int] = {}
        # ★ {(topic, partition, store_name): PartitionTransaction} — state 交易
        self._store_transactions: Dict[Tuple[str, int, str], PartitionTransaction] = {}
        # Passing zero or lower will flush the checkpoint after each processed message
        self._commit_interval = max(commit_interval, 0)

        self._commit_every = commit_every
        self._total_offsets_processed = 0

    def expired(self) -> bool:
        """
        ★ 兩種到期條件：
          1. 時間到了（monotonic clock 超過 commit_interval）
          2. 訊息數到了（0 < commit_every <= total_offsets_processed）
        """
        return (time.monotonic() - self._commit_interval) >= self._created_at or (
            0 < self._commit_every <= self._total_offsets_processed
        )

    def empty(self) -> bool:
        """
        ★ 沒有任何 offset → 不需要 commit，只需要 close
        """
        return not bool(self._tp_offsets)

    def store_offset(self, topic: str, partition: int, offset: int):
        """
        Store the offset of the processed message to the checkpoint.
        """
        tp = (topic, partition)
        stored_offset = self._tp_offsets.get(tp, -1)
        # ★ 安全檢查：offset 必須嚴格遞增
        if offset <= stored_offset:
            raise InvalidStoredOffset(
                f"Cannot store offset smaller or equal than already processed"
                f" one: {offset} <= {stored_offset}"
            )
        self._tp_offsets[tp] = offset
        # ★ 記錄第一筆 offset，backpressure 時要 seek 回這裡
        if tp not in self._starting_tp_offsets:
            self._starting_tp_offsets[tp] = offset
        self._total_offsets_processed += 1

    @abstractmethod
    def close(self):
        """
        Perform cleanup (when the checkpoint is empty) instead of committing.
        Needed for exactly-once, as Kafka transactions are timeboxed.
        """

    @abstractmethod
    def commit(self):
        """
        Commit the checkpoint.
        """
        pass
```

---

## 4. Checkpoint — 完整實作

檔案：`quixstreams/checkpointing/checkpoint.py:117-291`

```python
class Checkpoint(BaseCheckpoint):
    """
    Checkpoint implementation used by the application
    """

    def __init__(
        self,
        commit_interval: float,
        producer: InternalProducer,
        consumer: InternalConsumer,
        state_manager: StateStoreManager,
        sink_manager: SinkManager,
        dataframe_registry: DataFrameRegistry,
        exactly_once: bool = False,
        commit_every: int = 0,
    ):
        super().__init__(
            commit_interval=commit_interval,
            commit_every=commit_every,
        )

        self._state_manager = state_manager
        self._consumer = consumer
        self._producer = producer
        self._sink_manager = sink_manager
        self._dataframe_registry = dataframe_registry
        self._exactly_once = exactly_once
        # ★ Exactly-once 模式下，建立 checkpoint 時就開始 Kafka transaction
        if self._exactly_once:
            self._producer.begin_transaction()

    def get_store_transaction(
        self, stream_id: str, partition: int, store_name: str = DEFAULT_STATE_STORE_NAME
    ) -> PartitionTransaction:
        """
        Get a PartitionTransaction for the given store, topic and partition.
        It will return already started transaction if there's one.

        ★ 每個 (stream_id, partition, store_name) 只會有一個 transaction
          在同一個 checkpoint 內，所有對同一個 partition 的 state 操作
          都會累積在同一個 transaction 的 cache 中
        """
        transaction = self._store_transactions.get((stream_id, partition, store_name))
        if transaction is not None:
            return transaction

        store = self._state_manager.get_store(
            stream_id=stream_id, store_name=store_name
        )
        transaction = store.start_partition_transaction(partition=partition)

        self._store_transactions[(stream_id, partition, store_name)] = transaction
        return transaction

    def close(self):
        """
        Perform cleanup (when the checkpoint is empty) instead of committing.
        ★ Exactly-once 下，空的 checkpoint 也要 abort transaction
          因為 Kafka transaction 有 timeout 限制
        """
        if self._exactly_once:
            self._producer.abort_transaction()
```

---

## 5. Commit 五步驟詳解

檔案：`quixstreams/checkpointing/checkpoint.py:181-291`

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
```

### Step 1: Flush Sinks

```python
        # Step 1. Flush sinks
        logger.debug("Checkpoint: flushing sinks")
        backpressured = False
        for sink in self._sink_manager.sinks:
            if backpressured:
                # ★ 一個 sink backpressure 了，其他 sink 的資料也要丟掉
                # 避免重新處理時產生重複
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
                # ★ Backpressure 處理：
                # 1. Pause 所有 data partition
                # 2. Seek 回 checkpoint 開始時的 offset（_starting_tp_offsets）
                # 3. 等 retry_after 秒後 resume
                # 4. 不 commit offset → 下次重新處理這批訊息
                self._consumer.trigger_backpressure(
                    resume_after=exc.retry_after,
                    offsets_to_seek=self._starting_tp_offsets.copy(),
                )
                backpressured = True
        if backpressured:
            # ★ Backpressure → 提前返回，不執行 Step 2-5
            return
```

### Step 2: Produce Changelogs

```python
        # Step 2. Produce the changelogs
        for (
            stream_id,
            partition,
            store_name,
        ), transaction in self._store_transactions.items():
            # ★ 取得這個 stream_id 對應的所有 source topic
            topics = self._dataframe_registry.get_topics_for_stream_id(
                stream_id=stream_id
            )
            # ★ 收集這個 partition 上每個 topic 的最新 processed offset
            # 這些 offset 會寫到 changelog 訊息的 header 中
            # Recovery 時用於判斷 _should_apply_changelog()
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
            # ★ 呼叫 transaction.prepare() → 將 state 變更寫入 changelog topic
            transaction.prepare(processed_offsets=processed_offsets)
```

### Step 3: Flush Producer

```python
        # Step 3. Flush producer to trigger all delivery callbacks and ensure that
        # all messages are produced
        logger.debug("Checkpoint: flushing producer")
        unproduced_msg_count = self._producer.flush()
        if unproduced_msg_count > 0:
            raise CheckpointProducerTimeout(
                f"'{unproduced_msg_count}' messages failed to be produced before "
                f"the producer flush timeout"
            )
```

### Step 4: Commit Offsets

```python
        # Step 4. Commit offsets to Kafka
        # ★ offset + 1：Kafka 的 committed offset 代表「下次要讀的位置」
        offsets = [
            TopicPartition(topic=topic, partition=partition, offset=offset + 1)
            for (topic, partition), offset in self._tp_offsets.items()
        ]

        if self._exactly_once:
            # ★ Exactly-once：用 Kafka transaction 原子性地 commit offset + produced messages
            self._producer.commit_transaction(
                offsets, self._consumer.consumer_group_metadata()
            )
        else:
            # ★ At-least-once：直接 consumer commit（同步）
            logger.debug("Checkpoint: committing consumer")
            try:
                partitions = self._consumer.commit(offsets=offsets, asynchronous=False)
            except KafkaException as e:
                raise CheckpointConsumerCommitError(e.args[0]) from None

            for partition in partitions:
                if partition.error:
                    raise CheckpointConsumerCommitError(partition.error)
```

### Step 5: Flush State to Disk

```python
        # Step 5. Flush state store partitions to the disk together with changelog
        # offsets.
        # ★ 從 producer 拿到每個 changelog topic-partition 最後 produce 的 offset
        produced_offsets = self._producer.offsets
        for transaction in self._store_transactions.values():
            # ★ 取得這個 transaction 對應的 changelog topic-partition
            # 可能為 None（如果 changelog 被停用）
            changelog_tp = transaction.changelog_topic_partition
            # ★ changelog offset 也可能為 None（如果 transaction 沒有任何更新）
            changelog_offset = (
                produced_offsets.get(changelog_tp) if changelog_tp is not None else None
            )
            # ★ 將 cache 中的 state 變更寫入 RocksDB，同時記錄 changelog offset
            transaction.flush(changelog_offset=changelog_offset)
```

---

## 6. PartitionTransaction — State 變更的 Changelog 生產

### 6.1 Transaction 狀態機

檔案：`quixstreams/state/base/transaction.py:160-168`

```python
class PartitionTransactionStatus(enum.Enum):
    STARTED = 1   # ★ 接受 state 更新（get/set/delete）
    PREPARED = 2  # ★ Changelog 已 produce，不接受新更新，等待 flush
    COMPLETE = 3  # ★ 已 flush 到 RocksDB，交易完成
    FAILED = 4    # ★ 出錯，不可再使用
```

狀態轉移：`STARTED → prepare() → PREPARED → flush() → COMPLETE`

### 6.2 `prepare()` — 產生 changelog 訊息

檔案：`quixstreams/state/base/transaction.py:479-535`

```python
@validate_transaction_status(PartitionTransactionStatus.STARTED)
def prepare(self, processed_offsets: Optional[dict[str, int]] = None) -> None:
    """
    Produce changelog messages to the changelog topic for all changes accumulated
    in this transaction and prepare transaction to flush its state to the state
    store.

    After successful `prepare()`, the transaction status is changed to PREPARED,
    and it cannot receive updates anymore.

    If changelog is disabled for this application, no updates will be produced
    to the changelog topic.
    """
    try:
        self._prepare(processed_offsets=processed_offsets)
        self._status = PartitionTransactionStatus.PREPARED
    except Exception:
        self._status = PartitionTransactionStatus.FAILED
        raise

def _prepare(self, processed_offsets: Optional[dict[str, int]]):
    if self._changelog_producer is None:
        return  # ★ Changelog 被停用時直接跳過

    logger.debug(
        f"Flushing state changes to the changelog topic "
        f'topic_name="{self._changelog_producer.changelog_name}" '
        f"partition={self._changelog_producer.partition}"
    )
    # ★ 將 processed offsets 序列化為 JSON，放到 changelog header 中
    source_tp_offset_header = json_dumps(processed_offsets)
    column_families = self._update_cache.get_column_families()

    for cf_name in column_families:
        headers: Headers = {
            CHANGELOG_CF_MESSAGE_HEADER: cf_name,
            CHANGELOG_PROCESSED_OFFSETS_MESSAGE_HEADER: source_tp_offset_header,
        }

        # ★ Produce 所有 update（set）操作
        updates = self._update_cache.get_updates(cf_name=cf_name)
        for prefix_update_cache in updates.values():
            for key, value in prefix_update_cache.items():
                self._changelog_producer.produce(
                    key=key,
                    value=value,
                    headers=headers,
                )

        # ★ Produce 所有 delete 操作（value=None 代表 tombstone）
        deletes = self._update_cache.get_deletes(cf_name=cf_name)
        for key in deletes:
            self._changelog_producer.produce(
                key=key,
                value=None,
                headers=headers,
            )
```

### 6.3 `flush()` — 寫入 RocksDB

檔案：`quixstreams/state/base/transaction.py:537-583`

```python
@validate_transaction_status(
    PartitionTransactionStatus.STARTED, PartitionTransactionStatus.PREPARED
)
def flush(
    self,
    changelog_offset: Optional[int] = None,
):
    """
    Flush the recent updates to the database.
    It writes the WriteBatch to RocksDB and marks itself as finished.

    >***NOTE:*** If no keys have been modified during the transaction
        (i.e. no "set" or "delete" have been called at least once), it will
        not flush ANY data to the database including the offset to optimize I/O.
    """
    try:
        self._flush(changelog_offset)
        self._status = PartitionTransactionStatus.COMPLETE
    except Exception:
        self._status = PartitionTransactionStatus.FAILED
        raise

def _flush(self, changelog_offset: Optional[int]):
    # ★ 如果 cache 是空的（沒有任何 set/delete），直接跳過 → 優化 I/O
    if self._update_cache.is_empty():
        return

    # ★ 安全檢查：changelog offset 不能比已儲存的小
    if changelog_offset is not None:
        current_changelog_offset = self._partition.get_changelog_offset()
        if (
            current_changelog_offset is not None
            and changelog_offset < current_changelog_offset
        ):
            raise InvalidChangelogOffset(
                "Cannot set changelog offset lower than already saved one"
            )

    # ★ 將 cache 寫入 RocksDB（WriteBatch → db.write）
    self._partition.write(
        cache=self._update_cache,
        changelog_offset=changelog_offset,
    )
```

### 6.4 `PartitionTransactionCache` — Read-Your-Own-Writes Cache

檔案：`quixstreams/state/base/transaction.py:53-157`

```python
class PartitionTransactionCache:
    """
    A cache with the data updated in the current PartitionTransaction.
    It is used to read-your-own-writes before the transaction is committed to the Store.

    Internally, updates and deletes are separated into two separate structures
    to simplify the querying over them.
    """

    def __init__(self) -> None:
        # ★ {cf_name: {prefix: {key: value}}} — update 按 prefix 分桶，加速迭代
        self._updated: dict[str, dict[bytes, dict[bytes, bytes]]] = defaultdict(
            lambda: defaultdict(dict)
        )
        # ★ {cf_name: set[key]} — delete 不需要 prefix 分桶
        self._deleted: dict[str, set[bytes]] = defaultdict(set)
        self._empty = True

    def get(
        self, key: bytes, prefix: bytes, cf_name: str = "default",
    ) -> Union[bytes, Marker]:
        """
        ★ 三種返回值：
          - bytes: 在 cache 中找到更新的值
          - Marker.DELETED: 已被刪除（不需要查 store）
          - Marker.UNDEFINED: cache 中沒有（需要查 store）
        """
        if key in self._deleted[cf_name]:
            return Marker.DELETED

        return self._updated[cf_name][prefix].get(key, Marker.UNDEFINED)

    def set(self, key: bytes, value: bytes, prefix: bytes, cf_name: str = "default"):
        self._updated[cf_name][prefix][key] = value
        self._deleted[cf_name].discard(key)  # ★ 如果之前 delete 過，移除 delete 記錄
        self._empty = False

    def delete(self, key: Any, prefix: bytes, cf_name: str = "default"):
        self._updated[cf_name][prefix].pop(key, None)  # ★ 移除 update 記錄
        self._deleted[cf_name].add(key)
        self._empty = False

    def is_empty(self) -> bool:
        return self._empty

    def get_column_families(self) -> Set[str]:
        return set(self._updated.keys()) | set(self._deleted.keys())

    def get_updates(self, cf_name: str = "default") -> Dict[bytes, Dict[bytes, bytes]]:
        """Get all updated keys in format {prefix: {key: value}}"""
        return self._updated.get(cf_name, {})

    def get_deletes(self, cf_name: str = "default") -> Set[bytes]:
        """Get all deleted keys as a set"""
        return self._deleted[cf_name]
```

### 6.5 State 讀取流程

檔案：`quixstreams/state/base/transaction.py:373-389`

```python
@validate_transaction_status(PartitionTransactionStatus.STARTED)
def _get_bytes(
    self, key: K, prefix: bytes, cf_name: str = "default",
) -> Union[bytes, Literal[Marker.DELETED, Marker.UNDEFINED]]:
    key_serialized = self._serialize_key(key, prefix=prefix)

    # ★ 先查 cache
    cached = self._update_cache.get(
        key=key_serialized, prefix=prefix, cf_name=cf_name
    )

    if cached is Marker.UNDEFINED:
        # ★ Cache 沒有 → 查 RocksDB（或 Memory store）
        return self._partition.get(key_serialized, cf_name)

    # ★ 返回 cache 中的值（可能是 bytes 或 DELETED）
    return cached
```

### 6.6 State 寫入流程

檔案：`quixstreams/state/base/transaction.py:391-438`

```python
def set(self, key: K, value: V, prefix: bytes, cf_name: str = "default") -> None:
    try:
        value_serialized = self._serialize_value(value)
    except Exception:
        self._status = PartitionTransactionStatus.FAILED
        raise

    self._set_bytes(key, value_serialized, prefix, cf_name=cf_name)

@validate_transaction_status(PartitionTransactionStatus.STARTED)
def _set_bytes(
    self, key: K, value: bytes, prefix: bytes, cf_name: str = "default"
) -> None:
    try:
        key_serialized = self._serialize_key(key, prefix=prefix)
        # ★ 只寫到 cache，不直接寫 RocksDB
        # 等 flush() 時才一次性 WriteBatch 寫入
        self._update_cache.set(
            key=key_serialized,
            value=value,
            prefix=prefix,
            cf_name=cf_name,
        )
    except Exception:
        self._status = PartitionTransactionStatus.FAILED
        raise
```

---

## 7. Exactly-Once vs At-Least-Once

### 7.1 Exactly-Once 模式

**Transaction 生命週期**：

```
init_checkpoint()
  → producer.begin_transaction()          ← 開始 Kafka transaction

處理訊息...
  → state.set(key, value)                 ← 寫入 cache
  → store_offset(topic, partition, offset) ← 記錄 offset

commit_checkpoint()
  → Step 2: transaction.prepare()         ← Produce changelog (在 transaction 內)
  → Step 3: producer.flush()              ← 確保訊息送達
  → Step 4: producer.commit_transaction(  ← ★ 原子性 commit
      offsets,                               offset commit
      consumer_group_metadata                + 所有 produced 訊息
    )                                        一起 commit 或一起 fail
  → Step 5: transaction.flush()           ← 寫 RocksDB（best-effort）

init_checkpoint()
  → producer.begin_transaction()          ← 新的 transaction
```

**失敗處理**：
- Step 2-3 失敗 → checkpoint.close() → `producer.abort_transaction()` → offset 未 commit → 重新處理
- Step 4 失敗 → transaction abort → offset 未 commit → 重新處理
- Step 5 失敗 → offset 已 commit，state 可從 changelog recovery

**空 checkpoint 處理**：

```python
# checkpoint.close()
def close(self):
    if self._exactly_once:
        self._producer.abort_transaction()
        # ★ Kafka transaction 有 timeout 限制（預設 60 秒）
        # 即使沒有任何訊息，也要及時 abort 避免 timeout
```

### 7.2 At-Least-Once 模式（預設）

**差異**：
- 不使用 Kafka transaction
- Step 4 使用 `consumer.commit(offsets, asynchronous=False)`（同步 commit）
- 如果處理完但 commit 前掛掉 → 訊息會被重新處理（at-least-once 語意）

```python
# Step 4 的 at-least-once 路徑
logger.debug("Checkpoint: committing consumer")
try:
    partitions = self._consumer.commit(offsets=offsets, asynchronous=False)
except KafkaException as e:
    raise CheckpointConsumerCommitError(e.args[0]) from None

for partition in partitions:
    if partition.error:
        raise CheckpointConsumerCommitError(partition.error)
```

### 7.3 一致性保證比較

| | At-Least-Once | Exactly-Once |
|---|---|---|
| Producer + Consumer | 分開操作 | Kafka transaction 原子性 |
| 失敗時 | 可能重複處理 | Transaction abort，不重複 |
| State 一致性 | Changelog recovery + `_should_apply_changelog` | Changelog recovery + transaction |
| 效能 | 較好 | 較差（transaction overhead）|
| 設定 | `processing_guarantee="at-least-once"` | `processing_guarantee="exactly-once"` |

---

## 8. 端到端流程圖

### 正常 Commit 流程

```
   訊息 1 處理完
     │
     ▼
   store_offset(topic, partition, offset=100)
   _tp_offsets[(topic, 0)] = 100
   _starting_tp_offsets[(topic, 0)] = 100
   _total_offsets_processed = 1
     │
     ▼
   commit_checkpoint() → expired()? → No → 返回
     │
   訊息 2 處理完
     │
     ▼
   store_offset(topic, partition, offset=101)
   _tp_offsets[(topic, 0)] = 101
   _total_offsets_processed = 2
     │
     ▼
   commit_checkpoint() → expired()? → Yes! (5秒到了)
     │
     ▼
  ┌─ Checkpoint.commit() ─────────────────────────────────────┐
  │                                                            │
  │  Step 1: Flush Sinks                                       │
  │  ├─ sink.flush() for each sink                             │
  │  └─ 如果 SinkBackpressureError:                            │
  │     trigger_backpressure() → seek 回 offset=100 → return   │
  │                                                            │
  │  Step 2: Produce Changelogs                                │
  │  ├─ 收集 processed_offsets = {topic: 101}                  │
  │  └─ transaction.prepare(processed_offsets)                 │
  │     ├─ 對每個 cf_name:                                     │
  │     │   headers = {                                        │
  │     │     "__column_family__": cf_name,                    │
  │     │     "__processed_tp_offsets__": '{"topic": 101}'     │
  │     │   }                                                  │
  │     ├─ changelog_producer.produce(key, value, headers)     │
  │     │   for each update                                    │
  │     └─ changelog_producer.produce(key, None, headers)      │
  │         for each delete                                    │
  │                                                            │
  │  Step 3: Flush Producer                                    │
  │  └─ producer.flush() → 確保所有訊息送達 broker              │
  │                                                            │
  │  Step 4: Commit Offsets                                    │
  │  ├─ offsets = [(topic, 0, offset=102)]  ← offset + 1      │
  │  └─ consumer.commit(offsets, asynchronous=False)           │
  │     或 producer.commit_transaction(offsets, metadata)       │
  │                                                            │
  │  Step 5: Flush State to Disk                               │
  │  ├─ produced_offsets = producer.offsets                     │
  │  └─ transaction.flush(changelog_offset=...)                │
  │     └─ partition.write(cache, changelog_offset)            │
  │        └─ WriteBatch → RocksDB.write(batch)                │
  │                                                            │
  └────────────────────────────────────────────────────────────┘
     │
     ▼
   init_checkpoint() → 建新 checkpoint → 繼續處理下一批訊息
```

### Backpressure 流程

```
   commit_checkpoint()
     │
     ▼
   Checkpoint.commit()
     │
     ▼
   Step 1: sink.flush()
     │
     ▼
   SinkBackpressureError(retry_after=30)
     │
     ▼
   consumer.trigger_backpressure(
     resume_after=30,
     offsets_to_seek=_starting_tp_offsets  ← seek 回 checkpoint 開始的 offset
   )
     │
     ▼
   Pause all data partitions
   Seek back to starting offsets
   return（不執行 Step 2-5）
     │
     ▼
   init_checkpoint() → 建新 checkpoint
     │
     ▼
   主迴圈繼續...
   consumer.resume_backpressured()  ← 30秒後 resume
     │
     ▼
   訊息從 starting_offset 重新處理
```

---

## 9. 關鍵源碼索引

| 元件 | 檔案 | 行數 | 說明 |
|------|------|------|------|
| `BaseCheckpoint` | `quixstreams/checkpointing/checkpoint.py` | 29-114 | 基底類別：offset 追蹤 + 到期判斷 |
| `BaseCheckpoint.expired` | `quixstreams/checkpointing/checkpoint.py` | 58-66 | 到期判斷（時間 or 訊息數）|
| `BaseCheckpoint.store_offset` | `quixstreams/checkpointing/checkpoint.py` | 75-99 | 記錄已處理 offset |
| `Checkpoint` | `quixstreams/checkpointing/checkpoint.py` | 117-291 | Application 用的完整實作 |
| `Checkpoint.get_store_transaction` | `quixstreams/checkpointing/checkpoint.py` | 147-170 | 取得 state transaction |
| `Checkpoint.close` | `quixstreams/checkpointing/checkpoint.py` | 172-179 | 空 checkpoint 清理 |
| `Checkpoint.commit` | `quixstreams/checkpointing/checkpoint.py` | 181-291 | ★ 五步驟 commit 流程 |
| Step 1: Flush sinks | `quixstreams/checkpointing/checkpoint.py` | 193-223 | Sink flush + backpressure |
| Step 2: Produce changelogs | `quixstreams/checkpointing/checkpoint.py` | 225-244 | State → changelog topic |
| Step 3: Flush producer | `quixstreams/checkpointing/checkpoint.py` | 246-254 | 確保訊息送達 |
| Step 4: Commit offsets | `quixstreams/checkpointing/checkpoint.py` | 256-275 | Kafka offset commit |
| Step 5: Flush state | `quixstreams/checkpointing/checkpoint.py` | 277-291 | RocksDB 寫入 |
| `ProcessingContext` | `quixstreams/processing/context.py` | 23-106 | Checkpoint 管理者 |
| `ProcessingContext.init_checkpoint` | `quixstreams/processing/context.py` | 60-73 | 建立新 checkpoint |
| `ProcessingContext.commit_checkpoint` | `quixstreams/processing/context.py` | 75-96 | 到期判斷 + commit/close |
| `PartitionTransactionStatus` | `quixstreams/state/base/transaction.py` | 160-168 | Transaction 狀態機 |
| `PartitionTransaction` | `quixstreams/state/base/transaction.py` | 195-591 | State transaction 完整實作 |
| `PartitionTransaction.prepare` | `quixstreams/state/base/transaction.py` | 479-535 | Produce changelog 訊息 |
| `PartitionTransaction.flush` | `quixstreams/state/base/transaction.py` | 537-583 | 寫入 RocksDB |
| `PartitionTransactionCache` | `quixstreams/state/base/transaction.py` | 53-157 | Read-your-own-writes cache |
| `PartitionTransaction._get_bytes` | `quixstreams/state/base/transaction.py` | 373-389 | Cache-first 讀取 |
| `PartitionTransaction._set_bytes` | `quixstreams/state/base/transaction.py` | 424-438 | 寫入 cache |
| Application 主迴圈 | `quixstreams/app.py` | 928-955 | commit_checkpoint 呼叫點 |
| Application store_offset | `quixstreams/app.py` | 1030-1034 | store_offset 呼叫點 |
