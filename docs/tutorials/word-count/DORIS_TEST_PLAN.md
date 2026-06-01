# DorisSink 整合測試計劃(以 `tutorial_app_doris.py` 為基礎)

每個案例都用同一支 `tutorial_app_doris.py` 改幾行來驅動,對照「**改哪裡 → 建表 DDL → 執行 → 驗證 SQL**」。
對象:`quixstreams/sinks/community/doris.py`。範圍:整合測試(真實 Doris)。

---

## 0. 環境與通用操作

**拓樸**:Doris 4.x、FE/BE 分離(目前 compose 為 `fe-3.1.4 / be-3.1.4`,升 4.x 時換 image tag)。

| 用途 | host 位址 | 備註 |
|---|---|---|
| Stream Load 寫入 | `127.0.0.1:8040`(**BE** webserver) | 直送 BE,避開 FE→BE redirect(FE 會 redirect 到 BE 內網 IP,host 跟不進去) |
| 查詢 / DDL | `127.0.0.1:9030`(**FE** MySQL) | 容器名以 `docker ps` 為準(`doris-fe` 或 `doris-fe1`) |
| FE HTTP / 探活 | `127.0.0.1:8030` | `setup()` 打 `/api/bootstrap` |

**通用指令**(以下用 `FE` 代稱 FE 容器名,先 `docker ps` 確認):

```bash
# 建/改表(下 DDL)
docker exec -i doris-fe mysql -uroot -P9030 -h127.0.0.1 quixstreams -e "<DDL 或查詢>"

# 跑改過的 app(DorisSink 未發佈,需把 repo root 放進 PYTHONPATH)
cd /Users/stanhsu/projects/quix-streams
PYTHONPATH=$PWD .venv/bin/python docs/tutorials/word-count/tutorial_app_doris.py
```

**怎麼算「跑完」**:`ReviewGenerator` 送完 8 則(約 4 秒)後印 `Sent all product reviews`;sink 在 commit interval(預設 5s)flush,log 會出現
`Stream Load to '<table>': loaded=N, ...`。看到該行即可 `Ctrl+C` 停止,再查 Doris。

**每個 case 前重置**:`TRUNCATE TABLE <table>;`(或用各自獨立表名,跑完 `DROP`)。

> 註:以下 DDL 用 `replication_num=1`(測試夠用);你的 HA 叢集(多 BE)要對齊可改 `"replication_num"="3"`。VARIANT 案例的 `__headers`/`__value` 用 `JSON`/`VARIANT`,Doris 端自動接住 sink 送出的 JSON。

---

## 1. 核心寫入

### 案例 1 — 基本端到端(flatten + key/timestamp)

**目的**:最小路徑,確認 `{word,count}` + `__key` + `__timestamp` 正確落地。

**改哪裡**(`DorisSink(...)` 區塊,改 `table_name`;其餘維持原樣):

```python
    doris_sink = DorisSink(
        host=os.getenv("DORIS_HOST", "localhost"),
        http_port=int(os.getenv("DORIS_HTTP_PORT", "8040")),
        username=os.getenv("DORIS_USER", "root"),
        password=os.getenv("DORIS_PASSWORD", ""),
        database=os.getenv("DORIS_DATABASE", "quixstreams"),
        table_name="it01_basic",                 # ← 改這裡
        include_metadata={"key", "timestamp"},    # 維持
    )
```

**建表 DDL**:

```sql
CREATE TABLE quixstreams.it01_basic (
  `word`        VARCHAR(256) NOT NULL,
  `count`       BIGINT       NOT NULL,
  `__key`       VARCHAR(256) NULL,
  `__timestamp` DATETIME     NULL
)
DUPLICATE KEY(`word`, `count`)
DISTRIBUTED BY HASH(`word`) BUCKETS 4
PROPERTIES("replication_num"="1");
```

**驗證 SQL**:

```sql
USE quixstreams;
SELECT count(*) FROM it01_basic;                       -- 應 > 0(展開後的字數)
SELECT word, count, __key, __timestamp
FROM it01_basic ORDER BY word LIMIT 20;                -- __key ∈ {product_a/b/c}
```

**預期**:每個過濾後的 word 一列;`__key` 為可讀 product 字串;`__timestamp` 為合理 UTC 時間。

---

### 案例 2 — 全 metadata(`include_metadata=True`)

**目的**:驗證 6 個 metadata 欄位映射(`__key/__topic/__partition/__offset/__headers/__timestamp`)。

**改哪裡**:

```python
        table_name="it02_meta_all",
        include_metadata=True,        # ← 改成 True(全部 metadata)
```

**建表 DDL**(`__headers` 用 JSON):

```sql
CREATE TABLE quixstreams.it02_meta_all (
  `word`        VARCHAR(256) NOT NULL,
  `count`       BIGINT       NOT NULL,
  `__key`       VARCHAR(256) NULL,
  `__topic`     VARCHAR(256) NULL,
  `__partition` INT          NULL,
  `__offset`    BIGINT       NULL,
  `__headers`   JSON         NULL,
  `__timestamp` DATETIME     NULL
)
DUPLICATE KEY(`word`, `count`)
DISTRIBUTED BY HASH(`word`) BUCKETS 4
PROPERTIES("replication_num"="1");
```

**驗證 SQL**:

```sql
SELECT DISTINCT __topic, __partition FROM it02_meta_all;     -- __topic = source 預設 topic 名
SELECT word, __offset, __headers FROM it02_meta_all ORDER BY __offset LIMIT 20;
```

**預期**:`__topic` 為 source 的 topic 名;`__partition`/`__offset` 為合理整數;`__headers` 為 JSON(本例無自訂 header → `{}`)。

---

### 案例 3 — 不帶 metadata(`include_metadata=False`)

**目的**:只寫 value 欄,確認不產生任何 `__*` 欄。

**改哪裡**:

```python
        table_name="it03_no_meta",
        include_metadata=False,       # ← 改成 False
```

**建表 DDL**:

```sql
CREATE TABLE quixstreams.it03_no_meta (
  `word`  VARCHAR(256) NOT NULL,
  `count` BIGINT       NOT NULL
)
DUPLICATE KEY(`word`, `count`)
DISTRIBUTED BY HASH(`word`) BUCKETS 4
PROPERTIES("replication_num"="1");
```

**驗證 SQL**:

```sql
SELECT count(*) FROM it03_no_meta;
DESC it03_no_meta;        -- 只有 word/count,無 __key 等
```

**預期**:成功寫入;表中無 metadata 欄。

---

## 2. VARIANT:把 Kafka `__key` / `__value` 寫進去

### 案例 4 — 原始 value 進 VARIANT(`flatten_value=False`)★ 你的目標

**目的**:不攤平,把**整包 Kafka value** 灌進 `__value VARIANT`,`__key` 一併寫入。

**改哪裡**:跳過 word-count 轉換、直接 sink 原始 review;sink 設 `flatten_value=False`。

```python
    doris_sink = DorisSink(
        host=os.getenv("DORIS_HOST", "localhost"),
        http_port=int(os.getenv("DORIS_HTTP_PORT", "8040")),
        username=os.getenv("DORIS_USER", "root"),
        password=os.getenv("DORIS_PASSWORD", ""),
        database=os.getenv("DORIS_DATABASE", "quixstreams"),
        table_name="it04_variant",
        include_metadata={"key", "timestamp"},
        flatten_value=False,          # ← 整包 value 存進 __value
    )

    app = Application(...)            # 不變

    # 原本的 tokenize/filter/to_row 全拿掉,直接 sink 原始訊息:
    sdf = app.dataframe(source=ReviewGenerator())
    sdf.print()
    sdf.sink(doris_sink)
```

> 註:`flatten_value=False` 時 sink 產出 `{"__value": <value>, "__key": <key>, "__timestamp": ...}`。本例 value 是 review 字串 → `__value` 存成 JSON scalar。若想存物件,讓 value 是 dict 即可(見案例 5)。
> ⚠️ **VARIANT 不可當 key 欄/分桶鍵**,所以下面用 `__key VARCHAR` 當 DUPLICATE KEY,`__value` 放 VARIANT。

**建表 DDL**:

```sql
CREATE TABLE quixstreams.it04_variant (
  `__key`       VARCHAR(256) NOT NULL,
  `__value`     VARIANT      NULL,
  `__timestamp` DATETIME     NULL
)
DUPLICATE KEY(`__key`)
DISTRIBUTED BY HASH(`__key`) BUCKETS 4
PROPERTIES("replication_num"="1");
```

**驗證 SQL**:

```sql
SELECT count(*) FROM it04_variant;
SELECT __key, __value FROM it04_variant LIMIT 10;     -- __value 為原始 review 內容
```

**預期**:每則 review 一列;`__value` 存回原始值;`__key` 為 product 字串。

---

### 案例 5 — VARIANT subcolumn 查詢 + schema 演進

**目的**:value 為 **dict** 時,VARIANT 自動推斷 subcolumn、且不同訊息帶不同欄位不需改表。

**改哪裡**:把轉換改成輸出「帶不定欄位的 dict」(模擬 schema 演進),`flatten_value=False`。

```python
    # 取代 to_row:每筆是一個結構可能不同的 dict
    def to_doc(word_count_pair):
        word, count = word_count_pair
        doc = {"word": word, "count": count}
        if count > 1:                       # 部分訊息才有 extra 欄
            doc["repeated"] = True
            doc["score"] = round(count * 1.5, 2)
        return doc

    # sink: flatten_value=False, table_name="it05_variant_doc"
    ...
    sdf = sdf.apply(tokenize_and_count, expand=True)
    sdf = sdf.filter(should_skip)
    sdf = sdf.apply(to_doc)             # ← 換成 to_doc
    sdf.sink(doris_sink)               # DorisSink(..., flatten_value=False)
```

**建表 DDL**:

```sql
CREATE TABLE quixstreams.it05_variant_doc (
  `__key`   VARCHAR(256) NOT NULL,
  `__value` VARIANT      NULL
)
DUPLICATE KEY(`__key`)
DISTRIBUTED BY HASH(`__key`) BUCKETS 4
PROPERTIES("replication_num"="1");
```

**驗證 SQL**:

```sql
-- 自動推斷出的 subcolumn 結構
DESC it05_variant_doc ALL;

-- 用路徑存取 subcolumn(缺欄回 NULL,證明 schema 演進)
SELECT __value['word']                       AS word,
       CAST(__value['count'] AS BIGINT)       AS cnt,
       __value['repeated']                    AS repeated,   -- 多數列為 NULL
       CAST(__value['score'] AS DOUBLE)       AS score
FROM it05_variant_doc ORDER BY cnt DESC LIMIT 20;
```

**預期**:`__value` 同時容納有/無 `repeated`、`score` 的訊息;查缺欄回 NULL,**全程不需 ALTER TABLE**。

---

### 案例 6 — key+value 併進「同一個」VARIANT 欄

**目的**:把 `__key` 與 value 包進單一 `payload VARIANT`。sink **不會自動合併**,需在 pipeline 端組。

**改哪裡**:用 `apply(..., metadata=True)` 拿到 key,組成 `{"payload": {...}}`,`flatten_value=True`(攤平出單一 `payload` 欄)。

```python
    def to_payload(value, key, timestamp, headers):
        return {"payload": {"key": key.decode() if isinstance(key, bytes) else key,
                            "value": value}}

    # sink: flatten_value=True(預設), include_metadata=False, table_name="it06_payload"
    sdf = app.dataframe(source=ReviewGenerator())
    sdf = sdf.apply(to_payload, metadata=True)   # ← metadata=True 才拿得到 key
    sdf.sink(doris_sink)
```

**建表 DDL**:

```sql
CREATE TABLE quixstreams.it06_payload (
  `id`      BIGINT  NOT NULL AUTO_INCREMENT,
  `payload` VARIANT NULL
)
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 4
PROPERTIES("replication_num"="1");
```

**驗證 SQL**:

```sql
SELECT payload['key'] AS k, payload['value'] AS v FROM it06_payload LIMIT 10;
```

**預期**:單一 `payload` 欄同時含 `key` 與 `value`。

---

## 3. 路由與錯誤處理

### 案例 7 — 動態表名(`table_name` 為 callable)

**目的**:依訊息把資料寫到不同表。

**改哪裡**:

```python
    doris_sink = DorisSink(
        ...,
        table_name=lambda item: f"it07_{item.key.decode()}"   # ← 依 key 分表
            if isinstance(item.key, bytes) else f"it07_{item.key}",
        include_metadata={"key"},
    )
```

**建表 DDL**(每個 product 各一張;3 個 key → 3 張):

```sql
CREATE TABLE quixstreams.it07_product_a (`word` VARCHAR(256) NOT NULL, `count` BIGINT NOT NULL, `__key` VARCHAR(256))
  DUPLICATE KEY(`word`,`count`) DISTRIBUTED BY HASH(`word`) BUCKETS 2 PROPERTIES("replication_num"="1");
-- 同樣建 it07_product_b、it07_product_c
```

**驗證 SQL**:

```sql
SELECT 'a' t, count(*) FROM it07_product_a
UNION ALL SELECT 'b', count(*) FROM it07_product_b
UNION ALL SELECT 'c', count(*) FROM it07_product_c;
```

**預期**:三張表各收到對應 key 的子集,總和 = 全部列數。

---

### 案例 8 — Stream Load 失敗 + `on_stream_load_error`

**目的**:寫到不存在的表時,有 callback → 不崩;無 callback → raise。

**改哪裡(8a:有 callback)**:

```python
    def on_err(table, rows, exc):
        logger.error(f"DLQ: table={table} rows={len(rows)} err={exc}")

    doris_sink = DorisSink(
        ...,
        table_name="it08_does_not_exist",     # ← 故意不建這張表
        on_stream_load_error=on_err,           # ← 提供 callback
    )
```

**改哪裡(8b:無 callback)**:把上面 `on_stream_load_error=on_err` 移除。

**建表 DDL**:無(故意不建表)。

**驗證**:

- 8a:log 出現 `DLQ: ...`,程式**繼續執行不崩**。
- 8b:`app.run()` 拋 `DorisSinkException`(訊息含 `Status/Message/ErrorURL`),程式結束。

---

### 案例 9 — `max_filter_ratio`(容忍壞列)

**目的**:混入型別不符的列,比較 `0.0` 與 `0.5`。

**改哪裡**:在 `to_row` 隨機產生壞值,並設 `max_filter_ratio`。

```python
    def to_row(word_count_pair):
        word, count = word_count_pair
        # 約 10% 的列把 count 變成非數字字串(對 BIGINT 欄為壞列)
        from random import random
        return {"word": word, "count": ("bad" if random() < 0.1 else count)}

    doris_sink = DorisSink(..., table_name="it09_filter",
                           max_filter_ratio=0.0)   # 之後再改成 0.5 重跑
```

**建表 DDL**:同案例 1(`it09_filter`,欄位 `word/count`)。

**驗證**:

- `max_filter_ratio=0.0`:整批 Stream Load **失敗**(log 顯示 Fail,`NumberFilteredRows>0`)。
- `max_filter_ratio=0.5`:整批 **Success**,`NumberFilteredRows>0`,好列入庫。
  ```sql
  SELECT count(*) FROM it09_filter;   -- 小於送入總數(壞列被 filter)
  ```

---

## 4. Unique Key / 合併語意(需 UNIQUE KEY 表;4.x 支援 flexible)

### 案例 10 — 整列 upsert(`partial_update="none"`)

**改哪裡**:`table_name="it10_unique"`,sink 預設 `partial_update="none"`;to_row 維持 `{word,count}`。

**建表 DDL**(UNIQUE KEY + MoW):

```sql
CREATE TABLE quixstreams.it10_unique (
  `word`  VARCHAR(256) NOT NULL,
  `count` BIGINT       NULL,
  `__key` VARCHAR(256) NULL
)
UNIQUE KEY(`word`)
DISTRIBUTED BY HASH(`word`) BUCKETS 4
PROPERTIES("replication_num"="1", "enable_unique_key_merge_on_write"="true");
```

**驗證 SQL**:

```sql
SELECT count(*) AS distinct_words FROM it10_unique;   -- = 不重複 word 數(同 word 被合併)
SELECT word, count FROM it10_unique ORDER BY count DESC LIMIT 10;
```

**預期**:同 `word` 只剩一列(後到覆蓋先到)。

### 案例 11 — 固定欄部分更新(`partial_update="fixed"`)

**改哪裡**:

```python
    doris_sink = DorisSink(..., table_name="it11_partial",
        include_metadata=False,
        partial_update="fixed",
        partial_update_columns=["word", "count"],   # 必含 key 欄
    )
```

**建表 DDL**:UNIQUE KEY 表,多一個**不在更新清單**的欄(驗證它被保留):

```sql
CREATE TABLE quixstreams.it11_partial (
  `word`  VARCHAR(256) NOT NULL,
  `count` BIGINT       NULL,
  `note`  VARCHAR(64)  NULL DEFAULT 'keep'
)
UNIQUE KEY(`word`)
DISTRIBUTED BY HASH(`word`) BUCKETS 4
PROPERTIES("replication_num"="1", "enable_unique_key_merge_on_write"="true");
```

**驗證 SQL**:先手動 `INSERT` 幾列帶 `note`,跑 app 後:

```sql
SELECT word, count, note FROM it11_partial ORDER BY word LIMIT 20;  -- note 應維持原值
```

**預期**:只有 `word/count` 被更新,`note` 保留。

### 案例 12 — `merge_type="DELETE"`

**改哪裡**:`merge_type="DELETE"`,sink 送入的 key 會被刪。對既有 `it10_unique` 先灌資料,再用本 app 送同樣 word → 對應列被刪。

**驗證 SQL**:`SELECT count(*) FROM it10_unique;` 跑前後比較,送入的 word 消失。

> `partial_update="flexible"`(每列更新不同欄,JSON only)需 3.1+/4.x;升 4.x 後比照案例 11 改 `partial_update="flexible"` + VARIANT/JSON 來源測。

---

## 5. 大量寫入 + 吞吐(沿用 benchmark_doris.sh)

```bash
# 注意:benchmark_doris.sh 內 `docker exec -i doris mysql` 需改成 FE 容器名(doris-fe / doris-fe1)
docs/tutorials/word-count/benchmark_doris.sh                 # 10 萬筆,4/8/16 partition
TOTAL=5000000 docs/tutorials/word-count/benchmark_doris.sh   # 五百萬筆
```

- 吞吐:`rows/sec = count(*) / (max(processed_at) - min(processed_at))`,`processed_at` 由 Doris `DEFAULT CURRENT_TIMESTAMP(6)` 落地時蓋上。
- 兼作大量資料正確性驗證:跑完比對列數是否 = 預期展開列數。

---

## 6. 通過標準 / 注意

**通過標準**
- 案例 1~12 全通過;VARIANT(4~6)`__value`/`payload` 完整落地、subcolumn 可查、schema 演進免改表。
- 錯誤路徑:有 callback 不崩、無 callback 正確 raise `DorisSinkException`。
- 大量寫入無漏資料、無未預期失敗。

**注意 / 踩雷**
- **容器名**:FE/BE 拆分後是 `doris-fe`(或 `doris-fe1`),不再有 `doris`;所有 `docker exec doris ...`(含 `benchmark_doris.sh`)要改。
- **埠**:寫入 BE **8040**、查詢 FE **9030**、探活 FE **8030**;host 用 FE 8030 寫入會吃 redirect 失敗。
- **VARIANT 限制**:不可當 key/分桶鍵 → 表另設 key(`__key`/`id`);`flatten_value=False` 才會把整包 value 進 `__value`;key+value 同欄需 pipeline 端自組(案例 6)。
- **schema 不自動建立**:表須先建好,欄位要與 `to_row()`/`apply()` 輸出 + `include_metadata` 對齊,否則 Stream Load 失敗。
- **跑 app**:`PYTHONPATH=$PWD`(DorisSink 未發佈)或 `pip install -e .[doris]`;app 是 Source,送完不會自動結束,看到 `Stream Load ... loaded=` 後 `Ctrl+C`。
- **DorisSink 未發佈**:只在 repo 原始碼,site-packages 沒有。
- **版本**:目標 4.x;`partial_update="flexible"` 需 3.1+(現 3.1.4 / 4.x 皆滿足)。升 4.x 留意預設值變動。
