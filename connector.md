# Kafka → Kafka 高可用複製方案(Bitnami + MirrorMaker 2)

用 Bitnami 的 Kafka image 跑 **dedicated MirrorMaker 2**,做跨叢集 Kafka→Kafka 複製。
不需要 operator、不需要額外的 Connect chart——Bitnami kafka image 內含完整 Kafka 發行版,
`connect-mirror-maker.sh` 與 MM2 connector 都在裡面。

整套 = `ConfigMap`(mm2.properties)+ `Deployment`(replicas≥2,HA)+ 監控(JMX → Prometheus → Grafana)。

---

## 1. 架構

```
┌──────────────┐        MirrorSourceConnector         ┌──────────────┐
│ source Kafka │ ───────────────────────────────────► │ target Kafka │
│  (Bitnami)   │   MirrorCheckpoint / Heartbeat        │  (Bitnami)   │
└──────────────┘                                       └──────────────┘
                          ▲
                          │ 同一份 mm2.properties
              ┌───────────┴───────────┐
              │   MM2 Deployment      │   replicas: 2~3
              │  (bitnami/kafka img)  │   task 自動分配 + failover
              └───────────────────────┘
```

- **HA 原理**:dedicated MM2 內部就是一個 distributed Connect cluster。多個副本用「相同設定」會組成同一個 group,
  task 自動分配到不同副本;某副本掛掉會觸發 rebalance,task 自動轉移到存活副本。
- **協調狀態**:存在 target 叢集的 Connect 內部 topic(`config/offset/status.storage`),所以這些 topic 的 RF 必須 ≥2。

---

## 2. HA 的三個必要條件

| 條件 | 設定 | 原因 |
|------|------|------|
| 副本數 ≥ 2 | `replicas: 2`(含以上) | 單副本是單點故障 |
| 所有副本用同一份設定 | 共用同一個 ConfigMap | 才會組成同一個 cluster、共享 task 分配 |
| 內部 topic RF ≥ 2(建議 3) | `*.storage.replication.factor` | broker 掉一台,MM2「大腦」不能跟著壞 |
| `tasks.max` 夠大 | `tasks.max = 4`(> 副本數) | 否則多開的副本只待命,沒負載分散 |

> 副本數不必超過來源 topic 的 partition 總數——MM2 並行度上限就是 partition 數。

---

## 3. ConfigMap — mm2.properties

> 把 `NS`、service 名稱、RF 依你的環境調整。SASL 區塊視 Bitnami 是否開啟認證決定要不要留。

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: mm2-config
  namespace: kafka
data:
  mm2.properties: |
    clusters = source, target
    source.bootstrap.servers = source-kafka.kafka.svc.cluster.local:9092
    target.bootstrap.servers = target-kafka.kafka.svc.cluster.local:9092

    # ---- 複製流 ----
    source->target.enabled = true
    source->target.topics = .*
    # 排除 MM2 內部 / 系統 topic
    source->target.topics.exclude = .*[\-\.]internal, .*\.replica, __.*, mm2.*, .*\.checkpoints\.internal

    # 去掉 source. 前綴(同名複製)。要保留前綴就移除這行
    replication.policy.class = org.apache.kafka.connect.mirror.IdentityReplicationPolicy

    # ---- 並行度 (HA 負載分散) ----
    tasks.max = 4

    # ---- consumer offset / checkpoint 同步 ----
    sync.group.offsets.enabled = true
    sync.group.offsets.interval.seconds = 10
    emit.checkpoints.enabled = true
    emit.checkpoints.interval.seconds = 10
    emit.heartbeats.enabled = true
    emit.heartbeats.interval.seconds = 5
    refresh.topics.enabled = true
    refresh.topics.interval.seconds = 30

    # ---- (1) 被複製的資料 topic 在 target 的 RF ----
    replication.factor = 3

    # ---- (2) MM2 功能 topic RF ----
    checkpoints.topic.replication.factor = 3
    heartbeats.topic.replication.factor = 3
    offset-syncs.topic.replication.factor = 3

    # ---- (3) Connect 框架內部管理 topic RF (HA 協調狀態, 建在 target) ----
    offset.storage.replication.factor = 3
    status.storage.replication.factor = 3
    config.storage.replication.factor = 3

    # ---- Bitnami SASL/SCRAM 認證 (預設常開; 兩個叢集各設一次) ----
    # 密碼建議用 env 注入, 不要寫死在 ConfigMap (見 Deployment)
    source.security.protocol = SASL_PLAINTEXT
    source.sasl.mechanism = SCRAM-SHA-256
    target.security.protocol = SASL_PLAINTEXT
    target.sasl.mechanism = SCRAM-SHA-256
```

> 注意三組 RF 各管各的:(1) 你的業務資料、(2) MM2 心跳/檢查點/offset 同步、(3) Connect 框架內部狀態。HA 時三組都要 ≥2。

SASL 的 `jaas.config` 含密碼,改用啟動時注入(見下方 Deployment 的 `command`),避免明文進 ConfigMap。

---

## 4. Deployment — HA MM2(replicas=2)+ JMX exporter sidecar

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mm2
  namespace: kafka
  labels: { app: mm2 }
spec:
  replicas: 2                      # >=2 即為 HA
  selector:
    matchLabels: { app: mm2 }
  template:
    metadata:
      labels: { app: mm2 }
    spec:
      # 把副本打散到不同節點, 避免單節點故障一次帶走多副本
      topologySpreadConstraints:
        - maxSkew: 1
          topologyKey: kubernetes.io/hostname
          whenUnsatisfiable: ScheduleAnyway
          labelSelector:
            matchLabels: { app: mm2 }
      # 下載 JMX prometheus javaagent jar
      initContainers:
        - name: fetch-jmx-agent
          image: curlimages/curl:8.8.0
          command:
            - sh
            - -c
            - >
              curl -sSL -o /agent/jmx_prometheus_javaagent.jar
              https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/1.0.1/jmx_prometheus_javaagent-1.0.1.jar
          volumeMounts:
            - { name: jmx-agent, mountPath: /agent }
      containers:
        - name: mm2
          image: bitnami/kafka:3.7
          command:
            - sh
            - -c
            - |
              set -e
              # 1. 從 secret 注入密碼, 動態組出含認證的 mm2.properties
              cp /config/mm2.properties /tmp/mm2.properties
              cat >> /tmp/mm2.properties <<EOF
              source.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="$SRC_USER" password="$SRC_PASS";
              target.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="$DST_USER" password="$DST_PASS";
              EOF
              # 2. 安全追加 JMX exporter: 保留 image / entrypoint 原本的 KAFKA_OPTS, 不覆蓋
              export KAFKA_OPTS="${KAFKA_OPTS:-} -javaagent:/agent/jmx_prometheus_javaagent.jar=${METRICS_PORT}:/jmx-config/jmx.yml"
              # 3. 啟動 MM2 (用 exec 讓 MM2 成為 PID 1, 正確收到 K8s 的終止訊號)
              exec /opt/bitnami/kafka/bin/connect-mirror-maker.sh /tmp/mm2.properties
          env:
            - { name: SRC_USER, value: "user1" }
            - { name: DST_USER, value: "user1" }
            - name: SRC_PASS
              valueFrom: { secretKeyRef: { name: source-kafka-user-passwords, key: client-passwords } }
            - name: DST_PASS
              valueFrom: { secretKeyRef: { name: target-kafka-user-passwords, key: client-passwords } }
            # metrics port: 改成 SRE 監控的 port (containerPort 也要一起改)
            - { name: METRICS_PORT, value: "9100" }
          ports:
            - { name: metrics, containerPort: 9100 }   # 要跟 METRICS_PORT 一致
          resources:
            requests: { cpu: "500m", memory: "1Gi" }
            limits:   { cpu: "2",    memory: "2Gi" }
          volumeMounts:
            - { name: cfg,       mountPath: /config }
            - { name: jmx-agent, mountPath: /agent }
            - { name: jmx-config, mountPath: /jmx-config }
      volumes:
        - { name: cfg,       configMap: { name: mm2-config } }
        - { name: jmx-config, configMap: { name: mm2-jmx-config } }
        - { name: jmx-agent, emptyDir: {} }
```

> `client-passwords` 是 Bitnami 自動產生的 secret key(多使用者時是逗號分隔,取對應位置)。
> 確認你的 secret 名稱:`kubectl get secret -n kafka | grep user-passwords`。

---

## 5. 原理:它怎麼知道要從哪抓、怎麼監控上下游

監控 MM2 之前要先懂它內部「記到哪、怎麼比對上下游」的機制——所有指標都是從這幾個機制衍生出來的。

### 5.1 它怎麼知道「要複製哪些 topic」

MM2 的 `MirrorSourceConnector` 會**定期向 source 叢集查詢 topic 清單**(`refresh.topics.interval.seconds`,預設 30s),
拿到清單後用 `topics`(白名單,正則)和 `topics.exclude`(黑名單)過濾,得出該複製的 topic+partition。
來源新增 topic / partition 時,下一輪 refresh 就會自動納入,不用重啟。

```
source 叢集 ──(AdminClient: listTopics / describeTopics)──► MM2
                  每 refresh.topics.interval.seconds 一次
```

### 5.2 它怎麼知道「上次讀到哪、下次從哪抓」

這是核心。MM2 本質是 Connect 的 **source connector**,它的讀取進度由 **Connect 框架**管理:

1. MM2 用一個內部 **consumer**(屬於 MM2 自己的 consumer group)從 source 各 partition 拉資料。
2. 每成功把一批資料寫進 target,Connect 框架就把「source 端讀到的 offset」記進 **`offset.storage` topic**(建在 target 叢集)。
3. 重啟 / failover 後,新接手的副本**讀 `offset.storage`**,得知每個 source partition 上次讀到哪,從那之後繼續 `seek` 抓 ——**所以「從哪抓」的答案存在 target 的 `offset.storage` topic 裡**,不是靠 source 的 consumer group。

> 對照前面 Quix 的問題:這跟「offset 存哪、誰負責記進度」是同一類問題。
> MM2 把它讀取 source 的進度,記在 target 的 Connect 內部 topic;這也是為什麼那三個 `*.storage` topic 的 RF 要 ≥2——它就是 MM2 的「書籤」,壞了就不知道從哪抓。

### 5.3 三個內部 topic = 上下游對應 + 監控的資料來源

MM2 額外維護三個功能 topic,它們同時也是「監控上下游」的根據:

| Topic | 誰寫 | 內容 | 拿來監控什麼 |
|-------|------|------|------------|
| **heartbeats** | source→target 各發一份,帶**時間戳** | 心跳訊號 | **端到端延遲 / 鏈路存活**:比對同一筆 heartbeat 在 source 與 target 的時間戳差,就是複製延遲;target 收不到 = 鏈路斷 |
| **offset-syncs** | MirrorSourceConnector | `source offset ↔ target offset` 的對應表 | 換算「同一筆訊息在上下游各自的 offset」,是算 lag 與 offset 平移的依據 |
| **checkpoints** | MirrorCheckpointConnector | 各 **consumer group** 在 source 的 committed offset，換算成 target 的對應 offset | 監控/遷移**消費者進度**:failover 到 target 的 consumer 知道該從哪接 |

```
source.offset = 1000 ──┐ offset-syncs 表 ┌──► target.offset = 1000 (同名複製)
consumer group 在 source committed = 950
   └─(checkpoints: 用 offset-syncs 換算)─► 在 target 對應 = 950
```

### 5.4 「監控上下游」實際是怎麼量出來的

把上面機制組合起來,監控分三個層次:

1. **複製延遲(端到端)**
   - 指標法:`replication-latency-ms`——MirrorSourceConnector 在寫入 target 時,用「target 寫入時間 − source 訊息原始時間戳」算出來,直接由 JMX 暴露。
   - 心跳法:讀 target 的 heartbeats topic,拿最新一筆的時間戳跟 source 對應筆比較,得到鏈路延遲(連「沒有業務流量」時也能量)。

2. **複製是否落後 / 積壓(lag)**
   - source 端:MM2 的 consumer group 在 source 上的 **consumer lag**(= source LEO − MM2 已讀 offset)。用 broker 指標或 kafka-exporter 抓。
   - 跨叢集:用 offset-syncs 對應表,比對「source 最新 offset」與「已複製進 target 的 offset」差距。

3. **消費者進度同步(下游可接手)**
   - checkpoints + `sync.group.offsets.enabled=true`:MM2 把 source 端各 consumer group 的進度,**換算後寫回 target 的 `__consumer_offsets`**。
   - 監控點:target 上對應 group 的 committed offset 是否持續推進;落後代表 checkpoint 同步出問題。

> 一句話:**「從哪抓」看 target 的 `offset.storage`(MM2 自己的讀取書籤);「上下游延遲/落後」看 `replication-latency-ms` + heartbeats + offset-syncs;「下游消費者能否接手」看 checkpoints 同步回 target 的 group offset。**

---

## 6. 監控

MM2 = Connect,指標走 **JMX**。上面已用 `jmx_prometheus_javaagent` 把 JMX 轉成 Prometheus 格式,暴露在 `:9100/metrics`。

### 6.1 JMX exporter 設定 ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: mm2-jmx-config
  namespace: kafka
data:
  jmx.yml: |
    lowercaseOutputName: true
    lowercaseOutputLabelNames: true
    rules:
      # MM2 source connector: 複製延遲 / 速率 / 數量
      - pattern: "kafka.connect.mirror<type=MirrorSourceConnector, target=(.+), topic=(.+), partition=(.+)><>(replication-latency-ms-avg|replication-latency-ms-max|record-age-ms-avg|byte-rate|record-rate|record-count)"
        name: mm2_$4
        labels: { target: "$1", topic: "$2", partition: "$3" }
        type: GAUGE
      # MM2 checkpoint connector
      - pattern: "kafka.connect.mirror<type=MirrorCheckpointConnector, source=(.+), group=(.+)><>(checkpoint-latency-ms-avg|checkpoint-latency-ms-max)"
        name: mm2_checkpoint_$3
        labels: { source: "$1", group: "$2" }
        type: GAUGE
      # Connect worker / connector 狀態
      - pattern: "kafka.connect<type=connect-worker-metrics><>(connector-count|task-count|connector-startup-failure-total|task-startup-failure-total)"
        name: connect_worker_$1
        type: GAUGE
      - pattern: "kafka.connect<type=connector-task-metrics, connector=(.+), task=(.+)><>(status)"
        name: connect_task_status
        labels: { connector: "$1", task: "$2" }
      # Connect JVM rebalance
      - pattern: "kafka.connect<type=connect-coordinator-metrics><>(rebalance-latency-avg|rebalance-latency-max|failed-rebalance-total|rebalance-total)"
        name: connect_coordinator_$1
        type: GAUGE
      # JVM 基礎指標
      - pattern: "java.lang<type=Memory><HeapMemoryUsage>(used|max)"
        name: jvm_memory_heap_$1
        type: GAUGE
```

### 6.2 Service + ServiceMonitor(Prometheus Operator)

```yaml
apiVersion: v1
kind: Service
metadata:
  name: mm2-metrics
  namespace: kafka
  labels: { app: mm2 }
spec:
  clusterIP: None          # headless, 讓 Prometheus 抓到每個副本
  selector: { app: mm2 }
  ports:
    - { name: metrics, port: 9100, targetPort: 9100 }
---
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: mm2
  namespace: kafka
  labels: { release: prometheus }   # 對齊你的 Prometheus selector
spec:
  selector:
    matchLabels: { app: mm2 }
  endpoints:
    - { port: metrics, interval: 30s, path: /metrics }
```

> 沒裝 Prometheus Operator 就改用 annotation 抓取:
> `prometheus.io/scrape: "true"`、`prometheus.io/port: "9100"`、`prometheus.io/path: "/metrics"`。

### 6.3 關鍵指標

| 指標 | 含義 | 看什麼 |
|------|------|--------|
| `mm2_replication_latency_ms_max` | 訊息從 source 寫入到複製進 target 的延遲 | **複製是否落後**,最重要 |
| `mm2_record_age_ms_avg` | 被複製訊息的年齡 | 積壓程度 |
| `mm2_record_rate` / `mm2_byte_rate` | 複製吞吐 | 流量、是否卡住(掉到 0) |
| `connect_worker_task_count` | 執行中的 task 數 | failover/rebalance 後是否回穩 |
| `connect_task_status` | 每個 task 狀態(running/failed) | task 是否掛掉 |
| `connect_coordinator_failed_rebalance_total` | rebalance 失敗次數 | HA 協調是否異常 |
| consumer group lag(在 source 上) | MM2 consumer group 的落後量 | 用 kafka-exporter / broker 指標另抓 |

> 端到端落後也可用 **heartbeat topic**:比對 source 與 target 上 heartbeat 的時間戳差值。

### 6.4 建議告警(PrometheusRule)

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: mm2-alerts
  namespace: kafka
  labels: { release: prometheus }
spec:
  groups:
    - name: mm2
      rules:
        - alert: MM2ReplicationLagHigh
          expr: max(mm2_replication_latency_ms_max) by (topic) > 60000
          for: 5m
          labels: { severity: warning }
          annotations:
            summary: "MM2 複製延遲過高 (topic {{ $labels.topic }})"
            description: "複製延遲 > 60s 持續 5 分鐘"
        - alert: MM2ReplicationStalled
          expr: sum(rate(mm2_record_count[5m])) == 0
          for: 10m
          labels: { severity: critical }
          annotations:
            summary: "MM2 複製停滯"
            description: "10 分鐘內沒有任何複製進度, 可能 task 全掛或來源無資料"
        - alert: MM2TaskFailed
          expr: connect_task_status{status="failed"} > 0
          for: 1m
          labels: { severity: critical }
          annotations:
            summary: "MM2 task 失敗 ({{ $labels.connector }}/{{ $labels.task }})"
        - alert: MM2NoReplicas
          expr: sum(up{job=~".*mm2.*"}) < 2
          for: 2m
          labels: { severity: critical }
          annotations:
            summary: "MM2 存活副本 < 2, HA 已降級"
```

### 6.5 Grafana

匯入社群 dashboard 起步:**Kafka Connect / MirrorMaker 2**(Grafana.com dashboard,搜尋 "MirrorMaker 2" 或 "Kafka Connect")。
核心面板:replication latency(p99)、record/byte rate、task count、failed task、rebalance、JVM heap、consumer lag。

---

## 7. 部署步驟

```bash
# 1. 套用設定與監控 configmap
kubectl apply -f mm2-config.yaml
kubectl apply -f mm2-jmx-config.yaml

# 2. 部署 HA MM2
kubectl apply -f mm2-deployment.yaml
kubectl apply -f mm2-service.yaml
kubectl apply -f mm2-servicemonitor.yaml
kubectl apply -f mm2-alerts.yaml

# 3. 驗證
kubectl -n kafka get pods -l app=mm2          # 應有 2 個 Running
kubectl -n kafka logs -l app=mm2 --tail=50    # 看是否成功 assign task
kubectl -n kafka port-forward svc/mm2-metrics 9100:9100
curl -s localhost:9100/metrics | grep mm2_    # 確認指標有出來
```

### HA failover 驗證

```bash
# 砍掉一個副本, 觀察 task 是否轉移到另一個, 複製不中斷
kubectl -n kafka delete pod <mm2-pod-name>
kubectl -n kafka logs -l app=mm2 -f | grep -i rebalance
```

---

## 8. 檢查清單

- [ ] `replicas >= 2`
- [ ] 所有副本共用同一份 `mm2.properties`(同一 ConfigMap)
- [ ] 三組 RF 全部 ≥ 2(資料 / MM2 功能 / Connect 內部)
- [ ] `tasks.max` > 副本數
- [ ] SASL 密碼從 Secret 注入,未明文寫進 ConfigMap
- [ ] 副本以 `topologySpreadConstraints` 打散到不同節點
- [ ] JMX exporter `:9100` 有指標,ServiceMonitor 已被 Prometheus 抓到
- [ ] 告警:lag 過高 / 複製停滯 / task 失敗 / 副本 < 2
```
