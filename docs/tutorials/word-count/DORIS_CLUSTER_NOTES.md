# Doris 3 FE + 3 BE 叢集 — 啟動注意事項

Doris 4.0.5,3 FE + 3 BE HA 叢集,跑在 `docs/tutorials/docker-compose.yml`。

IP 對照(`doris-net` = `10.80.80.0/24`):

| 節點 | IP | 對 host 開的 port |
|---|---|---|
| doris-fe1 | .2 | 8030(http)、9030(mysql) |
| doris-fe2 | .3 | — |
| doris-fe3 | .4 | — |
| doris-be1 | .5 | 8040(webserver) |
| doris-be2 | .6 | — |
| doris-be3 | .7 | — |

---

## 啟動前

1. **記憶體**:6 節點 idle 約 8 GiB,加壓更高。Docker Desktop VM 若只有 ~12 GiB,先停掉其他不相關容器再開,否則 BE 容易 OOM 掉線。
2. **`vm.max_map_count`**:BE 要求 host(Docker VM)kernel `>= 2000000`。compose 裡的 `doris-init` 容器會在 FE/BE 啟動前自動設好,不用手動處理。
3. **子網不要撞**:`doris-net` 用 `10.80.80.0/24`。若報 `Pool overlaps with other one`,代表跟既有 Docker 網路重疊 — 用 `docker network ls` + `inspect` 查空段,改 compose 裡的 subnet 與各節點 `ipv4_address`(同時要改 FE/BE 的 `FE_SERVERS`/`BE_ADDR`,它們寫死了 IP)。

## 啟動

```bash
cd docs/tutorials
docker-compose up -d doris-fe1 doris-fe2 doris-fe3 doris-be1 doris-be2 doris-be3
```

## ⚠️ Follower FE 要等 ~60 秒才會 join(最重要)

剛起來時 `SHOW FRONTENDS` **只看得到 master(fe1)是正常的**,不是壞掉。

原因:image 的 `/usr/local/bin/init_fe.sh` 裡 `register_fe()` 會先跑滿一輪 `check_fe_registered`(約 60 秒)才真正執行 `ALTER SYSTEM ADD FOLLOWER`。fe2 / fe3 的 log 會一直印 `Waiting for master FE to be ready...`——這訊息會誤導,實際是它在等「自己被登記」,而登記要等那一輪跑完才發生。

→ **等約 1 分鐘**,fe2/fe3 就會自己加進叢集。BE 沒這問題,通常很快就 alive。

## 確認叢集就緒

```bash
# FE:應該 3 個 Alive=true(一個 IsMaster=true)
docker exec -i doris-fe1 mysql -uroot -P9030 -h127.0.0.1 -e "SHOW FRONTENDS\G" | grep -E "Host|Alive|IsMaster"

# BE:應該 3 個 Alive=true
docker exec -i doris-fe1 mysql -uroot -P9030 -h127.0.0.1 -e "SHOW BACKENDS\G" | grep -E "Host|Alive"
```

## 建表(必須手動,Doris image 不會自動跑 init script)

```bash
docker exec -i doris-fe1 mysql -uroot -P9030 -h127.0.0.1 < init_doris.sql
```

- `init_doris.sql` 用 `replication_num=3`(每筆 3 副本)。這需要**先有 3 個 BE alive**,否則會報
  `replication num should be less than the number of available backends`。所以一定要等叢集就緒再建表。
- 連線帳號 `root`、無密碼。

## 寫入 / Stream Load 一律打 BE `localhost:8040`

從 host 跑的 app(含 `consumer_doris.py`、benchmark)**不要打 FE:8030**。FE 收到 Stream Load 會 302 redirect 到 BE 的子網 IP(`10.80.80.x`),host 路由不到 → 卡住。直接打 **be1 的 `localhost:8040`** 就跳過 redirect。`consumer_doris.py` 預設就是 8040,不用改。

## 收掉 / 重置

```bash
docker-compose stop doris-fe1 doris-fe2 doris-fe3 doris-be1 doris-be2 doris-be3   # 保留資料
docker-compose rm -sf  doris-fe1 doris-fe2 doris-fe3 doris-be1 doris-be2 doris-be3 # 刪容器(volume 還在)
# 連 metadata/資料一起清(換版或壞掉時):
docker volume rm tutorials_doris-fe1-meta tutorials_doris-fe2-meta tutorials_doris-fe3-meta \
                 tutorials_doris-be1-storage tutorials_doris-be2-storage tutorials_doris-be3-storage
```

> 換版:改 compose 裡的 `apache/doris:fe-<ver>` / `be-<ver>`。大版本升級(如 4.0 → 4.1)建議連 volume 一起清,避免 metadata 不相容。
