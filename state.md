# Quix Streams State 機制深度源碼解讀

> State 是 Quix Streams 中讓 streaming application 能做 stateful 操作（aggregation, windowing 等）的核心。
> 底層用 RocksDB（或 Memory）儲存，透過 changelog topic 確保持久性與可恢復性。
> 本文件包含所有相關源碼，不需要另外開檔案閱讀。

---

## 目錄

1. [架構概觀](#1-架構概觀)
2. [StateStoreManager — 全域管理者](#2-statestoremanaer--全域管理者)
3. [Store — 抽象 Store 類別](#3-store--抽象-store-類別)
4. [RocksDBStore — RocksDB 實作](#4-rocksdbstore--rocksdb-實作)
5. [RocksDBStorePartition — 單一 RocksDB 實例](#5-rocksdbstorepartition--單一-rocksdb-實例)
6. [MemoryStore / MemoryStorePartition — 記憶體實作](#6-memorystore--memorystorepartition--記憶體實作)
7. [PartitionTransaction — State 交易](#7-partitiontransaction--state-交易)
8. [RocksDBPartitionTransaction — RocksDB 特化交易](#8-rocksdbpartitiontransaction--rocksdb-特化交易)
9. [State / TransactionState — 使用者介面](#9-state--transactionstate--使用者介面)
10. [Serialization — 序列化](#10-serialization--序列化)
11. [RocksDB Options — 設定](#11-rocksdb-options--設定)
12. [磁碟目錄結構](#12-磁碟目錄結構)
13. [端到端流程圖](#13-端到端流程圖)
14. [關鍵源碼索引](#14-關鍵源碼索引)

---

## 1. 架構概觀

```
使用者程式碼
  │
  ▼
State (介面層)
  │  state.get(key) / state.set(key, value)
  ▼
TransactionState (prefix 綁定)
  │  加上 message key 作為 prefix
  ▼
PartitionTransaction (交易層)
  │  讀：cache → RocksDB
  │  寫：只寫 cache
  ▼
PartitionTransactionCache (Read-Your-Own-Writes)
  │  {cf_name: {prefix: {key: value}}}
  ▼
StorePartition (儲存層)
  │  RocksDBStorePartition / MemoryStorePartition
  ▼
RocksDB / Dict (底層儲存)

──── Checkpoint commit 時 ────
  ▼
prepare(): cache → changelog topic (produce)
  ▼
flush(): cache → RocksDB WriteBatch → db.write()
```

**Column Family 設計**：
- `"default"` — 主要 state 資料
- `"__metadata__"` — 存 changelog offset 等 metadata
- `"__global-counter__"` — 全域序列號計數器
- Windowed store 額外有：`"__latest-timestamps__"`, `"__expiration-index__"` 等

---

## 2. StateStoreManager — 全域管理者

檔案：`quixstreams/state/manager.py:32-339`

```python
class StateStoreManager:
    """
    Class for managing state stores and partitions.

    StateStoreManager is responsible for:
     - reacting to rebalance callbacks
     - managing the individual state stores
     - providing access to store transactions
    """

    def __init__(
        self,
        group_id: Optional[str] = None,
        state_dir: Optional[Union[str, Path]] = None,
        rocksdb_options: Optional[RocksDBOptionsType] = None,
        producer: Optional[InternalProducer] = None,
        recovery_manager: Optional[RecoveryManager] = None,
        default_store_type: StoreTypes = RocksDBStore,
    ):
        if state_dir is not None:
            state_dir = Path(state_dir).absolute()
            if group_id is not None:
                state_dir = state_dir / group_id

        self._state_dir = state_dir             # ★ state 目錄 = state_dir / group_id
        self._rocksdb_options = rocksdb_options
        # ★ {stream_id: {store_name: Store}} — 所有已註冊的 store
        self._stores: Dict[Optional[str], Dict[str, Store]] = {}
        self._producer = producer
        self._recovery_manager = recovery_manager
        self._default_store_type = default_store_type
```

### 核心屬性與方法

```python
    @property
    def stores(self) -> Dict[Optional[str], Dict[str, Store]]:
        """Map of registered state stores: {stream_id: {store_name: store}}"""
        return self._stores

    @property
    def recovery_required(self) -> bool:
        """Whether recovery needs to be done."""
        if self._recovery_manager:
            return self._recovery_manager.has_assignments
        return False

    @property
    def using_changelogs(self) -> bool:
        """Whether the StateStoreManager is using changelog topics"""
        return bool(self._recovery_manager)

    def do_recovery(self) -> None:
        if self._recovery_manager is None:
            raise RuntimeError("a recovery manager is needed to do a recovery")
        return self._recovery_manager.do_recovery()

    def stop_recovery(self) -> None:
        if self._recovery_manager is None:
            raise RuntimeError("a recovery manager is needed to do a recovery")
        return self._recovery_manager.stop_recovery()

    def get_store(
        self, stream_id: str, store_name: str = DEFAULT_STATE_STORE_NAME
    ) -> Store:
        """
        ★ 被 Checkpoint.get_store_transaction() 呼叫
        """
        store = self._stores.get(stream_id, {}).get(store_name)
        if store is None:
            raise StoreNotRegisteredError(
                f'Store "{store_name}" (stream_id "{stream_id}") is not registered'
            )
        return store
```

### Changelog 設定

```python
    def _setup_changelogs(
        self,
        stream_id: Optional[str],
        store_name: str,
        topic_config: Optional[TopicConfig],
    ) -> Optional[ChangelogProducerFactory]:
        """
        ★ 如果有 recovery_manager + producer，建立 changelog topic 並返回 factory
        """
        if self._recovery_manager and self._producer:
            changelog_topic = self._recovery_manager.register_changelog(
                stream_id=stream_id,
                store_name=store_name,
                topic_config=topic_config or TopicConfig(
                    num_partitions=1, replication_factor=1
                ),
            )
            return ChangelogProducerFactory(
                changelog_name=changelog_topic.name,
                producer=self._producer,
            )
        return None
```

### Partition Assign / Revoke

```python
    def on_partition_assign(
        self,
        stream_id: Optional[str],
        partition: int,
        committed_offsets: dict[str, int],
    ) -> Dict[str, StorePartition]:
        """
        ★ Rebalance 時呼叫
        為每個 store 建立 StorePartition，並通知 RecoveryManager
        """
        store_partitions = {}
        for name, store in self._stores.get(stream_id, {}).items():
            store_partition = store.assign_partition(partition)
            store_partitions[name] = store_partition
        if self._recovery_manager and store_partitions:
            self._recovery_manager.assign_partition(
                topic=stream_id,
                partition=partition,
                committed_offsets=committed_offsets,
                store_partitions=store_partitions,
            )
        return store_partitions

    def on_partition_revoke(
        self, stream_id: str, partition: int,
    ) -> None:
        """
        ★ Rebalance 時呼叫
        停止 recovery 追蹤 + 關閉 RocksDB
        """
        if stores := self._stores.get(stream_id, {}).values():
            if self._recovery_manager:
                self._recovery_manager.revoke_partition(partition_num=partition)
            for store in stores:
                store.revoke_partition(partition=partition)

    def clear_stores(self) -> None:
        """Delete all state stores managed by StateStoreManager."""
        if any(
            store.partitions
            for stream_stores in self._stores.values()
            for store in stream_stores.values()
        ):
            raise PartitionStoreIsUsed(
                "Cannot clear stores with active partitions assigned"
            )
        if self._state_dir is not None:
            shutil.rmtree(self._state_dir, ignore_errors=True)
            logger.info(f"Removing state folder at {self._state_dir}")
```

---

## 3. Store — 抽象 Store 類別

檔案：`quixstreams/state/base/store.py:15-103`

```python
class Store(ABC):
    """
    Abstract state store.
    It keeps track of individual store partitions and provides access to the
    partitions' transactions.
    """

    def __init__(self, name: str, stream_id: Optional[str]) -> None:
        super().__init__()
        self._name = name
        self._stream_id = stream_id
        # ★ {partition_num: StorePartition} — 已 assign 的 partition
        self._partitions: Dict[int, StorePartition] = {}

    @abstractmethod
    def create_new_partition(self, partition: int) -> StorePartition:
        pass

    @property
    def stream_id(self) -> Optional[str]:
        return self._stream_id

    @property
    def name(self) -> str:
        return self._name

    @property
    def partitions(self) -> Dict[int, StorePartition]:
        return self._partitions

    def assign_partition(self, partition: int) -> StorePartition:
        """
        ★ Rebalance on_assign 時呼叫
        如果 partition 已 assign（同一個 pod 原本就有），直接返回
        否則建立新的 StorePartition（開啟 RocksDB）
        """
        store_partition = self._partitions.get(partition)
        if store_partition is not None:
            logger.debug(
                f'Partition "{partition}" for store "{self._name}" '
                f'(stream "{self._stream_id}") is already assigned'
            )
            return store_partition

        store_partition = self.create_new_partition(partition)
        self._partitions[partition] = store_partition
        logger.debug(
            'Assigned store partition "%s[%s]" (stream "%s")',
            self._name, partition, self._stream_id,
        )
        return store_partition

    def revoke_partition(self, partition: int):
        """
        ★ Rebalance on_revoke 時呼叫
        從 dict 移除並關閉 RocksDB
        """
        store_partition = self._partitions.pop(partition, None)
        if store_partition is None:
            return

        store_partition.close()
        logger.debug(
            'Revoked store partition "%s[%s]" (stream "%s")',
            self._name, partition, self._stream_id,
        )

    def start_partition_transaction(self, partition: int) -> PartitionTransaction:
        """
        ★ 被 Checkpoint.get_store_transaction() 呼叫
        建立新的 transaction 來操作 state
        """
        store_partition = self._partitions.get(partition)
        if store_partition is None:
            raise PartitionNotAssignedError(...)
        return store_partition.begin()
```

---

## 4. RocksDBStore — RocksDB 實作

檔案：`quixstreams/state/rocksdb/store.py:18-66`

```python
class RocksDBStore(Store):
    """
    RocksDB-based state store.
    """

    def __init__(
        self,
        name: str,
        stream_id: Optional[str],
        base_dir: str,
        changelog_producer_factory: Optional[ChangelogProducerFactory] = None,
        options: Optional[RocksDBOptionsType] = None,
    ):
        super().__init__(name, stream_id)

        # ★ 目錄結構：{base_dir}/{store_name}/{stream_id}/
        partitions_dir = Path(base_dir).absolute() / self._name
        if self._stream_id:
            partitions_dir = partitions_dir / self._stream_id

        self._partitions_dir = partitions_dir
        self._changelog_producer_factory = changelog_producer_factory
        self._options = options

    def create_new_partition(self, partition: int) -> RocksDBStorePartition:
        # ★ 每個 partition 一個 RocksDB 目錄：{partitions_dir}/{partition}/
        path = str((self._partitions_dir / str(partition)).absolute())

        changelog_producer: Optional[ChangelogProducer] = None
        if self._changelog_producer_factory:
            changelog_producer = (
                self._changelog_producer_factory.get_partition_producer(partition)
            )

        return RocksDBStorePartition(
            path=path, options=self._options, changelog_producer=changelog_producer
        )
```

---

## 5. RocksDBStorePartition — 單一 RocksDB 實例

### 5.1 StorePartition 抽象基底

檔案：`quixstreams/state/base/partition.py:21-116`

```python
class StorePartition(ABC):
    """
    A base class to access state in the underlying storage.
    It represents a single instance of some storage.
    """

    def __init__(
        self,
        dumps: DumpsFunc,
        loads: LoadsFunc,
        changelog_producer: Optional["ChangelogProducer"],
    ) -> None:
        super().__init__()
        self._dumps = dumps
        self._loads = loads
        self._changelog_producer = changelog_producer  # ★ 對應的 changelog producer

    @abstractmethod
    def close(self): ...

    @abstractmethod
    def get_changelog_offset(self) -> Optional[int]:
        """★ 從 RocksDB metadata 讀取上次 changelog 寫到哪裡"""
        ...

    @abstractmethod
    def write_changelog_offset(self, offset: int):
        """★ 只更新 changelog offset，不動 state data"""

    @abstractmethod
    def write(self, cache: PartitionTransactionCache, changelog_offset: Optional[int]):
        """★ 將 cache 寫入底層儲存（RocksDB WriteBatch）"""

    @abstractmethod
    def get(self, key: bytes, cf_name: str = "default") -> Union[bytes, Literal[Marker.UNDEFINED]]:
        """★ 讀取 key"""

    @abstractmethod
    def exists(self, key: bytes, cf_name: str = "default") -> bool:
        """★ 檢查 key 是否存在"""

    @abstractmethod
    def recover_from_changelog_message(
        self, key: bytes, value: Optional[bytes], cf_name: str, offset: int
    ):
        """★ Recovery 時套用 changelog 訊息"""

    @abstractmethod
    def begin(self) -> PartitionTransaction:
        """★ 建立新的 PartitionTransaction"""
        ...
```

### 5.2 RocksDBStorePartition 完整實作

檔案：`quixstreams/state/rocksdb/partition.py:35-394`

```python
class RocksDBStorePartition(StorePartition):
    """
    A base class to access state in RocksDB.
    It represents a single RocksDB database.

    Responsibilities:
     1. Managing access to the RocksDB instance
     2. Creating transactions to interact with data
     3. Flushing WriteBatches to the RocksDB

    It opens the RocksDB on `__init__`. If the db is locked by another process,
    it will retry according to `open_max_retries` and `open_retry_backoff` options.
    """

    def __init__(
        self,
        path: str,
        options: Optional[RocksDBOptionsType] = None,
        changelog_producer: Optional[ChangelogProducer] = None,
    ):
        if not options:
            options = RocksDBOptions()

        super().__init__(options.dumps, options.loads, changelog_producer)
        self._path = path
        self._options = options
        self._rocksdb_options = self._options.to_options()
        self._open_max_retries = self._options.open_max_retries
        self._open_retry_backoff = self._options.open_retry_backoff
        self._db = self._init_rocksdb()            # ★ 開啟 RocksDB
        self._cf_cache: Dict[str, Rdict] = {}      # ★ column family dict cache
        self._cf_handle_cache: Dict[str, ColumnFamily] = {}  # ★ CF handle cache
```

#### Recovery 寫入

```python
    def recover_from_changelog_message(
        self, key: bytes, value: Optional[bytes], cf_name: str, offset: int
    ):
        """★ Recovery 時逐條寫入"""
        cf_handle = self.get_column_family_handle(cf_name)
        batch = WriteBatch(raw_mode=True)
        if value is None:
            batch.delete(key, cf_handle)     # tombstone → 刪除
        else:
            batch.put(key, value, cf_handle) # 正常寫入
        self._update_changelog_offset(batch=batch, offset=offset)
        self._write(batch)
```

#### 正常處理寫入（flush 時呼叫）

```python
    def write(
        self,
        cache: PartitionTransactionCache,
        changelog_offset: Optional[int],
        batch: Optional[WriteBatch] = None,
    ):
        """★ 將整個 cache 一次性寫入 RocksDB"""
        if batch is None:
            batch = WriteBatch(raw_mode=True)

        # ★ 遍歷所有 column family 的更新
        column_families = cache.get_column_families()
        for cf_name in column_families:
            cf_handle = self.get_column_family_handle(cf_name)

            updates = cache.get_updates(cf_name=cf_name)
            for prefix_update_cache in updates.values():
                for key, value in prefix_update_cache.items():
                    batch.put(key, value, cf_handle)

            deletes = cache.get_deletes(cf_name=cf_name)
            for key in deletes:
                batch.delete(key, cf_handle)

        # ★ 順便記錄 changelog offset
        if changelog_offset is not None:
            self._update_changelog_offset(batch=batch, offset=changelog_offset)

        logger.debug(
            f"Flushing state changes to the disk "
            f'path="{self.path}" '
            f"changelog_offset={changelog_offset} "
            f"bytes_total={batch.size_in_bytes()}"
        )
        self._write(batch)
```

#### 讀取操作

```python
    def get(
        self, key: bytes, cf_name: str = "default"
    ) -> Union[bytes, Literal[Marker.UNDEFINED]]:
        """★ 直接從 RocksDB 讀取"""
        result = self.get_or_create_column_family(cf_name).get(
            key, default=Marker.UNDEFINED
        )
        return cast(Union[bytes, Literal[Marker.UNDEFINED]], result)

    def exists(self, key: bytes, cf_name: str = "default") -> bool:
        cf_dict = self.get_or_create_column_family(cf_name)
        return key in cf_dict

    def iter_items(
        self,
        lower_bound: bytes,   # inclusive
        upper_bound: bytes,   # exclusive
        backwards: bool = False,
        cf_name: str = "default",
    ) -> Iterator[tuple[bytes, bytes]]:
        """★ Range query，用於 windowed state"""
        cf = self.get_or_create_column_family(cf_name=cf_name)

        read_opt = ReadOptions()
        read_opt.set_iterate_lower_bound(lower_bound)
        read_opt.set_iterate_upper_bound(upper_bound)

        from_key = upper_bound if backwards else lower_bound
        items = cast(
            Iterator[tuple[bytes, bytes]],
            cf.items(from_key=from_key, read_opt=read_opt, backwards=backwards),
        )

        if not backwards:
            yield from items
        else:
            # ★ Backwards 時 lower bound 不被 Rdict 尊重，手動過濾
            for key, value in items:
                if key < lower_bound:
                    break
                yield key, value
```

#### Changelog Offset 管理

```python
    def get_changelog_offset(self) -> Optional[int]:
        """★ 從 __metadata__ CF 讀取 changelog offset"""
        metadata_cf = self.get_or_create_column_family(METADATA_CF_NAME)
        offset_bytes = metadata_cf.get(CHANGELOG_OFFSET_KEY)  # b"__changelog_offset__"
        if offset_bytes is None:
            return None
        return int_from_bytes(offset_bytes)

    def write_changelog_offset(self, offset: int):
        """★ 只更新 offset，不動 state（跳過 recovery 套用時用）"""
        batch = WriteBatch(raw_mode=True)
        self._update_changelog_offset(batch=batch, offset=offset)
        self._write(batch)

    def _update_changelog_offset(self, batch: WriteBatch, offset: int):
        """★ 在 WriteBatch 中加入 offset 更新"""
        batch.put(
            CHANGELOG_OFFSET_KEY,              # b"__changelog_offset__"
            int_to_bytes(offset),
            self.get_column_family_handle(METADATA_CF_NAME),  # "__metadata__" CF
        )
```

#### 關閉與底層寫入

```python
    def close(self):
        logger.debug(f'Closing rocksdb partition on "{self._path}"')
        # ★ 清除 CF cache 才能正確關閉 RocksDB
        self._cf_handle_cache = {}
        self._cf_cache = {}
        self._db.close()
        logger.debug(f'Closed rocksdb partition on "{self._path}"')

    def _write(self, batch: WriteBatch):
        """★ 最底層的 RocksDB 寫入"""
        self._db.write(batch)

    def begin(self) -> RocksDBPartitionTransaction:
        """★ 建立 RocksDB 特化的 transaction"""
        return RocksDBPartitionTransaction(
            partition=self,
            dumps=self._dumps,
            loads=self._loads,
            changelog_producer=self._changelog_producer,
        )
```

#### Column Family 管理

```python
    def get_or_create_column_family(self, cf_name: str) -> Rdict:
        """★ 取得或建立 column family（帶 cache）"""
        cf = self._cf_cache.get(cf_name)
        if cf is not None:
            return cf

        cf = self._db.create_cf(cf_name, self._rocksdb_options)
        self._cf_cache[cf_name] = cf
        return cf

    def get_column_family_handle(self, cf_name: str) -> ColumnFamily:
        """★ 取得 CF handle（用於 WriteBatch 操作）"""
        handle = self._cf_handle_cache.get(cf_name)
        if handle is not None:
            return handle

        handle = self._db.get_column_family_handle(cf_name)
        if handle is None:
            self.get_or_create_column_family(cf_name)
            handle = self._db.get_column_family_handle(cf_name)
        self._cf_handle_cache[cf_name] = handle
        return handle
```

### Metadata 常數

檔案：`quixstreams/state/metadata.py`

```python
SEPARATOR = b"|"
SEPARATOR_LENGTH = len(SEPARATOR)

CHANGELOG_CF_MESSAGE_HEADER = "__column_family__"
CHANGELOG_PROCESSED_OFFSETS_MESSAGE_HEADER = "__processed_tp_offsets__"
METADATA_CF_NAME = "__metadata__"

DEFAULT_PREFIX = b""

class Marker(enum.Enum):
    UNDEFINED = 1   # ★ Cache 中沒有，需要查 store
    DELETED = 2     # ★ 已刪除，不需查 store
```

檔案：`quixstreams/state/rocksdb/metadata.py`

```python
PROCESSED_OFFSET_KEY = b"__topic_offset__"
CHANGELOG_OFFSET_KEY = b"__changelog_offset__"

GLOBAL_COUNTER_CF_NAME = "__global-counter__"
GLOBAL_COUNTER_KEY = b"__global_counter__"
```

---

## 6. MemoryStore / MemoryStorePartition — 記憶體實作

### MemoryStore

檔案：`quixstreams/state/memory/store.py:14-47`

```python
class MemoryStore(Store):
    """
    In-memory state store.
    ★ 每次 partition assign 都需要完整的 changelog recovery（因為沒有磁碟）
    """

    def __init__(
        self,
        name: str,
        stream_id: Optional[str],
        changelog_producer_factory: Optional[ChangelogProducerFactory] = None,
    ) -> None:
        super().__init__(name, stream_id)
        self._changelog_producer_factory = changelog_producer_factory

    def create_new_partition(self, partition: int) -> MemoryStorePartition:
        changelog_producer: Optional[ChangelogProducer] = None
        if self._changelog_producer_factory:
            changelog_producer = (
                self._changelog_producer_factory.get_partition_producer(partition)
            )
        return MemoryStorePartition(changelog_producer)
```

### MemoryStorePartition

檔案：`quixstreams/state/memory/partition.py:35-148`

```python
class MemoryStorePartition(StorePartition):
    """
    Class to access in-memory state.
    """

    def __init__(self, changelog_producer: Optional[ChangelogProducer]) -> None:
        super().__init__(
            dumps=json_dumps,
            loads=json_loads,
            changelog_producer=changelog_producer,
        )
        self._changelog_offset: Optional[int] = None
        # ★ 簡單的 dict 結構：{cf_name: {key: value}}
        self._state: Dict[str, Dict[bytes, Any]] = {
            "default": {},
            METADATA_CF_NAME: {},
        }
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        self._closed = True

    def begin(self) -> PartitionTransaction:
        return PartitionTransaction(
            partition=self,
            dumps=self._dumps,
            loads=self._loads,
            changelog_producer=self._changelog_producer,
        )

    def write(
        self, cache: PartitionTransactionCache, changelog_offset: Optional[int],
    ) -> None:
        """★ 將 cache 套用到 in-memory dict"""
        if changelog_offset is not None:
            self._changelog_offset = changelog_offset

        for cf_name in cache.get_column_families():
            updates = cache.get_updates(cf_name=cf_name)
            for prefix_update_cache in updates.values():
                for key, value in prefix_update_cache.items():
                    self._state.setdefault(cf_name, {})[key] = value

            deletes = cache.get_deletes(cf_name=cf_name)
            for key in deletes:
                self._state[cf_name].pop(key, None)

    def recover_from_changelog_message(
        self, key: bytes, value: Optional[bytes], cf_name: str, offset: int
    ) -> None:
        """★ Recovery：直接寫入 dict"""
        if value:
            self._state.setdefault(cf_name, {})[key] = value
        else:
            self._state.setdefault(cf_name, {}).pop(key, None)
        self._changelog_offset = offset

    def get_changelog_offset(self) -> Optional[int]:
        return self._changelog_offset

    def write_changelog_offset(self, offset: int):
        self._changelog_offset = offset

    def get(
        self, key: bytes, cf_name: str = "default"
    ) -> Union[bytes, Literal[Marker.UNDEFINED]]:
        return self._state.get(cf_name, {}).get(key, Marker.UNDEFINED)

    def exists(self, key: bytes, cf_name: str = "default") -> bool:
        return key in self._state.get(cf_name, {})
```

---

## 7. PartitionTransaction — State 交易

檔案：`quixstreams/state/base/transaction.py:195-591`

（完整源碼已收錄在 `checkpoint.md` 第 6 節，此處只列出重點）

### 交易生命週期

```
begin()
  → PartitionTransaction(status=STARTED)
  → PartitionTransactionCache() 空 cache

使用者操作（status=STARTED 時才可）：
  → get(key, prefix)   : cache → store
  → set(key, value)    : → cache
  → delete(key)        : → cache
  → exists(key)        : cache → store

prepare(processed_offsets)
  → 產生 changelog 訊息（每個 update/delete 一條）
  → status = PREPARED

flush(changelog_offset)
  → partition.write(cache, changelog_offset)  → RocksDB WriteBatch
  → status = COMPLETE
```

### Key Serialization

```python
def _serialize_key(self, key: K, prefix: bytes) -> bytes:
    """
    ★ key 的格式：{prefix}|{key_bytes}
    prefix 通常是 message key 的序列化結果
    """
    key_bytes = key if isinstance(key, bytes) else serialize(key, dumps=self._dumps)
    prefix = prefix + SEPARATOR if prefix else b""
    return prefix + key_bytes
```

---

## 8. RocksDBPartitionTransaction — RocksDB 特化交易

檔案：`quixstreams/state/rocksdb/transaction.py:22-134`

```python
class RocksDBPartitionTransaction(PartitionTransaction[bytes, Any]):
    def __init__(
        self,
        partition: "RocksDBStorePartition",
        dumps: DumpsFunc,
        loads: LoadsFunc,
        changelog_producer: Optional["ChangelogProducer"] = None,
    ) -> None:
        super().__init__(
            partition=partition, dumps=dumps, loads=loads,
            changelog_producer=changelog_producer,
        )
        self._partition: RocksDBStorePartition = cast(
            "RocksDBStorePartition", self._partition
        )
        self._counter: Optional[int] = None
```

### Range Query — 合併 DB + Cache

```python
    def _get_items(
        self,
        start: int,
        end: int,
        prefix: bytes,
        backwards: bool = False,
        cf_name: str = "default",
    ) -> list[tuple[bytes, bytes]]:
        """
        ★ 合併 RocksDB 的資料和 cache 中的更新
        用於 windowed state 的 range query
        """
        start = max(start, 0)
        if start > end:
            return []

        seek_from_key = append_integer(base_bytes=prefix, integer=start)
        seek_to_key = append_integer(base_bytes=prefix, integer=end)

        # ★ 從 RocksDB 取得 range 內的資料
        db_items = self._partition.iter_items(
            lower_bound=seek_from_key,
            upper_bound=seek_to_key,
            cf_name=cf_name,
        )

        cache = self._update_cache
        update_cache = cache.get_updates(cf_name=cf_name).get(prefix, {})
        delete_cache = cache.get_deletes(cf_name=cf_name)

        # ★ 從 cache 取得 range 內的更新
        updated_items = (
            (key, value)
            for key, value in update_cache.items()
            if seek_from_key < key <= seek_to_key
        )

        # ★ 合併：DB 資料 + cache 更新，排除已刪除的 key
        merged_items = {}
        for key, value in chain(db_items, updated_items):
            if key not in delete_cache:
                merged_items[key] = value

        return sorted(merged_items.items(), key=lambda kv: kv[0], reverse=backwards)
```

### Global Counter — 全域序列號

```python
    @validate_transaction_status(PartitionTransactionStatus.STARTED)
    def prepare(self, processed_offsets: Optional[dict[str, int]] = None) -> None:
        """★ prepare 前先持久化 counter"""
        self._persist_counter()
        super().prepare(processed_offsets=processed_offsets)

    def _increment_counter(self) -> int:
        """
        ★ 全域遞增計數器，用於產生唯一 ID
        存在 __global-counter__ CF 中
        到 MAX_UINT64 (2^64-1) 時歸零
        """
        if self._counter is None:
            self._counter = self.get(
                key=GLOBAL_COUNTER_KEY,
                prefix=b"",
                default=-1,
                cf_name=GLOBAL_COUNTER_CF_NAME,
            )
        self._counter = self._counter + 1 if self._counter < MAX_UINT64 else 0
        return self._counter

    def _persist_counter(self) -> None:
        """★ 將 counter 寫入 cache（之後隨 flush 一起寫入 RocksDB）"""
        if self._counter is not None:
            self.set(
                value=self._counter,
                key=GLOBAL_COUNTER_KEY,
                prefix=b"",
                cf_name=GLOBAL_COUNTER_CF_NAME,
            )
```

---

## 9. State / TransactionState — 使用者介面

檔案：`quixstreams/state/base/state.py:17-174`

### State 抽象介面

```python
class State(ABC, Generic[K, V]):
    """
    Primary interface for working with key-value state data from `StreamingDataFrame`
    ★ 使用者在 .update(), .filter() 等 callback 中拿到的就是這個介面
    """

    @abstractmethod
    def get(self, key: K, default: Optional[V] = None) -> Optional[V]: ...

    @abstractmethod
    def set(self, key: K, value: V) -> None: ...

    @abstractmethod
    def delete(self, key: K): ...

    @abstractmethod
    def exists(self, key: K) -> bool: ...
```

### TransactionState — 綁定 prefix 的實作

```python
class TransactionState(State):
    """
    ★ 每個 message 的 state 操作都會自動綁定 message key 作為 prefix
    確保不同 key 的 state 不會互相干擾
    """
    __slots__ = ("_transaction", "_prefix")

    def __init__(self, prefix: bytes, transaction: "PartitionTransaction"):
        self._prefix = prefix
        self._transaction = transaction

    def get(self, key: K, default: Optional[V] = None) -> Optional[V]:
        # ★ 自動帶上 prefix
        return self._transaction.get(key=key, prefix=self._prefix, default=default)

    def set(self, key: K, value: V) -> None:
        return self._transaction.set(key=key, value=value, prefix=self._prefix)

    def delete(self, key: K):
        return self._transaction.delete(key=key, prefix=self._prefix)

    def exists(self, key: K) -> bool:
        return self._transaction.exists(key=key, prefix=self._prefix)
```

### as_state() — 建立 TransactionState

```python
# quixstreams/state/base/transaction.py:284-300
def as_state(self, prefix: Any = DEFAULT_PREFIX) -> State[K, V]:
    """
    ★ 使用者的 message key 會被序列化為 bytes 作為 prefix
    所有 state 操作的 key 都會自動加上 "{prefix}|" 前綴
    """
    return TransactionState(
        transaction=self,
        prefix=(
            prefix
            if isinstance(prefix, bytes)
            else serialize(prefix, dumps=self._dumps)
        ),
    )
```

---

## 10. Serialization — 序列化

檔案：`quixstreams/state/serialization.py:1-92`

```python
_int_packer = struct.Struct(">Q")    # ★ Big-endian unsigned 64-bit integer
_int_pack = _int_packer.pack
_int_unpack = _int_packer.unpack

_int_pair_pack_format = ">Q" + "c" * SEPARATOR_LENGTH + "Q"
_int_pair_packer = struct.Struct(_int_pair_pack_format)
_int_pair_pack = _int_pair_packer.pack
_int_pair_unpack = _int_pair_packer.unpack

DumpsFunc = Callable[[Any], bytes]    # ★ 序列化函式型別
LoadsFunc = Callable[[bytes], Any]    # ★ 反序列化函式型別


def serialize(value: Any, dumps: DumpsFunc) -> bytes:
    """★ 通用序列化，預設用 JSON"""
    try:
        return dumps(value)
    except Exception as exc:
        raise StateSerializationError(f'Failed to serialize value: "{value}"') from exc


def deserialize(value: bytes, loads: LoadsFunc) -> Any:
    try:
        return loads(value)
    except Exception as exc:
        raise StateSerializationError(
            f'Failed to deserialize value: "{value!r}"'
        ) from exc


def int_to_bytes(value: int) -> bytes:
    """★ Unsigned 64-bit int → 8 bytes (big-endian，RocksDB 可排序)"""
    return _int_pack(value)


def int_from_bytes(value: bytes) -> int:
    return _int_unpack(value)[0]


def encode_integer_pair(integer_1: int, integer_2: int) -> bytes:
    """
    ★ 編碼格式：<integer_1>|<integer_2>
    Big-endian 確保在 RocksDB 中可以按數值排序
    用於 windowed state 的 key：<prefix>|<start_ms>|<end_ms>
    """
    return _int_pair_pack(integer_1, SEPARATOR, integer_2)


def decode_integer_pair(value: bytes) -> tuple[int, int]:
    integer_1, _, integer_2 = _int_pair_unpack(value)
    return integer_1, integer_2


def append_integer(base_bytes: bytes, integer: int) -> bytes:
    """
    ★ 格式：<base_bytes>|<integer>
    用於 range query 的 seek key
    """
    return base_bytes + SEPARATOR + int_to_bytes(integer)
```

---

## 11. RocksDB Options — 設定

檔案：`quixstreams/state/rocksdb/options.py:25-88`

```python
@dataclasses.dataclass(frozen=True)
class RocksDBOptions(RocksDBOptionsType):
    """
    RocksDB database options.
    """

    write_buffer_size: int = 64 * 1024 * 1024         # 64MB memtable
    target_file_size_base: int = 64 * 1024 * 1024     # 64MB SST file
    max_write_buffer_number: int = 3                   # 最多 3 個 memtable
    block_cache_size: int = 128 * 1024 * 1024          # 128MB block cache
    bloom_filter_bits_per_key: int = 10                # Bloom filter 精度
    enable_pipelined_write: bool = False
    compression_type: CompressionType = "lz4"          # ★ 預設 LZ4 壓縮
    wal_dir: Optional[str] = None
    max_total_wal_size: int = 128 * 1024 * 1024        # 128MB WAL 上限
    db_log_dir: Optional[str] = None
    dumps: DumpsFunc = dumps                           # ★ 預設 JSON serialize
    loads: LoadsFunc = loads                           # ★ 預設 JSON deserialize
    open_max_retries: int = 10                         # DB 被鎖時重試次數
    open_retry_backoff: float = 3.0                    # 每次重試間隔秒數
    use_fsync: bool = True                             # ★ 預設用 fsync 確保持久性
    on_corrupted_recreate: bool = False                # 損壞時是否自動重建

    def to_options(self) -> rocksdict.Options:
        opts = rocksdict.Options(raw_mode=True)
        opts.create_if_missing(True)
        opts.set_write_buffer_size(self.write_buffer_size)
        opts.set_target_file_size_base(self.target_file_size_base)
        opts.set_max_write_buffer_number(self.max_write_buffer_number)
        opts.set_enable_pipelined_write(self.enable_pipelined_write)
        opts.set_use_fsync(self.use_fsync)
        if self.wal_dir is not None:
            opts.set_wal_dir(self.wal_dir)
        if self.db_log_dir is not None:
            opts.set_db_log_dir(self.db_log_dir)

        table_factory_options = rocksdict.BlockBasedOptions()
        table_factory_options.set_block_cache(rocksdict.Cache(self.block_cache_size))
        table_factory_options.set_bloom_filter(
            self.bloom_filter_bits_per_key, block_based=True
        )
        opts.set_block_based_table_factory(table_factory_options)
        compression_type = COMPRESSION_TYPES[self.compression_type]
        opts.set_compression_type(compression_type)
        opts.set_max_total_wal_size(size=self.max_total_wal_size)
        return opts
```

---

## 12. 磁碟目錄結構

```
{state_dir}/                          # 預設 "state"
└── {group_id}/                       # Kafka consumer group id
    └── {store_name}/                 # 例如 "default", "my-window"
        └── {stream_id}/             # topic name（或自定義 stream id）
            ├── 0/                   # Partition 0 的 RocksDB
            │   ├── MANIFEST-*
            │   ├── CURRENT
            │   ├── *.sst            # Sorted String Table
            │   ├── *.log            # Write-Ahead Log
            │   └── OPTIONS-*
            ├── 1/                   # Partition 1 的 RocksDB
            │   └── ...
            └── 5/                   # Partition 5 的 RocksDB
                └── ...
```

**每個 RocksDB 內的 Column Families**：

```
RocksDB (partition 0)
├── default              ← 主要 state 資料
├── __metadata__         ← changelog offset (b"__changelog_offset__")
├── __global-counter__   ← 全域序列號 (b"__global_counter__")
│
│  (Windowed store 額外有)
├── __latest-timestamps__
├── __expiration-index__
├── __deletion-index__
├── __value-deletion-index__
└── __values__
```

---

## 13. 端到端流程圖

### 一條訊息的 State 操作完整流程

```
Kafka Consumer poll()
  │
  ▼
Message: key=b"user-123", value=b'{"action":"click"}'
  │
  ▼
StreamingDataFrame callback:
  def process(value, state):
      count = state.get("click_count", 0)   ──────┐
      state.set("click_count", count + 1)   ──┐   │
      return value                             │   │
                                               │   │
  ┌────────────────────────────────────────────┘   │
  │                                                │
  ▼                                                ▼
TransactionState                             TransactionState
  prefix = serialize("user-123")               prefix = serialize("user-123")
  │                                            │
  ▼                                            ▼
PartitionTransaction.set(                  PartitionTransaction.get(
  key="click_count",                         key="click_count",
  value=1,                                   prefix=b'"user-123"',
  prefix=b'"user-123"'                     )
)                                            │
  │                                          ▼
  ▼                                        _get_bytes()
_set_bytes()                                 │
  │                                          ├─ Cache.get() → UNDEFINED
  ▼                                          │
Cache.set(                                   ▼
  key=b'"user-123"|"click_count"',         partition.get(
  value=serialize(1),                        key=b'"user-123"|"click_count"'
  prefix=b'"user-123"'                     )
)                                            │
  │                                          ▼
  ▼                                        RocksDB.get() → b'\x00'  (=0)
✓ 寫入 cache 完成                             │
                                             ▼
                                           deserialize(b'\x00') → 0
                                             │
                                             ▼
                                           return 0


  ════════ Checkpoint commit 時 ════════

  Step 2: transaction.prepare()
    │
    ▼
  _prepare():
    changelog_producer.produce(
      key=b'"user-123"|"click_count"',
      value=serialize(1),
      headers={
        "__column_family__": "default",
        "__processed_tp_offsets__": '{"my-topic": 42}'
      }
    )
    │
    ▼
  Kafka changelog topic: my-topic--default--changelog [partition 0]

  Step 5: transaction.flush()
    │
    ▼
  _flush():
    partition.write(cache, changelog_offset=7)
      │
      ▼
    WriteBatch:
      PUT b'"user-123"|"click_count"' = serialize(1)  [default CF]
      PUT b"__changelog_offset__" = int_to_bytes(7)   [__metadata__ CF]
      │
      ▼
    RocksDB.write(batch)  ← 原子性寫入磁碟
```

---

## 14. 關鍵源碼索引

| 元件 | 檔案 | 行數 | 說明 |
|------|------|------|------|
| `StateStoreManager` | `quixstreams/state/manager.py` | 32-339 | 全域 store 管理 |
| `StateStoreManager.on_partition_assign` | `quixstreams/state/manager.py` | 294-321 | Rebalance assign |
| `StateStoreManager.on_partition_revoke` | `quixstreams/state/manager.py` | 323-339 | Rebalance revoke |
| `StateStoreManager.get_store` | `quixstreams/state/manager.py` | 128-143 | 取得 store |
| `Store` (abstract) | `quixstreams/state/base/store.py` | 15-103 | 抽象 store |
| `Store.assign_partition` | `quixstreams/state/base/store.py` | 60-85 | 建立 partition |
| `Store.revoke_partition` | `quixstreams/state/base/store.py` | 87-103 | 關閉 partition |
| `StorePartition` (abstract) | `quixstreams/state/base/partition.py` | 21-116 | 抽象 partition |
| `RocksDBStore` | `quixstreams/state/rocksdb/store.py` | 18-66 | RocksDB store |
| `RocksDBStorePartition` | `quixstreams/state/rocksdb/partition.py` | 35-394 | RocksDB partition |
| `RocksDBStorePartition.write` | `quixstreams/state/rocksdb/partition.py` | 85-126 | Cache → RocksDB |
| `RocksDBStorePartition.recover_from_changelog_message` | `quixstreams/state/rocksdb/partition.py` | 71-83 | Recovery 寫入 |
| `RocksDBStorePartition.get` | `quixstreams/state/rocksdb/partition.py` | 135-150 | RocksDB 讀取 |
| `RocksDBStorePartition.iter_items` | `quixstreams/state/rocksdb/partition.py` | 152-199 | Range query |
| `RocksDBStorePartition.get_changelog_offset` | `quixstreams/state/rocksdb/partition.py` | 220-230 | 讀 changelog offset |
| `RocksDBStorePartition.close` | `quixstreams/state/rocksdb/partition.py` | 245-255 | 關閉 RocksDB |
| `MemoryStore` | `quixstreams/state/memory/store.py` | 14-47 | 記憶體 store |
| `MemoryStorePartition` | `quixstreams/state/memory/partition.py` | 35-148 | 記憶體 partition |
| `PartitionTransaction` | `quixstreams/state/base/transaction.py` | 195-591 | State 交易 |
| `PartitionTransaction.prepare` | `quixstreams/state/base/transaction.py` | 479-535 | Produce changelog |
| `PartitionTransaction.flush` | `quixstreams/state/base/transaction.py` | 537-583 | 寫入 store |
| `PartitionTransaction._get_bytes` | `quixstreams/state/base/transaction.py` | 373-389 | Cache-first 讀取 |
| `PartitionTransaction._set_bytes` | `quixstreams/state/base/transaction.py` | 424-438 | 寫入 cache |
| `PartitionTransaction.as_state` | `quixstreams/state/base/transaction.py` | 284-300 | 建立 State 介面 |
| `PartitionTransactionCache` | `quixstreams/state/base/transaction.py` | 53-157 | RYOW cache |
| `RocksDBPartitionTransaction` | `quixstreams/state/rocksdb/transaction.py` | 22-134 | RocksDB 特化交易 |
| `RocksDBPartitionTransaction._get_items` | `quixstreams/state/rocksdb/transaction.py` | 41-95 | 合併 DB+Cache |
| `RocksDBPartitionTransaction._increment_counter` | `quixstreams/state/rocksdb/transaction.py` | 108-125 | 全域計數器 |
| `State` (abstract) | `quixstreams/state/base/state.py` | 17-90 | 使用者介面 |
| `TransactionState` | `quixstreams/state/base/state.py` | 92-174 | Prefix 綁定實作 |
| `RocksDBOptions` | `quixstreams/state/rocksdb/options.py` | 25-88 | RocksDB 設定 |
| Serialization | `quixstreams/state/serialization.py` | 1-92 | 序列化工具 |
| Metadata 常數 | `quixstreams/state/metadata.py` | 1-16 | Header/CF 常數 |
| RocksDB Metadata | `quixstreams/state/rocksdb/metadata.py` | 1-5 | RocksDB key 常數 |
