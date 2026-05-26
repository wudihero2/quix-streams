from typing import Any, Optional
from pydantic import BaseModel


class DagNode(BaseModel):
    id: str
    label: str
    type: str


class DagEdge(BaseModel):
    source: str
    target: str


class DagSnapshot(BaseModel):
    nodes: list[DagNode]
    edges: list[DagEdge]
    topics: list[str]


class PartitionThroughput(BaseModel):
    topic: str
    partition: int
    message_count: int
    byte_count: int


class ThroughputSnapshot(BaseModel):
    timestamp: float
    total_messages: int
    total_bytes: int
    partitions: dict[str, PartitionThroughput]


class ErrorSample(BaseModel):
    type: str
    message: str
    traceback: list[str]
    context: dict[str, Any]
    timestamp: float


class ErrorEntry(BaseModel):
    key: str
    count: int
    samples: list[ErrorSample]
    first_seen: float
    last_seen: float


class ResourceSnapshot(BaseModel):
    timestamp: float
    cpu_percent: float
    memory_rss_bytes: int
    memory_vms_bytes: int
    state_disk_total_bytes: Optional[int] = None
    state_disk_used_bytes: Optional[int] = None
    state_disk_free_bytes: Optional[int] = None
    state_dir_bytes: Optional[int] = None


class BrokerInfo(BaseModel):
    state: str
    is_up: bool


class BrokerHealthSnapshot(BaseModel):
    brokers: dict[str, BrokerInfo]
    all_brokers_up: bool
    any_broker_unavailable_since: Optional[float] = None
    broker_count: int


class DashboardSnapshot(BaseModel):
    dag: Optional[DagSnapshot] = None
    throughput: list[ThroughputSnapshot] = []
    errors: list[ErrorEntry] = []
    resources: list[ResourceSnapshot] = []
    broker_health: Optional[BrokerHealthSnapshot] = None
