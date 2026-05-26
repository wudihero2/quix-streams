export interface DagNode {
	id: string;
	label: string;
	type: string;
}

export interface DagEdge {
	source: string;
	target: string;
}

export interface DagSnapshot {
	nodes: DagNode[];
	edges: DagEdge[];
	topics: string[];
}

export interface PartitionThroughput {
	topic: string;
	partition: number;
	message_count: number;
	byte_count: number;
}

export interface ThroughputSnapshot {
	timestamp: number;
	total_messages: number;
	total_bytes: number;
	partitions: Record<string, PartitionThroughput>;
}

export interface ErrorSample {
	type: string;
	message: string;
	traceback: string[];
	context: Record<string, unknown>;
	timestamp: number;
}

export interface ErrorEntry {
	key: string;
	count: number;
	samples: ErrorSample[];
	first_seen: number;
	last_seen: number;
}

export interface ResourceSnapshot {
	timestamp: number;
	cpu_percent: number;
	memory_rss_bytes: number;
	memory_vms_bytes: number;
	state_disk_total_bytes?: number;
	state_disk_used_bytes?: number;
	state_disk_free_bytes?: number;
	state_dir_bytes?: number;
}

export interface BrokerInfo {
	state: string;
	is_up: boolean;
}

export interface BrokerHealthSnapshot {
	brokers: Record<string, BrokerInfo>;
	all_brokers_up: boolean;
	any_broker_unavailable_since: number | null;
	broker_count: number;
}

export interface DashboardSnapshot {
	dag: DagSnapshot | null;
	throughput: ThroughputSnapshot[];
	errors: ErrorEntry[];
	resources: ResourceSnapshot[];
	broker_health: BrokerHealthSnapshot | null;
}
