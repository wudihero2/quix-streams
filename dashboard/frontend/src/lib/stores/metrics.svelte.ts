import type { DashboardSnapshot, DagSnapshot, ThroughputSnapshot, ErrorEntry, ResourceSnapshot, BrokerHealthSnapshot } from '$lib/types';

const API_BASE = 'http://localhost:8000';

class MetricsStore {
	dag: DagSnapshot | null = $state(null);
	throughput: ThroughputSnapshot[] = $state([]);
	errors: ErrorEntry[] = $state([]);
	resources: ResourceSnapshot[] = $state([]);
	broker_health: BrokerHealthSnapshot | null = $state(null);
	/** 'connecting' = waiting for backend, 'connected' = data flowing, 'disconnected' = was connected then lost */
	status: 'connecting' | 'connected' | 'disconnected' = $state('connecting');

	#eventSource: EventSource | null = null;
	#reconnectTimer: ReturnType<typeof setTimeout> | null = null;
	#hasConnectedOnce = false;

	get connected() {
		return this.status === 'connected';
	}

	connect() {
		if (this.#eventSource) return;

		if (!this.#hasConnectedOnce) {
			this.status = 'connecting';
		}
		this.#eventSource = new EventSource(`${API_BASE}/api/stream`);

		this.#eventSource.onopen = () => {
			this.#hasConnectedOnce = true;
			this.status = 'connected';
		};

		this.#eventSource.onmessage = (event) => {
			try {
				const snapshot: DashboardSnapshot = JSON.parse(event.data);
				this.dag = snapshot.dag;
				this.throughput = snapshot.throughput;
				this.errors = snapshot.errors;
				this.resources = snapshot.resources;
				this.broker_health = snapshot.broker_health;
				if (!this.#hasConnectedOnce) {
					this.#hasConnectedOnce = true;
				}
				this.status = 'connected';
			} catch {
				// ignore parse errors
			}
		};

		this.#eventSource.onerror = () => {
			// If we never connected, keep showing "Connecting..." instead of "Disconnected"
			this.status = this.#hasConnectedOnce ? 'disconnected' : 'connecting';
			this.#eventSource?.close();
			this.#eventSource = null;
			// Auto-reconnect after 3 seconds
			this.#reconnectTimer = setTimeout(() => this.connect(), 3000);
		};
	}

	disconnect() {
		if (this.#reconnectTimer) {
			clearTimeout(this.#reconnectTimer);
			this.#reconnectTimer = null;
		}
		if (this.#eventSource) {
			this.#eventSource.close();
			this.#eventSource = null;
		}
		this.status = 'disconnected';
	}
}

export const metrics = new MetricsStore();
