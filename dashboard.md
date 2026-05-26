# Quix Streams Monitoring Dashboard - Implementation Plan

## Context

The quix-streams project needs a real-time monitoring dashboard to visualize pipeline DAGs, track throughput/lag, monitor resources, and surface errors. This implementation covers the full stack: Python metrics collectors integrated with the existing Application class, a FastAPI backend that consumes metrics from a Kafka topic, and a SvelteKit frontend.

**Transport**: Route A only (Kafka topic `__quix_metrics`), using the existing `InternalProducer`.
**Integration**: Method 1 (external wrapper via `MetricsAgent`), no modifications to quixstreams core.
**Location**: All new code lives in `/dashboard/` within this repo.

---

## Directory Structure

```
dashboard/
├── collector/                    # Python metrics SDK (pip-installable)
│   ├── pyproject.toml
│   ├── quix_metrics/
│   │   ├── __init__.py
│   │   ├── protocol.py          # MetricsCollector Protocol
│   │   ├── kafka_exporter.py    # KafkaMetricsExporter
│   │   ├── agent.py             # MetricsAgent (hooks into Application)
│   │   ├── dag_extractor.py     # Extract DAG from DataFrameRegistry
│   │   ├── throughput.py        # ThroughputTracker (checkpoint piggyback)
│   │   ├── errors.py            # ErrorInterceptor
│   │   ├── resources.py         # ResourceCollector (psutil)
│   │   └── broker_health.py     # BrokerHealthCollector
│   └── tests/
├── backend/                      # FastAPI backend service
│   ├── pyproject.toml
│   ├── app/
│   │   ├── __init__.py
│   │   ├── main.py              # FastAPI app + SSE endpoint
│   │   ├── aggregator.py        # MetricsAggregator (Kafka consumer + in-memory)
│   │   ├── models.py            # Pydantic models for API responses
│   │   └── config.py            # Settings (broker address, topic name, etc.)
│   └── tests/
└── frontend/                     # SvelteKit app
    ├── package.json
    ├── svelte.config.js
    ├── vite.config.ts
    ├── src/
    │   ├── routes/
    │   │   ├── +layout.svelte
    │   │   ├── +page.svelte          # Dashboard overview
    │   │   ├── dag/+page.svelte      # Full-screen DAG view
    │   │   ├── errors/+page.svelte   # Error list
    │   │   └── resources/+page.svelte # Resource monitoring
    │   ├── lib/
    │   │   ├── stores/
    │   │   │   └── metrics.ts        # SSE-backed Svelte store
    │   │   ├── components/
    │   │   │   ├── DagView.svelte    # @xyflow/svelte DAG renderer
    │   │   │   ├── ThroughputChart.svelte
    │   │   │   ├── LagTable.svelte
    │   │   │   ├── ResourceGauges.svelte
    │   │   │   ├── ErrorList.svelte
    │   │   │   └── BrokerStatus.svelte
    │   │   └── types.ts              # TypeScript interfaces
    │   └── app.html
    └── tailwind.config.js
```

---

## Phase 1: Python Metrics Collector (`dashboard/collector/`)

### 1.1 `protocol.py` — MetricsCollector Protocol

```python
class MetricsCollector(Protocol):
    def emit(self, envelope: dict) -> None: ...
    def flush(self) -> None: ...
```

### 1.2 `kafka_exporter.py` — KafkaMetricsExporter

- Wraps an existing `confluent_kafka.Producer` (or creates a lightweight one)
- `emit(envelope)`: JSON-serialize → `producer.produce("__quix_metrics", key=..., value=...)`
- Key format: `{consumer_group}.{type}` (for compaction)
- Fire-and-forget: errors logged but never block

### 1.3 `agent.py` — MetricsAgent

The main entry point. Hooks into `Application` via its public/semi-public attributes:

```python
class MetricsAgent:
    def __init__(self, app: Application, broker_address: str, consumer_group: str = None):
        # 1. Create KafkaMetricsExporter (reuse app's broker config)
        # 2. Hook _on_message_processed → ThroughputTracker
        # 3. Wrap error callbacks → ErrorInterceptor
        # 4. Monkey-patch app.run() to extract DAG at startup
        # 5. Start background timer for resource/offset/broker metrics
```

Key hooks (all verified in codebase):
- `app._on_message_processed` — instance attribute at `app.py:351`
- `app._on_processing_error` — instance attribute at `app.py:352`
- `app._dataframe_registry` — instance attribute at `app.py:390`
- `app._consumer` — instance attribute at `app.py:360`
- `app._state_manager` — instance attribute at `app.py:380`
- `app._producer` — InternalProducer at `app.py:364`

### 1.4 `dag_extractor.py`

Uses `registry._registry` (dict[str, Stream]) + `stream.full_tree()` (`stream.py:378`) to build DAG JSON.

### 1.5 `throughput.py` — ThroughputTracker

Lightweight per-message callback: only increments `dict[(topic, partition)] += ctx.size`. Flush triggered by a 10s timer (not per-message decision).

### 1.6 `errors.py` — ErrorInterceptor

Wraps each error callback. Accumulates counts + up to 3 distinct samples. Flushes every 10s.

### 1.7 `resources.py` — ResourceCollector

Background thread, collects every 30s: `psutil.cpu_percent()`, `Process().memory_info()`, state dir disk usage.

### 1.8 `broker_health.py` — BrokerHealthCollector

Reads `consumer._broker_states` and `consumer._broker_unavailable_since` every 30s.

---

## Phase 2: FastAPI Backend (`dashboard/backend/`)

### 2.1 `aggregator.py` — MetricsAggregator

- Background thread consuming `__quix_metrics` topic from `earliest`
- In-memory storage:
  - `dag_snapshot: dict | None`
  - `throughput_windows: dict` (sliding 1m/5m)
  - `offset_data: dict` (latest per partition)
  - `recent_errors: deque(maxlen=1000)`
  - `resource_history: deque(maxlen=100)`
  - `broker_health: dict`
- `get_snapshot() -> dict` for SSE

### 2.2 `main.py` — FastAPI App

Endpoints:
- `GET /api/stream` — SSE, pushes snapshot every 5s
- `GET /api/dag` — Latest DAG JSON
- `GET /api/offsets` — Current offset/lag data
- `GET /api/throughput` — Throughput windows
- `GET /api/errors` — Recent errors
- `GET /api/resources` — Resource history
- `GET /api/brokers` — Broker health

CORS middleware enabled for frontend dev server.

### 2.3 `config.py`

Pydantic Settings: `BROKER_ADDRESS`, `METRICS_TOPIC`, `SSE_INTERVAL`, `CONSUMER_GROUP`.

---

## Phase 3: SvelteKit Frontend (`dashboard/frontend/`)

### 3.1 Tech Stack

- SvelteKit (Svelte 5 with runes)
- `@xyflow/svelte` for DAG visualization
- `chart.js` + `svelte-chartjs` for time-series charts
- Tailwind CSS for styling
- Native `EventSource` API for SSE

### 3.2 Core Store (`lib/stores/metrics.ts`)

A reactive store backed by SSE:
```ts
// Uses $state rune + EventSource
// Auto-reconnects on disconnect
// Provides: dag, throughput, offsets, errors, resources, brokers
```

### 3.3 Pages

| Route | Content |
|-------|---------|
| `/` | Dashboard: mini DAG, throughput sparkline, lag table, resource bars, recent errors |
| `/dag` | Full-screen interactive DAG with @xyflow/svelte, heatmap coloring by throughput |
| `/errors` | Filterable error table with counts + samples |
| `/resources` | CPU/Memory/Disk charts over time |

### 3.4 Components

- **DagView.svelte**: Renders DAG nodes (stream operations) and edges using @xyflow/svelte with Dagre layout
- **ThroughputChart.svelte**: Line chart showing msg/s and bytes/s over time
- **LagTable.svelte**: Table showing per-partition lag with color coding
- **ResourceGauges.svelte**: Progress bars for CPU, memory, disk
- **ErrorList.svelte**: Timestamped error entries with type badges
- **BrokerStatus.svelte**: Broker connectivity indicators

---

## Implementation Order

1. **Collector** — `protocol.py` → `kafka_exporter.py` → `agent.py` → individual collectors
2. **Backend** — `config.py` → `aggregator.py` → `main.py`
3. **Frontend** — SvelteKit scaffold → SSE store → Dashboard page → DAG page → remaining pages

---

## Verification

1. **Collector**: Unit tests with a mock producer; integration test with a real Kafka broker producing to `__quix_metrics`
2. **Backend**: Start backend pointing at test Kafka, verify `/api/stream` SSE output with `curl`
3. **Frontend**: `npm run dev`, verify dashboard renders with live data from backend
4. **End-to-end**: Run a sample quix-streams app with `MetricsAgent` attached → backend consumes → frontend displays DAG + live metrics

---

## Key Files to Reuse

| What | File | Line |
|------|------|------|
| Stream topology | `quixstreams/core/stream/stream.py` | `full_tree()` at L378 |
| Registry | `quixstreams/dataframe/registry.py` | `_registry` at L23 |
| Message callback | `quixstreams/app.py` | `_on_message_processed` at L351, called at L1037 |
| Error callbacks | `quixstreams/error_callbacks.py` | Types at L6-10 |
| Broker states | `quixstreams/kafka/consumer.py` | `_broker_states` at L135, `_stats_cb` at L183 |
| Offsets | `quixstreams/kafka/consumer.py` | `get_watermark_offsets` at L486, `committed` at L468 |
| State dir | `quixstreams/state/manager.py` | `_state_dir` at L57 |
| Message size | `quixstreams/models/messagecontext.py` | `.size` at L48 |
| Producer | `quixstreams/internal_producer.py` | `produce()` at L181 |
| Topic naming | `quixstreams/models/topics/manager.py` | `_internal_name()` at L438 |
