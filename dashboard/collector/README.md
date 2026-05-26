# quix-metrics

Metrics collector for the Quix Streams monitoring dashboard. Hooks into a running `Application` instance and periodically exports pipeline metrics (DAG topology, throughput, errors, resource usage, broker health) to a Kafka topic (`__quix_metrics`).

## Install

```bash
pip install -e dashboard/collector
```

Requires `confluent-kafka` and `psutil` (pulled in automatically).

## Quick Start

```python
from quixstreams import Application
from quix_metrics import MetricsAgent

app = Application(broker_address="localhost:9092", consumer_group="my-pipeline")
sdf = app.dataframe(app.topic("input"))
sdf = sdf.apply(lambda row: row)
sdf = sdf.to_topic(app.topic("output"))

# Attach the metrics agent — that's it
agent = MetricsAgent(app)

app.run()
```

`MetricsAgent` automatically:

1. Extracts the pipeline DAG when `app.run()` is called
2. Tracks per-partition message throughput via the `on_message_processed` hook
3. Intercepts processing errors and collects samples
4. Collects CPU / memory / disk usage in a background thread
5. Reports broker connectivity state

All metrics are written to the `__quix_metrics` Kafka topic as JSON.

## Configuration

```python
agent = MetricsAgent(
    app,
    broker_address="localhost:9092",  # default: reuses app's broker config
    consumer_group="my-pipeline",     # default: reuses app's consumer group
    flush_interval=10.0,              # seconds between throughput/error flushes
    resource_interval=30.0,           # seconds between resource snapshots
)
```

| Parameter            | Default                  | Description                                      |
|----------------------|--------------------------|--------------------------------------------------|
| `app`                | *(required)*             | A `quixstreams.Application` instance              |
| `broker_address`     | app's broker address     | Kafka bootstrap servers for the metrics producer  |
| `consumer_group`     | app's consumer group     | Used as a key prefix in metric envelopes          |
| `flush_interval`     | `10.0`                   | How often to flush throughput and error metrics    |
| `resource_interval`  | `30.0`                   | How often to collect CPU/memory/disk metrics       |

## Stopping

The agent runs daemon threads that stop automatically when the process exits. To stop explicitly:

```python
agent.stop()
```

## Metrics Topic Format

Each message on `__quix_metrics` is a JSON envelope:

```json
{
  "type": "throughput",
  "consumer_group": "my-pipeline",
  "timestamp": 1700000000.123,
  "data": { ... }
}
```

Message key format: `{consumer_group}.{type}` (e.g. `my-pipeline.throughput`).

### Envelope types

**`dag`** — emitted once at startup

```json
{
  "type": "dag",
  "data": {
    "nodes": [
      { "id": "topic:input", "label": "input", "type": "topic" },
      { "id": "stream:input:0", "label": "ApplyFunction: <lambda>", "type": "applyfunction" }
    ],
    "edges": [
      { "source": "topic:input", "target": "stream:input:0" }
    ],
    "topics": ["input"]
  }
}
```

**`throughput`** — emitted every `flush_interval` seconds

```json
{
  "type": "throughput",
  "data": {
    "total_messages": 150,
    "total_bytes": 48000,
    "partitions": {
      "input:0": { "topic": "input", "partition": 0, "message_count": 100, "byte_count": 32000 },
      "input:1": { "topic": "input", "partition": 1, "message_count": 50, "byte_count": 16000 }
    }
  }
}
```

**`errors`** — emitted every `flush_interval` seconds (only when errors exist)

```json
{
  "type": "errors",
  "data": [
    {
      "key": "processing:ValueError",
      "count": 3,
      "samples": [
        {
          "type": "ValueError",
          "message": "invalid literal",
          "traceback": ["Traceback ...\n", "..."],
          "context": { "topic": "input", "partition": 0, "offset": 42 },
          "timestamp": 1700000000.0
        }
      ]
    }
  ]
}
```

**`resources`** — emitted every `resource_interval` seconds

```json
{
  "type": "resources",
  "data": {
    "cpu_percent": 12.5,
    "memory_rss_bytes": 104857600,
    "memory_vms_bytes": 209715200,
    "state_disk_total_bytes": 500000000000,
    "state_disk_used_bytes": 200000000000,
    "state_disk_free_bytes": 300000000000,
    "state_dir_bytes": 1048576
  }
}
```

**`broker_health`** — emitted every `flush_interval` seconds

```json
{
  "type": "broker_health",
  "data": {
    "brokers": {
      "broker1:9092": { "state": "UP", "is_up": true },
      "broker2:9092": { "state": "DOWN", "is_up": false }
    },
    "all_brokers_up": false,
    "any_broker_unavailable_since": null,
    "broker_count": 2
  }
}
```

## Architecture

```
Application
    │
    ├── on_message_processed ──▶ ThroughputTracker
    ├── on_processing_error  ──▶ ErrorInterceptor
    ├── run() (patched)      ──▶ DAG extractor
    │
    └── MetricsAgent
         ├── flush thread (10s)    ──▶ KafkaMetricsExporter ──▶ __quix_metrics
         └── resource thread (30s) ──▶ KafkaMetricsExporter ──▶ __quix_metrics
```

## Running Tests

```bash
cd dashboard/collector
pip install -e ".[dev]"
pytest tests/ -v
```
