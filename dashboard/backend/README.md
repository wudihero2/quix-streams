# quix-dashboard-backend

FastAPI backend for the Quix Streams monitoring dashboard. Consumes metrics from the `__quix_metrics` Kafka topic and exposes them via SSE and REST endpoints.

## Install

```bash
cd dashboard/backend
pip install -e .
```

## Quick Start

```bash
# Start with default settings (Kafka at localhost:9092)
uvicorn app.main:app --reload --port 8000

# Or specify a custom broker address
DASHBOARD_BROKER_ADDRESS=kafka:9092 uvicorn app.main:app --port 8000
```

The server starts a background Kafka consumer that reads from `__quix_metrics` (from `earliest`) and aggregates data in memory.

## Configuration

All settings are configured via environment variables with the `DASHBOARD_` prefix:

| Environment Variable         | Default                    | Description                                |
|------------------------------|----------------------------|--------------------------------------------|
| `DASHBOARD_BROKER_ADDRESS`   | `localhost:9092`           | Kafka bootstrap servers                    |
| `DASHBOARD_METRICS_TOPIC`    | `__quix_metrics`           | Kafka topic to consume metrics from        |
| `DASHBOARD_CONSUMER_GROUP`   | `quix-dashboard-backend`   | Consumer group for the backend consumer    |
| `DASHBOARD_SSE_INTERVAL`     | `5.0`                      | Seconds between SSE pushes                 |

## API Endpoints

### SSE (Server-Sent Events)

**`GET /api/stream`** — Live dashboard feed

Pushes a full snapshot every `SSE_INTERVAL` seconds. Each SSE message is a JSON object containing `dag`, `throughput`, `errors`, `resources`, and `broker_health`.

```bash
curl -N http://localhost:8000/api/stream
```

### REST

| Method | Endpoint            | Description                        |
|--------|---------------------|------------------------------------|
| GET    | `/api/dag`          | Latest pipeline DAG topology       |
| GET    | `/api/throughput`   | Throughput time-series (last ~10m) |
| GET    | `/api/errors`       | Recent errors (last 1000)          |
| GET    | `/api/resources`    | Resource usage history (last 100)  |
| GET    | `/api/brokers`      | Broker health status               |

### Example

```bash
# Get pipeline DAG
curl http://localhost:8000/api/dag | python3 -m json.tool

# Get current throughput
curl http://localhost:8000/api/throughput | python3 -m json.tool

# Get broker health
curl http://localhost:8000/api/brokers | python3 -m json.tool
```

## Architecture

```
__quix_metrics (Kafka topic)
        │
        ▼
  MetricsAggregator (background thread)
        │
        ├── dag_snapshot         (latest DAG, replaced on each update)
        ├── throughput_windows   (deque, last 60 snapshots)
        ├── recent_errors        (deque, last 1000 entries)
        ├── resource_history     (deque, last 100 snapshots)
        └── broker_health        (latest state)
        │
        ▼
  FastAPI endpoints
        ├── GET /api/stream      (SSE, pushes every 5s)
        ├── GET /api/dag
        ├── GET /api/throughput
        ├── GET /api/errors
        ├── GET /api/resources
        └── GET /api/brokers
```

## Interactive API Docs

FastAPI auto-generates OpenAPI docs:

- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## Running Tests

```bash
cd dashboard/backend
pip install -e ".[dev]"
pytest tests/ -v
```
