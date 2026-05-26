# Quix Streams Monitoring Dashboard

Real-time monitoring dashboard for Quix Streams pipelines. Visualizes pipeline DAGs, tracks throughput/lag, monitors system resources, and surfaces processing errors.

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌───────────────┐     ┌──────────────┐
│  Your Pipeline  │     │  __quix_metrics   │     │  FastAPI      │     │  SvelteKit   │
│  + MetricsAgent │────▶│  (Kafka topic)    │────▶│  Backend      │────▶│  Frontend    │
│                 │     │                   │     │  :8000        │ SSE │  :5173       │
└─────────────────┘     └───────────────────┘     └───────────────┘     └──────────────┘
```

## Quick Start (one command)

```bash
cd dashboard
./run.sh
```

This will:

1. Start Kafka via Docker Compose
2. Create a Python venv and install all dependencies
3. Launch a demo pipeline (fake sensor data)
4. Start the FastAPI backend on http://localhost:8000
5. Start the SvelteKit frontend on http://localhost:5173

Press `Ctrl+C` to stop everything.

## Manual Setup (step by step)

### Prerequisites

- Docker (for Kafka)
- Python 3.9+
- Node.js 18+

### Step 1: Start Kafka

```bash
cd dashboard
docker compose up -d
```

Wait for Kafka to be ready:

```bash
docker exec broker kafka-broker-api-versions --bootstrap-server localhost:9092
```

### Step 2: Install Python packages

```bash
python3 -m venv .venv
source .venv/bin/activate

# Install quix-streams from the repo root
pip install -e ..

# Install the metrics collector
pip install -e collector

# Install the backend
pip install -e backend
```

### Step 3: Run the demo pipeline

```bash
python demo_pipeline.py
```

This starts a pipeline that:
- Generates fake IoT sensor readings (temperature, humidity) every 0.5s
- Converts Celsius to Fahrenheit
- Tags alert levels (normal / warning / critical)
- Writes enriched data to `sensor-readings-enriched` topic
- Exports metrics (DAG, throughput, errors, resources, broker health) to `__quix_metrics`

### Step 4: Start the backend

In a new terminal:

```bash
source .venv/bin/activate
cd backend
uvicorn app.main:app --reload --port 8000
```

Verify it's working:

```bash
# SSE stream
curl -N http://localhost:8000/api/stream

# REST endpoints
curl http://localhost:8000/api/dag | python3 -m json.tool
curl http://localhost:8000/api/throughput | python3 -m json.tool
curl http://localhost:8000/api/brokers | python3 -m json.tool
```

### Step 5: Start the frontend

In another terminal:

```bash
cd frontend
npm install
npm run dev
```

Open http://localhost:5173.

## Use with Your Own Pipeline

Add two lines to any existing Quix Streams application:

```python
from quixstreams import Application
from quix_metrics import MetricsAgent   # <-- add this

app = Application(broker_address="localhost:9092", consumer_group="my-app")

# ... define your topics and dataframes as usual ...

agent = MetricsAgent(app)               # <-- add this
app.run()
```

That's it. The `MetricsAgent` hooks into the app automatically. Then start the backend and frontend to see live metrics.

## Project Structure

```
dashboard/
├── run.sh                  # One-command launcher
├── docker-compose.yml      # Kafka (KRaft mode, single node)
├── demo_pipeline.py        # Demo: sensor data → Quix Streams → metrics
│
├── collector/              # Python metrics SDK
│   ├── quix_metrics/       # MetricsAgent, exporters, collectors
│   └── README.md
│
├── backend/                # FastAPI backend
│   ├── app/                # SSE + REST endpoints, Kafka aggregator
│   └── README.md
│
└── frontend/               # SvelteKit app
    ├── src/
    │   ├── routes/         # Pages: /, /dag, /errors, /resources
    │   └── lib/            # SSE store, components, types
    └── README.md
```

## Stopping & Cleanup

```bash
# If using run.sh, just Ctrl+C

# Or manually:
docker compose down          # stop Kafka
rm -rf .venv                 # remove Python venv
rm -rf frontend/node_modules # remove Node modules
```

## Configuration

### Backend environment variables

| Variable                     | Default                  | Description                     |
|------------------------------|--------------------------|---------------------------------|
| `DASHBOARD_BROKER_ADDRESS`   | `localhost:9092`         | Kafka bootstrap servers         |
| `DASHBOARD_METRICS_TOPIC`    | `__quix_metrics`         | Metrics topic name              |
| `DASHBOARD_CONSUMER_GROUP`   | `quix-dashboard-backend` | Backend consumer group          |
| `DASHBOARD_SSE_INTERVAL`     | `5.0`                    | Seconds between SSE pushes      |

### MetricsAgent parameters

| Parameter          | Default              | Description                        |
|--------------------|----------------------|------------------------------------|
| `broker_address`   | app's broker address | Kafka bootstrap servers            |
| `consumer_group`   | app's consumer group | Key prefix in metric envelopes     |
| `flush_interval`   | `10.0`               | Throughput/error flush interval (s) |
| `resource_interval`| `30.0`               | Resource collection interval (s)    |
