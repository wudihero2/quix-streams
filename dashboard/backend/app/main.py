"""FastAPI application with SSE and REST endpoints for the monitoring dashboard."""

import asyncio
import json
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

from .aggregator import MetricsAggregator
from .config import settings
from .models import (
    BrokerHealthSnapshot,
    DagSnapshot,
    ErrorEntry,
    ResourceSnapshot,
    ThroughputSnapshot,
)

logger = logging.getLogger(__name__)

aggregator = MetricsAggregator()


@asynccontextmanager
async def lifespan(app: FastAPI):
    aggregator.start()
    yield
    aggregator.stop()


app = FastAPI(
    title="Quix Streams Dashboard",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


async def _sse_generator():
    """Generate SSE events with dashboard snapshots."""
    while True:
        snapshot = aggregator.get_snapshot()
        data = json.dumps(snapshot)
        yield f"data: {data}\n\n"
        await asyncio.sleep(settings.sse_interval)


@app.get("/api/stream")
async def stream_metrics():
    """SSE endpoint that pushes dashboard snapshots."""
    return StreamingResponse(
        _sse_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/api/dag", response_model=DagSnapshot)
async def get_dag():
    dag = aggregator.get_dag()
    if dag is None:
        return {"nodes": [], "edges": [], "topics": []}
    return dag


@app.get("/api/throughput", response_model=list[ThroughputSnapshot])
async def get_throughput():
    return aggregator.get_throughput()


@app.get("/api/errors", response_model=list[ErrorEntry])
async def get_errors():
    return aggregator.get_errors()


@app.get("/api/resources", response_model=list[ResourceSnapshot])
async def get_resources():
    return aggregator.get_resources()


@app.get("/api/brokers", response_model=BrokerHealthSnapshot)
async def get_brokers():
    health = aggregator.get_broker_health()
    if health is None:
        return {"brokers": {}, "all_brokers_up": False, "broker_count": 0}
    return health
