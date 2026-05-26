"""Consumes metrics from Kafka and aggregates them in memory."""

import json
import logging
import threading
import time
from collections import deque
from typing import Any, Optional

from confluent_kafka import Consumer, KafkaError

from .config import settings

logger = logging.getLogger(__name__)

THROUGHPUT_WINDOW_SIZE = 60  # Keep last 60 snapshots (~10min at 10s intervals)
RESOURCE_HISTORY_SIZE = 100
ERROR_HISTORY_SIZE = 1000


class MetricsAggregator:
    """Background Kafka consumer that aggregates metrics in memory."""

    def __init__(self):
        self._dag_snapshot: Optional[dict] = None
        self._throughput_windows: deque[dict] = deque(maxlen=THROUGHPUT_WINDOW_SIZE)
        self._errors_by_key: dict[str, dict] = {}
        self._resource_history: deque[dict] = deque(maxlen=RESOURCE_HISTORY_SIZE)
        self._broker_health: Optional[dict] = None
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._consumer_thread: Optional[threading.Thread] = None

    def start(self):
        """Start the background consumer thread."""
        self._consumer_thread = threading.Thread(
            target=self._consume_loop, daemon=True, name="metrics-aggregator"
        )
        self._consumer_thread.start()
        logger.info("MetricsAggregator started")

    def stop(self):
        """Stop the background consumer thread."""
        self._stop_event.set()
        if self._consumer_thread:
            self._consumer_thread.join(timeout=10)
        logger.info("MetricsAggregator stopped")

    def _consume_loop(self):
        consumer = Consumer({
            "bootstrap.servers": settings.broker_address,
            "group.id": settings.consumer_group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        })
        consumer.subscribe([settings.metrics_topic])

        try:
            while not self._stop_event.is_set():
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.warning("Consumer error: %s", msg.error())
                    continue

                try:
                    envelope = json.loads(msg.value().decode("utf-8"))
                    self._process_envelope(envelope)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    logger.debug("Failed to decode metrics message", exc_info=True)
        finally:
            consumer.close()

    def _process_envelope(self, envelope: dict):
        msg_type = envelope.get("type")
        data = envelope.get("data", {})
        timestamp = envelope.get("timestamp", time.time())

        with self._lock:
            if msg_type == "dag":
                self._dag_snapshot = data
            elif msg_type == "throughput":
                self._throughput_windows.append({
                    "timestamp": timestamp,
                    **data,
                })
            elif msg_type == "errors":
                for error in data:
                    key = error.get("key", "unknown")
                    first_seen = error.get("samples", [{}])[0].get("timestamp", timestamp)
                    last_seen = error.get("samples", [{}])[-1].get("timestamp", timestamp)
                    if key in self._errors_by_key:
                        existing = self._errors_by_key[key]
                        existing["count"] += error.get("count", 0)
                        existing["first_seen"] = min(existing["first_seen"], first_seen)
                        existing["last_seen"] = max(existing["last_seen"], last_seen)
                        samples = existing.get("samples", [])
                        samples.extend(error.get("samples", []))
                        existing["samples"] = samples[-3:]
                    else:
                        error["first_seen"] = first_seen
                        error["last_seen"] = last_seen
                        self._errors_by_key[key] = error
                    if len(self._errors_by_key) > ERROR_HISTORY_SIZE:
                        oldest_key = min(self._errors_by_key, key=lambda k: self._errors_by_key[k]["last_seen"])
                        del self._errors_by_key[oldest_key]
            elif msg_type == "resources":
                self._resource_history.append({
                    "timestamp": timestamp,
                    **data,
                })
            elif msg_type == "broker_health":
                self._broker_health = data

    def get_dag(self) -> Optional[dict]:
        with self._lock:
            return self._dag_snapshot

    def get_throughput(self) -> list[dict]:
        with self._lock:
            return list(self._throughput_windows)

    def get_errors(self) -> list[dict]:
        with self._lock:
            return list(self._errors_by_key.values())

    def get_resources(self) -> list[dict]:
        with self._lock:
            return list(self._resource_history)

    def get_broker_health(self) -> Optional[dict]:
        with self._lock:
            return self._broker_health

    def get_snapshot(self) -> dict:
        """Get a full snapshot of all metrics for SSE."""
        with self._lock:
            return {
                "dag": self._dag_snapshot,
                "throughput": list(self._throughput_windows),
                "errors": list(self._errors_by_key.values())[-50:],
                "resources": list(self._resource_history)[-20:],  # Last 20 for SSE
                "broker_health": self._broker_health,
            }
