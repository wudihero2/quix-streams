"""Lightweight per-message throughput tracking."""

import threading
import time
from typing import Any


class ThroughputTracker:
    """Tracks message counts and byte sizes per topic-partition."""

    def __init__(self):
        self._lock = threading.Lock()
        self._counts: dict[tuple[str, int], int] = {}
        self._bytes: dict[tuple[str, int], int] = {}

    def on_message_processed(self, topic: str, partition: int, offset: int) -> None:
        """Called for each processed message. Increments counters."""
        key = (topic, partition)
        with self._lock:
            self._counts[key] = self._counts.get(key, 0) + 1

    def on_message_processed_with_size(
        self, topic: str, partition: int, offset: int, size: int
    ) -> None:
        """Called for each processed message with byte size."""
        key = (topic, partition)
        with self._lock:
            self._counts[key] = self._counts.get(key, 0) + 1
            self._bytes[key] = self._bytes.get(key, 0) + size

    def collect_and_reset(self) -> dict[str, Any]:
        """Collect current counters and reset them."""
        with self._lock:
            counts = dict(self._counts)
            bytes_map = dict(self._bytes)
            self._counts.clear()
            self._bytes.clear()

        partitions = {}
        for (topic, partition), count in counts.items():
            key = f"{topic}:{partition}"
            partitions[key] = {
                "topic": topic,
                "partition": partition,
                "message_count": count,
                "byte_count": bytes_map.get((topic, partition), 0),
            }

        total_messages = sum(counts.values())
        total_bytes = sum(bytes_map.values())

        return {
            "total_messages": total_messages,
            "total_bytes": total_bytes,
            "partitions": partitions,
        }
