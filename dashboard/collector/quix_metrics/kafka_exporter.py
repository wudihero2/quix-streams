import json
import logging
from typing import Optional

from confluent_kafka import Producer

logger = logging.getLogger(__name__)

METRICS_TOPIC = "__quix_metrics"


class KafkaMetricsExporter:
    """Exports metrics envelopes to a Kafka topic as JSON."""

    def __init__(
        self,
        broker_address: str,
        topic: str = METRICS_TOPIC,
        producer: Optional[Producer] = None,
    ):
        self._topic = topic
        if producer is not None:
            self._producer = producer
        else:
            self._producer = Producer({"bootstrap.servers": broker_address})

    def emit(self, envelope: dict) -> None:
        key = f"{envelope.get('consumer_group', 'unknown')}.{envelope.get('type', 'unknown')}"
        try:
            self._producer.produce(
                topic=self._topic,
                key=key.encode("utf-8"),
                value=json.dumps(envelope).encode("utf-8"),
                on_delivery=self._on_delivery,
            )
            self._producer.poll(0)
        except Exception:
            logger.debug("Failed to emit metrics envelope", exc_info=True)

    def flush(self) -> None:
        try:
            self._producer.flush(timeout=5.0)
        except Exception:
            logger.debug("Failed to flush metrics producer", exc_info=True)

    @staticmethod
    def _on_delivery(err, msg):
        if err is not None:
            logger.debug("Metrics delivery failed: %s", err)
