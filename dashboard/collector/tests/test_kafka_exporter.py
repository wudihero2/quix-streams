import json
from unittest.mock import MagicMock

from quix_metrics.kafka_exporter import KafkaMetricsExporter


def test_emit_produces_message():
    mock_producer = MagicMock()
    exporter = KafkaMetricsExporter(
        broker_address="localhost:9092", producer=mock_producer
    )

    envelope = {
        "type": "throughput",
        "consumer_group": "test-group",
        "timestamp": 1234567890.0,
        "data": {"total_messages": 10},
    }
    exporter.emit(envelope)

    mock_producer.produce.assert_called_once()
    call_kwargs = mock_producer.produce.call_args
    assert call_kwargs.kwargs["topic"] == "__quix_metrics"
    assert call_kwargs.kwargs["key"] == b"test-group.throughput"
    payload = json.loads(call_kwargs.kwargs["value"])
    assert payload["type"] == "throughput"
    assert payload["data"]["total_messages"] == 10


def test_emit_handles_producer_error():
    mock_producer = MagicMock()
    mock_producer.produce.side_effect = Exception("Kafka down")
    exporter = KafkaMetricsExporter(
        broker_address="localhost:9092", producer=mock_producer
    )

    # Should not raise
    exporter.emit({"type": "test", "consumer_group": "g"})


def test_flush():
    mock_producer = MagicMock()
    exporter = KafkaMetricsExporter(
        broker_address="localhost:9092", producer=mock_producer
    )
    exporter.flush()
    mock_producer.flush.assert_called_once_with(timeout=5.0)
