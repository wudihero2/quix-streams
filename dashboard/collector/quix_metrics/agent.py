"""MetricsAgent: hooks into a quix-streams Application to collect and export metrics."""

import logging
import threading
import time
from typing import Optional

from .broker_health import BrokerHealthCollector
from .dag_extractor import extract_dag
from .errors import ErrorInterceptor
from .kafka_exporter import KafkaMetricsExporter
from .resources import ResourceCollector
from .throughput import ThroughputTracker

logger = logging.getLogger(__name__)

DEFAULT_FLUSH_INTERVAL = 10.0
DEFAULT_RESOURCE_INTERVAL = 30.0


class MetricsAgent:
    """
    External wrapper that hooks into a quix-streams Application to
    collect and export metrics to a Kafka topic.

    Usage:
        app = Application(broker_address="localhost:9092")
        agent = MetricsAgent(app)
        # agent hooks are installed; just run the app normally
        app.run()
    """

    def __init__(
        self,
        app,
        broker_address: Optional[str] = None,
        consumer_group: Optional[str] = None,
        flush_interval: float = DEFAULT_FLUSH_INTERVAL,
        resource_interval: float = DEFAULT_RESOURCE_INTERVAL,
    ):
        self._app = app
        self._consumer_group = consumer_group or app._config.consumer_group
        self._flush_interval = flush_interval
        self._resource_interval = resource_interval
        self._stop_event = threading.Event()

        # Resolve broker address
        broker_addr = broker_address or str(
            app._config.broker_address.as_librdkafka_dict().get(
                "bootstrap.servers", "localhost:9092"
            )
        )

        # Create exporter
        self._exporter = KafkaMetricsExporter(broker_address=broker_addr)

        # Create collectors
        self._throughput = ThroughputTracker()
        self._errors = ErrorInterceptor()
        state_dir = getattr(app._state_manager, "_state_dir", None)
        if state_dir is None:
            logger.warning("Could not access app._state_manager._state_dir; state dir metrics will be unavailable")
        self._resources = ResourceCollector(state_dir=state_dir)
        self._broker_health = BrokerHealthCollector()

        # Install hooks
        self._install_hooks()

        # Start background flush thread
        self._flush_thread = threading.Thread(
            target=self._flush_loop, daemon=True, name="quix-metrics-flush"
        )
        self._flush_thread.start()

        # Start background resource thread
        self._resource_thread = threading.Thread(
            target=self._resource_loop, daemon=True, name="quix-metrics-resources"
        )
        self._resource_thread.start()

        logger.info(
            "MetricsAgent started for consumer group '%s'", self._consumer_group
        )

    def _install_hooks(self):
        """Hook into Application callbacks."""
        app = self._app

        # Hook on_message_processed
        original_on_processed = app._on_message_processed

        def on_message_processed(topic, partition, offset):
            self._throughput.on_message_processed(topic, partition, offset)
            if original_on_processed is not None:
                original_on_processed(topic, partition, offset)

        app._on_message_processed = on_message_processed

        # Hook on_processing_error
        original_on_error = app._on_processing_error
        app._on_processing_error = self._errors.wrap_processing_error(original_on_error)

        # Monkey-patch app.run to emit DAG at startup
        original_run = app.run

        def patched_run(*args, **kwargs):
            self._emit_dag()
            return original_run(*args, **kwargs)

        app.run = patched_run

    def _emit_dag(self):
        """Extract and emit the DAG topology."""
        try:
            dag = extract_dag(self._app._dataframe_registry)
            self._exporter.emit({
                "type": "dag",
                "consumer_group": self._consumer_group,
                "timestamp": time.time(),
                "data": dag,
            })
        except Exception:
            logger.debug("Failed to extract/emit DAG", exc_info=True)

    def _flush_loop(self):
        """Periodically flush throughput and error metrics."""
        while not self._stop_event.wait(self._flush_interval):
            try:
                self._flush_throughput()
                self._flush_errors()
                self._flush_broker_health()
            except Exception:
                logger.debug("Error in metrics flush loop", exc_info=True)

    def _resource_loop(self):
        """Periodically collect resource metrics."""
        while not self._stop_event.wait(self._resource_interval):
            try:
                self._flush_resources()
            except Exception:
                logger.debug("Error in resource collection loop", exc_info=True)

    def _flush_throughput(self):
        data = self._throughput.collect_and_reset()
        if data["total_messages"] > 0:
            self._exporter.emit({
                "type": "throughput",
                "consumer_group": self._consumer_group,
                "timestamp": time.time(),
                "data": data,
            })

    def _flush_errors(self):
        errors = self._errors.collect_and_reset()
        if errors:
            self._exporter.emit({
                "type": "errors",
                "consumer_group": self._consumer_group,
                "timestamp": time.time(),
                "data": errors,
            })

    def _flush_resources(self):
        data = self._resources.collect()
        self._exporter.emit({
            "type": "resources",
            "consumer_group": self._consumer_group,
            "timestamp": time.time(),
            "data": data,
        })

    def _flush_broker_health(self):
        consumer = getattr(self._app, "_consumer", None)
        if consumer is not None:
            data = self._broker_health.collect(consumer)
            self._exporter.emit({
                "type": "broker_health",
                "consumer_group": self._consumer_group,
                "timestamp": time.time(),
                "data": data,
            })

    def stop(self):
        """Stop the metrics agent."""
        self._stop_event.set()
        self._flush_thread.join(timeout=5)
        self._resource_thread.join(timeout=5)
        self._exporter.flush()
        logger.info("MetricsAgent stopped")
