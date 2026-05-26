"""
Demo pipeline: generates fake sensor data, processes it with Quix Streams,
and exports metrics via MetricsAgent.

Usage:
    pip install -e . -e dashboard/collector
    python dashboard/demo_pipeline.py
"""

import logging
import math
import os
import random
import time

from quixstreams import Application
from quixstreams.sources import Source

from quix_metrics import MetricsAgent

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 1) Source: generates fake IoT sensor readings continuously
# ---------------------------------------------------------------------------
class SensorSource(Source):
    """Generates temperature/humidity sensor events in a loop."""

    _sensors = ["sensor-A", "sensor-B", "sensor-C", "sensor-D"]

    def __init__(self):
        super().__init__(name="raw-sensors")

    def run(self):
        i = 0
        # Cycle through phases: burst → slow → medium → burst ...
        # Each phase lasts 10-30 seconds with different throughput rates
        phase_start = time.time()
        phase_duration = random.uniform(10, 30)
        phase_delay = self._pick_phase_delay()

        while self.running:
            sensor_id = self._sensors[i % len(self._sensors)]
            value = {
                "sensor_id": sensor_id,
                "temperature": round(20 + 10 * math.sin(i / 10) + random.gauss(0, 1), 2),
                "humidity": round(50 + 20 * math.cos(i / 15) + random.gauss(0, 2), 2),
                "ts": time.time(),
            }
            event = self.serialize(key=sensor_id, value=value)
            self.produce(key=event.key, value=event.value)
            i += 1

            # Switch to a new phase after duration expires
            if time.time() - phase_start > phase_duration:
                phase_start = time.time()
                phase_duration = random.uniform(10, 30)
                phase_delay = self._pick_phase_delay()

            # Add jitter within the phase
            jitter = random.uniform(0.8, 1.2)
            time.sleep(phase_delay * jitter)

    @staticmethod
    def _pick_phase_delay():
        """Pick a random delay per message for the current phase."""
        phase = random.choice(["burst", "fast", "medium", "slow", "crawl"])
        delays = {
            "burst": 0.02,   # ~50 msg/s
            "fast": 0.1,     # ~10 msg/s
            "medium": 0.3,   # ~3 msg/s
            "slow": 0.8,     # ~1.2 msg/s
            "crawl": 2.0,    # ~0.5 msg/s
        }
        return delays[phase]


# ---------------------------------------------------------------------------
# 2) Processing functions
# ---------------------------------------------------------------------------
def celsius_to_fahrenheit(row):
    row["temperature_f"] = round(row["temperature"] * 9 / 5 + 32, 2)
    return row


def is_high_temperature(row):
    return row["temperature"] > 25


def tag_alert_level(row):
    t = row["temperature"]
    if t > 28:
        row["alert"] = "critical"
    elif t > 25:
        row["alert"] = "warning"
    else:
        row["alert"] = "normal"
    return row


# ---------------------------------------------------------------------------
# 3) Main
# ---------------------------------------------------------------------------
def main():
    app = Application(
        broker_address=os.getenv("BROKER_ADDRESS", "localhost:9092"),
        consumer_group="sensor-pipeline",
        auto_offset_reset="earliest",
    )

    # Output topics
    alerts_topic = app.topic(name="sensor-alerts")
    all_readings_topic = app.topic(name="sensor-readings-enriched")

    # Build pipeline
    sdf = app.dataframe(source=SensorSource())

    # Branch 1: enrich all readings
    sdf = sdf.apply(celsius_to_fahrenheit)
    sdf = sdf.apply(tag_alert_level)
    sdf.print()
    sdf.to_topic(all_readings_topic)

    # Attach the MetricsAgent — this is the key line!
    agent = MetricsAgent(app)

    logger.info("Starting sensor pipeline with MetricsAgent...")
    app.run()


if __name__ == "__main__":
    main()
