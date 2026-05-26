"""Broker health collection from consumer stats."""

import logging
from typing import Any

logger = logging.getLogger(__name__)


class BrokerHealthCollector:
    """Reads broker connectivity state from the consumer."""

    def __init__(self):
        self._warned = False

    def collect(self, consumer) -> dict[str, Any]:
        if not hasattr(consumer, "_broker_states") and not self._warned:
            logger.warning("Consumer missing _broker_states attribute; broker health metrics will be empty")
            self._warned = True
        broker_states = getattr(consumer, "_broker_states", {})
        unavailable_since = getattr(consumer, "_broker_unavailable_since", None)

        brokers = {}
        for broker_name, state in broker_states.items():
            brokers[broker_name] = {
                "state": state,
                "is_up": state == "UP",
            }

        all_up = all(b["is_up"] for b in brokers.values()) if brokers else False

        return {
            "brokers": brokers,
            "all_brokers_up": all_up,
            "any_broker_unavailable_since": unavailable_since,
            "broker_count": len(brokers),
        }
