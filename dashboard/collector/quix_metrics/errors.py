"""Error interception and collection."""

import logging
import threading
import traceback
import time
from collections import defaultdict
from typing import Any, Optional


MAX_SAMPLES = 3


class ErrorInterceptor:
    """Wraps error callbacks, accumulates counts and sample tracebacks."""

    def __init__(self):
        self._lock = threading.Lock()
        self._counts: dict[str, int] = defaultdict(int)
        self._samples: dict[str, list[dict]] = defaultdict(list)

    def wrap_processing_error(self, original_callback):
        """Return a wrapper that intercepts processing errors."""

        def wrapper(exc, row, logger_inst):
            self._record_error("processing", exc, context={
                "topic": getattr(row, "topic", None),
                "partition": getattr(row, "partition", None),
                "offset": getattr(row, "offset", None),
            })
            if original_callback is not None:
                return original_callback(exc, row, logger_inst)
            return True

        return wrapper

    def _record_error(self, error_type: str, exc: Exception, context: Optional[dict] = None):
        key = f"{error_type}:{type(exc).__name__}"
        with self._lock:
            self._counts[key] += 1
            if len(self._samples[key]) < MAX_SAMPLES:
                self._samples[key].append({
                    "type": type(exc).__name__,
                    "message": str(exc),
                    "traceback": traceback.format_exception(type(exc), exc, exc.__traceback__),
                    "context": context or {},
                    "timestamp": time.time(),
                })

    def collect_and_reset(self) -> list[dict[str, Any]]:
        """Collect error data and reset counters."""
        with self._lock:
            errors = []
            for key, count in self._counts.items():
                errors.append({
                    "key": key,
                    "count": count,
                    "samples": list(self._samples.get(key, [])),
                })
            self._counts.clear()
            self._samples.clear()
        return errors
