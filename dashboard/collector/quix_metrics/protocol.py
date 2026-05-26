from typing import Protocol, runtime_checkable


@runtime_checkable
class MetricsCollector(Protocol):
    """Protocol for metrics exporters."""

    def emit(self, envelope: dict) -> None:
        """Emit a single metrics envelope."""
        ...

    def flush(self) -> None:
        """Flush any buffered metrics."""
        ...
