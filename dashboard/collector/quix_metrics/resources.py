"""System resource collection via psutil."""

import os
import shutil
import time
from pathlib import Path
from typing import Any, Optional

import psutil

STATE_DIR_CACHE_TTL = 300.0


class ResourceCollector:
    """Collects CPU, memory, and disk usage metrics."""

    def __init__(self, state_dir: Optional[Path] = None):
        self._process = psutil.Process(os.getpid())
        self._state_dir = state_dir
        self._state_dir_bytes_cache: Optional[int] = None
        self._state_dir_bytes_cached_at: float = 0.0
        self._process.cpu_percent()

    def collect(self) -> dict[str, Any]:
        cpu_percent = self._process.cpu_percent()
        mem_info = self._process.memory_info()

        result = {
            "cpu_percent": cpu_percent,
            "memory_rss_bytes": mem_info.rss,
            "memory_vms_bytes": mem_info.vms,
        }

        if self._state_dir and self._state_dir.exists():
            try:
                usage = shutil.disk_usage(self._state_dir)
                result["state_disk_total_bytes"] = usage.total
                result["state_disk_used_bytes"] = usage.used
                result["state_disk_free_bytes"] = usage.free

                now = time.monotonic()
                if now - self._state_dir_bytes_cached_at > STATE_DIR_CACHE_TTL:
                    self._state_dir_bytes_cache = sum(
                        f.stat().st_size
                        for f in self._state_dir.rglob("*")
                        if f.is_file()
                    )
                    self._state_dir_bytes_cached_at = now
                result["state_dir_bytes"] = self._state_dir_bytes_cache
            except OSError:
                pass

        return result
