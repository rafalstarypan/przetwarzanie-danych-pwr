"""Wspólne typy i stałe dla modułów akwizycji."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


HDFS_ROOT_RAW = "/raw"
HDFS_ROOT_META = "/meta"
HDFS_ROOT_LOGS = "/logs"
HDFS_MANIFEST_PATH = "/meta/manifest.json"

MONTHS_YYYYMM = [
    "202503", "202504", "202505", "202506", "202507",
    "202508", "202509", "202510", "202511",
]
# Citi Bike: pełne 8 miesięcy zgodnie z zakresem projektu (marzec–listopad 2025).
# Dla NOAA / Events korzystamy z tej samej listy (9 elementów, listopad włącznie).


@dataclass
class SourceResult:
    source: str
    new_files: int = 0
    skipped_files: int = 0
    bytes_pulled: int = 0
    errors: int = 0
    retries: int = 0
    details: list[str] = field(default_factory=list)

    def summary_line(self) -> str:
        return (
            f"{self.source:<20} {self.new_files:>3} new, "
            f"{self.skipped_files:>3} skipped, "
            f"{self.bytes_pulled/1e6:>8.2f} MB pulled, "
            f"{self.errors} errors"
            + (f" ({self.retries} retries)" if self.retries else "")
        )


def now_iso_utc() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def human_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024.0:
            return f"{n:.1f} {unit}"
        n /= 1024.0
    return f"{n:.1f} PB"
