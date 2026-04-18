"""Manifest akwizycji w HDFS — pojedynczy plik JSON pod /meta/manifest.json.

Pełni rolę źródła prawdy o tym, które pliki zostały już pobrane. Re-run pipeline'u
sprawdza manifest przed każdym pobraniem i pomija pliki już obecne.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Optional

from . import hdfs_utils
from .common import HDFS_MANIFEST_PATH, now_iso_utc


MANIFEST_SCHEMA_VERSION = 1


class Manifest:
    def __init__(self) -> None:
        self.version: int = MANIFEST_SCHEMA_VERSION
        self.last_updated: str = now_iso_utc()
        self.sources: dict[str, dict[str, dict[str, Any]]] = {
            "citibike": {},
            "noaa": {},
            "events": {},
            "station_to_borough": {},
        }

    def load(self, logger: logging.Logger) -> None:
        if not hdfs_utils.exists(HDFS_MANIFEST_PATH):
            logger.info("[manifest] brak %s w HDFS, startuję z pustym manifestem", HDFS_MANIFEST_PATH)
            return
        raw = hdfs_utils.cat_bytes(HDFS_MANIFEST_PATH)
        try:
            data = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Manifest w HDFS jest uszkodzony: {exc}") from exc
        self.version = data.get("version", MANIFEST_SCHEMA_VERSION)
        self.last_updated = data.get("last_updated", now_iso_utc())
        incoming = data.get("sources", {})
        for key in self.sources:
            self.sources[key] = incoming.get(key, {})
        logger.info("[manifest] załadowany z %s (%d wpisów)", HDFS_MANIFEST_PATH, self.total_entries())

    def save(self, logger: logging.Logger) -> None:
        self.last_updated = now_iso_utc()
        payload = {
            "version": self.version,
            "last_updated": self.last_updated,
            "sources": self.sources,
        }
        blob = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
        hdfs_utils.put_bytes(blob, HDFS_MANIFEST_PATH, logger)
        hdfs_utils.setrep(HDFS_MANIFEST_PATH, 3, wait=True)
        logger.info("[manifest] zapisany w %s (%d wpisów)", HDFS_MANIFEST_PATH, self.total_entries())

    def is_downloaded(self, source: str, filename: str) -> bool:
        entry = self.sources.get(source, {}).get(filename)
        if not entry:
            return False
        return entry.get("size_bytes", 0) > 0

    def mark(self, source: str, filename: str, entry: dict[str, Any]) -> None:
        if source not in self.sources:
            self.sources[source] = {}
        self.sources[source][filename] = entry

    def get_entry(self, source: str, filename: str) -> Optional[dict[str, Any]]:
        return self.sources.get(source, {}).get(filename)

    def entries_for(self, source: str) -> dict[str, dict[str, Any]]:
        return self.sources.get(source, {})

    def total_entries(self) -> int:
        return sum(len(v) for v in self.sources.values())
