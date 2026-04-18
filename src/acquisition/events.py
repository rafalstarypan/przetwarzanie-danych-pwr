"""Akwizycja NYC Permitted Event Information przez Socrata SODA API.

Dataset NYC Open Data — ID konfigurowalny przez zmienną środowiskową `NYC_EVENTS_DATASET_ID`
(default: `bkfu-528j` — "NYC Permitted Event Information - Historical", obejmuje dane
od 2008 i zawiera archiwalne eventy 2025).

UWAGA: dataset `tvpp-9vvx` to wariant "Current" — tylko nadchodzący ~1 miesiąc,
nie nadaje się do analizy historycznej (2025 zwraca 0 rekordów).

Granulacja: 1 plik CSV per miesiąc, zakres marzec–listopad 2025.
"""
from __future__ import annotations

import calendar
import hashlib
import logging
import os
import time
from pathlib import Path

import requests

from . import hdfs_utils
from .common import HDFS_ROOT_RAW, MONTHS_YYYYMM, SourceResult, now_iso_utc
from .manifest import Manifest


SOURCE = "events"
DEFAULT_DATASET_ID = "bkfu-528j"
PAGE_LIMIT = 50000
MAX_RETRIES = 3


def _dataset_id() -> str:
    return os.environ.get("NYC_EVENTS_DATASET_ID", DEFAULT_DATASET_ID)


def _filename_for(yyyymm: str) -> str:
    return f"events_{yyyymm}.csv"


def _month_bounds(yyyymm: str) -> tuple[str, str]:
    year = int(yyyymm[:4])
    month = int(yyyymm[4:6])
    last_day = calendar.monthrange(year, month)[1]
    return f"{year:04d}-{month:02d}-01T00:00:00", f"{year:04d}-{month:02d}-{last_day:02d}T23:59:59"


def _fetch_month(dataset_id: str, app_token: str, yyyymm: str, logger: logging.Logger) -> tuple[bytes, int]:
    start_iso, end_iso = _month_bounds(yyyymm)
    base_url = f"https://data.cityofnewyork.us/resource/{dataset_id}.csv"
    headers = {"X-App-Token": app_token} if app_token else {}

    all_chunks: list[str] = []
    header_line: str | None = None
    total_rows = 0
    offset = 0
    result_retries = 0

    while True:
        params = {
            "$where": f"start_date_time between '{start_iso}' and '{end_iso}'",
            "$order": "start_date_time",
            "$limit": PAGE_LIMIT,
            "$offset": offset,
        }

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = requests.get(base_url, params=params, headers=headers, timeout=120)
            except requests.RequestException as exc:
                if attempt == MAX_RETRIES:
                    raise
                backoff = 2 ** attempt
                logger.warning("[%s] %s: %s — retry %d/%d za %ds", SOURCE, yyyymm, exc, attempt, MAX_RETRIES, backoff)
                time.sleep(backoff)
                result_retries += 1
                continue
            if resp.status_code == 429:
                if attempt == MAX_RETRIES:
                    resp.raise_for_status()
                backoff = 2 ** attempt
                logger.warning("[%s] %s: HTTP 429 — retry %d/%d za %ds", SOURCE, yyyymm, attempt, MAX_RETRIES, backoff)
                time.sleep(backoff)
                result_retries += 1
                continue
            resp.raise_for_status()
            break

        text = resp.text
        if not text.strip():
            break
        lines = text.splitlines(keepends=False)
        if not lines:
            break
        page_header, *page_rows = lines
        if header_line is None:
            header_line = page_header
            all_chunks.append(page_header)
        if not page_rows:
            break
        all_chunks.extend(page_rows)
        total_rows += len(page_rows)
        if len(page_rows) < PAGE_LIMIT:
            break
        offset += PAGE_LIMIT

    if header_line is None:
        # Pusty wynik — stworzymy plik tylko z informacją o pustym miesiącu
        all_chunks = ["# brak rekordów dla okresu"]
    blob = ("\n".join(all_chunks) + "\n").encode("utf-8")
    return blob, total_rows


def run(run_date: str, manifest: Manifest, logger: logging.Logger, tmp_dir: Path) -> SourceResult:
    result = SourceResult(source=SOURCE)
    logger.info("[%s] start (dataset=%s)", SOURCE, _dataset_id())

    app_token = os.environ.get("SOCRATA_APP_TOKEN", "").strip()
    if not app_token:
        logger.warning("[%s] brak SOCRATA_APP_TOKEN — używam anonimowego dostępu (niższy rate limit)", SOURCE)

    hdfs_dir = f"{HDFS_ROOT_RAW}/{SOURCE}/{run_date}"
    dataset_id = _dataset_id()

    for yyyymm in MONTHS_YYYYMM:
        filename = _filename_for(yyyymm)
        if manifest.is_downloaded(SOURCE, filename):
            entry = manifest.get_entry(SOURCE, filename) or {}
            saved_dataset = entry.get("dataset_id")
            if saved_dataset and saved_dataset != dataset_id:
                logger.info(
                    "[%s] %s: manifest ma wpis z innego datasetu (%s → %s) — pobieram ponownie",
                    SOURCE, filename, saved_dataset, dataset_id,
                )
            else:
                size_mb = entry.get("size_bytes", 0) / 1e6
                logger.info("[%s] skip %s (already in manifest, %.1f MB)", SOURCE, filename, size_mb)
                result.skipped_files += 1
                continue

        logger.info("[%s] fetch %s", SOURCE, yyyymm)
        t0 = time.time()
        try:
            blob, row_count = _fetch_month(dataset_id, app_token, yyyymm, logger)
        except requests.HTTPError as exc:
            body = exc.response.text[:200] if exc.response else ""
            logger.error("[%s] %s HTTP FAIL: %s — %s", SOURCE, yyyymm, exc, body)
            result.errors += 1
            continue
        except requests.RequestException as exc:
            logger.error("[%s] %s network FAIL: %s", SOURCE, yyyymm, exc)
            result.errors += 1
            continue
        dt = time.time() - t0

        local_path = tmp_dir / filename
        local_path.write_bytes(blob)
        logger.info("[%s] %s: %d rekordów, %.1f KB w %.1fs", SOURCE, yyyymm, row_count, len(blob) / 1024, dt)

        hdfs_path = f"{hdfs_dir}/{filename}"
        try:
            hdfs_utils.put(local_path, hdfs_path, logger)
            hdfs_utils.setrep(hdfs_path, 3, wait=True)
        except hdfs_utils.HdfsError as exc:
            logger.error("[%s] HDFS %s FAIL: %s", SOURCE, filename, exc)
            result.errors += 1
            local_path.unlink(missing_ok=True)
            continue

        sha = hashlib.sha256(blob).hexdigest()
        manifest.mark(SOURCE, filename, {
            "run_date": run_date,
            "hdfs_path": hdfs_path,
            "size_bytes": len(blob),
            "sha256": sha,
            "record_count": row_count,
            "dataset_id": dataset_id,
            "downloaded_at": now_iso_utc(),
        })
        result.new_files += 1
        result.bytes_pulled += len(blob)
        local_path.unlink(missing_ok=True)

    logger.info("[%s] done: %s", SOURCE, result.summary_line())
    return result
