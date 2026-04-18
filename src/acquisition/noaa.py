"""Akwizycja NOAA GHCN-Daily przez CDO v2 REST API.

Pobieramy dane dzienne dla stacji Central Park (USW00094728), datatypes TMAX/TMIN/PRCP/SNOW,
w granulacji miesięcznej — po jednym pliku JSON na miesiąc, co pozwala na naturalny
przyrostowy re-run.
"""
from __future__ import annotations

import calendar
import hashlib
import json
import logging
import os
import time
from pathlib import Path

import requests

from . import hdfs_utils
from .common import HDFS_ROOT_RAW, MONTHS_YYYYMM, SourceResult, now_iso_utc
from .manifest import Manifest


SOURCE = "noaa"
STATION_ID = "GHCND:USW00094728"   # Central Park, NY
DATASET_ID = "GHCND"
DATATYPES = ["TMAX", "TMIN", "PRCP", "SNOW"]
BASE_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"
PAGE_LIMIT = 1000
MAX_RETRIES = 3


def _filename_for(yyyymm: str) -> str:
    return f"noaa_USW00094728_{yyyymm}.json"


def _month_bounds(yyyymm: str) -> tuple[str, str]:
    year = int(yyyymm[:4])
    month = int(yyyymm[4:6])
    last_day = calendar.monthrange(year, month)[1]
    return f"{year:04d}-{month:02d}-01", f"{year:04d}-{month:02d}-{last_day:02d}"


def _fetch_month(token: str, yyyymm: str, logger: logging.Logger) -> list[dict]:
    start, end = _month_bounds(yyyymm)
    all_results: list[dict] = []
    offset = 1  # CDO v2 używa 1-based offset
    while True:
        params = [
            ("datasetid", DATASET_ID),
            ("stationid", STATION_ID),
            ("startdate", start),
            ("enddate", end),
            ("units", "metric"),
            ("limit", PAGE_LIMIT),
            ("offset", offset),
        ]
        for dt in DATATYPES:
            params.append(("datatypeid", dt))

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = requests.get(
                    BASE_URL,
                    params=params,
                    headers={"token": token},
                    timeout=60,
                )
            except requests.RequestException as exc:
                if attempt == MAX_RETRIES:
                    raise
                backoff = 2 ** attempt
                logger.warning("[%s] %s: %s — retry %d/%d za %ds", SOURCE, yyyymm, exc, attempt, MAX_RETRIES, backoff)
                time.sleep(backoff)
                continue
            if resp.status_code == 429:
                if attempt == MAX_RETRIES:
                    resp.raise_for_status()
                backoff = 2 ** attempt
                logger.warning("[%s] %s: HTTP 429 — retry %d/%d za %ds", SOURCE, yyyymm, attempt, MAX_RETRIES, backoff)
                time.sleep(backoff)
                continue
            resp.raise_for_status()
            break

        data = resp.json()
        page = data.get("results", [])
        all_results.extend(page)
        if len(page) < PAGE_LIMIT:
            break
        offset += PAGE_LIMIT
        time.sleep(0.25)   # uprzejmość dla CDO (rate limit 5 req/s)

    return all_results


def run(run_date: str, manifest: Manifest, logger: logging.Logger, tmp_dir: Path) -> SourceResult:
    result = SourceResult(source=SOURCE)
    logger.info("[%s] start", SOURCE)

    token = os.environ.get("NOAA_TOKEN", "").strip()
    if not token:
        logger.error("[%s] brak NOAA_TOKEN w środowisku — pomijam całe źródło", SOURCE)
        result.errors += 1
        return result

    hdfs_dir = f"{HDFS_ROOT_RAW}/{SOURCE}/{run_date}"

    for yyyymm in MONTHS_YYYYMM:
        filename = _filename_for(yyyymm)
        if manifest.is_downloaded(SOURCE, filename):
            entry = manifest.get_entry(SOURCE, filename) or {}
            size_kb = entry.get("size_bytes", 0) / 1024
            logger.info("[%s] skip %s (already in manifest, %.1f KB)", SOURCE, filename, size_kb)
            result.skipped_files += 1
            continue

        logger.info("[%s] fetch %s (%s..%s)", SOURCE, yyyymm, *_month_bounds(yyyymm))
        t0 = time.time()
        try:
            records = _fetch_month(token, yyyymm, logger)
        except requests.HTTPError as exc:
            logger.error("[%s] %s HTTP FAIL: %s — %s", SOURCE, yyyymm, exc, exc.response.text[:200] if exc.response else "")
            result.errors += 1
            continue
        except requests.RequestException as exc:
            logger.error("[%s] %s network FAIL: %s", SOURCE, yyyymm, exc)
            result.errors += 1
            continue
        dt = time.time() - t0

        payload = {
            "station": STATION_ID,
            "month": yyyymm,
            "start_date": _month_bounds(yyyymm)[0],
            "end_date": _month_bounds(yyyymm)[1],
            "datatypes": DATATYPES,
            "fetched_at": now_iso_utc(),
            "record_count": len(records),
            "results": records,
        }
        blob = json.dumps(payload, indent=2).encode("utf-8")
        local_path = tmp_dir / filename
        local_path.write_bytes(blob)
        logger.info("[%s] %s: %d rekordów, %.1f KB w %.1fs", SOURCE, yyyymm, len(records), len(blob) / 1024, dt)

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
            "record_count": len(records),
            "downloaded_at": now_iso_utc(),
        })
        result.new_files += 1
        result.bytes_pulled += len(blob)
        local_path.unlink(missing_ok=True)

    logger.info("[%s] done: %s", SOURCE, result.summary_line())
    return result
