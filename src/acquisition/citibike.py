"""Akwizycja Citi Bike Trip Data ze S3 (bucket `tripdata`, anonimowy dostęp).

Pliki miesięczne w formacie `YYYYMM-citibike-tripdata.csv.zip` (dla 2020+).
Zakres projektu: marzec–listopad 2025.
"""
from __future__ import annotations

import hashlib
import logging
import time
from pathlib import Path

import boto3
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError

from . import hdfs_utils
from .common import HDFS_ROOT_RAW, MONTHS_YYYYMM, SourceResult, now_iso_utc
from .manifest import Manifest


SOURCE = "citibike"
S3_BUCKET = "tripdata"
S3_REGION = "us-east-1"


def _filename_for(yyyymm: str) -> str:
    return f"{yyyymm}-citibike-tripdata.csv.zip"


def _sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def run(run_date: str, manifest: Manifest, logger: logging.Logger, tmp_dir: Path) -> SourceResult:
    result = SourceResult(source=SOURCE)
    logger.info("[%s] start", SOURCE)

    client = boto3.client("s3", region_name=S3_REGION, config=Config(signature_version=UNSIGNED))
    hdfs_dir = f"{HDFS_ROOT_RAW}/{SOURCE}/{run_date}"

    for yyyymm in MONTHS_YYYYMM:
        filename = _filename_for(yyyymm)

        if manifest.is_downloaded(SOURCE, filename):
            entry = manifest.get_entry(SOURCE, filename) or {}
            size_mb = entry.get("size_bytes", 0) / 1e6
            logger.info("[%s] skip %s (already in manifest, %.1f MB)", SOURCE, filename, size_mb)
            result.skipped_files += 1
            continue

        # HEAD — sprawdź czy plik istnieje na S3
        try:
            head = client.head_object(Bucket=S3_BUCKET, Key=filename)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "?")
            if code in ("404", "NoSuchKey"):
                logger.warning("[%s] plik %s nie istnieje na S3 jeszcze — pomijam", SOURCE, filename)
                # Nie liczymy jako error — prawdopodobnie dane jeszcze nie wypuszczone
                continue
            logger.error("[%s] HEAD %s FAIL: %s", SOURCE, filename, exc)
            result.errors += 1
            continue

        source_size = head["ContentLength"]
        source_etag = head.get("ETag", "").strip('"')
        source_lm = head.get("LastModified")
        logger.info(
            "[%s] downloading %s (%.1f MB) from s3://%s/",
            SOURCE, filename, source_size / 1e6, S3_BUCKET,
        )

        local_path = tmp_dir / filename
        t0 = time.time()
        try:
            client.download_file(S3_BUCKET, filename, str(local_path))
        except ClientError as exc:
            logger.error("[%s] download %s FAIL: %s", SOURCE, filename, exc)
            result.errors += 1
            local_path.unlink(missing_ok=True)
            continue
        dt = time.time() - t0
        local_size = local_path.stat().st_size

        if local_size != source_size:
            logger.error(
                "[%s] %s rozmiar niezgodny (local=%d, s3=%d) — pomijam",
                SOURCE, filename, local_size, source_size,
            )
            result.errors += 1
            local_path.unlink(missing_ok=True)
            continue

        logger.info(
            "[%s] downloaded %d B in %.1fs (%.2f MB/s)",
            SOURCE, local_size, dt, local_size / 1e6 / max(dt, 0.01),
        )

        sha = _sha256_of(local_path)

        hdfs_path = f"{hdfs_dir}/{filename}"
        try:
            hdfs_utils.put(local_path, hdfs_path, logger)
            hdfs_utils.setrep(hdfs_path, 3, wait=True)
        except hdfs_utils.HdfsError as exc:
            logger.error("[%s] HDFS put/setrep %s FAIL: %s", SOURCE, filename, exc)
            result.errors += 1
            local_path.unlink(missing_ok=True)
            continue

        manifest.mark(SOURCE, filename, {
            "run_date": run_date,
            "hdfs_path": hdfs_path,
            "size_bytes": local_size,
            "sha256": sha,
            "source_etag": source_etag,
            "source_last_modified": source_lm.isoformat() if source_lm else None,
            "downloaded_at": now_iso_utc(),
        })
        result.new_files += 1
        result.bytes_pulled += local_size

        local_path.unlink(missing_ok=True)

    logger.info("[%s] done: %s", SOURCE, result.summary_line())
    return result
