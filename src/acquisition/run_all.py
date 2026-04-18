"""Orkiestrator pipeline'u akwizycji.

Uruchomienie:
    python -m src.acquisition.run_all --source all
    python -m src.acquisition.run_all --source citibike,noaa
    python -m src.acquisition.run_all --source s2b   # alias na station_to_borough

Kroki:
    1. wczytuje .env (tokeny NOAA + Socrata)
    2. sprawdza, czy klaster HDFS żyje
    3. ładuje manifest z /meta/manifest.json
    4. uruchamia kolejno: citibike → noaa → events → station_to_borough
    5. zapisuje manifest z powrotem do HDFS
    6. kopiuje plik logu do HDFS /logs/
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime, date
from pathlib import Path

from dotenv import load_dotenv

from . import citibike, events, hdfs_utils, noaa, station_to_borough
from .common import HDFS_ROOT_LOGS, SourceResult
from .manifest import Manifest


SOURCE_ALIASES = {
    "citibike": "citibike",
    "noaa": "noaa",
    "events": "events",
    "s2b": "station_to_borough",
    "station_to_borough": "station_to_borough",
}
SOURCE_ORDER = ["citibike", "noaa", "events", "station_to_borough"]
SOURCE_MODULES = {
    "citibike": citibike,
    "noaa": noaa,
    "events": events,
    "station_to_borough": station_to_borough,
}


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Akwizycja danych projektu Citi Bike NYC → HDFS")
    p.add_argument(
        "--source",
        default="all",
        help="Lista źródeł po przecinku (citibike,noaa,events,s2b) lub 'all'. Default: all.",
    )
    p.add_argument(
        "--run-date",
        default=date.today().isoformat(),
        help="Data uruchomienia (format YYYY-MM-DD). Default: dzisiaj.",
    )
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return p.parse_args()


def _resolve_sources(raw: str) -> list[str]:
    if raw.strip().lower() == "all":
        return list(SOURCE_ORDER)
    picked = []
    for token in raw.split(","):
        key = token.strip().lower()
        if not key:
            continue
        if key not in SOURCE_ALIASES:
            raise SystemExit(f"Nieznane źródło: '{key}'. Dozwolone: {sorted(SOURCE_ALIASES)}")
        picked.append(SOURCE_ALIASES[key])
    # Zachowaj kanoniczną kolejność (np. s2b zawsze po citibike)
    return [s for s in SOURCE_ORDER if s in set(picked)]


def _setup_logger(log_path: Path, level: str) -> logging.Logger:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("acquisition")
    logger.setLevel(getattr(logging, level))
    logger.handlers.clear()

    fmt = logging.Formatter(
        "%(asctime)s %(levelname)-5s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(sh)
    logger.propagate = False
    return logger


def main() -> int:
    args = _parse_args()
    load_dotenv()  # ładuje .env z cwd

    sources = _resolve_sources(args.source)

    repo_root = Path(__file__).resolve().parents[2]
    ts = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")
    log_path = repo_root / "logs" / "acquisition" / f"{ts}.log"
    tmp_dir = repo_root / "data" / "tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    logger = _setup_logger(log_path, args.log_level)
    logger.info("=" * 70)
    logger.info("=== acquisition run started (run_date=%s, sources=%s) ===", args.run_date, ",".join(sources))
    logger.info("log file: %s", log_path)

    t_start = time.time()

    try:
        hdfs_utils.check_cluster_alive()
    except hdfs_utils.HdfsError as exc:
        logger.error("Klaster HDFS nie działa: %s", exc)
        return 2

    manifest = Manifest()
    try:
        manifest.load(logger)
    except Exception as exc:
        logger.error("Błąd ładowania manifestu: %s", exc)
        return 2

    results: list[SourceResult] = []
    for src in sources:
        module = SOURCE_MODULES[src]
        try:
            res = module.run(args.run_date, manifest, logger, tmp_dir)
        except Exception:
            logger.exception("[%s] nieoczekiwany błąd — kontynuuję z kolejnymi źródłami", src)
            res = SourceResult(source=src, errors=1)
        results.append(res)

    # Zapis manifestu po wszystkich źródłach — nawet częściowy postęp trafia do HDFS.
    try:
        manifest.save(logger)
    except Exception as exc:
        logger.error("Nie udało się zapisać manifestu: %s", exc)

    total_new = sum(r.new_files for r in results)
    total_skipped = sum(r.skipped_files for r in results)
    total_bytes = sum(r.bytes_pulled for r in results)
    total_errors = sum(r.errors for r in results)
    duration = time.time() - t_start

    logger.info("=== SUMMARY ===")
    for r in results:
        logger.info("  %s", r.summary_line())
    logger.info(
        "  TOTAL: %d new, %d skipped, %.1f MB, %d errors, duration %.1fs",
        total_new, total_skipped, total_bytes / 1e6, total_errors, duration,
    )

    # Kopia logu do HDFS /logs/ (replikacja 3).
    # Flushujemy handlery żeby plik zawierał wszystko do tego momentu.
    for h in logger.handlers:
        h.flush()
    try:
        hdfs_log_path = f"{HDFS_ROOT_LOGS}/acquisition-{ts}.log"
        hdfs_utils.put(log_path, hdfs_log_path, logger)
        hdfs_utils.setrep(hdfs_log_path, 3, wait=True)
        logger.info("[log] lokalny → HDFS %s", hdfs_log_path)
    except hdfs_utils.HdfsError as exc:
        logger.warning("[log] nie udało się wgrać logu do HDFS: %s", exc)

    logger.info("=== acquisition run finished (%s errors) ===", total_errors)
    return 0 if total_errors == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
