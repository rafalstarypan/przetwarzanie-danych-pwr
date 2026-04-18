"""Akwizycja Z4 — zbiór wyliczany `station_to_borough`.

Krok 1: pobiera GeoJSON granic dzielnic NYC (Socrata dataset `tqmj-j8zm`).
Krok 2: wyciąga unikalne stacje (start_station_id, name, lat, lng) z najnowszego
        pliku Citi Bike znajdującego się w HDFS (wg manifestu).
Krok 3: point-in-polygon (shapely) → `borough`. Fallback "Other" dla stacji poza NYC.
Krok 4: zapis CSV + oryginalny GeoJSON do HDFS.

Plik wynikowy (CSV): start_station_id,station_name,latitude,longitude,borough
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import os
import time
import zipfile
from pathlib import Path

import requests
from shapely.geometry import shape, Point

from . import hdfs_utils
from .common import HDFS_ROOT_RAW, SourceResult, now_iso_utc
from .manifest import Manifest


SOURCE = "station_to_borough"
GEOJSON_DATASET = "tqmj-j8zm"   # Borough Boundaries @ NYC Open Data
GEOJSON_URL = f"https://data.cityofnewyork.us/resource/{GEOJSON_DATASET}.geojson"
FALLBACK_BOROUGH = "Other"


def _stations_csv_name(run_date: str) -> str:
    return f"stations_{run_date}.csv"


def _geojson_name(run_date: str) -> str:
    return f"borough_boundaries_{run_date}.geojson"


def _download_borough_geojson(logger: logging.Logger, tmp_dir: Path) -> tuple[Path, bytes]:
    logger.info("[%s] GET %s", SOURCE, GEOJSON_URL)
    headers = {}
    tok = os.environ.get("SOCRATA_APP_TOKEN", "").strip()
    if tok:
        headers["X-App-Token"] = tok
    resp = requests.get(GEOJSON_URL, headers=headers, timeout=60)
    resp.raise_for_status()
    blob = resp.content
    path = tmp_dir / "borough_boundaries.geojson"
    path.write_bytes(blob)
    logger.info("[%s] GeoJSON pobrany (%.1f KB)", SOURCE, len(blob) / 1024)
    return path, blob


def _load_polygons(geojson_bytes: bytes) -> list[tuple[str, object]]:
    """Zwraca listę (borough_name, shapely_geom)."""
    data = json.loads(geojson_bytes.decode("utf-8"))
    polys: list[tuple[str, object]] = []
    # Socrata zwraca zarówno GeoJSON FeatureCollection, jak i (rzadziej) listę Features.
    features = data.get("features") if isinstance(data, dict) else data
    if features is None:
        raise RuntimeError(f"Nieznana struktura GeoJSON: klucze={list(data.keys()) if isinstance(data, dict) else 'list'}")
    for feat in features:
        props = feat.get("properties", {}) or {}
        geom = feat.get("geometry")
        if geom is None:
            continue
        # Borough Boundaries ma pole `boro_name`; zabezpieczamy się na wypadek zmiany schematu.
        name = props.get("boro_name") or props.get("BoroName") or props.get("borough") or "Unknown"
        polys.append((name, shape(geom)))
    if not polys:
        raise RuntimeError("GeoJSON nie zawiera żadnych poligonów granic dzielnic")
    return polys


def _pick_latest_citibike_file(manifest: Manifest, logger: logging.Logger) -> str | None:
    """Zwraca nazwę najnowszego pliku Citi Bike w manifeście (po YYYYMM)."""
    entries = manifest.entries_for("citibike")
    if not entries:
        logger.error("[%s] brak plików Citi Bike w manifeście — uruchom najpierw akwizycję citibike", SOURCE)
        return None
    # Klucze typu "202511-citibike-tripdata.csv.zip"
    sorted_keys = sorted(entries.keys(), reverse=True)
    return sorted_keys[0]


def _extract_stations_from_zip(zip_path: Path, logger: logging.Logger) -> list[tuple[str, str, float, float]]:
    """Czyta jedynie unikalne kombinacje (start_station_id, name, lat, lng) z CSV w ZIP-ie."""
    unique: dict[str, tuple[str, str, float, float]] = {}
    with zipfile.ZipFile(zip_path) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise RuntimeError(f"ZIP {zip_path.name} nie zawiera pliku CSV")
        csv_name = csv_names[0]
        logger.info("[%s] czytam stacje z %s (w ZIP)", SOURCE, csv_name)
        with zf.open(csv_name) as fh:
            text = io.TextIOWrapper(fh, encoding="utf-8", newline="")
            reader = csv.DictReader(text)
            required = {"start_station_id", "start_station_name", "start_lat", "start_lng"}
            missing = required - set(reader.fieldnames or [])
            if missing:
                raise RuntimeError(f"CSV Citi Bike brak kolumn: {missing}")
            for row in reader:
                sid = (row.get("start_station_id") or "").strip()
                if not sid or sid in unique:
                    continue
                name = (row.get("start_station_name") or "").strip()
                try:
                    lat = float(row["start_lat"])
                    lng = float(row["start_lng"])
                except (TypeError, ValueError):
                    continue
                unique[sid] = (sid, name, lat, lng)
    logger.info("[%s] unikalnych stacji: %d", SOURCE, len(unique))
    return list(unique.values())


def _assign_boroughs(stations, polygons, logger: logging.Logger) -> list[tuple[str, str, float, float, str]]:
    out: list[tuple[str, str, float, float, str]] = []
    unmatched = 0
    for sid, name, lat, lng in stations:
        pt = Point(lng, lat)   # GeoJSON: (x=lng, y=lat)
        borough = None
        for bname, poly in polygons:
            if poly.contains(pt) or poly.intersects(pt):
                borough = bname
                break
        if borough is None:
            borough = FALLBACK_BOROUGH
            unmatched += 1
        out.append((sid, name, lat, lng, borough))
    if unmatched:
        logger.warning(
            "[%s] %d/%d stacji poza granicami NYC → borough='%s'",
            SOURCE, unmatched, len(stations), FALLBACK_BOROUGH,
        )
    return out


def _write_csv(rows, path: Path) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["start_station_id", "station_name", "latitude", "longitude", "borough"])
        for sid, name, lat, lng, borough in rows:
            w.writerow([sid, name, f"{lat:.6f}", f"{lng:.6f}", borough])


def run(run_date: str, manifest: Manifest, logger: logging.Logger, tmp_dir: Path) -> SourceResult:
    result = SourceResult(source=SOURCE)
    logger.info("[%s] start", SOURCE)

    stations_filename = _stations_csv_name(run_date)
    geojson_filename = _geojson_name(run_date)

    if manifest.is_downloaded(SOURCE, stations_filename) and manifest.is_downloaded(SOURCE, geojson_filename):
        logger.info("[%s] skip — %s i %s już w manifeście", SOURCE, stations_filename, geojson_filename)
        result.skipped_files += 2
        return result

    hdfs_dir = f"{HDFS_ROOT_RAW}/{SOURCE}/{run_date}"

    # Krok 1 — GeoJSON granic
    try:
        geojson_path, geojson_blob = _download_borough_geojson(logger, tmp_dir)
    except requests.RequestException as exc:
        logger.error("[%s] GeoJSON FAIL: %s", SOURCE, exc)
        result.errors += 1
        return result

    try:
        polygons = _load_polygons(geojson_blob)
        logger.info("[%s] wczytano %d poligonów dzielnic: %s",
                    SOURCE, len(polygons), ", ".join(b for b, _ in polygons))
    except Exception as exc:
        logger.error("[%s] parse GeoJSON FAIL: %s", SOURCE, exc)
        result.errors += 1
        return result

    # Krok 2 — najnowszy Citi Bike z HDFS
    latest_cb = _pick_latest_citibike_file(manifest, logger)
    if not latest_cb:
        result.errors += 1
        return result
    entry = manifest.get_entry("citibike", latest_cb) or {}
    hdfs_cb_path = entry.get("hdfs_path")
    if not hdfs_cb_path:
        logger.error("[%s] brak hdfs_path dla %s", SOURCE, latest_cb)
        result.errors += 1
        return result

    local_cb = tmp_dir / latest_cb
    logger.info("[%s] pobieram najnowszy Citi Bike: %s", SOURCE, hdfs_cb_path)
    t0 = time.time()
    try:
        hdfs_utils.get(hdfs_cb_path, local_cb, logger)
    except hdfs_utils.HdfsError as exc:
        logger.error("[%s] HDFS get %s FAIL: %s", SOURCE, hdfs_cb_path, exc)
        result.errors += 1
        return result
    logger.info("[%s] pobrano %.1f MB w %.1fs", SOURCE, local_cb.stat().st_size / 1e6, time.time() - t0)

    # Krok 3 — stacje + przypisanie borough
    try:
        stations = _extract_stations_from_zip(local_cb, logger)
    except Exception as exc:
        logger.error("[%s] extract stations FAIL: %s", SOURCE, exc)
        result.errors += 1
        local_cb.unlink(missing_ok=True)
        return result
    local_cb.unlink(missing_ok=True)

    enriched = _assign_boroughs(stations, polygons, logger)
    borough_counts: dict[str, int] = {}
    for *_, b in enriched:
        borough_counts[b] = borough_counts.get(b, 0) + 1
    logger.info("[%s] rozkład dzielnic: %s", SOURCE, borough_counts)

    # Krok 4 — zapis CSV + GeoJSON do HDFS
    csv_path = tmp_dir / stations_filename
    _write_csv(enriched, csv_path)
    csv_size = csv_path.stat().st_size

    geojson_hdfs_path = f"{hdfs_dir}/{geojson_filename}"
    csv_hdfs_path = f"{hdfs_dir}/{stations_filename}"
    try:
        hdfs_utils.put(geojson_path, geojson_hdfs_path, logger)
        hdfs_utils.setrep(geojson_hdfs_path, 3, wait=True)
        hdfs_utils.put(csv_path, csv_hdfs_path, logger)
        hdfs_utils.setrep(csv_hdfs_path, 3, wait=True)
    except hdfs_utils.HdfsError as exc:
        logger.error("[%s] HDFS put FAIL: %s", SOURCE, exc)
        result.errors += 1
        csv_path.unlink(missing_ok=True)
        geojson_path.unlink(missing_ok=True)
        return result

    manifest.mark(SOURCE, geojson_filename, {
        "run_date": run_date,
        "hdfs_path": geojson_hdfs_path,
        "size_bytes": len(geojson_blob),
        "sha256": hashlib.sha256(geojson_blob).hexdigest(),
        "source_url": GEOJSON_URL,
        "downloaded_at": now_iso_utc(),
    })
    csv_bytes = csv_path.read_bytes()
    manifest.mark(SOURCE, stations_filename, {
        "run_date": run_date,
        "hdfs_path": csv_hdfs_path,
        "size_bytes": csv_size,
        "sha256": hashlib.sha256(csv_bytes).hexdigest(),
        "station_count": len(enriched),
        "borough_counts": borough_counts,
        "built_from_citibike": latest_cb,
        "built_at": now_iso_utc(),
    })

    result.new_files += 2
    result.bytes_pulled += len(geojson_blob) + csv_size

    csv_path.unlink(missing_ok=True)
    geojson_path.unlink(missing_ok=True)

    logger.info("[%s] done: %s", SOURCE, result.summary_line())
    return result
