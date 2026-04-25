#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

STATIONS_PATH=$(docker exec master bash -c '
  hdfs dfs -ls /raw/station_to_borough/ 2>/dev/null | awk "/\\/raw\\/station_to_borough\\// {print \$NF}" | sort | tail -1
')

if [[ -z "${STATIONS_PATH}" ]]; then
  echo "ERROR: no station_to_borough run found in /raw/station_to_borough/" >&2
  exit 1
fi

STATIONS_FILE=$(docker exec master bash -c "
  hdfs dfs -ls '${STATIONS_PATH}' | awk '/stations_.*\\.csv$/ {print \$NF}' | head -1
")

if [[ -z "${STATIONS_FILE}" ]]; then
  echo "ERROR: no stations_*.csv inside ${STATIONS_PATH}" >&2
  exit 1
fi

echo "[stage1] using stations file: ${STATIONS_FILE}"

"${REPO_ROOT}/scripts/mr/_run_job.sh" \
  E1 \
  pl.pwr.bigdata.mr.stage1.CleanDriver \
  /processed/citibike \
  /processed/T1 \
  "${STATIONS_FILE}"
