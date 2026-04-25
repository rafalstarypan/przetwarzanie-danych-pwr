#!/usr/bin/env bash
set -euo pipefail

MONTH_FILTER=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --month) MONTH_FILTER="$2"; shift 2;;
    *) echo "Unknown arg: $1"; exit 2;;
  esac
done

echo "[prepare] ensuring unzip available in master ..."
docker exec master bash -c 'command -v unzip >/dev/null 2>&1 || (yum install -y unzip || dnf install -y unzip || apt-get update && apt-get install -y unzip)'

echo "[prepare] listing ZIP files under /raw/citibike/ ..."
ZIP_LIST=$(docker exec master bash -c '
  hdfs dfs -ls -R /raw/citibike/ 2>/dev/null | awk "/\\.zip$/ {print \$NF}"
')

if [[ -z "${ZIP_LIST}" ]]; then
  echo "[prepare] ERROR: no ZIP files found under /raw/citibike/" >&2
  exit 1
fi

docker exec master hdfs dfs -mkdir -p /processed/citibike

while IFS= read -r zip_path; do
  [[ -z "${zip_path}" ]] && continue
  fname=$(basename "${zip_path}" .zip)

  if [[ -n "${MONTH_FILTER}" ]]; then
    YYYYMM="${MONTH_FILTER//-/}"
    if [[ "${fname}" != ${YYYYMM}* ]]; then
      echo "[prepare] skip ${fname} (not ${YYYYMM})"
      continue
    fi
  fi

  echo "[prepare] unpacking ${zip_path} -> /processed/citibike/${fname}.csv"
  docker exec master bash -c "
    set -euo pipefail
    rm -rf /tmp/unzip-${fname}
    mkdir -p /tmp/unzip-${fname}
    hdfs dfs -get '${zip_path}' /tmp/unzip-${fname}/${fname}.zip
    cd /tmp/unzip-${fname}
    unzip -o -q ${fname}.zip
    CSV_FILE=\$(find . -maxdepth 2 -name '*.csv' | head -1)
    if [[ -z \"\${CSV_FILE}\" ]]; then
      echo '[prepare] ERROR: no CSV inside zip' >&2
      exit 1
    fi
    hdfs dfs -put -f \"\${CSV_FILE}\" /processed/citibike/${fname}.csv
    hdfs dfs -setrep -w 3 /processed/citibike/${fname}.csv >/dev/null
    rm -rf /tmp/unzip-${fname}
  "
done <<< "${ZIP_LIST}"

echo "[prepare] done. /processed/citibike/ contents:"
docker exec master hdfs dfs -ls /processed/citibike/
