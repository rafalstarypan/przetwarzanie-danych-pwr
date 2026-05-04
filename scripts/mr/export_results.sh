#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
OUT_DIR="${REPO_ROOT}/outputs/etap7"
mkdir -p "${OUT_DIR}"

H_W="date\tborough\ttotal_rides\tmember_rides\tavg_duration_min\ttemp\tprecip\tsnow\tevent_types\texpected_rides\tevent_intensity\tanomaly\tdemand_level"
H_T3A="borough\tmean\tstddev\tq20\tq40\tq60\tq80"
H_T3B="borough\tbeta0\tbeta1_temp\tbeta2_precip\tbeta3_snow\trmse"
H_T3C="event_type\tp33\tp66"

echo "[export] merging HDFS outputs in /tmp/etap7 inside master ..."
docker exec master bash -c '
  set -euo pipefail
  rm -rf /tmp/etap7 && mkdir -p /tmp/etap7
  for d in W T3a T3b T3c; do
    if hdfs dfs -test -d /processed/$d 2>/dev/null; then
      hdfs dfs -getmerge /processed/$d /tmp/etap7/${d}.body.tsv
    else
      echo "WARN: /processed/$d missing" >&2
    fi
  done
'

echo "[export] copying merged files from container -> host ..."
docker cp master:/tmp/etap7/. "${OUT_DIR}/"

prepend_header() {
  local file="$1"
  local header="$2"
  if [[ -f "${OUT_DIR}/${file}.body.tsv" ]]; then
    { printf "%b\n" "${header}"; cat "${OUT_DIR}/${file}.body.tsv"; } > "${OUT_DIR}/${file}.tsv"
    rm "${OUT_DIR}/${file}.body.tsv"
    echo "[export] ${file}.tsv  ($(wc -l < "${OUT_DIR}/${file}.tsv") lines)"
  else
    echo "[export] ${file}: missing, skipped"
  fi
}

prepend_header "W"   "${H_W}"
prepend_header "T3a" "${H_T3A}"
prepend_header "T3b" "${H_T3B}"
prepend_header "T3c" "${H_T3C}"

echo
echo "[export] done. Files in: ${OUT_DIR}"
ls -lh "${OUT_DIR}"
