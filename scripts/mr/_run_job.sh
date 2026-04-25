#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 4 ]]; then
  echo "Usage: $0 <stage_label> <driver_class> <input> <output> [extra_args...]" >&2
  exit 2
fi

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
STAGE="$1"
DRIVER="$2"
INPUT="$3"
OUTPUT="$4"
shift 4
EXTRA="$*"

TS=$(date +%Y%m%dT%H%M%S)
LOG_DIR="${REPO_ROOT}/logs/mapreduce"
LOG="${LOG_DIR}/${TS}_${STAGE}.log"
mkdir -p "${LOG_DIR}"

{
  echo "=== ${STAGE} start: $(date -Iseconds) ==="
  echo "Driver: ${DRIVER}"
  echo "Input:  ${INPUT}"
  echo "Output: ${OUTPUT}"
  echo "Extra:  ${EXTRA}"
} | tee -a "${LOG}"

START_NS=$(date +%s%N)

set +e
docker exec master bash -c "
  hdfs dfs -rm -r -f '${OUTPUT}' >/dev/null 2>&1 || true
  hadoop jar /opt/mr/mr-jobs.jar ${DRIVER} ${INPUT} ${OUTPUT} ${EXTRA}
" 2>&1 | tee -a "${LOG}"
RC=${PIPESTATUS[0]}
set -e

END_NS=$(date +%s%N)
DURATION_MS=$(( (END_NS - START_NS) / 1000000 ))

echo "=== ${STAGE} done: $(date -Iseconds), rc=${RC}, duration=${DURATION_MS} ms ===" | tee -a "${LOG}"
exit ${RC}
