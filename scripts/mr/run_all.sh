#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
TS=$(date +%Y%m%dT%H%M%S)
SUMMARY_LOG="${REPO_ROOT}/logs/mapreduce/run_all_${TS}.log"
mkdir -p "$(dirname "${SUMMARY_LOG}")"

MONTH_FILTER=""
SKIP_PREPARE=0
SKIP_BUILD=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --month) MONTH_FILTER="$2"; shift 2;;
    --skip-prepare) SKIP_PREPARE=1; shift;;
    --skip-build) SKIP_BUILD=1; shift;;
    *) echo "Unknown arg: $1"; exit 2;;
  esac
done

{
  echo "=== run_all start: $(date -Iseconds) ==="
  echo "Month filter: ${MONTH_FILTER:-<all>}"
  echo "Skip prepare: ${SKIP_PREPARE}"
  echo "Skip build:   ${SKIP_BUILD}"
} | tee -a "${SUMMARY_LOG}"

PIPELINE_START_NS=$(date +%s%N)

if [[ "${SKIP_BUILD}" -eq 0 ]]; then
  "${REPO_ROOT}/scripts/mr/build.sh" 2>&1 | tee -a "${SUMMARY_LOG}"
fi

if [[ "${SKIP_PREPARE}" -eq 0 ]]; then
  if [[ -n "${MONTH_FILTER}" ]]; then
    "${REPO_ROOT}/scripts/mr/prepare_inputs.sh" --month "${MONTH_FILTER}" 2>&1 | tee -a "${SUMMARY_LOG}"
  else
    "${REPO_ROOT}/scripts/mr/prepare_inputs.sh" 2>&1 | tee -a "${SUMMARY_LOG}"
  fi
fi

run_stage() {
  local label="$1"
  local script="$2"
  local stage_start_ns
  stage_start_ns=$(date +%s%N)
  echo "--- ${label} START $(date -Iseconds) ---" | tee -a "${SUMMARY_LOG}"
  "${script}" 2>&1 | tee -a "${SUMMARY_LOG}"
  local stage_end_ns
  stage_end_ns=$(date +%s%N)
  local dur_ms=$(( (stage_end_ns - stage_start_ns) / 1000000 ))
  echo "--- ${label} END   $(date -Iseconds) duration=${dur_ms} ms ---" | tee -a "${SUMMARY_LOG}"
}

run_stage "Stage 1" "${REPO_ROOT}/scripts/mr/run_stage1.sh"
run_stage "Stage 2" "${REPO_ROOT}/scripts/mr/run_stage2.sh"
run_stage "Stage 3" "${REPO_ROOT}/scripts/mr/run_stage3.sh"
run_stage "Stage 4" "${REPO_ROOT}/scripts/mr/run_stage4.sh"

PIPELINE_END_NS=$(date +%s%N)
TOTAL_MS=$(( (PIPELINE_END_NS - PIPELINE_START_NS) / 1000000 ))

{
  echo "=== run_all DONE $(date -Iseconds) ==="
  echo "TOTAL pipeline duration: ${TOTAL_MS} ms"
  echo "Summary log: ${SUMMARY_LOG}"
} | tee -a "${SUMMARY_LOG}"
