#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

echo "[stage3] E3a — borough_stats"
"${REPO_ROOT}/scripts/mr/_run_job.sh" \
  E3a \
  pl.pwr.bigdata.mr.stage3.BoroughStatsDriver \
  /processed/T2 \
  /processed/T3a

echo "[stage3] E3b — regression OLS per borough"
"${REPO_ROOT}/scripts/mr/_run_job.sh" \
  E3b \
  pl.pwr.bigdata.mr.stage3.RegressionDriver \
  /processed/T2 \
  /processed/T3b

echo "[stage3] E3c — event_count_stats"
"${REPO_ROOT}/scripts/mr/_run_job.sh" \
  E3c \
  pl.pwr.bigdata.mr.stage3.EventStatsDriver \
  /processed/T2 \
  /processed/T3c
