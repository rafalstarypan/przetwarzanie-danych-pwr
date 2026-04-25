#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

NUM_REDUCERS_E2A="${NUM_REDUCERS_E2A:-1}"

echo "[stage2] E2a — rides aggregation"
"${REPO_ROOT}/scripts/mr/_run_job.sh" \
  E2a \
  pl.pwr.bigdata.mr.stage2.RidesAggDriver \
  /processed/T1 \
  /processed/T2_rides \
  "${NUM_REDUCERS_E2A}"

echo "[stage2] E2b — events aggregation"
"${REPO_ROOT}/scripts/mr/_run_job.sh" \
  E2b \
  pl.pwr.bigdata.mr.stage2.EventsAggDriver \
  /raw/events \
  /processed/T2_events

echo "[stage2] E2c — final join (T2_rides + NOAA + T2_events) → T2"
"${REPO_ROOT}/scripts/mr/_run_job.sh" \
  E2c \
  pl.pwr.bigdata.mr.stage2.JoinDriver \
  /processed/T2_rides \
  /processed/T2 \
  /raw/noaa \
  /processed/T2_events
