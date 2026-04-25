#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

echo "[stage4] E4 — enrich (T2 + T3a + T3b + T3c) → W"
"${REPO_ROOT}/scripts/mr/_run_job.sh" \
  E4 \
  pl.pwr.bigdata.mr.stage4.EnrichDriver \
  /processed/T2 \
  /processed/W \
  /processed/T3a \
  /processed/T3b \
  /processed/T3c
