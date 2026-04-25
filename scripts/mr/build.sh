#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
SRC_HOST="${REPO_ROOT}/src/mapreduce"
OUT_HOST="${REPO_ROOT}/build"

mkdir -p "${OUT_HOST}"

echo "[build] copying sources to master:/opt/mr/src ..."
docker exec master mkdir -p /opt/mr/src /opt/mr/classes
docker exec master rm -rf /opt/mr/src/pl /opt/mr/classes
docker cp "${SRC_HOST}/pl" master:/opt/mr/src/

echo "[build] compiling inside master container ..."
docker exec master bash -c '
  set -euo pipefail
  cd /opt/mr
  mkdir -p classes
  rm -rf classes/*
  find src -name "*.java" > sources.txt
  javac -classpath "$(hadoop classpath)" -d classes @sources.txt
  jar cf mr-jobs.jar -C classes .
'

echo "[build] downloading mr-jobs.jar to ${OUT_HOST}/ ..."
docker cp master:/opt/mr/mr-jobs.jar "${OUT_HOST}/mr-jobs.jar"

SIZE=$(du -h "${OUT_HOST}/mr-jobs.jar" | cut -f1)
echo "[build] OK: ${OUT_HOST}/mr-jobs.jar (${SIZE})"
