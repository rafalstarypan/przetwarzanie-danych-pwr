#!/usr/bin/env bash
set -euo pipefail

DATE_TARGET="${1:-2025-06-15}"
BOROUGH_TARGET="${2:-Manhattan}"

echo "=== Spot-check: ${DATE_TARGET} ${BOROUGH_TARGET} ==="

echo
echo "--- HDFS layout ---"
docker exec master hdfs dfs -ls -R /processed | awk 'NR<=40 {print}'

echo
echo "--- /processed sizes ---"
docker exec master hdfs dfs -du -h /processed

echo
echo "--- replication ---"
docker exec master hdfs fsck /processed -files 2>/dev/null | grep -E 'Average block replication|Corrupt' || true

echo
echo "--- T3a (borough_stats) ---"
docker exec master hdfs dfs -cat /processed/T3a/part-r-* 2>/dev/null

echo
echo "--- T3b (regression) ---"
docker exec master hdfs dfs -cat /processed/T3b/part-r-* 2>/dev/null

echo
echo "--- T3c (event_stats) ---"
docker exec master hdfs dfs -cat /processed/T3c/part-r-* 2>/dev/null

echo
echo "--- W rekord: ${DATE_TARGET} ${BOROUGH_TARGET} ---"
docker exec master bash -c "
  hdfs dfs -cat /processed/W/part-* 2>/dev/null \
    | awk -F'\t' -v d='${DATE_TARGET}' -v b='${BOROUGH_TARGET}' '\$1==d && \$2==b'
"

echo
echo "--- W: pierwsze 10 rekordów ---"
docker exec master bash -c "hdfs dfs -cat /processed/W/part-* 2>/dev/null | head -10"

echo
echo "--- W: liczba rekordów ---"
docker exec master bash -c "hdfs dfs -cat /processed/W/part-* 2>/dev/null | wc -l"

echo
echo "Schemat W: date  borough  total_rides  member_rides  avg_duration_min  temp  precip  snow  event_types  expected_rides  event_intensity  anomaly  demand_level"
