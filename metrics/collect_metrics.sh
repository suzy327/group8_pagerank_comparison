#!/bin/bash
ALGO=$1
DATASET=$2
JOB_ID=$3

if [ -z "$ALGO" ] || [ -z "$DATASET" ] || [ -z "$JOB_ID" ]; then
  echo "❌ 用法: $0 <giraph|mapreduce> <dataset> <job_id>" >&2
  exit 1
fi

if [ "$ALGO" != "giraph" ] && [ "$ALGO" != "mapreduce" ]; then
  echo "❌ 算法必须是 giraph 或 mapreduce" >&2
  exit 1
fi

ROOT_DIR=$(dirname $(dirname $(realpath "$0")))
RESULT_FILE="$ROOT_DIR/results/metrics.csv"

mkdir -p "$ROOT_DIR/results"

if [ ! -f "$RESULT_FILE" ]; then
  echo "algorithm,dataset,job_id,total_execution_time,avg_iteration_time,network_communication,peak_memory_usage,iterations_to_converge,convergence_rate,cpu_utilization" > "$RESULT_FILE"
fi

if [ "$ALGO" == "giraph" ]; then
  EXEC_TIME=$(grep "Total execution time" job.log 2>/dev/null | awk '{print $5}' | tr -d '[:space:]' || echo "0")
  ITERATIONS=$(grep "Superstep" job.log 2>/dev/null | wc -l || echo "1")
  AVG_ITER_TIME=$(echo "scale=4; $EXEC_TIME / $ITERATIONS" | bc 2>/dev/null || echo "0")
  NETWORK_COMM=$(grep "Bytes sent" job.log 2>/dev/null | awk '{sum+=$3} END {printf "%.2f", sum/1048576}' 2>/dev/null || echo "0")
  PEAK_MEM=$(yarn logs -applicationId "$JOB_ID" 2>/dev/null | grep "Memory Used" | awk '{print $3}' | sort -nr | head -1 | awk '{printf "%.2f", $1/1024}' 2>/dev/null || echo "0")
  CONVERGE_ITER=$ITERATIONS
  CONVERGE_RATE=$(echo "scale=4; 100 * (1.0 - 0.0001) / ($ITERATIONS * 1.0)" | bc 2>/dev/null || echo "0")
  CPU_UTIL=$(sar -u 1 10 2>/dev/null | awk 'NR>3 {sum+=$3} END {printf "%.1f", sum/10}' 2>/dev/null || echo "0")
elif [ "$ALGO" == "mapreduce" ]; then
  EXEC_TIME=$(hadoop job -status "$JOB_ID" 2>/dev/null | grep "Run time" | awk '{print $4/1000}' | tr -d '[:space:]' || echo "0")
  ITERATIONS=$(hadoop job -history "$JOB_ID" 2>/dev/null | grep "Reduce" | grep "SUCCEEDED" | wc -l || echo "1")
  AVG_ITER_TIME=$(echo "scale=4; $EXEC_TIME / $ITERATIONS" | bc 2>/dev/null || echo "0")
  MAP_OUTPUT=$(hadoop job -history "$JOB_ID" 2>/dev/null | grep "Map input records" | awk '{sum+=$1} END {print sum*100}' 2>/dev/null || echo "0")
  REDUCE_INPUT=$(hadoop job -history "$JOB_ID" 2>/dev/null | grep "Reduce input records" | awk '{sum+=$1} END {print sum*100}' 2>/dev/null || echo "0")
  NETWORK_COMM=$(echo "scale=2; ($MAP_OUTPUT + $REDUCE_INPUT) / 1048576" | bc 2>/dev/null || echo "0")
  PEAK_MEM=$(yarn logs -applicationId "$JOB_ID" 2>/dev/null | grep "Memory Used" | awk '{print $3}' | sort -nr | head -1 | awk '{printf "%.2f", $1/1024}' 2>/dev/null || echo "0")
  CONVERGE_ITER=$ITERATIONS
  CONVERGE_RATE=$(echo "scale=4; 100 * (1.0 - 0.0001) / ($ITERATIONS * 1.0)" | bc 2>/dev/null || echo "0")
  CPU_UTIL=$(sar -u 1 10 2>/dev/null | awk 'NR>3 {sum+=$3} END {printf "%.1f", sum/10}' 2>/dev/null || echo "0")
fi

echo "$ALGO,$DATASET,$JOB_ID,$EXEC_TIME,$AVG_ITER_TIME,$NETWORK_COMM,$PEAK_MEM,$CONVERGE_ITER,$CONVERGE_RATE,$CPU_UTIL" >> "$RESULT_FILE"
exit 0