#!/bin/bash

# ================= 配置区 =================
DATA_DIR="code/data" 
RESULT_DIR="experiment_results"
CSV_FILE="$RESULT_DIR/metrics.csv"
mkdir -p $RESULT_DIR/logs
# =========================================

# 初始化 CSV
echo "algorithm,dataset,job_id,total_execution_time,avg_iteration_time,network_communication,peak_memory_usage,iterations_to_converge,final_diff,cpu_utilization" > $CSV_FILE

# 准备环境
export HADOOP_CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath):$(pwd)/pagerank-compare.jar
export GIRAPH_JAR=/usr/local/hadoop/share/hadoop/yarn/lib/giraph.jar
declare -A DATASETS
DATASETS=( ["small"]="cit-HepPh.txt" ["medium"]="twitter_combined.txt" ["large"]="web-BerkStan.txt" )

# ================= 1. 预处理数据 =================
echo ">>> Step 1: Preprocessing Data..."
for name in "${!DATASETS[@]}"; do
    raw_file="$DATA_DIR/${DATASETS[$name]}"
    if [ -f "$raw_file" ]; then
        echo "Processing $name..."
        java -cp classes DataPreprocess "$raw_file" "${name}_mr.txt" "${name}_giraph.txt"
        
        hdfs dfs -mkdir -p /user/root/input/$name
        hdfs dfs -rm -r -f /user/root/input/$name/mr.txt
        hdfs dfs -rm -r -f /user/root/input/$name/giraph.txt
        hdfs dfs -put "${name}_mr.txt" /user/root/input/$name/mr.txt
        hdfs dfs -put "${name}_giraph.txt" /user/root/input/$name/giraph.txt
    else
        echo "⚠️ Warning: File $raw_file not found."
    fi
done

# ================= 2. 实验循环 =================
for name in "${!DATASETS[@]}"; do
    hdfs dfs -test -e /user/root/input/$name/mr.txt
    if [ $? -ne 0 ]; then continue; fi

    for algo in "giraph" "mapreduce"; do
        for run in {1..3}; do
            echo "=========================================================="
            echo "Running $algo on $name (Run $run/3)"
            echo "=========================================================="

            stop-all.sh > /dev/null 2>&1
            sleep 5
            start-all.sh > /dev/null 2>&1
            sleep 10
            hdfs dfsadmin -safemode leave > /dev/null 2>&1

            JOB_ID="${algo}_${name}_${run}"
            LOG="$RESULT_DIR/logs/${JOB_ID}.log"
            MON="$RESULT_DIR/logs/${JOB_ID}_mon.log"
            hdfs dfs -rm -r /user/root/output/$JOB_ID > /dev/null 2>&1
            
            sar -u -r -n DEV 1 > $MON 2>&1 &
            MON_PID=$!

            start_ts=$(date +%s)

            if [ "$algo" == "giraph" ]; then
                # 🔴 修改点：将 maxSupersteps 和 SplitMasterWorker 合并，用逗号分隔
                # 这样可以确保参数传递给 Giraph 配置
                hadoop jar $GIRAPH_JAR \
                    org.apache.giraph.GiraphRunner \
                    PageRankGiraph \
                    -mc PageRankMasterCompute \
                    -vif org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat \
                    -vip /user/root/input/$name/giraph.txt \
                    -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat \
                    -op /user/root/output/$JOB_ID \
                    -w 1 \
                    -ca giraph.SplitMasterWorker=false,giraph.maxSupersteps=500 > $LOG 2>&1
            else
                hadoop jar pagerank-compare.jar \
                    PageRankMapReduce \
                    /user/root/input/$name/mr.txt \
                    /user/root/output/$JOB_ID > $LOG 2>&1
            fi

            end_ts=$(date +%s)
            duration=$((end_ts - start_ts))
            kill $MON_PID

            # 🔴 修改点：增加 Total Diff 关键字抓取，作为 final_diff 的保底
            if [ "$algo" == "giraph" ]; then
                echo ">>> Fetching logs..."
                APP_ID=$(grep -o "application_[0-9_]*" $LOG | head -1)
                if [ -n "$APP_ID" ]; then
                    for node in master slave1 slave2; do
                        ssh $node "grep -r -E 'finishSuperstep|Final Diff|Total Diff' /usr/local/hadoop/logs/userlogs/$APP_ID 2>/dev/null" >> $LOG
                    done
                fi
            fi

            echo ">>> Parsing results..."
            java -cp classes LogAnalyzer $algo $name $JOB_ID $LOG $MON $duration >> $CSV_FILE
            echo ">>> Finished in ${duration}s"
        done
    done
done
echo "✅ All Done! Check $CSV_FILE"