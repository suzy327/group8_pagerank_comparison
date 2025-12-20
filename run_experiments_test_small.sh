#!/bin/bash

# ================= 配置区 =================
DATA_DIR="code/data" 
# =========================================

RESULT_DIR="test_results_small"
CSV_FILE="$RESULT_DIR/metrics.csv"
mkdir -p $RESULT_DIR/logs

# 初始化 CSV
echo "algorithm,dataset,job_id,total_execution_time,avg_iteration_time,network_communication,peak_memory_usage,iterations_to_converge,convergence_rate,cpu_utilization" > $CSV_FILE

# 环境变量
export HADOOP_CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath):$(pwd)/pagerank-compare.jar
export GIRAPH_JAR=/usr/local/hadoop/share/hadoop/yarn/lib/giraph.jar

# 🔴 只定义 small 一个数据集
declare -A DATASETS
DATASETS=( ["small"]="cit-HepPh.txt" )

# 1. 预处理
echo ">>> Step 1: Preprocessing Data..."
for name in "${!DATASETS[@]}"; do
    raw_file="$DATA_DIR/${DATASETS[$name]}"
    if [ -f "$raw_file" ]; then
        echo "Processing $name..."
        java -cp classes DataPreprocess "$raw_file" "${name}_mr.txt" "${name}_giraph.txt"
        
        hdfs dfs -mkdir -p /user/root/input/$name
        hdfs dfs -put -f "${name}_mr.txt" /user/root/input/$name/mr.txt
        hdfs dfs -put -f "${name}_giraph.txt" /user/root/input/$name/giraph.txt
    else
        echo "⚠️ Error: $raw_file not found!"
        exit 1
    fi
done

# 2. 运行循环 (只跑 1 次用于测试)
for name in "${!DATASETS[@]}"; do
    for algo in "giraph" ; do
        # 🔴 这里改成只跑 1 次
        for run in 1; do
            echo "=========================================================="
            echo "TEST RUN: $algo on $name"
            echo "=========================================================="

            echo ">>> Restarting Cluster..."
            stop-all.sh > /dev/null 2>&1
            sleep 5
            start-all.sh > /dev/null 2>&1
            sleep 10
            hdfs dfsadmin -safemode leave > /dev/null 2>&1

            JOB_ID="${algo}_${name}_test"
            LOG="$RESULT_DIR/logs/${JOB_ID}.log"
            MON="$RESULT_DIR/logs/${JOB_ID}_mon.log"
            hdfs dfs -rm -r /user/root/output/$JOB_ID > /dev/null 2>&1
            
            sar -u -r 1 > $MON 2>&1 &
            MON_PID=$!

            start_ts=$(date +%s)

            if [ "$algo" == "giraph" ]; then
                hadoop jar $GIRAPH_JAR \
                    org.apache.giraph.GiraphRunner \
                    PageRankGiraph \
                    -mc PageRankMasterCompute \
                    -vif org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat \
                    -vip /user/root/input/$name/giraph.txt \
                    -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat \
                    -op /user/root/output/$JOB_ID \
                    -w 1 -ca giraph.SplitMasterWorker=false > $LOG 2>&1
            else
                hadoop jar pagerank-compare.jar \
                    PageRankMapReduce \
                    /user/root/input/$name/mr.txt \
                    /user/root/output/$JOB_ID > $LOG 2>&1
            fi

            end_ts=$(date +%s)
            duration=$((end_ts - start_ts))
            kill $MON_PID

            # --- E. 抓取从节点日志 (关键步骤) ---  
            # MapReduce 不需要这一步，因为它打印在 Driver 上
            # Giraph 需要去 Slave 节点抓取 finishSuperstep 日志
            if [ "$algo" == "giraph" ]; then
                echo ">>> Hunting for Giraph logs on Slave nodes..."
                
                # 从主日志中提取 Application ID
                APP_ID=$(grep -o "application_[0-9_]*" $LOG | head -1)
                
                if [ -n "$APP_ID" ]; then
                    # 遍历所有节点 (Master, Slave1, Slave2)
                    for node in master slave1 slave2; do
                        echo "    Fetching from $node..."
                        # 远程执行 grep，查找 finishSuperstep，并将结果追加到本地 LOG 文件
                        # 2>/dev/null 用于屏蔽 SSH 的连接警告信息
                        ssh $node "grep -r 'finishSuperstep' /usr/local/hadoop/logs/userlogs/$APP_ID 2>/dev/null" >> $LOG
                    done
                else
                    echo "    ⚠️ Warning: Could not find Application ID, skipping log fetch."
                fi
            fi

            echo ">>> Parsing results..."
            java -cp classes LogAnalyzer $algo $name $JOB_ID $LOG $MON $duration >> $CSV_FILE
            
            echo ">>> Finished in ${duration}s"
        done
    done
done

echo "✅ Test Completed! Check $CSV_FILE"
