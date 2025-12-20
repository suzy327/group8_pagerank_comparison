#!/bin/bash

# ================= 配置修改区 =================
# 🔴 数据集所在路径
DATA_DIR="code/data" 
# ===========================================

RESULT_DIR="experiment_results"
CSV_FILE="$RESULT_DIR/metrics.csv"
mkdir -p $RESULT_DIR/logs

# 初始化 CSV 表头
echo "algorithm,dataset,job_id,total_execution_time,avg_iteration_time,network_communication,peak_memory_usage,iterations_to_converge,convergence_rate,cpu_utilization" > $CSV_FILE

# 准备 Jar 包环境
export HADOOP_CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath):$(pwd)/pagerank-compare.jar
export GIRAPH_JAR=/usr/local/hadoop/share/hadoop/yarn/lib/giraph.jar

# 定义数据集 (Name : FileName)
declare -A DATASETS
DATASETS=( ["small"]="cit-HepPh.txt" ["medium"]="twitter_combined.txt" ["large"]="web-BerkStan.txt" )

# ================= 1. 预处理数据 =================
echo ">>> Step 1: Preprocessing Data (Java)..."
for name in "${!DATASETS[@]}"; do
    raw_file="$DATA_DIR/${DATASETS[$name]}"
    
    # 检查本地源文件是否存在
    if [ -f "$raw_file" ]; then
        echo "Processing $name ($raw_file)..."
        # 调用 Java 工具转换数据格式
        java -cp classes DataPreprocess "$raw_file" "${name}_mr.txt" "${name}_giraph.txt"
        
        # 1. 确保目录存在
        hdfs dfs -mkdir -p /user/root/input/$name
        
        # 2. 🔴 修改处：先强制删除旧文件 (防止覆盖失败或残留)
        echo "    Cleaning up old files in HDFS..."
        hdfs dfs -rm -r -f /user/root/input/$name/mr.txt
        hdfs dfs -rm -r -f /user/root/input/$name/giraph.txt

        # 3. 上传新文件
        echo "    Uploading new files..."
        hdfs dfs -put "${name}_mr.txt" /user/root/input/$name/mr.txt
        hdfs dfs -put "${name}_giraph.txt" /user/root/input/$name/giraph.txt
    else
        echo "⚠️ Warning: Local file $raw_file not found, skipping..."
    fi
done

# ================= 2. 开始实验循环 =================
# 顺序: Small -> Medium -> Large
for name in "${!DATASETS[@]}"; do
    # 再次检查 HDFS 数据是否存在，不存在则跳过该数据集
    hdfs dfs -test -e /user/root/input/$name/mr.txt
    if [ $? -ne 0 ]; then 
        echo "❌ HDFS data for $name missing, skipping experiment."
        continue
    fi

    for algo in "giraph" "mapreduce"; do
        # 每个实验重复 3 次
        for run in {1..3}; do
            echo "=========================================================="
            echo "Running $algo on $name (Run $run/3)"
            echo "=========================================================="

            # --- A. 重启集群 (确保内存清理干净，实验环境纯净) ---
            echo ">>> Restarting Cluster..."
            stop-all.sh > /dev/null 2>&1
            sleep 5
            start-all.sh > /dev/null 2>&1
            sleep 10
            hdfs dfsadmin -safemode leave > /dev/null 2>&1

            # --- B. 准备日志和输出路径 ---
            JOB_ID="${algo}_${name}_${run}"
            LOG="$RESULT_DIR/logs/${JOB_ID}.log"
            MON="$RESULT_DIR/logs/${JOB_ID}_mon.log"
            
            # 删除旧的输出目录 (此处也保留了清理逻辑)
            hdfs dfs -rm -r /user/root/output/$JOB_ID > /dev/null 2>&1
            
            # --- C. 启动监控 (关键: -n DEV 用于抓取网络流量) ---
            sar -u -r -n DEV 1 > $MON 2>&1 &
            MON_PID=$!

            start_ts=$(date +%s)

            # --- D. 执行任务 ---
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
            
            # 停止监控
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

            # --- F. Java解析结果并写入CSV ---
            echo ">>> Parsing results..."
            java -cp classes LogAnalyzer $algo $name $JOB_ID $LOG $MON $duration >> $CSV_FILE
            
            echo ">>> Finished in ${duration}s"
        done
    done
done

echo "✅ All Done! Check $CSV_FILE"