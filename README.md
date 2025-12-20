# group8_pagerank_comparison
PageRank算法实现比较


# 1. 清理 HDFS 上的旧目录 (防止报错)
hdfs dfs -rm -r /user/root/input

# 2. 创建新目录
hdfs dfs -mkdir -p /user/root/input

# 3. 上传文件
hdfs dfs -put mr_input.txt /user/root/input/
hdfs dfs -put giraph_input.txt /user/root/input/