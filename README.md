# PageRank 算法实现比较

## 研究目的
本实验旨在比较 Apache Giraph (基于 BSP 模型) 和 Hadoop MapReduce 在运行 PageRank 算法时的差异。通过实际部署与对比，深入理解 BSP (Bulk Synchronous Parallel) 模型的设计理念及其在图计算中的优势。

## 研究内容
* **对比分析**：对比 Giraph 和 MapReduce 在执行 PageRank 等图迭代计算任务时的差异。
* **机制探讨**：重点探讨两者在数据通信方式、任务调度机制及迭代开销等方面的不同。
* **性能评估**：分析这些差异对算法性能（执行时间、内存占用）与可扩展性（网络通信量）的具体影响。

## 实验

### 实验环境

#### 硬件配置
集群由 3 台阿里云 ECS 服务器组成，配置如下：

| 节点角色 | 实例规格 | vCPU | 内存 | 存储 | 带宽 |
| :--- | :--- | :--- | :--- | :--- | :--- |
| Master | ecs.u1-c1m2.xlarge | 4 vCPU | 8 GiB | 40GiB ESSD | 100Mbps |
| Slave1 | ecs.u1-c1m2.xlarge | 4 vCPU | 8 GiB | 40GiB ESSD | 100Mbps |
| Slave2 | ecs.u1-c1m2.xlarge | 4 vCPU | 8 GiB | 40GiB ESSD | 100Mbps |

#### 软件环境
* **操作系统**: Ubuntu 20.04 64位 (安全加固)
* **JDK 版本**: Java 8
* **Hadoop 版本**: Hadoop 3.x (配置为 Yarn 模式)
* **Giraph 版本**: Apache Giraph 1.4.0-SNAPSHOT (针对 Hadoop 3 进行源码修改编译)
* **ZooKeeper**: 集成在 Giraph 中，已解决版本兼容性问题

### 实验负载

#### 数据集 (Datasets)
本实验选用不同规模的图数据集以测试系统的可扩展性：

| 数据集名称 | 规模 | 节点数 | 边数 | 特点 |
| :--- | :--- | :--- | :--- | :--- |
| **Small** (Cit-HepPh) | 小规模 | 27,770 | 352,807 | 高能物理论文引用网络，天然有向图  |
| **Medium** (Twitter) | 中等规模 | 81,306 | 1,768,149 | 社交关注网络，非对称结构  |
| **Large** (Web-BerkStan) | 大规模 | 685,230 | 7,600,595 | 伯克利-斯坦福网页链接图，用于测试 Shuffle 瓶颈  |

#### 指标定义表
<img width="662" height="895" alt="屏幕截图 2025-12-24 172847" src="https://github.com/user-attachments/assets/73792511-11f0-4cf8-9d3b-20c5b69caebf" />

#### 关键指标 (Metrics)
1.  **Total Execution Time**: 作业总执行时间。
2.  **Avg Iteration Time**: 单次迭代平均耗时，验证 BSP 模型优势。
3.  **Network Communication**: 网络通信量，验证数据流处理差异。
4.  **Peak Memory Usage**: 峰值内存占用。

#### 实验约束
* **收敛阈值**: 统一设定为 `0.0001`。
* **重复次数**: 每组实验重复运行 **3次** 取平均值。

### 实验步骤

1.  配置 Hadoop 集群（Master + 2 Slaves）及 SSH 免密登录。
2.  针对 Hadoop 3.x 移除 SASL 属性及 ZooKeeper 版本不一致问题，修改 Giraph 源码（`SaslNettyClient.java`, `InProcessZooKeeperRunner.java` 等）并重新编译。
3.  将编译好的 Giraph Jar 包分发至所有节点的 Hadoop Yarn Lib 目录，解决 ClassNotFound 问题。

> **截图说明**：下图展示了 Hadoop 集群启动后的 JPS 进程状态（Master包含NameNode/ResourceManager，Slave包含DataNode/NodeManager）。
>
> <img width="710" height="284" alt="a66758ea-ed26-42b2-b861-f9e7eaff0471" src="https://github.com/user-attachments/assets/6a4e484a-0b30-4ae1-968e-c265a42384a7" />
> <img width="354" height="184" alt="077aaa62-1e51-4667-a9dc-f738a634bc72" src="https://github.com/user-attachments/assets/65133a31-49f5-419d-a3ef-b1d2e27a5cfe" />
> <img width="308" height="166" alt="6569115b-cee7-427a-95a7-1a8ed9ce0600" src="https://github.com/user-attachments/assets/7e28d917-f5f7-445a-844b-78ab83cd3786" />

#### 运行日志

```
root@master:~/group8_pagerank_comparison# hdfs dfs -ls /user/root/output/
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/usr/local/hadoop/share/hadoop/common/lib/slf4j-reload4j-1.7.36.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/usr/local/hadoop/share/hadoop/yarn/lib/giraph.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.slf4j.impl.Reload4jLoggerFactory]
Found 18 items
drwxr-xr-x   - root supergroup          0 2025-12-21 03:28 /user/root/output/giraph_large_1
drwxr-xr-x   - root supergroup          0 2025-12-21 03:32 /user/root/output/giraph_large_2
drwxr-xr-x   - root supergroup          0 2025-12-21 03:37 /user/root/output/giraph_large_3
drwxr-xr-x   - root supergroup          0 2025-12-21 00:55 /user/root/output/giraph_medium_1
drwxr-xr-x   - root supergroup          0 2025-12-21 00:57 /user/root/output/giraph_medium_2
drwxr-xr-x   - root supergroup          0 2025-12-21 00:59 /user/root/output/giraph_medium_3
drwxr-xr-x   - root supergroup          0 2025-12-21 12:06 /user/root/output/giraph_small_{1}
drwxr-xr-x   - root supergroup          0 2025-12-21 00:01 /user/root/output/giraph_small_2
drwxr-xr-x   - root supergroup          0 2025-12-21 00:03 /user/root/output/giraph_small_3
drwxr-xr-x   - root supergroup          0 2025-12-21 04:59 /user/root/output/mapreduce_large_1
drwxr-xr-x   - root supergroup          0 2025-12-21 06:21 /user/root/output/mapreduce_large_2
drwxr-xr-x   - root supergroup          0 2025-12-21 07:43 /user/root/output/mapreduce_large_3
drwxr-xr-x   - root supergroup          0 2025-12-21 01:47 /user/root/output/mapreduce_medium_1
drwxr-xr-x   - root supergroup          0 2025-12-21 02:35 /user/root/output/mapreduce_medium_2
drwxr-xr-x   - root supergroup          0 2025-12-21 03:23 /user/root/output/mapreduce_medium_3
drwxr-xr-x   - root supergroup          0 2025-12-21 00:20 /user/root/output/mapreduce_small_1
drwxr-xr-x   - root supergroup          0 2025-12-21 00:36 /user/root/output/mapreduce_small_2
drwxr-xr-x   - root supergroup          0 2025-12-21 00:53 /user/root/output/mapreduce_small_3
```

#### 部分日志截图
<img width="1580" height="256" alt="84ecaddc-aa6c-41b0-aed1-977020d0afea" src="https://github.com/user-attachments/assets/65c3f2d6-0a84-4ce1-89d8-178a2f305840" />

#### 部分代码启动日志
<img width="1280" height="488" alt="df7b5f2c-3e08-454e-8330-6c83af8e4279" src="https://github.com/user-attachments/assets/a5928660-d8bd-4137-814f-0bd8dad5cf19" />

#### 主要代码：
1. MapReduce 实现 (PageRankMapReduce.java)
- 驱动模式 (Driver)：使用一个 while 循环在 main 函数中控制迭代。每一次迭代都是一个独立的 MapReduce Job。
- 收敛控制：利用 Hadoop 的 Counter（计数器）来累加全局误差（Total Diff）。Driver 读取计数器值，如果小于阈值或达到最大迭代次数（500次），则停止循环。
- 计算逻辑：
  - Mapper：不仅分发 PageRank 值，还传递图结构（struct:）和旧的 PageRank 值（old:）以便计算 Diff。
  - Reducer：聚合新的 PageRank 值，计算 abs(NewPR - OldPR) 并更新计数器。
2. Giraph 实现 (PageRankGiraph.java & PageRankMasterCompute.java)
- 模型：基于 BSP（Bulk Synchronous Parallel）模型，以顶点为中心。
- Worker 逻辑 (PageRankGiraph)：
  - 第 0 超步初始化。
  - 后续超步接收消息、计算新 PR 值、计算局部 Diff 并通过 aggregate("sum_diff", ...) 上报给 Master。
  - 根据 Master 计算的全局 Diff 判断是否 voteToHalt()（停止计算）。
- Master 逻辑 (PageRankMasterCompute)：
  - 注册聚合器 DoubleSumAggregator。
  - 在每个超步结束时检查全局 Diff，如果小于阈值（0.0001），则协调全体 Worker 停止计算。

### 实验结果与分析

<img width="1280" height="428" alt="ecf7416f-c58b-4dac-a4e0-533233b9c33e" src="https://github.com/user-attachments/assets/3a1a2dac-752f-4713-a0b2-a09c88a4a6c4" />

<img width="498" height="415" alt="779f712c-a88f-4096-a8d5-997581d02ba5" src="https://github.com/user-attachments/assets/8001002d-7e88-4b5b-b61f-83c1657e71d9" />

#### 执行时间 vs 图规模 (对数坐标分析)
- 现象: 在对数坐标系下，Giraph 的增长曲线斜率显著小于 MapReduce。
- 数据:
  - Small (350k 边): Giraph 36s vs MapReduce 954s (快 26倍)
  - Medium (1.7M 边): Giraph 61s vs MapReduce 2818s (快 46倍)
  - Large (7.6M 边): Giraph 214s vs MapReduce 4888s (快 23倍)
- 分析: Giraph 表现出更好的可扩展性。随着数据量增加，MapReduce 的 Shuffle 开销呈非线性增长，而 Giraph 仅随边数线性增加通信量。

<img width="452" height="377" alt="65cf0416-f573-44fc-b9e1-5caef6756d1c" src="https://github.com/user-attachments/assets/b746c4b8-a65f-4e56-86fe-2644ee641d87" />

#### 通信量增长倍数 (MapReduce / Giraph)
- 计算结果:
  - Small: 27.0 倍
  - Medium: 39.5 倍
  - Large: 12.1 倍
- 分析: MapReduce 必须在网络中传输图的拓扑结构，这部分数据占据了绝大部分带宽。Giraph 仅传输 PageRank 值，实现了极致的通信压缩。Large 数据集倍数下降可能是因为 MapReduce 在处理大规模数据时进行了更多的本地聚合或者 Giraph 的消息量随活跃顶点数增加而自然增长。

<img width="488" height="407" alt="511e17d9-c803-4432-b5dd-aa2bf61d2dd7" src="https://github.com/user-attachments/assets/163139e2-3b88-4319-b9dc-4c8a02240a8a" />

#### 迭代时间增长曲线 (Giraph 线性验证)
- 单位边处理时间 (Time per 1M Edges):
  - Small: 2.34 秒/百万边
  - Medium: 0.36 秒/百万边
  - Large: 0.24 秒/百万边
- 分析: Giraph 的单位边处理效率随规模增大而提高。在小图（Small）上，BSP 模型的同步开销（ZooKeeper Barrier、Worker 间握手）占据了主导地位，计算时间很短。随着图规模增大，固定开销被摊薄，Giraph 展现出了真实的线性计算能力。

<img width="461" height="384" alt="64e528c8-aa9b-4be6-bd2e-4c991c9a8642" src="https://github.com/user-attachments/assets/b7159d36-8996-4c4a-b6ac-27be7005df2f" />

#### 内存使用效率
- 对比: Giraph 的峰值内存实际上比 MapReduce 更低 (约为 MapReduce 的 70%-80%)。
  - Giraph: ~3.0 GB
  - MapReduce: ~3.8 - 4.3 GB
- 分析:通常认为 Giraph 是内存密集型，但在本实验的数据规模（千万级边）下，图数据完全可以放入内存。相比之下，MapReduce 启动的 JVM Container（Mapper/Reducer）本身就有较大的基础内存开销，且为了排序性能通常会分配较大的 Buffer。因此在图能够放入内存的前提下，Giraph 不仅速度快，资源利用率（内存效率）也高于 MapReduce。

### 结论

本实验通过在三种不同规模（Small, Medium, Large）的图数据集上部署并运行 PageRank 算法，详细对比了 Apache Giraph 与 Hadoop MapReduce 的性能表现。

#### 压倒性的性能优势
Giraph 在所有测试场景下均表现出显著的性能优势。
- 执行速度：在不同数据集上，Giraph 的总执行时间比 MapReduce 快 23倍 至 46倍。
- 迭代效率：随着图数据规模的增加，MapReduce 的单次迭代耗时呈非线性急剧增长，而 Giraph 保持了较好的线性增长趋势，证明了其优异的可扩展性。

#### 通信与 I/O 机制的本质差异
造成性能巨大差异的根本原因在于两者底层的计算模型与数据流处理方式不同：
- 网络通信 (Network Overhead)：
  - MapReduce：由于其无状态特性，每次迭代（Map -> Reduce）都必须通过网络传输完整的图拓扑结构（节点及其邻接表）以及 PageRank 值，导致网络通信量巨大（本实验中 MR 通信量是 Giraph 的 27倍 - 40倍）。
  - Giraph (BSP)：图结构常驻内存（Memory-resident），Worker 之间在 Superstep 边界仅需传输活跃节点的更新消息（数值），极大地降低了网络带宽压力。
- 磁盘 I/O (Disk I/O)：
  - MapReduce：每次迭代结束必须将结果写入 HDFS 磁盘，下一次迭代再读取，产生昂贵的磁盘 I/O 和序列化/反序列化开销。
  - Giraph (BSP)：利用长运行的 Worker 进程将图顶点状态保存在内存中，整个迭代过程主要在内存中进行，消除了中间结果的磁盘落地开销。

#### 资源利用与适用场景
内存利用率：在图数据能够完全装入内存的前提下，Giraph 相比启动大量 JVM Container 的 MapReduce 具有更高的资源利用效率（本实验中 Giraph 峰值内存约为 MR 的 70%-80%）。
- 适用性总结：
  - Giraph 是处理多轮迭代图算法（如 PageRank, SSSP, Connected Components）的最佳选择，尤其适用于对实时性要求较高且集群内存充足的场景。
  - MapReduce 更适合处理一次性扫描的离线批处理任务（如 ETL, 简单的统计分析），在图计算领域由于 Shuffle 瓶颈已不再适用。

### 分工
- 向鑫宇：前期调研（算法与平台介绍）、服务器环境配置 。
- 余富康：数据集准备、指标定义、GitHub 仓库创建与维护 。
- 刘泽源：实验代码实现与执行、数据采集 。
- 纪传昊：实验结果分析、结论撰写、演示视频制作

