import csv
import re
import math

# Dataset sizes (Edge counts)
# Small: 352,807
# Medium: 1,768,149
# Large: 7,600,595
DATASET_EDGES = {
    'small': 352807,
    'medium': 1768149,
    'large': 7600595
}

def parse_value(val):
    if isinstance(val, str):
        # Remove units and convert to float
        clean = re.sub(r'[^\d\.]', '', val)
        try:
            return float(clean)
        except ValueError:
            return 0.0
    return val

def load_data(filepath):
    data = {}
    with open(filepath, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            algo = row['algorithm']
            dataset = row['dataset']
            key = (algo, dataset)
            
            if key not in data:
                data[key] = {
                    'total_execution_time': [],
                    'avg_iteration_time': [],
                    'network_communication': [],
                    'peak_memory_usage': []
                }
            
            data[key]['total_execution_time'].append(parse_value(row['total_execution_time']))
            data[key]['avg_iteration_time'].append(parse_value(row['avg_iteration_time']))
            data[key]['network_communication'].append(parse_value(row['network_communication']))
            data[key]['peak_memory_usage'].append(parse_value(row['peak_memory_usage']))
    
    # Calculate averages
    averaged_data = {}
    for key, metrics in data.items():
        averaged_data[key] = {k: sum(v)/len(v) for k, v in metrics.items()}
    
    return averaged_data

def analyze():
    data = load_data('/root/group8_pagerank_comparison/experiment_results/metrics.csv')
    datasets = ['small', 'medium', 'large']

    print("=== 1. 执行时间 vs 图规模分析 (数值拟合 Log-Log Slope) ===")
    print(f"{'Dataset':<10} | {'Edges':<12} | {'Giraph Time(s)':<15} | {'MR Time(s)':<15} | {'Giraph Slope':<12} | {'MR Slope':<12}")
    print("-" * 90)
    
    prev_edges = 0
    prev_g_time = 0
    prev_m_time = 0
    
    for ds in datasets:
        edges = DATASET_EDGES[ds]
        g_time = data[('giraph', ds)]['total_execution_time']
        m_time = data[('mapreduce', ds)]['total_execution_time']
        
        g_slope = "N/A"
        m_slope = "N/A"
        
        if prev_edges > 0:
            # Calculate log-log slope: log(T2/T1) / log(E2/E1)
            # Slope represents the exponent alpha in Time ~ Edges^alpha
            edge_log_diff = math.log(edges) - math.log(prev_edges)
            if edge_log_diff != 0:
                g_slope = f"{(math.log(g_time) - math.log(prev_g_time)) / edge_log_diff:.2f}"
                m_slope = f"{(math.log(m_time) - math.log(prev_m_time)) / edge_log_diff:.2f}"
        
        print(f"{ds:<10} | {edges:<12} | {g_time:<15.2f} | {m_time:<15.2f} | {g_slope:<12} | {m_slope:<12}")
        
        prev_edges = edges
        prev_g_time = g_time
        prev_m_time = m_time
    print("* Slope < 1 表示随着规模增大，单位效率在提升（固定开销被摊薄）。")
    
    print("\n=== 2. 通信量增长倍数 (MapReduce / Giraph) ===")
    print(f"{'Dataset':<10} | {'Giraph (MB)':<15} | {'MapReduce (MB)':<15} | {'Ratio (MR/Giraph)':<20}")
    print("-" * 70)
    
    for ds in datasets:
        g_net = data[('giraph', ds)]['network_communication']
        m_net = data[('mapreduce', ds)]['network_communication']
        ratio = m_net / g_net if g_net > 0 else 0
        print(f"{ds:<10} | {g_net:<15.2f} | {m_net:<15.2f} | {ratio:<20.2f}")
    
    print("\n=== 3. 迭代时间增长曲线分析 (Giraph) ===")
    print(f"{'Dataset':<10} | {'Edges':<12} | {'Avg Iter Time (s)':<20} | {'Time/1M Edges (s)':<20}")
    print("-" * 70)
    
    prev_time = 0
    prev_edges = 0
    
    for ds in datasets:
        edges = DATASET_EDGES[ds]
        time = data[('giraph', ds)]['avg_iteration_time']
        normalized = time / (edges / 1_000_000)
        print(f"{ds:<10} | {edges:<12} | {time:<20.4f} | {normalized:<20.4f}")
        
    print("\n=== 4. 内存使用效率评估 ===")
    print(f"{'Dataset':<10} | {'Giraph Mem (GB)':<15} | {'MR Mem (GB)':<15} | {'Mem Increase':<15} | {'Speedup (Time)':<15}")
    print("-" * 80)
    
    for ds in datasets:
        g_mem = data[('giraph', ds)]['peak_memory_usage']
        m_mem = data[('mapreduce', ds)]['peak_memory_usage']
        g_time = data[('giraph', ds)]['total_execution_time']
        m_time = data[('mapreduce', ds)]['total_execution_time']
        
        mem_ratio = g_mem / m_mem
        speedup = m_time / g_time
        
        print(f"{ds:<10} | {g_mem:<15.2f} | {m_mem:<15.2f} | {mem_ratio:<15.2f} | {speedup:<15.2f}")

if __name__ == "__main__":
    analyze()
