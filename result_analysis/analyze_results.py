import csv
import io
import re

# Data from the file
csv_data = """algorithm,dataset,job_id,total_execution_time,avg_iteration_time,network_communication,peak_memory_usage,iterations_to_converge,final_diff,cpu_utilization
giraph,small,giraph_small_1,36.00s,0.82s,1872.22MB,3.07GB,44,0.000000,12.59%
giraph,small,giraph_small_2,35.00s,0.80s,1892.99MB,3.39GB,44,0.000000,20.70%
giraph,small,giraph_small_3,38.00s,0.86s,2061.89MB,2.64GB,44,0.000000,13.83%
mapreduce,small,mapreduce_small_1,959.00s,21.80s,52342.92MB,3.46GB,44,0.000080,13.36%
mapreduce,small,mapreduce_small_2,953.00s,21.66s,52429.22MB,4.04GB,44,0.000080,12.11%
mapreduce,small,mapreduce_small_3,951.00s,21.61s,52678.33MB,3.85GB,44,0.000080,11.44%
giraph,medium,giraph_medium_1,60.00s,0.62s,3328.21MB,2.99GB,97,0.000000,8.66%
giraph,medium,giraph_medium_2,63.00s,0.65s,3500.17MB,2.95GB,97,0.000000,8.67%
giraph,medium,giraph_medium_3,61.00s,0.63s,3394.06MB,2.92GB,97,0.000000,8.76%
mapreduce,medium,mapreduce_medium_1,2813.00s,30.25s,159414.25MB,4.03GB,93,0.000080,11.06%
mapreduce,medium,mapreduce_medium_2,2829.00s,30.42s,140124.58MB,4.63GB,93,0.000080,11.19%
mapreduce,medium,mapreduce_medium_3,2814.00s,30.26s,103839.57MB,4.36GB,93,0.000080,10.76%
giraph,large,giraph_large_1,210.00s,1.78s,7421.33MB,2.91GB,118,0.000000,4.04%
giraph,large,giraph_large_2,205.00s,1.74s,7387.48MB,3.18GB,118,0.000000,5.36%
giraph,large,giraph_large_3,228.00s,1.93s,8264.55MB,3.62GB,118,0.000000,46.31%
mapreduce,large,mapreduce_large_1,4900.00s,48.51s,157844.00MB,3.44GB,101,0.000070,12.37%
mapreduce,large,mapreduce_large_2,4874.00s,48.26s,79561.97MB,4.08GB,101,0.000070,11.42%
mapreduce,large,mapreduce_large_3,4891.00s,48.43s,42640.25MB,3.70GB,101,0.000070,12.20%
"""

def clean_val(x):
    if isinstance(x, str):
        return float(re.sub(r'[^\d\.]', '', x))
    return x

data = []
reader = csv.DictReader(io.StringIO(csv_data))
for row in reader:
    cleaned_row = {}
    for k, v in row.items():
        if k in ['total_execution_time', 'avg_iteration_time', 'network_communication', 'peak_memory_usage', 'cpu_utilization']:
            cleaned_row[k] = clean_val(v)
        else:
            cleaned_row[k] = v
    data.append(cleaned_row)

# Group by dataset and algorithm
grouped = {}
for row in data:
    key = (row['dataset'], row['algorithm'])
    if key not in grouped:
        grouped[key] = {'total_execution_time': [], 'avg_iteration_time': [], 'network_communication': [], 'peak_memory_usage': []}
    
    grouped[key]['total_execution_time'].append(row['total_execution_time'])
    grouped[key]['avg_iteration_time'].append(row['avg_iteration_time'])
    grouped[key]['network_communication'].append(row['network_communication'])
    grouped[key]['peak_memory_usage'].append(row['peak_memory_usage'])

# Calculate averages
averages = {}
for key, metrics in grouped.items():
    averages[key] = {k: sum(v)/len(v) for k, v in metrics.items()}

print("=== Average Metrics by Dataset and Algorithm ===")
print(f"{'Dataset':<10} | {'Algorithm':<10} | {'Total Time':<12} | {'Avg Iter Time':<15} | {'Network (MB)':<15} | {'Memory (GB)':<12}")
print("-" * 90)

datasets = ['small', 'medium', 'large']
for ds in datasets:
    for algo in ['giraph', 'mapreduce']:
        key = (ds, algo)
        if key in averages:
            avg = averages[key]
            print(f"{ds:<10} | {algo:<10} | {avg['total_execution_time']:<12.2f} | {avg['avg_iteration_time']:<15.2f} | {avg['network_communication']:<15.2f} | {avg['peak_memory_usage']:<12.2f}")

print("\n=== Performance Comparison (MapReduce / Giraph Ratio) ===")
print(f"{'Dataset':<10} | {'Time Ratio':<12} | {'Iter Time Ratio':<15} | {'Network Ratio':<15} | {'Memory Ratio':<12}")
print("-" * 75)

for ds in datasets:
    giraph_key = (ds, 'giraph')
    mr_key = (ds, 'mapreduce')
    
    if giraph_key in averages and mr_key in averages:
        g_avg = averages[giraph_key]
        m_avg = averages[mr_key]
        
        time_ratio = m_avg['total_execution_time'] / g_avg['total_execution_time']
        iter_ratio = m_avg['avg_iteration_time'] / g_avg['avg_iteration_time']
        net_ratio = m_avg['network_communication'] / g_avg['network_communication']
        mem_ratio = m_avg['peak_memory_usage'] / g_avg['peak_memory_usage']
        
        print(f"{ds:<10} | {time_ratio:<12.2f} | {iter_ratio:<15.2f} | {net_ratio:<15.2f} | {mem_ratio:<12.2f}")
