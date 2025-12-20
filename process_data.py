cat << 'EOF' > preprocess.py
import sys, json, os

if len(sys.argv) < 2:
    print("Usage: python3 preprocess.py <input_file>")
    sys.exit(1)

input_file = sys.argv[1]
if not os.path.exists(input_file):
    print(f"❌ Error: File {input_file} not found!")
    sys.exit(1)

graph = {}
print(f"Reading {input_file}...")
with open(input_file, 'r') as f:
    for line in f:
        if line.startswith("#"): continue
        parts = line.strip().split()
        if len(parts) < 2: continue
        src, dst = parts[0], parts[1]
        if src not in graph: graph[src] = []
        graph[src].append(dst)
        if dst not in graph: graph[dst] = []

print("Generating MR input...")
with open('mr_input.txt', 'w') as f:
    for node, neighbors in graph.items():
        f.write(f"{node}\t1.0\t{','.join(neighbors)}\n")

print("Generating Giraph input...")
with open('giraph_input.txt', 'w') as f:
    for node, neighbors in graph.items():
        edges = [[int(n), 0] for n in neighbors]
        line = [int(node), 1.0, edges]
        f.write(json.dumps(line) + "\n")
print("✅ Done!")
EOF