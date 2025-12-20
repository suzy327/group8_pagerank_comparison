import java.io.*;
import java.util.*;

public class DataPreprocess {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Usage: java DataPreprocess <input_file>");
            return;
        }

        File inputFile = new File(args[0]);
        if (!inputFile.exists()) {
            System.out.println("❌ Error: File not found: " + args[0]);
            return;
        }

        // 1. 读取数据并构建图
        System.out.println("Reading " + inputFile.getName() + "...");
        Map<String, List<String>> graph = new HashMap<>();
        
        try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("#")) continue;
                String[] parts = line.trim().split("\\s+");
                if (parts.length < 2) continue;
                
                String src = parts[0];
                String dst = parts[1];
                
                if (!graph.containsKey(src)) graph.put(src, new ArrayList<String>());
                graph.get(src).add(dst);
                
                // 确保目标节点也存在于图中（即使没有出边）
                if (!graph.containsKey(dst)) graph.put(dst, new ArrayList<String>());
            }
        }

        System.out.println("Total nodes: " + graph.size());

        // 2. 生成 MapReduce 输入文件
        System.out.println("Generating mr_input.txt...");
        try (PrintWriter pw = new PrintWriter("mr_input.txt")) {
            for (Map.Entry<String, List<String>> entry : graph.entrySet()) {
                String node = entry.getKey();
                List<String> neighbors = entry.getValue();
                String neighborsStr = String.join(",", neighbors);
                pw.println(node + "\t1.0\t" + neighborsStr);
            }
        }

        // 3. 生成 Giraph 输入文件 (JSON格式)
        System.out.println("Generating giraph_input.txt...");
        try (PrintWriter pw = new PrintWriter("giraph_input.txt")) {
            for (Map.Entry<String, List<String>> entry : graph.entrySet()) {
                String node = entry.getKey();
                List<String> neighbors = entry.getValue();
                
                StringBuilder sb = new StringBuilder();
                sb.append("[").append(node).append(",1.0,[");
                for (int i = 0; i < neighbors.size(); i++) {
                    sb.append("[").append(neighbors.get(i)).append(",0]"); // 边权重设为0
                    if (i < neighbors.size() - 1) sb.append(",");
                }
                sb.append("]]");
                pw.println(sb.toString());
            }
        }

        System.out.println("✅ Data preprocessing done!");
    }
}