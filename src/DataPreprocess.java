import java.io.*;
import java.util.*;

public class DataPreprocess {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Usage: java DataPreprocess <inputFile> <mrOutput> <giraphOutput>");
            return;
        }
        String inputFile = args[0];
        String mrOutput = args[1];
        String giraphOutput = args[2];

        System.out.println("Processing " + inputFile + "...");
        // 使用 Map 存储邻接表
        Map<String, List<String>> graph = new HashMap<>();

        try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("#")) continue;
                String[] parts = line.trim().split("\\s+");
                if (parts.length < 2) continue;
                
                String src = parts[0];
                String dst = parts[1];
                
                graph.putIfAbsent(src, new ArrayList<>());
                graph.get(src).add(dst);
                graph.putIfAbsent(dst, new ArrayList<>()); // 确保目标点存在
            }
        }

        System.out.println("Nodes: " + graph.size() + ". Writing outputs...");

        // 写 MR 文件
        try (PrintWriter pw = new PrintWriter(mrOutput)) {
            for (Map.Entry<String, List<String>> e : graph.entrySet()) {
                String neighbors = String.join(",", e.getValue());
                // 格式: ID \t 1.0 \t 邻居1,邻居2
                pw.println(e.getKey() + "\t1.0\t" + neighbors);
            }
        }

        // 写 Giraph JSON 文件
        try (PrintWriter pw = new PrintWriter(giraphOutput)) {
            for (Map.Entry<String, List<String>> e : graph.entrySet()) {
                StringBuilder sb = new StringBuilder();
                // 格式: [ID, 1.0, [[Neighbor1, 0], [Neighbor2, 0]]]
                sb.append("[").append(e.getKey()).append(",1.0,[");
                List<String> neighbors = e.getValue();
                for (int i = 0; i < neighbors.size(); i++) {
                    sb.append("[").append(neighbors.get(i)).append(",0]");
                    if (i < neighbors.size() - 1) sb.append(",");
                }
                sb.append("]]");
                pw.println(sb.toString());
            }
        }
        System.out.println("✅ Conversion done.");
    }
}