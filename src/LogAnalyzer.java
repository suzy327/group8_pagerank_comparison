import java.io.*;
import java.util.*;
import java.util.regex.*;

public class LogAnalyzer {
    public static void main(String[] args) throws Exception {
        if (args.length < 6) return;
        
        String algo = args[0];
        String dataset = args[1];
        String jobId = args[2];
        String logFile = args[3];
        String monitorFile = args[4];
        double totalTime = Double.parseDouble(args[5]);

        int iterations = 0;
        double networkMB = 0.0;
        double cpuUtil = 0.0;
        double peakMemGB = 0.0;
        double finalDiff = 0.0; 
        boolean foundFinal = false; // 标记是否找到了标准的 Final Diff

        // 1. 分析作业日志
        try (BufferedReader br = new BufferedReader(new FileReader(logFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                // 🔴 优先抓取 "Final Diff" (收敛成功时)
                if (line.contains("Final Diff:")) {
                     Matcher m = Pattern.compile("Final Diff:\\s*([0-9\\.E-]+)").matcher(line);
                     if (m.find()) {
                         finalDiff = Double.parseDouble(m.group(1));
                         foundFinal = true;
                     }
                }
                // 🔴 保底抓取 "Total Diff" (如果任务被强制kill，就用这个值)
                // 只有在还没找到 Final Diff 的情况下才不断更新这个值
                else if (!foundFinal && line.contains("Total Diff:")) {
                     Matcher m = Pattern.compile("Total Diff:\\s*([0-9\\.E-]+)").matcher(line);
                     if (m.find()) finalDiff = Double.parseDouble(m.group(1));
                }

                if (algo.equals("giraph")) {
                    String lowerLine = line.toLowerCase();
                    if (lowerLine.contains("superstep") && (lowerLine.contains("finish") || lowerLine.contains("completed"))) {
                        Matcher m = Pattern.compile("superstep\\s*[:=]?\\s*(-?\\d+)").matcher(lowerLine);
                        if (m.find()) {
                            int currentStep = Integer.parseInt(m.group(1));
                            if (currentStep > iterations) iterations = currentStep;
                        }
                    }
                } else {
                    if (line.contains("completed successfully")) iterations++;
                    // MapReduce 保底抓取
                    if (!foundFinal && line.contains("Diff:")) {
                        Matcher m = Pattern.compile("Diff:\\s*([0-9\\.E-]+)").matcher(line);
                        if (m.find()) finalDiff = Double.parseDouble(m.group(1));
                    }
                }
            }
        }

        if (iterations == 0) iterations = 1;
        double avgIterTime = totalTime / iterations;

        // 2. 分析监控日志 (SAR)
        List<Double> cpus = new ArrayList<>();
        List<Double> mems = new ArrayList<>();
        List<Double> netRates = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(monitorFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.trim().isEmpty() || line.contains("Linux") || line.contains("Average") || line.contains("IFACE")) continue;
                String[] p = line.trim().split("\\s+");
                int valStart = 1; 
                if (p.length > 1 && (p[1].equals("AM") || p[1].equals("PM"))) valStart = 2;

                int allIndex = -1;
                for(int i=0; i<p.length; i++) if(p[i].equals("all")) allIndex = i;
                if (allIndex != -1 && p.length > allIndex + 3) {
                    try { cpus.add(Double.parseDouble(p[allIndex + 1]) + Double.parseDouble(p[allIndex + 3])); } catch (Exception e) {}
                }
                if (allIndex == -1 && !line.contains("CPU") && p.length > valStart + 2) {
                    try {
                        double kbUsed = Double.parseDouble(p[valStart + 2]);
                        if (kbUsed > 1024) mems.add(kbUsed / (1024.0 * 1024.0)); 
                    } catch (Exception e) {}
                }
                if (!line.contains("CPU") && p.length > valStart + 5) {
                    if (!p[valStart].equals("lo")) { 
                        try { netRates.add(Double.parseDouble(p[valStart + 3]) + Double.parseDouble(p[valStart + 4])); } catch (Exception e) {}
                    }
                }
            }
        } catch (Exception e) {}

        if (!cpus.isEmpty()) cpuUtil = cpus.stream().mapToDouble(d->d).average().orElse(0.0);
        if (!mems.isEmpty()) peakMemGB = mems.stream().mapToDouble(d->d).max().orElse(0.0);
        if (!netRates.isEmpty()) networkMB = (netRates.stream().mapToDouble(d->d).average().orElse(0.0) * totalTime) / 1024.0;

        // 3. 输出 CSV
        System.out.printf("%s,%s,%s,%.2fs,%.2fs,%.2fMB,%.2fGB,%d,%.6f,%.2f%%%n",
            algo, dataset, jobId, totalTime, avgIterTime, networkMB, peakMemGB, iterations, finalDiff, cpuUtil);
    }
}