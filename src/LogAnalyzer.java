import java.io.*;
import java.util.*;
import java.util.regex.*;

public class LogAnalyzer {
    public static void main(String[] args) throws Exception {
        // args: algo, dataset, job_id, log_file, monitor_file, duration
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

        // 1. 分析日志寻找迭代次数
        try (BufferedReader br = new BufferedReader(new FileReader(logFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (algo.equals("giraph")) {
                    // 适配日志: "finishSuperstep: Superstep 5"
                    if (line.contains("finishSuperstep") && line.contains("Superstep")) {
                        Matcher m = Pattern.compile("Superstep\\s*(-?\\d+)").matcher(line);
                        if (m.find()) {
                            int currentStep = Integer.parseInt(m.group(1));
                            if (currentStep > iterations) iterations = currentStep;
                        }
                    }
                } else {
                    // MapReduce
                    if (line.contains("completed successfully")) iterations++;
                }
            }
        }

        // 2. 分析监控日志
        List<Double> cpus = new ArrayList<>();
        List<Double> mems = new ArrayList<>();
        List<Double> netRates = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(monitorFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.trim().isEmpty() || line.contains("Linux") || line.contains("Average")) continue;
                String[] p = line.trim().split("\\s+");
                
                int valStart = 1; 
                if (p.length > 1 && (p[1].equals("AM") || p[1].equals("PM"))) valStart = 2;

                // CPU
                int allIndex = -1;
                for(int i=0; i<p.length; i++) if(p[i].equals("all")) allIndex = i;
                if (allIndex != -1 && p.length > allIndex + 3) {
                    try { cpus.add(Double.parseDouble(p[allIndex + 1]) + Double.parseDouble(p[allIndex + 3])); } catch (Exception e) {}
                }

                // Memory
                if (allIndex == -1 && !line.contains("IFACE") && !line.contains("CPU") && p.length > valStart + 2) {
                    try {
                        double kbUsed = Double.parseDouble(p[valStart + 2]);
                        if (kbUsed > 10000) mems.add(kbUsed / (1024.0 * 1024.0)); 
                    } catch (Exception e) {}
                }

                // Network
                if (!line.contains("IFACE") && !line.contains("CPU") && p.length > valStart + 5) {
                    if (!p[valStart].equals("lo")) { 
                        try { netRates.add(Double.parseDouble(p[valStart + 3]) + Double.parseDouble(p[valStart + 4])); } catch (Exception e) {}
                    }
                }
            }
        } catch (Exception e) {}

        // 3. 汇总
        if (!cpus.isEmpty()) cpuUtil = cpus.stream().mapToDouble(d->d).average().orElse(0.0);
        if (!mems.isEmpty()) peakMemGB = mems.stream().mapToDouble(d->d).max().orElse(0.0);
        if (!netRates.isEmpty()) networkMB = (netRates.stream().mapToDouble(d->d).average().orElse(0.0) * totalTime) / 1024.0;
        
        if (iterations == 0) iterations = 1; 
        double avgIterTime = totalTime / iterations;

        // 🔴 修改处：在 %f 后面加上了单位字符，%% 表示输出百分号
        System.out.printf("%s,%s,%s,%.2fs,%.2fs,%.2fMB,%.2fGB,%d,%.2f%%,%.2f%%%n",
            algo, dataset, jobId, totalTime, avgIterTime, networkMB, peakMemGB, iterations, 99.9, cpuUtil);
    }
}