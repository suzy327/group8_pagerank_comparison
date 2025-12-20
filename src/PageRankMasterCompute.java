import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.hadoop.io.DoubleWritable;

public class PageRankMasterCompute extends DefaultMasterCompute {

    @Override
    public void initialize() throws InstantiationException, IllegalAccessException {
        // 注册聚合器
        registerAggregator("sum_diff", DoubleSumAggregator.class);
    }

    @Override
    public void compute() {
        // 获取当前误差
        double sumDiff = ((DoubleWritable) getAggregatedValue("sum_diff")).get();
        
        // 使用 System.err，它会直接打入 stderr 日志文件，不需要 yarn logs 聚合
        System.err.println("MY_DEBUG_MARKER | Step: " + getSuperstep() + " | Global Diff: " + sumDiff);

        // 🟢 这里的控制权交给命令行参数和 Worker，Master 不再调用 haltComputation
        // 如果你想恢复自动收敛，等看到日志后再取消下面的注释
        if (getSuperstep() > 1 && sumDiff < 0.0001) {
            haltComputation();
        }
    }
}