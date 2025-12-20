import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import java.io.IOException;

public class PageRankGiraph extends BasicComputation<LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {
    public static final double THRESHOLD = 0.0001;

    @Override
    public void compute(Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
                        Iterable<DoubleWritable> messages) throws IOException {
        
        if (getSuperstep() == 0) {
            vertex.setValue(new DoubleWritable(1.0));
            sendMessageToAllEdges(vertex, new DoubleWritable(1.0 / vertex.getNumEdges()));
        } else {
            double sum = 0;
            for (DoubleWritable msg : messages) { sum += msg.get(); }
            
            double oldPr = vertex.getValue().get();
            double newPr = 0.15 + 0.85 * sum;
            vertex.setValue(new DoubleWritable(newPr));
            
            // 累加误差
            double diff = Math.abs(newPr - oldPr);
            aggregate("sum_diff", new DoubleWritable(diff));

            // 获取上一轮的总误差
            double globalDiff = ((DoubleWritable) getAggregatedValue("sum_diff")).get();
            
            // 逻辑修正：
            // 1. 如果收敛，Master 会通过 haltComputation 停止，这里 Worker 配合 voteToHalt
            // if (getSuperstep() > 1 && globalDiff < THRESHOLD) {
            //     vertex.voteToHalt();
            // } 
            // // 🔴 2. 这里的硬限制从 100 改成 500
            // else if (getSuperstep() < 500) { 
            //     sendMessageToAllEdges(vertex, new DoubleWritable(newPr / vertex.getNumEdges()));
            // } else {
            //     vertex.voteToHalt();
            // }
            if (getSuperstep() > 1 && globalDiff < THRESHOLD) {
                vertex.voteToHalt();
            } 
            else if (getSuperstep() < 500) { 
                sendMessageToAllEdges(vertex, new DoubleWritable(newPr / vertex.getNumEdges()));
            } else {
                vertex.voteToHalt();
            }
        }
    }
}