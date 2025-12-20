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
            
            // 判断收敛 (从第2轮开始判断)
            if (getSuperstep() > 1 && globalDiff < THRESHOLD) {
                vertex.voteToHalt();
            } else if (getSuperstep() < 100) { // 最大100轮兜底
                sendMessageToAllEdges(vertex, new DoubleWritable(newPr / vertex.getNumEdges()));
            } else {
                vertex.voteToHalt();
            }
        }
    }
}
