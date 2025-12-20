
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import java.io.IOException;

public class PageRankGiraph extends BasicComputation<LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {
    
    @Override
    public void compute(Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
                        Iterable<DoubleWritable> messages) throws IOException {
        
        if (getSuperstep() == 0) {
            // 第0轮：初始化，向所有邻居发送 1.0 / 出度
            vertex.setValue(new DoubleWritable(1.0)); 
            sendMessageToAllEdges(vertex, new DoubleWritable(1.0 / vertex.getNumEdges()));
        } else {
            double sum = 0;
            for (DoubleWritable message : messages) {
                sum += message.get();
            }
            
            // PageRank公式: 0.15 + 0.85 * sum
            double newPr = 0.15 + 0.85 * sum;
            vertex.setValue(new DoubleWritable(newPr));

            // 运行 30 轮后停止
            if (getSuperstep() < 30) {
                sendMessageToAllEdges(vertex, new DoubleWritable(newPr / vertex.getNumEdges()));
            } else {
                vertex.voteToHalt();
            }
        }
    }
}