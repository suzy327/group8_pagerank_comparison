import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
public class PageRankMasterCompute extends DefaultMasterCompute {
    @Override
    public void initialize() throws InstantiationException, IllegalAccessException {
        registerPersistentAggregator("sum_diff", DoubleSumAggregator.class);
    }
}
