import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class PageRankMapReduce {
    public static enum COUNTER { DIFF_X_100000 } // 放大误差存入整数计数器

    public static class PRMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length < 2) return;
            String id = parts[0];
            double pr = Double.parseDouble(parts[1]);
            String links = (parts.length > 2) ? parts[2] : "";
            
            context.write(new Text(id), new Text("struct:" + links));
            context.write(new Text(id), new Text("old:" + pr)); // 传递旧值
            
            if (!links.isEmpty()) {
                String[] targets = links.split(",");
                double vote = pr / targets.length;
                for (String t : targets) context.write(new Text(t), new Text("val:" + vote));
            }
        }
    }

    public static class PRReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String links = "";
            double sum = 0, oldPr = 0;
            for (Text val : values) {
                String s = val.toString();
                if (s.startsWith("struct:")) links = s.substring(7);
                else if (s.startsWith("val:")) sum += Double.parseDouble(s.substring(4));
                else if (s.startsWith("old:")) oldPr = Double.parseDouble(s.substring(4));
            }
            double newPr = 0.15 + (0.85 * sum);
            // 计算误差并累加到 Counter
            double diff = Math.abs(newPr - oldPr);
            context.getCounter(COUNTER.DIFF_X_100000).increment((long)(diff * 100000.0));
            context.write(key, new Text(newPr + "\t" + links));
        }
    }

    public static void main(String[] args) throws Exception {
        String inputBase = args[0];
        String outputBase = args[1];
        int i = 0;
        while (i < 50) {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "MR Iter " + i);
            job.setJarByClass(PageRankMapReduce.class);
            job.setMapperClass(PRMapper.class);
            job.setReducerClass(PRReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            String in = (i == 0) ? inputBase : outputBase + "/iter_" + (i-1);
            String out = outputBase + "/iter_" + i;
            FileInputFormat.addInputPath(job, new Path(in));
            FileOutputFormat.setOutputPath(job, new Path(out));

            job.waitForCompletion(true);

            long diffLong = job.getCounters().findCounter(COUNTER.DIFF_X_100000).getValue();
            double diff = diffLong / 100000.0;
            System.out.println("Iteration " + i + " Diff: " + diff);

            if (i > 0 && diff < 0.0001) break;
            i++;
        }
    }
}
