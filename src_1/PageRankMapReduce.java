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

    public static class PRMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length < 2) return;

            String id = parts[0];
            double pr = Double.parseDouble(parts[1]);
            String links = (parts.length > 2) ? parts[2] : "";

            context.write(new Text(id), new Text("struct:" + links));

            if (!links.isEmpty()) {
                String[] targets = links.split(",");
                double vote = pr / targets.length;
                for (String target : targets) {
                    context.write(new Text(target), new Text("val:" + vote));
                }
            }
        }
    }

    public static class PRReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String links = "";
            double sum = 0;

            for (Text val : values) {
                String str = val.toString();
                if (str.startsWith("struct:")) {
                    links = str.substring(7);
                } else if (str.startsWith("val:")) {
                    sum += Double.parseDouble(str.substring(4));
                }
            }

            double newPr = 0.15 + (0.85 * sum);
            context.write(key, new Text(newPr + "\t" + links));
        }
    }

    public static void main(String[] args) throws Exception {
        String inputPath = args[0];
        String outputPath = args[1];
        int loops = 10; 

        for (int i = 0; i < loops; i++) {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "MR PageRank Iter " + i);
            job.setJarByClass(PageRankMapReduce.class);
            job.setMapperClass(PRMapper.class);
            job.setReducerClass(PRReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            String in = (i == 0) ? inputPath : outputPath + "/iter_" + (i - 1);
            String out = outputPath + "/iter_" + i;

            FileInputFormat.addInputPath(job, new Path(in));
            FileOutputFormat.setOutputPath(job, new Path(out));
            job.waitForCompletion(true);
        }
    }
}