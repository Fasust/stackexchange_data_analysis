
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: Main <input path> <output path>");
            System.exit(-1);
        }

        try {
            final Job job = Job.getInstance(new Configuration());
            job.setJarByClass(Main.class);
            job.setJobName("Word counter");
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.setMapperClass(Map.class);
            job.setReducerClass(Reduce.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            System.exit(job.waitForCompletion(true) ? 0 : 1);
            
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
