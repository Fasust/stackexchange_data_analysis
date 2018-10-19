package tasks.warmup.stopwords;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import xml_reader.XmlInputFormat;

public class StopWordsMain {

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: StopWordsMain <stopwords path> <input path> <output path>");
            System.exit(-1);
        }

        try {
            final Job job = Job.getInstance(new Configuration());
            job.setJarByClass(StopWordsMain.class);
            job.setJobName("Stop Words.");

            //Input Format
            //job.setInputFormatClass(XmlInputFormat.class);

            //Mapper
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            //Reducer
            job.setReducerClass(Reduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            //Process Args
            MultipleInputs.addInputPath(job, new Path(args[0]),
                    TextInputFormat.class, MapStopwords.class); //We use MultipleInputs since we need more than one file.
            MultipleInputs.addInputPath(job, new Path(args[1]),
                    XmlInputFormat.class, Map.class);
            FileOutputFormat.setOutputPath(job, new Path(args[2]));

            job.waitForCompletion(true);

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
