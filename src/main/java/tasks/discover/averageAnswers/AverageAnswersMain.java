package tasks.discover.averageAnswers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import xml_reader.XmlInputFormat;

public class AverageAnswersMain {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: AverageAnswersMain <input path> <output path>");
            System.exit(-1);
        }

        try {
            final Job job = Job.getInstance(new Configuration());
            job.setJarByClass(AverageAnswersMain.class);
            job.setJobName("Average Answers");

            //Input Format
            job.setInputFormatClass(XmlInputFormat.class);

            //Mapper
            job.setMapperClass(Map.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            //Reducer
            job.setReducerClass(Reduce.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(NullWritable.class);

            //Process Args
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            job.waitForCompletion(true);

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
