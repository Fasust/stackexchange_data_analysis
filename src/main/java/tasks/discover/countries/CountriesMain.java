package tasks.discover.countries;

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

public class CountriesMain {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: CountriesMain <input path> <output path>");
            System.exit(-1);
        }

        try {
            final Job job = Job.getInstance(new Configuration());
            job.setJarByClass(CountriesMain.class);
            job.setJobName("Countries");

            //Input Format
            job.setInputFormatClass(XmlInputFormat.class);

            //Mapper
            job.setMapperClass(Map.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            //Reducer
            job.setReducerClass(Reduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

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
