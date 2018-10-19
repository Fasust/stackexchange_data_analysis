package tasks.numbers.wordcountcombiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import tasks.warmup.wordcount.Map;
import tasks.warmup.wordcount.Reduce;
import xml_reader.XmlInputFormat;

public class WordCountCombinerMain {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: WordCountCombinerMain <input path> <output path>");
            System.exit(-1);
        }

        try {
            final Job job = Job.getInstance(new Configuration());
            job.setJarByClass(WordCountCombinerMain.class);
            job.setJobName("Word Count Combiner.");

            //Input Format
            job.setInputFormatClass(XmlInputFormat.class);

            //Mapper
            job.setMapperClass(Map.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            //Reducer & Combiner
            job.setReducerClass(Reduce.class);
            job.setCombinerClass(Reduce.class); //We use a combiner between mapping and reducing stages.
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);

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
