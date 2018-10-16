package tasks.discover.favoriteQuestions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import xml_reader.XmlInputFormat;

public class FavoriteQuestionsMain {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: WordCountMain <input path> <output path>");
            System.exit(-1);
        }

        try {
            final Job job = Job.getInstance(new Configuration());
            job.setJarByClass(FavoriteQuestionsMain.class);
            job.setJobName("Favorite Questions");

            //Input Format
            job.setInputFormatClass(XmlInputFormat.class);

            //Mapper
            job.setMapperClass(Map.class);
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(Text.class);

            //Reducer
            job.setNumReduceTasks(1);
            job.setReducerClass(Reduce.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Text.class);

            //Sort
            job.setSortComparatorClass(LongWritable.DecreasingComparator.class);

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
