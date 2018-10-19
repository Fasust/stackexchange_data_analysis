package tasks.discover.countries;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) {
        int locationCount = 0;

        for (IntWritable value : values) {
            locationCount++;
        }

        try {
            context.write(key ,new IntWritable(locationCount));
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

}
