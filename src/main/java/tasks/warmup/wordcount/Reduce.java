package tasks.warmup.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) {
        int finalValue = 0;
        for (IntWritable value : values) {
            finalValue += value.get();
        }
        try {
            context.write(new Text(key) ,new IntWritable(finalValue));
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

}
