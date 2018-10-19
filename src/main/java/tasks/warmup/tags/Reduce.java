package tasks.warmup.tags;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reduce extends Reducer<Text, IntWritable, Text, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) {
        int length = 0;

        for (IntWritable value : values) { //Add one for each value associated to the key.
            length++;
        }
        if(length == 1){ //If we just have a single value, is a valid tag.
            try {
                context.write(new Text(key) , NullWritable.get());
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

}
