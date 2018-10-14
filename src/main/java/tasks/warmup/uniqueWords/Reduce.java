package tasks.warmup.uniqueWords;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reduce extends Reducer<Text, IntWritable, Text, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) {
        int length = 0;

        for (IntWritable value : values) {
            length += value.get();
        }

        if(length == 1){
            try {
                context.write(new Text(key) , NullWritable.get());
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

}
