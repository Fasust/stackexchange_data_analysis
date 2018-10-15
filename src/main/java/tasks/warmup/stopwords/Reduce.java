package tasks.warmup.stopwords;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reduce extends Reducer<Text, Text, Text, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) {
        for (Text value : values) {
            if (value.toString().equals("stopword")) {
                return;
            }
        }
        try {
            context.write(new Text(key) , NullWritable.get());
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
