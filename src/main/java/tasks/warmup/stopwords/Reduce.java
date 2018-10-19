package tasks.warmup.stopwords;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reduce extends Reducer<Text, Text, Text, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) {
        int count = 0;
        for (Text value : values) {
            count++;
            if (value.toString().equals("stopword")) { //If we detect a stop word string on the value array, we have to discard the word.
                return;
            }
        }
        try {
            for (int i = 0; i < count; i++) { //To maintain the original number of words.
                context.write(new Text(key) , NullWritable.get());
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
