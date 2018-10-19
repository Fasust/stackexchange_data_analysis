package tasks.discover.uniqueUsers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class Reduce extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) {
        int length = 0;
        String userName = "";

        //Check if there is Multiple user entries with the same id (Uniqueness).
        for (Text value : values) {
            userName = value.toString();
            length ++;
        }
        
        if(length == 1) { //If we just have one entry for this id, we write it to the output.
            try {
                context.write(key, new Text(userName));
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }

    }


}
