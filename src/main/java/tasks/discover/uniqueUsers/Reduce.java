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

        for (Text value : values) {
            userName = value.toString();
            length ++;
        }

        //check if there is only one user with ID (Unique)
        if(length == 1){
            try {
                context.write(key, new Text(userName));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }


}
