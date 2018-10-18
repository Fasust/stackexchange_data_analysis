package tasks.discover.counting;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class Reduce extends Reducer<Text, IntWritable, IntWritable, NullWritable> {
    private static int uniqueUserCount;


    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) {
        int length = 0;

        for (IntWritable value : values) {
            length ++;
        }

        //check if there is only one user with ID (Unique)
        if(length == 1){

            //If yes, add them
            uniqueUserCount++;
        }

    }

    @Override
    protected void cleanup(org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException, InterruptedException {
        try {
            context.write(new IntWritable(uniqueUserCount) , NullWritable.get());
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

        super.cleanup(context);
    }

}
