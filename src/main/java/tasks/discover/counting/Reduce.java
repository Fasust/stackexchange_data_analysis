package tasks.discover.counting;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class Reduce extends Reducer<Text, IntWritable, Text, NullWritable> {
    private static ArrayList<Text> uniqueUser;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        uniqueUser = new ArrayList<>();
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) {
        int length = 0;

        for (IntWritable value : values) {
            length ++;
        }

        //check if there is only one user with ID (Unique)
        if(length == 1){

            //If yes, add them
           uniqueUser.add(key);
        }

    }

    @Override
    protected void cleanup(org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException, InterruptedException {
        try {
            context.write(new Text(uniqueUser.size() + "") , NullWritable.get());
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

        super.cleanup(context);
    }

}
