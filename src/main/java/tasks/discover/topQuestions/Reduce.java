package tasks.discover.topQuestions;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class Reduce extends Reducer<LongWritable, Text, LongWritable, Text> {

    //List of Top Users consisting of there Id and Reputation.
    private static int writeCount = 0;
    private static final int OUTPUT_SIZE = 10;

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //If we have reached the Output Size, exit immediately (redundant but slightly more efficient).
        if(writeCount >= OUTPUT_SIZE){
            return;
        }

        //Iterate through the questions and keep adding them to context, as long as we have not reached the OUTPUT_SIZE.
        for(Text question : values){
            if(writeCount < OUTPUT_SIZE){
                context.write(key, question);
                writeCount++;
            }
        }
    }
}
