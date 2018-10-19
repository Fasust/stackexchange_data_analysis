package tasks.discover.averageAnswers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reduce extends Reducer<Text, IntWritable, LongWritable, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) {
        long totalResponses = 0;
        long numberOfQuestions = 0;

        //Iterate through all answerCount and keep track of the total number of answers and of the number of Questions
        for (IntWritable answerCount : values) {
            numberOfQuestions++;
            totalResponses += answerCount.get();
        }

        try {
            //average by dividing total number of responses by number of questions
            context.write(new LongWritable(totalResponses / numberOfQuestions) ,NullWritable.get());
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

}
