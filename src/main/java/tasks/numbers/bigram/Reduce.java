package tasks.numbers.bigram;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    private List<Pair<String,Integer>> topBigrams = new ArrayList<>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) {
        int count = 0;
        for (IntWritable value : values) {
            count += value.get();
        }

        //All the elements in the list have always the same value.
        if (topBigrams.size() == 0) {//If there is no elements yet, we add it since its the highest we've seen.
            topBigrams.add(new Pair<>(key.toString(),count));
        } else if (count == topBigrams.get(0).getValue()) {//If it has the same value as the current one, we add it.
            topBigrams.add(new Pair<>(key.toString(),count));
        } else if (count > topBigrams.get(0).getValue()) {//If we find a new high value, we clean the list and we add the new value.
            topBigrams.clear();
            topBigrams.add(new Pair<>(key.toString(),count));
        }
    }

    @Override
    protected void cleanup(Context context) {
        try {
            for (Pair<String,Integer> bigram : topBigrams) {
                context.write(new Text(bigram.getKey()) ,new IntWritable(bigram.getValue()));
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
