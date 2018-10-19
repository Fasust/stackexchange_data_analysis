package tasks.numbers.trigram;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    private List<Pair<String,Integer>> topTrigrams = new ArrayList<>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) {
        int count = 0;
        for (IntWritable value : values) {
            count += value.get();
        }

        //All the elements in the list have always the same value.
        if (topTrigrams.size() == 0) {//If there is no elements yet, we add it since its the highest we've seen.
            topTrigrams.add(new Pair<>(key.toString(),count));
        } else if (count == topTrigrams.get(0).getValue()) {//If it has the same value as the current one, we add it.
            topTrigrams.add(new Pair<>(key.toString(),count));
        } else if (count > topTrigrams.get(0).getValue()) {//If we find a new high value, we clean the list and we add the new value.
            topTrigrams.clear();
            topTrigrams.add(new Pair<>(key.toString(),count));
        }
    }

    @Override
    protected void cleanup(Context context) {
        try {
            for (Pair<String,Integer> trigram : topTrigrams) {
                context.write(new Text(trigram.getKey()) ,new IntWritable(trigram.getValue()));
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
