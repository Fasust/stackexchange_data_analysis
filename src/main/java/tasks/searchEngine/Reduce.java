package tasks.searchEngine;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Reduce extends Reducer<Text, IntWritable, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) {
        StringBuilder output = new StringBuilder();
        HashSet<Integer> entrySet = new HashSet<>();

        //Convert to Int HashSet to remove Repetitions
        for(IntWritable entry : values){
            entrySet.add(entry.get());
        }

        for (Integer e : entrySet){
            output.append(e).append(",");
        }

        try {
            context.write(key, new Text(output.toString()));

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
