package tasks.discover.names;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;


public class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    private TreeMap<Integer, String> nameMap = new TreeMap<>();
    private static final int OUTPUT_SIZE = 10;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int nameCount = 0;
        for(IntWritable value : values){
            nameCount++;
        }

        //Add to sorted map
        nameMap.put(nameCount, key.toString());

        //Make sure the map only consists of OUTPUT_SIZE amount of values (Highest)
        while (nameMap.size() > OUTPUT_SIZE){
           nameMap.remove(nameMap.firstKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        //Output all Values in nameMap
        for(Integer key : nameMap.keySet()) {
            context.write(
                    new Text(nameMap.get(key)),
                    new IntWritable(key)
            );
        }

        super.cleanup(context);
    }
}
