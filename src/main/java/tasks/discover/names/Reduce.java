package tasks.discover.names;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeSet;


public class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    private TreeSet<NameObject> nameMap = new TreeSet<>();
    private static final int OUTPUT_SIZE = 10;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) {
        int nameCount = 0;
        for(IntWritable value : values){
            nameCount++;
        }

        //Add to sorted map
        nameMap.add(new NameObject(nameCount,key.toString()));

        //Make sure the map only consists of OUTPUT_SIZE amount of values (Highest).
        while (nameMap.size() > OUTPUT_SIZE){
           nameMap.pollFirst();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        //Output all values in nameMap
        for(NameObject name : nameMap) {
            context.write(
                    new Text(name.getName()),
                    new IntWritable(name.getNameCount())
            );
        }

        super.cleanup(context);
    }
}
