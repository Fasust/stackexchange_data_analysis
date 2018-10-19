package tasks.warmup.stopwords;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapStopwords extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) {

        try {

            String line = value.toString();
            String[] words = line.split("\n"); //In this file, we split by new line.
            for (String word : words) {//We use the keyword "sotpwords" to differentiate it from the "popularwords".
                context.write(new Text(word), new Text("stopword"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
