package tasks.warmup.stopwords;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapStopwords extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) {

        try {

            String line = value.toString();
            String[] words = line.split("\n");
            //Tips: regex to remove all non-words and digits. [\\W|\\d]
            for (String word : words) {
                context.write(new Text(word), new Text("stopword"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
