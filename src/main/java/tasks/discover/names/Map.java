package tasks.discover.names;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.NodeList;
import utility.TextParser;
import utility.XMLParser;


public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context){
        try {

            //Parse XML String to NodeList
            NodeList nList = XMLParser.xmlStringToNodelist(value.toString(),"row");

            for (int i = 0; i < nList.getLength(); i++) {

                //Get User Name
                String name = nList.item(i).getAttributes().getNamedItem("DisplayName").getNodeValue();

                //Split by spaces and remove breaks
                name = name.replaceAll("\n", " ").replaceAll("\r", " ");
                String[] words = name.split(" ");
                for (String word : words) {
                    context.write(new Text(word), new IntWritable(1));
                }

                //Map with rep as key and id as value
                context.write(new Text(name), new IntWritable(1));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
