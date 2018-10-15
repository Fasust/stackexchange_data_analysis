package tasks.discover.counting;


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

                //Get The Contents of the Title
                String id = nList.item(i).getAttributes().getNamedItem("Id").getNodeValue();

                context.write(new Text(id), new IntWritable(1));

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
