package tasks.discover.topUsers;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.NodeList;
import utility.XMLParser;


public class Map extends Mapper<LongWritable, Text, LongWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context){
        try {

            //Parse XML String to NodeList
            NodeList nList = XMLParser.xmlStringToNodelist(value.toString(),"row");

            for (int i = 0; i < nList.getLength(); i++) {

                //Get User ID and Reputation
                String id = nList.item(i).getAttributes().getNamedItem("Id").getNodeValue();
                String reputation = nList.item(i).getAttributes().getNamedItem("Reputation").getNodeValue();

                //Map with rep as key and id as value
                context.write(new LongWritable(Integer.parseInt(reputation)), new Text(id));

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
