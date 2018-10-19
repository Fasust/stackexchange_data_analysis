package tasks.discover.uniqueUsers;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.NodeList;
import utility.XMLParser;


public class Map extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context){
        try {

            //Parse XML String to NodeList
            NodeList nList = XMLParser.xmlStringToNodelist(value.toString(),"row");

            for (int i = 0; i < nList.getLength(); i++) {
                //Get User ID and Name
                String id = nList.item(i).getAttributes().getNamedItem("Id").getNodeValue();
                String userName = nList.item(i).getAttributes().getNamedItem("DisplayName").getNodeValue();

                context.write(new Text(id), new Text(userName));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
