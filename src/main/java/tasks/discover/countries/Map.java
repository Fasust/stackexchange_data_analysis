package tasks.discover.countries;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.NodeList;
import utility.XMLParser;


public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) {
        try {

            //Parse XML String to NodeList
            NodeList nList = XMLParser.xmlStringToNodelist(value.toString(),"row");

            for (int i = 0; i < nList.getLength(); i++) {

                /**
                 * I would have liked to parse the location to limit it to just the "Country",
                 * but there is no restriction on what users can type in the field location,
                 * thusly there is no real schema to follow and thusly im just using in the un-parsed string.
                 */
                
                String location = nList.item(i).getAttributes().getNamedItem("Location").getNodeValue();
                context.write(new Text(location), new IntWritable(1));

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
