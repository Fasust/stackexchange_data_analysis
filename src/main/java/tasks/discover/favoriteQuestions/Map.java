package tasks.discover.favoriteQuestions;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.NodeList;
import utility.TextParser;
import utility.XMLParser;


public class Map extends Mapper<LongWritable, Text, LongWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context){
        try {

            //Parse XML String to NodeList
            NodeList nList = XMLParser.xmlStringToNodelist(value.toString(),"row");

            for (int i = 0; i < nList.getLength(); i++) {

                String type = nList.item(i).getAttributes().getNamedItem("PostTypeId").getNodeValue();

                //Check if the Post is a Question
                if(type.equals("1")){

                    //Get ID, body and FavoriteCount
                    String id = nList.item(i).getAttributes().getNamedItem("Id").getNodeValue();
                    String body = nList.item(i).getAttributes().getNamedItem("Body").getNodeValue();
                    String favoriteCount = nList.item(i).getAttributes().getNamedItem("FavoriteCount").getNodeValue();

                    //parse question body
                    body = TextParser.parseInputXml(body);

                    //Map with score as key and id as value
                    context.write(new LongWritable(Integer.parseInt(favoriteCount)), new Text(id + " | " +  body));
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
