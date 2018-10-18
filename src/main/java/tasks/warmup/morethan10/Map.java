package tasks.warmup.morethan10;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.NodeList;
import utility.TextParser;
import utility.XMLParser;

public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) {

        try {

            //Parse XML String to NodeList
            NodeList nList = XMLParser.xmlStringToNodelist(value.toString(),"row");


            for (int i = 0; i < nList.getLength(); i++) {

                String type = nList.item(i).getAttributes().getNamedItem("PostTypeId").getNodeValue();

                //Get all Posts of type=Question
                if (type.equals("1")) {

                    //Get The Contents of the Post
                    String postBody = nList.item(i).getAttributes().getNamedItem("Title").getNodeValue();

                    //Parse String by removing tags, special Characters and contractions and then splitting it into words
                    String[] words = TextParser.parseInputXml(postBody).split("[^A-Za-z']");
                    if (words.length > 10) { //If the title has more than 10 words, we add it to the output.
                        context.write(new Text("Titles with more than 10 words: "), new IntWritable(1));
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
