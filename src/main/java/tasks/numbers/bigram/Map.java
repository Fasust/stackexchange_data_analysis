package tasks.numbers.bigram;

import javafx.util.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.NamedNodeMap;
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

                NamedNodeMap nodeMap = nList.item(i).getAttributes();

                String type = nodeMap.getNamedItem("PostTypeId").getNodeValue();

                //Get all Posts of type=Question
                if (type.equals("1")) {

                    //Get the contents of the post title.
                    String title = nodeMap.getNamedItem("Title").getNodeValue();

                    //Parse String by removing tags, special Characters and contractions and then splitting it into words.
                    String[] words = TextParser.parseInputXml(title).split("[^A-Za-z']");

                    String prev = "";

                    for (String word : words) {

                        if (!prev.equals("")) {//For each word, we check if we have a bigram. If we do, we write to context.
                            context.write(new Text(prev + " " + word), new IntWritable(1));
                        }

                        prev = word;
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
