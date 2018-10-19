package tasks.warmup.uniqueWords;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import utility.TextParser;
import utility.XMLParser;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;


public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context){
        try {

            //Parse XML String to NodeList
            NodeList nList = XMLParser.xmlStringToNodelist(value.toString(),"row");

            for (int i = 0; i < nList.getLength(); i++) {

                String type = nList.item(i).getAttributes().getNamedItem("PostTypeId").getNodeValue();

                //Get all Posts of type=Question
                if (type.equals("1")) {

                    //Get The Contents of the Title
                    String titleBody = nList.item(i).getAttributes().getNamedItem("Title").getNodeValue();

                    //Parse String by removing tags, special Characters and contractions and then splitting it into words
                    String[] words = TextParser.parseInputXml(titleBody).split("[^A-Za-z']");
                    for (String word : words) {
                        if(word.isEmpty()){ //If the word is equal to the empty string we discard it.
                            continue;
                        }
                        context.write(new Text(word), new IntWritable(1));
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
