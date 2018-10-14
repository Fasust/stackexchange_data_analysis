package tasks.warmup.morethan10;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import utility.TextParser;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        try {

            //Convert String to XML Document
            InputStream is = new ByteArrayInputStream(value.toString().getBytes());
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(is);
            doc.getDocumentElement().normalize();

            //Create a Node List from XML
            NodeList nList = doc.getElementsByTagName("row");

            for (int i = 0; i < nList.getLength(); i++) {

                String id = nList.item(i).getAttributes().getNamedItem("Id").getNodeValue();

                String type = nList.item(i).getAttributes().getNamedItem("PostTypeId").getNodeValue();

                //Get all Posts of type=Question
                if (type.equals("1")) {

                    //Get The Contents of the Post
                    String postBody = nList.item(i).getAttributes().getNamedItem("Title").getNodeValue();

                    //Parse String by removing tags, special Characters and contractions and then splitting it into words
                    String[] words = TextParser.parseInputXml(postBody).split("[^A-Za-z']");
                    if (words.length >= 10) { //If the title has 10 or more words, we add it to the output.
                        /**
                         * Two ways of doing it here:
                         *          1.Use the same key for all the titles with words >= 10. And then just add and
                         *          output on the reducer.
                         *
                         *          2.Use the id of each post as key, and use the run method on the reducer to count
                         *          them all.
                         */
                        //Way 1.
                        context.write(new Text("Titles with more than 10 words: "), new IntWritable(1));
                        //Way 2.
                        //context.write(new Text(id), new IntWritable(1));
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
