package tasks.searchEngine;

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
                String id;
                StringBuilder wordBase = new StringBuilder();

                switch (type){
                    case "1": //Question
                        id = nList.item(i).getAttributes().getNamedItem("Id").getNodeValue();
                        wordBase.append(nList.item(i).getAttributes().getNamedItem("Title").getNodeValue());
                        break;
                    case "2": //Answer
                        id = nList.item(i).getAttributes().getNamedItem("ParentId").getNodeValue();
                        break;
                    default:
                        return;
                }
                wordBase.append( nList.item(i).getAttributes().getNamedItem("Body").getNodeValue());

                //Parse String by removing tags, special Characters and contractions and then splitting it into words
                String[] words = TextParser.parseInputXml(wordBase.toString()).split("[^A-Za-z']");

                for (String word : words) {
                    if(word.isEmpty()){ //If the word is the empty string, we discard it.
                        continue;
                    }
                    //We write the word as the key and the id associated as the value.
                    context.write(new Text(word), new IntWritable(Integer.parseInt(id)));
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
