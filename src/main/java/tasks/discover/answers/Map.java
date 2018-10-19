package tasks.discover.answers;

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

                String type = nList.item(i).getAttributes().getNamedItem("PostTypeId").getNodeValue();

                //Get all Posts of type=Question
                if (type.equals("1")) {

                    //Get the answer count of the Post
                    String answerCount = nList.item(i).getAttributes().getNamedItem("AnswerCount").getNodeValue();

                    if (Integer.parseInt(answerCount) >= 1) { //If the question has 1 or more answers, we keep it.
                        context.write(new Text("answer"), new IntWritable(1));
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
