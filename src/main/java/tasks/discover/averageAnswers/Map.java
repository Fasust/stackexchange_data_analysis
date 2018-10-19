package tasks.discover.averageAnswers;


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

                    String answerCount = nList.item(i).getAttributes().getNamedItem("AnswerCount").getNodeValue();

                    //We just pass the number of answers for that post.
                    context.write(new Text("answerCount"), new IntWritable(Integer.parseInt(answerCount)));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
