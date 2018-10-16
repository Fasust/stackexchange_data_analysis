package tasks.discover.countries;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.NodeList;
import utility.CountryValidator;
import utility.TextParser;
import utility.XMLParser;


public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final CountryValidator countryValidator = new CountryValidator();

    @Override
    protected void map(LongWritable key, Text value, Context context) {
        try {

            //Parse XML String to NodeList
            NodeList nList = XMLParser.xmlStringToNodelist(value.toString(),"row");

            for (int i = 0; i < nList.getLength(); i++) {

                /**
                 * Users have no restrictions when entering their location.
                 * Thusly I decided to split the location string and check if a given substring is a valid ISO country
                 * If this is the case, Im counting it as the country of origin. If not, the location is invalid.
                 */

                String location = nList.item(i).getAttributes().getNamedItem("Location").getNodeValue();
                String[] potentialCountries = TextParser.parseInputXml(location).split("[^A-Za-z']");

                for(String coun : potentialCountries){
                    if(countryValidator.isCountry(coun)){
                        context.write(new Text(coun), new IntWritable(1));
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
