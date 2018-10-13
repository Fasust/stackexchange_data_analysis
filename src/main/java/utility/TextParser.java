package utility;

public class TextParser {

    /**
     * Method to parse the input text, removing HTML language tags and english contractions.
     * @param input The input text.
     * @return The parsed text.
     */
    public static String parseInputXml(String input) {

        input = input.replaceAll("<[^>]*>","");
        //Special cases
        input = input.replaceAll("can't"," can not");
        input = input.replaceAll("won't"," will not");
        //General cases
        input = input.replaceAll("'m"," am");
        input = input.replaceAll("n't"," not");
        input = input.replaceAll("'ll"," will");
        input = input.replaceAll("'s"," is");
        input = input.replaceAll("'re"," are");
        input = input.replaceAll("'ve"," have");
        input = input.toLowerCase();

        return input;
    }
}
