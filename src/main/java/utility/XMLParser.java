package utility;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class XMLParser {

    /**
     * Parse XML string to a NodeList.
     * @param xmlString input XML String
     * @param nodeTagName every element with this Tag Name will be turned into a Node in the final List
     * @return Parsed NodeList
     * @throws ParserConfigurationException
     * @throws IOException
     * @throws SAXException
     */
    public static NodeList xmlStringToNodelist(String xmlString, String nodeTagName) throws ParserConfigurationException, IOException, SAXException {

        //Convert String to XML Document
        InputStream is = new ByteArrayInputStream(xmlString.getBytes());
        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
        Document doc = dBuilder.parse(is);
        doc.getDocumentElement().normalize();

        //Create a Node List from XML
        return doc.getElementsByTagName(nodeTagName);
    }
}
