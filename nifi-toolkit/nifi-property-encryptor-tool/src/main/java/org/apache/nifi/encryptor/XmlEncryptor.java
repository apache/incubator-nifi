package org.apache.nifi.encryptor;

import org.apache.nifi.properties.SensitivePropertyProvider;
import org.apache.nifi.properties.SensitivePropertyProviderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.jar.Attributes;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;


public class XmlEncryptor {

//TODO:    private static final String XML_DECLARATION_REGEX = "/<\?xml version="1.0" encoding=\"UTF-8\"\?>/";
    protected static final String ENCRYPTION_NONE = "none";
    protected static final String ENCRYPTION_EMPTY = "";
    private static final Logger logger = LoggerFactory.getLogger(XmlEncryptor.class);
    protected final SensitivePropertyProvider decryptionProvider;
    protected final SensitivePropertyProvider encryptionProvider;
    protected final SensitivePropertyProviderFactory providerFactory;

    protected static final String PROPERTY_ELEMENT = "property";

    protected static final String ENCRYPTION_ATTRIBUTE_NAME = "encryption";

    public XmlEncryptor(final SensitivePropertyProvider encryptionProvider, final SensitivePropertyProvider decryptionProvider, final SensitivePropertyProviderFactory providerFactory) {
        this.decryptionProvider = decryptionProvider;
        this.encryptionProvider = encryptionProvider;
        this.providerFactory = providerFactory;
    }

//    public static boolean supportsFile(String filePath) {
//        Object doc;
//        try {
//            InputStream rawFileContents = loadXmlFile(filePath);
//            doc = new XmlSlurper().invokeMethod("parseText", new Object[]{rawFileContents});
//        } catch (Throwable ignored) {
//            return false;
//        }
//
//        return doc != null;
//    }

    public static InputStream loadXmlFile(final String xmlFilePath) throws IOException {
        File xmlFile = new File(xmlFilePath);
        if (xmlFile.canRead()) {
            try {
                FileInputStream xmlContent = new FileInputStream(xmlFile);
                return xmlContent;
            } catch (RuntimeException e) {
                throw new IOException("Cannot load XML from " + xmlFilePath, e);
            }
        } else {
            throw new IOException("File at " + xmlFilePath + " must exist and be readable by user running this tool.");
        }
    }

    public void decrypt(final InputStream encryptedXmlContent, final OutputStream decryptedOutputStream) {
        XMLOutputFactory factory = XMLOutputFactory.newInstance();
        factory.setProperty("com.ctc.wstx.outputValidateStructure", false);

        try {
            XMLEventReader eventReader = getXMLReader(encryptedXmlContent);
            XMLEventWriter xmlWriter = factory.createXMLEventWriter(decryptedOutputStream);

            while(eventReader.hasNext()) {
                XMLEvent event = eventReader.nextEvent();

                if (isEncryptedElement(event)) {
                        System.out.println("Event: " + event.getLocation());
                        xmlWriter.add(convertToDecryptedElement(event));
                        xmlWriter.add(decryptElementCharacters(eventReader.nextEvent()));
                } else {
                    try {
                        xmlWriter.add(event);
                    } catch (Exception e) {
                        throw new RuntimeException("Cannot decrypt XML content", e);
                    }
                }
            }

            eventReader.close();
            xmlWriter.flush();
            xmlWriter.close();

        } catch (Exception e) {
            throw new RuntimeException("Cannot decrypt XML content", e);
        }
    }

    /**
     * Decrypt the XMLEvent element characters/value, which should contain an encrypted value
     * @param xmlEvent The encrypted Characters event to be decrypted
     * @return The decrypted Characters event
     */
    private Characters decryptElementCharacters(final XMLEvent xmlEvent) {
        assert(xmlEvent.isCharacters());
        final XMLEventFactory eventFactory = XMLEventFactory.newInstance();
        final String encryptedCharacters = xmlEvent.asCharacters().getData().trim();
        String decryptedCharacters = decryptionProvider.unprotect(encryptedCharacters, providerFactory.getPropertyContext("rgg", "egg"));
        return eventFactory.createCharacters(decryptedCharacters);
    }

    /**
     * Takes a StartElement and updates the 'encrypted' attribute to empty string to remove the encryption method/scheme
     * @param xmlEvent The opening/start XMLEvent for an encrypted property
     * @return The updated element to be written to XML file
     */
    private StartElement convertToDecryptedElement(final XMLEvent xmlEvent) {
        assert isEncryptedElement(xmlEvent);
        return updateElementAttribute(xmlEvent, ENCRYPTION_ATTRIBUTE_NAME, "");
    }

    // TODO: Not sure if there's a more convenient way for doing this..
    private StartElement updateElementAttribute(final XMLEvent xmlEvent, final String attributeName, final String attributeValue) {
        final XMLEventFactory eventFactory = XMLEventFactory.newInstance();
        StartElement encryptedElement = xmlEvent.asStartElement();

        Iterator<Attribute> currentAttributes = encryptedElement.getAttributes();
        ArrayList<Attribute> updatedAttributes = new ArrayList<>();

        while (currentAttributes.hasNext()) {
            final Attribute attribute = currentAttributes.next();
            if (attribute.getName().equals(new QName(attributeName))) {
                updatedAttributes.add(eventFactory.createAttribute(attribute.getName(), attributeValue));
            } else {
                updatedAttributes.add(attribute);
            }
        }

        return eventFactory.createStartElement(encryptedElement.getName(), updatedAttributes.iterator(), encryptedElement.getNamespaces());
    }

    /**
     *     private boolean isProperty(final StartElement startElement, final String propertyName) {
     *         boolean found = false;
     *
     *         final Iterator<Attribute> attributes = startElement.getAttributes();
     *         while (attributes.hasNext()) {
     *             final Attribute attribute = attributes.next();
     *             if (NAME_ATTRIBUTE.equals(attribute.getName().getLocalPart())) {
     *                 if (propertyName.equals(attribute.getValue())) {
     *                     found = true;
     *                     break;
     *                 }
     *             }
     *         }
     *
     *         return found;
     *     }
     */

    public boolean isEncryptedElement(final XMLEvent xmlEvent) {
        return xmlEvent.isStartElement() &&
               xmlEvent.asStartElement().getName().toString().equals(PROPERTY_ELEMENT) &&
               elementHasEncryptionAttribute(xmlEvent.asStartElement());
    }

    private boolean elementHasEncryptionAttribute(final StartElement xmlEvent) {
        return xmlElementHasAttribute(xmlEvent, ENCRYPTION_ATTRIBUTE_NAME);
    }

    private boolean xmlElementHasAttribute(final StartElement xmlEvent, final String attributeName) {
        return !Objects.isNull(xmlEvent.getAttributeByName(new QName(attributeName)));
    }

//    public String encrypt(final String plainXmlContent) {
//        try {
//            Object doc = new XmlSlurper().invokeMethod("parseText", new Object[]{plainXmlContent});
//
//            final GPathResult[] nodesToEncrypt = doc.invokeMethod("depthFirst", new Object[0]).invokeMethod("findAll", new Object[]{new Closure(this, this) {
//                public Boolean doCall(GPathResult node) {
//                    return node.invokeMethod("text", new Object[0]) && node.encryption.equals(ENCRYPTION_NONE);
//                }
//
//            }});
//
//            logger.debug("Encrypting " + String.class.invokeMethod("valueOf", new Object[]{nodesToEncrypt.invokeMethod("size", new Object[0])}) + " element(s) of XML document");
//
//            if (nodesToEncrypt.invokeMethod("size", new Object[0]).equals(0)) {
//                return plainXmlContent;
//            }
//
//
//            nodesToEncrypt.invokeMethod("each", new Object[]{new Closure(this, this) {
//                public Object doCall(Object node) {
//                    String groupIdentifier = (String) node.invokeMethod("parent", new Object[0]).identifier;
//                    String propertyName = (String) node.name;
//                    String encryptedValue = XmlEncryptor.this.encryptionProvider.protect(node.invokeMethod("text", new Object[0]).invokeMethod("trim", new Object[0]), providerFactory.getPropertyContext(groupIdentifier, propertyName));
//                    node.encryption = XmlEncryptor.this.encryptionProvider.getIdentifierKey();
//                    return node.invokeMethod("replaceBody", new Object[]{encryptedValue});
//                }
//
//            }});
//
//            // Does not preserve whitespace formatting or comments
//            final String updatedXml = XmlUtil.invokeMethod("serialize", new Object[]{doc});
//            logger.debug("Updated XML content: " + updatedXml);
//            return updatedXml;
//        } catch (Exception e) {
//            throw new RuntimeException("Cannot encrypt XML content", e);
//        }
//
//    }
////
//    public void writeXmlFile(OutputStream updatedXmlContent, final String outputXmlPath, String inputXmlPath) throws IOException {
//        File outputXmlFile = new File(outputXmlPath);
//        FileWriter writer = new FileOutputStream(outputXmlFile);
//
//        if (!outputXmlFile.exists() && outputXmlFile.canWrite()) {
//            writer.write
//        } else {
//
//        }
//
//
////        if (ToolUtilities.invokeMethod("isSafeToWrite", new Object[]{outputXmlFile}).asBoolean()) {
////            String finalXmlContent = serializeXmlContentAndPreserveFormatIfPossible(updatedXmlContent, inputXmlPath);
////            outputXmlFile.text = finalXmlContent;
////        } else {
////            throw new IOException("The XML file at " + outputXmlPath + " must be writable by the user running this tool");
////        }
//    }



//    public static InputStream loadXmlFile(final String xmlFilePath) throws IOException {
//        File xmlFile = new File(xmlFilePath);
//        if (xmlFile.canRead()) {
//            try {
//                FileInputStream xmlContent = new FileInputStream(xmlFile);
//                return xmlContent;
//            } catch (RuntimeException e) {
//                throw new IOException("Cannot load XML from " + xmlFilePath, e);
//            }
//        } else {
//            throw new IOException("File at " + xmlFilePath + " must exist and be readable by user running this tool.");
//        }
//    }
//
//
//    public String serializeXmlContentAndPreserveFormatIfPossible(String updatedXmlContent, String inputXmlPath) {
//        String finalXmlContent;
//        File inputXmlFile = new File(inputXmlPath);
//        if (ToolUtilities.invokeMethod("canRead", new Object[]{inputXmlFile}).asBoolean()) {
//            String originalXmlContent = new File(inputXmlPath).text;
//            // Instead of just writing the XML content to a file, this method attempts to maintain
//            // the structure of the original file.
//            finalXmlContent = ((String) (serializeXmlContentAndPreserveFormat(updatedXmlContent, originalXmlContent).invokeMethod("join", new Object[]{"\n"})));
//        } else {
//            finalXmlContent = updatedXmlContent;
//        }
//
//        return finalXmlContent;
//    }
//
//    /**
//     * Given an original XML file and updated XML content, create the lines for an updated, minimally altered, serialization.
//     * Concrete classes extending this class must implement this method using specific knowledge of the XML document.
//     *
//     * @param finalXmlContent the xml content to serialize
//     * @param inputXmlFile    the original input xml file to use as a template for formatting the serialization
//     * @return the lines of serialized finalXmlContent that are close in raw format to originalInputXmlFile
//     */
//    public abstract List<String> serializeXmlContentAndPreserveFormat(String updatedXmlContent, String originalXmlContent);
//
//    public static String markXmlNodesForEncryption(String plainXmlContent, final String gPath, Object gPathCallback) {
//        String markedXmlContent;
//        try {
//            Object doc = new XmlSlurper().invokeMethod("parseText", new Object[]{plainXmlContent});
//            // Find the provider element by class even if it has been renamed
//            final Object sensitiveProperties = gPathCallback.call(doc."${gPath}");
//
//            logger.debug("Marking " + String.class.invokeMethod("valueOf", new Object[]{sensitiveProperties.invokeMethod("size", new Object[0])}) + " sensitive element(s) of XML to be encrypted");
//
//            if (sensitiveProperties.invokeMethod("size", new Object[0]).equals(0)) {
//                logger.debug("No populated sensitive properties found in XML content");
//                return plainXmlContent;
//            }
//
//
//            sensitiveProperties.invokeMethod("each", new Object[]{new Closure(this, this) {
//                public String doCall(Object it) {
//                    return it.encryption = ENCRYPTION_NONE;
//                }
//
//                public String doCall() {
//                    return doCall(null);
//                }
//
//            }});
//
//            // Does not preserve whitespace formatting or comments
//            // TODO: Switch to XmlParser & XmlNodePrinter to maintain "empty" element structure
//            return ((String) (markedXmlContent = ((String) (XmlUtil.invokeMethod("serialize", new Object[]{doc})))));
//        } catch (Exception e) {
//            logger.debug("Encountered exception", e);
//            throw new RuntimeException(e);
//        }
//
//    }

    // TODO: remove SAX parsing
//    public XMLReader getSaxParser() throws ParserConfigurationException, SAXException {
//        return getSaxParser(false, true, false);
//    }
//
//    public XMLReader getSaxParser(final boolean validating, final boolean namespaceAware, boolean allowDocTypeDeclaration) throws ParserConfigurationException, SAXException {
//        SAXParserFactory factory = SAXParserFactory.newInstance();
//        factory.setNamespaceAware(namespaceAware);
//        factory.setValidating(validating);
//        setQuietly(factory, XMLConstants.FEATURE_SECURE_PROCESSING, true);
//        setQuietly(factory, "http://apache.org/xml/features/disallow-doctype-decl", !allowDocTypeDeclaration);
//        return factory.newSAXParser().getXMLReader();
//    }
//
//    private static void setQuietly(SAXParserFactory factory, String feature, boolean value) {
//        try {
//            factory.setFeature(feature, value);
//        }
//        catch (ParserConfigurationException | SAXNotSupportedException | SAXNotRecognizedException ignored) { }
//    }


    // Streaming API for XML (stax) -- read only
//    public XMLStreamReader getStreamXmlReader(final StreamSource streamSource) {
//        Objects.requireNonNull(streamSource, "StreamSource required");
//
//        final XMLInputFactory inputFactory = XMLInputFactory.newFactory();
//        inputFactory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
//        inputFactory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
//
//        try {
//            return inputFactory.createXMLStreamReader(streamSource);
//        } catch (final XMLStreamException e) {
//            logger.error("XML stream reader creation failed", e);
//        }
//    }


    public XMLEventReader getXMLReader(final InputStream fileStream) throws XMLStreamException, FileNotFoundException {
        XMLInputFactory xmlInputFactory = XMLInputFactory.newFactory();
        xmlInputFactory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
        xmlInputFactory.setProperty(XMLInputFactory.SUPPORT_DTD, false);

        return xmlInputFactory.createXMLEventReader(fileStream);
    }

//    private File getTemporaryFile(final File ) {
//
//    }


    // Stax
//    private static List<String> parseXML(String fileName) throws FileNotFoundException {
//        List<String> empList = new ArrayList<>();
//        String emp = null;
//        XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
//        try {
//            XMLEventReader xmlEventReader = xmlInputFactory.createXMLEventReader(new FileInputStream(fileName));
//            while(xmlEventReader.hasNext()){
//                XMLEvent xmlEvent = xmlEventReader.nextEvent();
//                if (xmlEvent.isStartElement()){
//                    StartElement startElement = xmlEvent.asStartElement();
//                    if(startElement.getName().getLocalPart().equals("Employee")){
//                        emp = new String();
//                        //Get the 'id' attribute from Employee element
//                        Attribute idAttr = startElement.getAttributeByName(new QName("id"));
//                        if(idAttr != null){
//                            emp.setId(Integer.parseInt(idAttr.getValue()));
//                        }
//                    }
//                    //set the other varibles from xml elements
//                    else if(startElement.getName().getLocalPart().equals("age")){
//                        xmlEvent = xmlEventReader.nextEvent();
//                        emp.setAge(Integer.parseInt(xmlEvent.asCharacters().getData()));
//                    }else if(startElement.getName().getLocalPart().equals("name")){
//                        xmlEvent = xmlEventReader.nextEvent();
//                        emp.setName(xmlEvent.asCharacters().getData());
//                    }else if(startElement.getName().getLocalPart().equals("gender")){
//                        xmlEvent = xmlEventReader.nextEvent();
//                        emp.setGender(xmlEvent.asCharacters().getData());
//                    }else if(startElement.getName().getLocalPart().equals("role")){
//                        xmlEvent = xmlEventReader.nextEvent();
//                        emp.setRole(xmlEvent.asCharacters().getData());
//                    }
//                }
//                //if Employee end element is reached, add employee object to list
//                if(xmlEvent.isEndElement()){
//                    EndElement endElement = xmlEvent.asEndElement();
//                    if(endElement.getName().getLocalPart().equals("Employee")){
//                        empList.add(emp);
//                    }
//                }
//            }
//
//        } catch (FileNotFoundException | XMLStreamException e) {
//            e.printStackTrace();
//        }
//        return empList;
//    }
}
