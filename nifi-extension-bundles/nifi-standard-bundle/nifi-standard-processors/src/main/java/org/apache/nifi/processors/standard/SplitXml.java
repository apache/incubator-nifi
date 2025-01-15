/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.standard;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.util.XmlElementNotifier;
import org.apache.nifi.xml.processing.ProcessingException;
import org.apache.nifi.xml.processing.sax.StandardInputSourceParser;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_COUNT;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_ID;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_INDEX;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.SEGMENT_ORIGINAL_FILENAME;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.copyAttributesToOriginal;

@SideEffectFree
@SupportsBatching
@Tags({"xml", "split"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Splits an XML File into multiple separate FlowFiles, each comprising a child or descendant of the original root element")
@WritesAttributes({
        @WritesAttribute(attribute = "fragment.identifier",
                description = "All split FlowFiles produced from the same parent FlowFile will have the same randomly generated UUID added for this attribute"),
        @WritesAttribute(attribute = "fragment.index",
                description = "A one-up number that indicates the ordering of the split FlowFiles that were created from a single parent FlowFile"),
        @WritesAttribute(attribute = "fragment.count",
                description = "The number of split FlowFiles generated from the parent FlowFile"),
        @WritesAttribute(attribute = "segment.original.filename ", description = "The filename of the parent FlowFile")
})
@SystemResourceConsideration(resource = SystemResource.MEMORY, description = "The entirety of the FlowFile's content (as a Document object) is read into memory, " +
        "in addition to all of the generated FlowFiles representing the split XML. A Document object can take approximately 10 times as much memory as the size of " +
        "the XML. For example, a 1 MB XML document may use 10 MB of memory. If many splits are generated due to the size of the XML, a two-phase approach may be " +
        "necessary to avoid excessive use of memory.")
public class SplitXml extends AbstractProcessor {

    public static final PropertyDescriptor SPLIT_DEPTH = new PropertyDescriptor.Builder()
            .name("Split Depth")
            .description("Indicates the XML-nesting depth to start splitting XML fragments. A depth of 1 means split the root's children, whereas a depth of"
                    + " 2 means split the root's children's children and so forth.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            SPLIT_DEPTH
    );

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original FlowFile that was split into segments. If the FlowFile fails processing, nothing will be sent to this relationship")
            .build();
    public static final Relationship REL_SPLIT = new Relationship.Builder()
            .name("split")
            .description("All segments of the original FlowFile will be routed to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a FlowFile fails processing for any reason (for example, the FlowFile is not valid XML), it will be routed to this relationship")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_ORIGINAL,
            REL_SPLIT,
            REL_FAILURE
    );

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }

        final int depth = context.getProperty(SPLIT_DEPTH).asInteger();
        final ComponentLog logger = getLogger();

        final List<FlowFile> splits = new ArrayList<>();
        final String fragmentIdentifier = UUID.randomUUID().toString();
        final AtomicInteger numberOfRecords = new AtomicInteger(0);
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(FRAGMENT_ID.key(), fragmentIdentifier);
        attributes.put(SEGMENT_ORIGINAL_FILENAME.key(), original.getAttribute(CoreAttributes.FILENAME.key()));
        final XmlSplitterSaxParser parser = new XmlSplitterSaxParser(xmlTree -> {
            FlowFile split = session.create(original);
            split = session.write(split, out -> out.write(xmlTree.getBytes(StandardCharsets.UTF_8)));
            attributes.put(FRAGMENT_INDEX.key(), Integer.toString(numberOfRecords.getAndIncrement()));
            split = session.putAllAttributes(split, attributes);
            splits.add(split);
        }, depth);

        final AtomicBoolean failed = new AtomicBoolean(false);
        session.read(original, rawIn -> {
            try (final InputStream in = new java.io.BufferedInputStream(rawIn)) {
                try {
                    final StandardInputSourceParser inputSourceParser = new StandardInputSourceParser();
                    inputSourceParser.setNamespaceAware(true);
                    inputSourceParser.parse(new InputSource(in), parser);
                } catch (final ProcessingException e) {
                    logger.error("Parsing failed {}", original, e);
                    failed.set(true);
                }
            }
        });

        if (failed.get()) {
            session.transfer(original, REL_FAILURE);
            session.remove(splits);
        } else {
            splits.forEach((split) -> {
                split = session.putAttribute(split, FRAGMENT_COUNT.key(), Integer.toString(numberOfRecords.get()));
                session.transfer(split, REL_SPLIT);
            });

            final FlowFile originalToTransfer = copyAttributesToOriginal(session, original, fragmentIdentifier, numberOfRecords.get());
            session.transfer(originalToTransfer, REL_ORIGINAL);
            logger.info("Split {} into {} FlowFiles", originalToTransfer, splits.size());
        }
    }

    private static class XmlSplitterSaxParser implements ContentHandler {

        private static final String XML_PROLOGUE = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
        private final XmlElementNotifier notifier;
        private final int splitDepth;
        private final StringBuilder sb = new StringBuilder(XML_PROLOGUE);
        private int depth = 0;
        private final Map<String, String> prefixMap = new TreeMap<>();

        public XmlSplitterSaxParser(XmlElementNotifier notifier, int splitDepth) {
            this.notifier = notifier;
            this.splitDepth = splitDepth;
        }

        @Override
        public void characters(char[] ch, int start, int length) throws SAXException {
            // if we're not at a level where we care about capturing text, then return
            if (depth <= splitDepth) {
                return;
            }

            // capture text
            for (int i = start; i < start + length; i++) {
                char c = ch[i];
                switch (c) {
                    case '<':
                        sb.append("&lt;");
                        break;
                    case '>':
                        sb.append("&gt;");
                        break;
                    case '&':
                        sb.append("&amp;");
                        break;
                    case '\'':
                        sb.append("&apos;");
                        break;
                    case '"':
                        sb.append("&quot;");
                        break;
                    default:
                        sb.append(c);
                        break;
                }
            }
        }

        @Override
        public void endDocument() {
        }

        @Override
        public void endElement(String uri, String localName, String qName) {
            // We have finished processing this element. Decrement the depth.
            int newDepth = --depth;

            // if we're at a level where we care about capturing text, then add the closing element
            if (newDepth >= splitDepth) {
                // Add the element end tag.
                sb.append("</").append(qName).append(">");
            }

            // If we have now returned to level 1, we have finished processing
            // a 2nd-level element. Send notification with the XML text and
            // erase the String Builder so that we can start
            // processing a new 2nd-level element.
            if (newDepth == splitDepth) {
                String elementTree = sb.toString();
                notifier.onXmlElementFound(elementTree);
                // Reset the StringBuilder to just the XML prolog.
                sb.setLength(XML_PROLOGUE.length());
            }
        }

        @Override
        public void endPrefixMapping(String prefix) {
            prefixMap.remove(prefixToNamespace(prefix));
        }

        @Override
        public void ignorableWhitespace(char[] ch, int start, int length) {
        }

        @Override
        public void processingInstruction(String target, String data) {
        }

        @Override
        public void setDocumentLocator(Locator locator) {
        }

        @Override
        public void skippedEntity(String name) {
        }

        @Override
        public void startDocument() {
        }

        @Override
        public void startElement(final String uri, final String localName, final String qName, final Attributes atts) {
            // Increment the current depth because start a new XML element.
            int newDepth = ++depth;
            // Output the element and its attributes if it is
            // not the root element.
            if (newDepth > splitDepth) {
                sb.append("<");
                sb.append(qName);

                final Set<String> attributeNames = new HashSet<>();
                int attCount = atts.getLength();
                for (int i = 0; i < attCount; i++) {
                    String attName = atts.getQName(i);
                    attributeNames.add(attName);
                    String attValue = StringEscapeUtils.escapeXml10(atts.getValue(i));
                    sb.append(" ").append(attName).append("=").append("\"").append(attValue).append("\"");
                }

                // If this is the first node we're outputting write out
                // any additional namespace declarations that are required
                if (splitDepth == newDepth - 1) {
                    for (Entry<String, String> entry : prefixMap.entrySet()) {
                        // If we've already added this namespace as an attribute then continue
                        if (attributeNames.contains(entry.getKey())) {
                            continue;
                        }
                        sb.append(" ");
                        sb.append(entry.getKey());
                        sb.append("=\"");
                        sb.append(entry.getValue());
                        sb.append("\"");
                    }
                }

                sb.append(">");
            }
        }

        @Override
        public void startPrefixMapping(String prefix, String uri) {
            final String ns = prefixToNamespace(prefix);
            prefixMap.put(ns, uri);
        }

        private String prefixToNamespace(String prefix) {
            final String ns;
            if (prefix.isEmpty()) {
                ns = "xmlns";
            } else {
                ns = "xmlns:" + prefix;
            }
            return ns;
        }
    }

}
