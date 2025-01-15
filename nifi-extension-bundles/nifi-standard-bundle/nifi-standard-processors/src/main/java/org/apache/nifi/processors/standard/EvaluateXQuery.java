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

import net.sf.saxon.s9api.Processor;
import net.sf.saxon.s9api.SaxonApiException;
import net.sf.saxon.s9api.XQueryCompiler;
import net.sf.saxon.s9api.XQueryEvaluator;
import net.sf.saxon.s9api.XQueryExecutable;
import net.sf.saxon.s9api.XdmItem;
import net.sf.saxon.s9api.XdmNode;
import net.sf.saxon.s9api.XdmValue;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.SystemResourceConsiderations;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.xml.DocumentTypeAllowedDocumentProvider;
import org.apache.nifi.xml.processing.ProcessingException;
import org.apache.nifi.xml.processing.parsers.StandardDocumentProvider;
import org.apache.nifi.xml.processing.transform.StandardTransformProvider;
import org.w3c.dom.Document;

import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

@SideEffectFree
@SupportsBatching
@Tags({"XML", "evaluate", "XPath", "XQuery"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription(
        "Evaluates one or more XQueries against the content of a FlowFile.  The results of those XQueries are assigned "
        + "to FlowFile Attributes or are written to the content of the FlowFile itself, depending on configuration of "
        + "the Processor.  XQueries are entered by adding user-defined properties; the name of the property maps to the "
        + "Attribute Name into which the result will be placed (if the Destination is 'flowfile-attribute'; otherwise, "
        + "the property name is ignored).  The value of the property must be a valid XQuery.  If the XQuery returns more "
        + "than one result, new attributes or FlowFiles (for Destinations of 'flowfile-attribute' or 'flowfile-content' "
        + "respectively) will be created for each result (attributes will have a '.n' one-up number appended to the "
        + "specified attribute name).  If any provided XQuery returns a result, the FlowFile(s) will be routed to "
        + "'matched'. If no provided XQuery returns a result, the FlowFile will be routed to 'unmatched'.  If the "
        + "Destination is 'flowfile-attribute' and the XQueries matche nothing, no attributes will be applied to the "
        + "FlowFile.")
@WritesAttribute(attribute = "user-defined", description = "This processor adds user-defined attributes if the <Destination> property is set to flowfile-attribute .")
@DynamicProperty(name = "A FlowFile attribute(if <Destination> is set to 'flowfile-attribute'", value = "An XQuery", description = "If <Destination>='flowfile-attribute' "
        + "then the FlowFile attribute is set to the result of the XQuery.  If <Destination>='flowfile-content' then the FlowFile content is set to the result of the XQuery.")
@SystemResourceConsiderations({
        @SystemResourceConsideration(resource = SystemResource.MEMORY, description = "Processing requires reading the entire FlowFile into memory")
})
public class EvaluateXQuery extends AbstractProcessor {

    public static final String DESTINATION_ATTRIBUTE = "flowfile-attribute";
    public static final String DESTINATION_CONTENT = "flowfile-content";

    public static final String OUTPUT_METHOD_XML = "xml";
    public static final String OUTPUT_METHOD_HTML = "html";
    public static final String OUTPUT_METHOD_TEXT = "text";

    public static final PropertyDescriptor DESTINATION = new PropertyDescriptor.Builder()
            .name("Destination")
            .description(
                    "Indicates whether the results of the XQuery evaluation are written to the FlowFile content or a "
                    + "FlowFile attribute. If set to <flowfile-content>, only one XQuery may be specified and the property "
                    + "name is ignored.  If set to <flowfile-attribute> and the XQuery returns more than one result, "
                    + "multiple attributes will be added to theFlowFile, each named with a '.n' one-up number appended to "
                    + "the specified attribute name")
            .required(true)
            .allowableValues(DESTINATION_CONTENT, DESTINATION_ATTRIBUTE)
            .defaultValue(DESTINATION_CONTENT)
            .build();

    public static final PropertyDescriptor XML_OUTPUT_METHOD = new PropertyDescriptor.Builder()
            .name("Output: Method")
            .description("Identifies the overall method that should be used for outputting a result tree.")
            .required(true)
            .allowableValues(OUTPUT_METHOD_XML, OUTPUT_METHOD_HTML, OUTPUT_METHOD_TEXT)
            .defaultValue(OUTPUT_METHOD_XML)
            .build();

    public static final PropertyDescriptor XML_OUTPUT_OMIT_XML_DECLARATION = new PropertyDescriptor.Builder()
            .name("Output: Omit XML Declaration")
            .description("Specifies whether the processor should output an XML declaration when transforming a result tree.")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor XML_OUTPUT_INDENT = new PropertyDescriptor.Builder()
            .name("Output: Indent")
            .description("Specifies whether the processor may add additional whitespace when outputting a result tree.")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor VALIDATE_DTD = new PropertyDescriptor.Builder()
            .displayName("Allow DTD")
            .name("Validate DTD")
            .description("Allow embedded Document Type Declaration in XML. "
                    + "This feature should be disabled to avoid XML entity expansion vulnerabilities.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            DESTINATION,
            XML_OUTPUT_METHOD,
            XML_OUTPUT_OMIT_XML_DECLARATION,
            XML_OUTPUT_INDENT,
            VALIDATE_DTD
    );

    public static final Relationship REL_MATCH = new Relationship.Builder()
            .name("matched")
            .description("FlowFiles are routed to this relationship when the XQuery is successfully evaluated and the FlowFile "
                    + "is modified as a result")
            .build();

    public static final Relationship REL_NO_MATCH = new Relationship.Builder()
            .name("unmatched")
            .description("FlowFiles are routed to this relationship when the XQuery does not match the content of the FlowFile "
                    + "and the Destination is set to flowfile-content")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles are routed to this relationship when the XQuery cannot be evaluated against the content of "
                    + "the FlowFile.")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_MATCH,
            REL_NO_MATCH,
            REL_FAILURE
    );

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(context));

        final String destination = context.getProperty(DESTINATION).getValue();
        if (DESTINATION_CONTENT.equals(destination)) {
            int xQueryCount = 0;
            for (final PropertyDescriptor desc : context.getProperties().keySet()) {
                if (desc.isDynamic()) {
                    xQueryCount++;
                }
            }
            if (xQueryCount != 1) {
                results.add(new ValidationResult.Builder().subject("XQueries").valid(false)
                        .explanation("Exactly one XQuery must be set if using destination of " + DESTINATION_CONTENT).build());
            }
        }
        return results;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .expressionLanguageSupported(ExpressionLanguageScope.NONE)
                .addValidator(new XQueryValidator())
                .required(false)
                .dynamic(true)
                .build();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final List<FlowFile> flowFileBatch = session.get(50);
        if (flowFileBatch.isEmpty()) {
            return;
        }
        final ComponentLog logger = getLogger();
        final Map<String, XQueryExecutable> attributeToXQueryMap = new HashMap<>();

        final Processor proc = new Processor(false);
        final XQueryCompiler comp = proc.newXQueryCompiler();

        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (!entry.getKey().isDynamic()) {
                continue;
            }
            final XQueryExecutable exp;
            try {
                exp = comp.compile(entry.getValue());
                attributeToXQueryMap.put(entry.getKey().getName(), exp);
            } catch (SaxonApiException e) {
                throw new ProcessException(e);  // should not happen because we've already validated the XQuery (in XQueryValidator)
            }
        }

        final String destination = context.getProperty(DESTINATION).getValue();
        final boolean validateDeclaration = context.getProperty(VALIDATE_DTD).asBoolean();

        flowFileLoop:
        for (FlowFile flowFile : flowFileBatch) {
            if (!isScheduled()) {
                session.rollback();
                return;
            }

            final AtomicReference<DOMSource> sourceRef = new AtomicReference<>(null);
            try {
                session.read(flowFile, rawIn -> {
                    try (final InputStream in = new BufferedInputStream(rawIn)) {
                        final StandardDocumentProvider documentProvider = validateDeclaration
                                ? new DocumentTypeAllowedDocumentProvider()
                                : new StandardDocumentProvider();
                        documentProvider.setNamespaceAware(true);
                        final Document document = documentProvider.parse(in);
                        sourceRef.set(new DOMSource(document));
                    }
                });
            } catch (final Exception e) {
                logger.error("Input parsing failed {}", flowFile, e);
                session.transfer(flowFile, REL_FAILURE);
                continue;
            }

            final Map<String, String> xQueryResults = new HashMap<>();
            List<FlowFile> childrenFlowFiles = new ArrayList<>();

            for (final Map.Entry<String, XQueryExecutable> entry : attributeToXQueryMap.entrySet()) {
                try {
                    XQueryEvaluator qe = entry.getValue().load();
                    qe.setSource(sourceRef.get());
                    XdmValue result = qe.evaluate();

                    if (DESTINATION_ATTRIBUTE.equals(destination)) {
                        int index = 1;
                        for (XdmItem item : result) {
                            String value = formatItem(item, context);
                            String attributeName = entry.getKey();
                            if (result.size() > 1) {
                                attributeName += "." + index++;
                            }
                            xQueryResults.put(attributeName, value);
                        }
                    } else { // if (DESTINATION_CONTENT.equals(destination)){
                        if (result.isEmpty()) {
                            logger.info("No XQuery results found {}", flowFile);
                            session.transfer(flowFile, REL_NO_MATCH);
                            continue flowFileLoop;
                        } else if (result.size() == 1) {
                            final XdmItem item = result.itemAt(0);
                            flowFile = session.write(flowFile, rawOut -> {
                                try (final OutputStream out = new BufferedOutputStream(rawOut)) {
                                    writeFormattedItem(item, context, out);
                                } catch (final ProcessingException e) {
                                    throw new IOException("Writing Formatted Output failed", e);
                                }
                            });
                        } else {
                            for (final XdmItem item : result) {
                                FlowFile ff = session.clone(flowFile);
                                ff = session.write(ff, rawOut -> {
                                    try (final OutputStream out = new BufferedOutputStream(rawOut)) {
                                        try {
                                            writeFormattedItem(item, context, out);
                                        } catch (final ProcessingException e) {
                                            throw new IOException("Writing Formatted Output failed", e);
                                        }
                                    }
                                });
                                childrenFlowFiles.add(ff);
                            }
                        }
                    }
                } catch (final SaxonApiException e) {
                    logger.error("XQuery Property [{}] processing failed", entry.getKey(), e);
                    session.transfer(flowFile, REL_FAILURE);
                    session.remove(childrenFlowFiles);
                    continue flowFileLoop;
                } catch (final IOException e) {
                    logger.error("XQuery Property [{}] configuration failed", entry.getKey(), e);
                    session.transfer(flowFile, REL_FAILURE);
                    session.remove(childrenFlowFiles);
                    continue flowFileLoop;
                }
            }

            if (DESTINATION_ATTRIBUTE.equals(destination)) {
                flowFile = session.putAllAttributes(flowFile, xQueryResults);
                final Relationship destRel = xQueryResults.isEmpty() ? REL_NO_MATCH : REL_MATCH;
                logger.info("XQuery results found [{}] for {}", xQueryResults.size(), flowFile);
                session.transfer(flowFile, destRel);
                session.getProvenanceReporter().modifyAttributes(flowFile);
            } else { // if (DESTINATION_CONTENT.equals(destination)) {
                if (!childrenFlowFiles.isEmpty()) {
                    logger.info("XQuery results found [{}] for {} FlowFiles created [{}]", xQueryResults.size(), flowFile, childrenFlowFiles.size());
                    session.transfer(childrenFlowFiles, REL_MATCH);
                    session.remove(flowFile);
                } else {
                    logger.info("XQuery results found for {} content updated", flowFile);
                    session.transfer(flowFile, REL_MATCH);
                    session.getProvenanceReporter().modifyContent(flowFile);
                }
            }
        } // end flowFileLoop
    }

    private String formatItem(XdmItem item, ProcessContext context) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        writeFormattedItem(item, context, baos);
        return baos.toString(StandardCharsets.UTF_8);
    }

    void writeFormattedItem(XdmItem item, ProcessContext context, OutputStream out) throws IOException {

        if (item.isAtomicValue()) {
            out.write(item.getStringValue().getBytes(StandardCharsets.UTF_8));
        } else { // item is an XdmNode
            XdmNode node = (XdmNode) item;
            switch (node.getNodeKind()) {
                case DOCUMENT:
                case ELEMENT:
                    final StandardTransformProvider transformProvider = new StandardTransformProvider();
                    final String method = context.getProperty(XML_OUTPUT_METHOD).getValue();
                    transformProvider.setMethod(method);

                    final boolean indentEnabled = context.getProperty(XML_OUTPUT_INDENT).asBoolean();
                    transformProvider.setIndent(indentEnabled);

                    final boolean omitXmlDeclaration = context.getProperty(XML_OUTPUT_OMIT_XML_DECLARATION).asBoolean();
                    transformProvider.setOmitXmlDeclaration(omitXmlDeclaration);

                    final StreamResult result = new StreamResult(out);
                    transformProvider.transform(node.asSource(), result);
                    break;
                default:
                    out.write(node.getStringValue().getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    private static class XQueryValidator implements Validator {

        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext validationContext) {
            try {
                final Processor proc = new Processor(false);
                final XQueryCompiler comp = proc.newXQueryCompiler();
                String error = null;
                try {
                    comp.compile(input);
                } catch (final Exception e) {
                    error = e.toString();
                }
                return new ValidationResult.Builder().input(input).subject(subject).valid(error == null).explanation(error).build();
            } catch (final Exception e) {
                return new ValidationResult.Builder().input(input).subject(subject).valid(false)
                        .explanation("Unable to initialize XQuery engine due to " + e).build();
            }
        }
    }
}
