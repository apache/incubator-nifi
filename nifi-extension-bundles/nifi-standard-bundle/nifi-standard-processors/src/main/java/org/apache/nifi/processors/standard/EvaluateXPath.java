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

import net.sf.saxon.xpath.XPathEvaluator;
import net.sf.saxon.xpath.XPathFactoryImpl;
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
import org.apache.nifi.annotation.lifecycle.OnScheduled;
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
import org.apache.nifi.processors.standard.xml.DocumentTypeAllowedDocumentProvider;
import org.apache.nifi.xml.processing.ProcessingException;
import org.apache.nifi.xml.processing.parsers.StandardDocumentProvider;
import org.apache.nifi.xml.processing.transform.StandardTransformProvider;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.namespace.QName;
import javax.xml.transform.Source;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
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

import static javax.xml.xpath.XPathConstants.NODESET;
import static javax.xml.xpath.XPathConstants.STRING;

@SideEffectFree
@SupportsBatching
@Tags({"XML", "evaluate", "XPath"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Evaluates one or more XPaths against the content of a FlowFile. The results of those XPaths are assigned to "
        + "FlowFile Attributes or are written to the content of the FlowFile itself, depending on configuration of the "
        + "Processor. XPaths are entered by adding user-defined properties; the name of the property maps to the Attribute "
        + "Name into which the result will be placed (if the Destination is flowfile-attribute; otherwise, the property name is ignored). "
        + "The value of the property must be a valid XPath expression. If the XPath evaluates to more than one node and the Return Type is "
        + "set to 'nodeset' (either directly, or via 'auto-detect' with a Destination of "
        + "'flowfile-content'), the FlowFile will be unmodified and will be routed to failure. If the XPath does not "
        + "evaluate to a Node, the FlowFile will be routed to 'unmatched' without having its contents modified. If Destination is "
        + "flowfile-attribute and the expression matches nothing, attributes will be created with empty strings as the value, and the "
        + "FlowFile will always be routed to 'matched'")
@WritesAttribute(attribute = "user-defined", description = "This processor adds user-defined attributes if the <Destination> property is set to flowfile-attribute.")
@DynamicProperty(name = "A FlowFile attribute(if <Destination> is set to 'flowfile-attribute'", value = "An XPath expression", description = "If <Destination>='flowfile-attribute' "
        + "then the FlowFile attribute is set to the result of the XPath Expression.  If <Destination>='flowfile-content' then the FlowFile content is set to the result of the XPath Expression.")
@SystemResourceConsiderations({
        @SystemResourceConsideration(resource = SystemResource.MEMORY, description = "Processing requires reading the entire FlowFile into memory")
})
public class EvaluateXPath extends AbstractProcessor {

    public static final String DESTINATION_ATTRIBUTE = "flowfile-attribute";
    public static final String DESTINATION_CONTENT = "flowfile-content";
    public static final String RETURN_TYPE_AUTO = "auto-detect";
    public static final String RETURN_TYPE_NODESET = "nodeset";
    public static final String RETURN_TYPE_STRING = "string";

    public static final PropertyDescriptor DESTINATION = new PropertyDescriptor.Builder()
            .name("Destination")
            .description("Indicates whether the results of the XPath evaluation are written to the FlowFile content or a FlowFile attribute; "
                    + "if using attribute, must specify the Attribute Name property. If set to flowfile-content, only one XPath may be specified, "
                    + "and the property name is ignored.")
            .required(true)
            .allowableValues(DESTINATION_CONTENT, DESTINATION_ATTRIBUTE)
            .defaultValue(DESTINATION_CONTENT)
            .build();

    public static final PropertyDescriptor RETURN_TYPE = new PropertyDescriptor.Builder()
            .name("Return Type")
            .description("Indicates the desired return type of the Xpath expressions.  Selecting 'auto-detect' will set the return type to 'nodeset' "
                    + "for a Destination of 'flowfile-content', and 'string' for a Destination of 'flowfile-attribute'.")
            .required(true)
            .allowableValues(RETURN_TYPE_AUTO, RETURN_TYPE_NODESET, RETURN_TYPE_STRING)
            .defaultValue(RETURN_TYPE_AUTO)
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
            RETURN_TYPE,
            VALIDATE_DTD
    );

    public static final Relationship REL_MATCH = new Relationship.Builder()
            .name("matched")
            .description("FlowFiles are routed to this relationship "
                    + "when the XPath is successfully evaluated and the FlowFile is modified as a result")
            .build();
    public static final Relationship REL_NO_MATCH = new Relationship.Builder()
            .name("unmatched")
            .description("FlowFiles are routed to this relationship "
                    + "when the XPath does not match the content of the FlowFile and the Destination is set to flowfile-content")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles are routed to this relationship "
                    + "when the XPath cannot be evaluated against the content of the FlowFile; for instance, if the FlowFile is not valid XML, or if the Return "
                    + "Type is 'nodeset' and the XPath evaluates to multiple nodes")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_MATCH,
            REL_NO_MATCH,
            REL_FAILURE
    );

    private final AtomicReference<XPathFactory> factoryRef = new AtomicReference<>();

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(context));

        final String destination = context.getProperty(DESTINATION).getValue();
        if (DESTINATION_CONTENT.equals(destination)) {
            int xpathCount = 0;

            for (final PropertyDescriptor desc : context.getProperties().keySet()) {
                if (desc.isDynamic()) {
                    xpathCount++;
                }
            }

            if (xpathCount != 1) {
                results.add(new ValidationResult.Builder().subject("XPaths").valid(false)
                        .explanation("Exactly one XPath must be set if using destination of " + DESTINATION_CONTENT).build());
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

    @OnScheduled
    public void initializeXPathFactory() {
        factoryRef.set(new XPathFactoryImpl());
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .expressionLanguageSupported(ExpressionLanguageScope.NONE)
                .addValidator(new XPathValidator())
                .required(false)
                .dynamic(true)
                .build();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final List<FlowFile> flowFiles = session.get(50);
        if (flowFiles.isEmpty()) {
            return;
        }

        final ComponentLog logger = getLogger();

        final XPathFactory factory = factoryRef.get();
        final XPathEvaluator xpathEvaluator = (XPathEvaluator) factory.newXPath();
        final Map<String, XPathExpression> attributeToXPathMap = new HashMap<>();

        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (!entry.getKey().isDynamic()) {
                continue;
            }
            final XPathExpression xpathExpression;
            try {
                xpathExpression = xpathEvaluator.compile(entry.getValue());
                attributeToXPathMap.put(entry.getKey().getName(), xpathExpression);
            } catch (XPathExpressionException e) {
                throw new ProcessException(e);  // should not happen because we've already validated the XPath (in XPathValidator)
            }
        }

        final String destination = context.getProperty(DESTINATION).getValue();
        final QName returnType;

        switch (context.getProperty(RETURN_TYPE).getValue()) {
            case RETURN_TYPE_AUTO:
                if (DESTINATION_ATTRIBUTE.equals(destination)) {
                    returnType = STRING;
                } else if (DESTINATION_CONTENT.equals(destination)) {
                    returnType = NODESET;
                } else {
                    throw new IllegalStateException("The only possible destinations should be CONTENT or ATTRIBUTE...");
                }
                break;
            case RETURN_TYPE_NODESET:
                returnType = NODESET;
                break;
            case RETURN_TYPE_STRING:
                returnType = STRING;
                break;
            default:
                throw new IllegalStateException("There are no other return types...");
        }

        final boolean validatingDeclaration = context.getProperty(VALIDATE_DTD).asBoolean();

        final StandardTransformProvider transformProvider = new StandardTransformProvider();
        transformProvider.setIndent(true);
        flowFileLoop:
        for (FlowFile flowFile : flowFiles) {
            final AtomicReference<Throwable> error = new AtomicReference<>(null);
            final AtomicReference<Source> sourceRef = new AtomicReference<>(null);

            try {
                session.read(flowFile, rawIn -> {
                    try (final InputStream in = new BufferedInputStream(rawIn)) {
                        final StandardDocumentProvider documentProvider = validatingDeclaration
                                ? new DocumentTypeAllowedDocumentProvider()
                                : new StandardDocumentProvider();
                        final Document document = documentProvider.parse(in);
                        sourceRef.set(new DOMSource(document));
                    }
                });
            } catch (final Exception e) {
                logger.error("Input parsing failed {}", flowFile, e);
                session.transfer(flowFile, REL_FAILURE);
                continue;
            }

            final Map<String, String> xpathResults = new HashMap<>();

            for (final Map.Entry<String, XPathExpression> entry : attributeToXPathMap.entrySet()) {
                Object result;
                try {
                    result = entry.getValue().evaluate(sourceRef.get(), returnType);
                    if (result == null) {
                        continue;
                    }
                } catch (final XPathExpressionException e) {
                    logger.error("XPath Property [{}] evaluation on {} failed", flowFile, entry.getKey(), e);
                    session.transfer(flowFile, REL_FAILURE);
                    continue flowFileLoop;
                }

                if (returnType == NODESET) {
                    final NodeList nodeList = (NodeList) result;
                    if (nodeList.getLength() == 0) {
                        logger.info("XPath evaluation on {} produced no results", flowFile);
                        session.transfer(flowFile, REL_NO_MATCH);
                        continue flowFileLoop;
                    } else if (nodeList.getLength() > 1) {
                        logger.error("XPath evaluation on {} produced unexpected results [{}]", flowFile, nodeList.getLength());
                        session.transfer(flowFile, REL_FAILURE);
                        continue flowFileLoop;
                    }
                    final Node firstNode = nodeList.item(0);
                    final Source sourceNode = new DOMSource(firstNode);

                    if (DESTINATION_ATTRIBUTE.equals(destination)) {
                        try {
                            ByteArrayOutputStream baos = new ByteArrayOutputStream();
                            final StreamResult streamResult = new StreamResult(baos);
                            transformProvider.transform(sourceNode, streamResult);
                            xpathResults.put(entry.getKey(), baos.toString(StandardCharsets.UTF_8));
                        } catch (final ProcessingException e) {
                            error.set(e);
                        }

                    } else if (DESTINATION_CONTENT.equals(destination)) {
                        flowFile = session.write(flowFile, rawOut -> {
                            try (final OutputStream out = new BufferedOutputStream(rawOut)) {
                                final StreamResult streamResult = new StreamResult(out);
                                transformProvider.transform(sourceNode, streamResult);
                            } catch (final ProcessingException e) {
                                error.set(e);
                            }
                        });
                    }

                } else {
                    final String resultString = (String) result;

                    if (DESTINATION_ATTRIBUTE.equals(destination)) {
                        xpathResults.put(entry.getKey(), resultString);
                    } else if (DESTINATION_CONTENT.equals(destination)) {
                        flowFile = session.write(flowFile, rawOut -> {
                            try (final OutputStream out = new BufferedOutputStream(rawOut)) {
                                out.write(resultString.getBytes(StandardCharsets.UTF_8));
                            }
                        });
                    }
                }
            }

            if (error.get() == null) {
                if (DESTINATION_ATTRIBUTE.equals(destination)) {
                    flowFile = session.putAllAttributes(flowFile, xpathResults);
                    final Relationship destRel = xpathResults.isEmpty() ? REL_NO_MATCH : REL_MATCH;
                    logger.info("XPath evaluation on {} completed with results [{}]: content updated", flowFile, xpathResults.size());
                    session.transfer(flowFile, destRel);
                    session.getProvenanceReporter().modifyAttributes(flowFile);
                } else if (DESTINATION_CONTENT.equals(destination)) {
                    logger.info("XPath evaluation on {} completed: content updated", flowFile);
                    session.transfer(flowFile, REL_MATCH);
                    session.getProvenanceReporter().modifyContent(flowFile);
                }
            } else {
                logger.error("XPath evaluation on {} failed", flowFile, error.get());
                session.transfer(flowFile, REL_FAILURE);
            }
        }
    }

    private static class XPathValidator implements Validator {

        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext validationContext) {
            try {
                XPathFactory factory = new XPathFactoryImpl();
                final XPathEvaluator evaluator = (XPathEvaluator) factory.newXPath();

                String error = null;
                try {
                    evaluator.compile(input);
                } catch (final Exception e) {
                    error = e.toString();
                }

                return new ValidationResult.Builder().input(input).subject(subject).valid(error == null).explanation(error).build();
            } catch (final Exception e) {
                return new ValidationResult.Builder().input(input).subject(subject).valid(false)
                        .explanation("Unable to initialize XPath engine due to " + e).build();
            }
        }
    }
}
