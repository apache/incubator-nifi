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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.xml.processing.ProcessingException;
import org.apache.nifi.xml.processing.stream.StandardXMLStreamReaderProvider;
import org.apache.nifi.xml.processing.stream.XMLStreamReaderProvider;
import org.apache.nifi.xml.processing.validation.SchemaValidator;
import org.apache.nifi.xml.processing.validation.StandardSchemaValidator;
import org.xml.sax.SAXException;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.Source;
import javax.xml.transform.stax.StAXSource;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"xml", "schema", "validation", "xsd"})
@WritesAttributes({
    @WritesAttribute(attribute = "validatexml.invalid.error", description = "If the flow file is routed to the invalid relationship "
            + "the attribute will contain the error message resulting from the validation failure.")
})
@CapabilityDescription("Validates XML contained in a FlowFile. By default, the XML is contained in the FlowFile content. If the 'XML Source Attribute' property is set, the XML to be validated "
        + "is contained in the specified attribute. It is not recommended to use attributes to hold large XML documents; doing so could adversely affect system performance. "
        + "Full schema validation is performed if the processor is configured with the XSD schema details. Otherwise, the only validation performed is "
        + "to ensure the XML syntax is correct and well-formed, e.g. all opening tags are properly closed.")
@SystemResourceConsideration(resource = SystemResource.MEMORY, description = "While this processor supports processing XML within attributes, it is strongly discouraged to hold "
        + "large amounts of data in attributes. In general, attribute values should be as small as possible and hold no more than a couple hundred characters.")
@Restricted(
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.REFERENCE_REMOTE_RESOURCES,
                        explanation = "Schema configuration can reference resources over HTTP"
                )
        }
)
public class ValidateXml extends AbstractProcessor {

    public static final String ERROR_ATTRIBUTE_KEY = "validatexml.invalid.error";

    public static final PropertyDescriptor SCHEMA_FILE = new PropertyDescriptor.Builder()
            .name("Schema File")
            .displayName("Schema File")
            .description("The file path or URL to the XSD Schema file that is to be used for validation. If this property is blank, only XML syntax/structure will be validated.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE, ResourceType.URL)
            .build();
    public static final PropertyDescriptor XML_SOURCE_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("XML Source Attribute")
            .displayName("XML Source Attribute")
            .description("The name of the attribute containing XML to be validated. If this property is blank, the FlowFile content will be validated.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.ATTRIBUTE_KEY_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            SCHEMA_FILE,
            XML_SOURCE_ATTRIBUTE
    );

    public static final Relationship REL_VALID = new Relationship.Builder()
            .name("valid")
            .description("FlowFiles that are successfully validated against the schema, if provided, or verified to be well-formed XML are routed to this relationship")
            .build();
    public static final Relationship REL_INVALID = new Relationship.Builder()
            .name("invalid")
            .description("FlowFiles that are not valid according to the specified schema or contain invalid XML are routed to this relationship")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_VALID,
            REL_INVALID
    );

    private static final String SCHEMA_LANGUAGE = "http://www.w3.org/2001/XMLSchema";
    private static final SchemaValidator SCHEMA_VALIDATOR = new StandardSchemaValidator();
    private static final XMLStreamReaderProvider READER_PROVIDER = new StandardXMLStreamReaderProvider();

    private final AtomicReference<Schema> schemaRef = new AtomicReference<>();

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnScheduled
    public void parseSchema(final ProcessContext context) throws SAXException {
        if (context.getProperty(SCHEMA_FILE).isSet()) {
            final URL url = context.getProperty(SCHEMA_FILE).evaluateAttributeExpressions().asResource().asURL();
            final SchemaFactory schemaFactory = SchemaFactory.newInstance(SCHEMA_LANGUAGE);
            final Schema schema = schemaFactory.newSchema(url);
            schemaRef.set(schema);
        } else {
            schemaRef.set(null);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final List<FlowFile> flowFiles = session.get(50);
        if (flowFiles.isEmpty()) {
            return;
        }

        final ComponentLog logger = getLogger();
        final boolean attributeContainsXML = context.getProperty(XML_SOURCE_ATTRIBUTE).isSet();

        for (FlowFile flowFile : flowFiles) {
            final AtomicBoolean valid = new AtomicBoolean(true);
            final AtomicReference<Exception> exception = new AtomicReference<>(null);

            try {
                if (attributeContainsXML) {
                    // If XML source attribute is set, validate attribute value
                    String xml = flowFile.getAttribute(context.getProperty(XML_SOURCE_ATTRIBUTE).evaluateAttributeExpressions().getValue());
                    ByteArrayInputStream inputStream = new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));

                    validate(inputStream);
                } else {
                    // If XML source attribute is not set, validate flowfile content
                    session.read(flowFile, this::validate);
                }
            } catch (final RuntimeException e) {
                valid.set(false);
                exception.set(e);
            }

            // determine source location of XML for logging purposes
            String xmlSource = attributeContainsXML ? "attribute '" + context.getProperty(XML_SOURCE_ATTRIBUTE).evaluateAttributeExpressions().getValue() + "'" : "content";
            if (valid.get()) {
                if (context.getProperty(SCHEMA_FILE).isSet()) {
                    logger.debug("Successfully validated XML in {} of {} against schema; routing to 'valid'", xmlSource, flowFile);
                } else {
                    logger.debug("Successfully validated XML is well-formed in {} of {}; routing to 'valid'", xmlSource, flowFile);
                }
                session.getProvenanceReporter().route(flowFile, REL_VALID);
                session.transfer(flowFile, REL_VALID);
            } else {
                flowFile = session.putAttribute(flowFile, ERROR_ATTRIBUTE_KEY, exception.get().getLocalizedMessage());
                if (context.getProperty(SCHEMA_FILE).isSet()) {
                    logger.info("Failed to validate XML in {} of {} against schema due to {}; routing to 'invalid'", xmlSource, flowFile, exception.get().getLocalizedMessage());
                } else {
                    logger.info("Failed to validate XML is well-formed in {} of {} due to {}; routing to 'invalid'", xmlSource, flowFile, exception.get().getLocalizedMessage());
                }
                session.getProvenanceReporter().route(flowFile, REL_INVALID);
                session.transfer(flowFile, REL_INVALID);
            }
        }
    }

    private void validate(final InputStream in) {
        final Schema schema = schemaRef.get();
        if (schema == null) {
            // Parse Document without schema validation
            final XMLStreamReader reader = READER_PROVIDER.getStreamReader(new StreamSource(in));
            try {
                while (reader.hasNext()) {
                    reader.next();
                }
            } catch (final XMLStreamException e) {
                throw new ProcessingException("Reading stream failed: " + e.getMessage(), e);
            }
        } else {
            final XMLStreamReaderProvider readerProvider = new StandardXMLStreamReaderProvider();
            final XMLStreamReader reader = readerProvider.getStreamReader(new StreamSource(in));
            final Source source = new StAXSource(reader);
            SCHEMA_VALIDATOR.validate(schema, source);
        }
    }
}
