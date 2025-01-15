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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.LookupService;
import org.apache.nifi.lookup.StringLookupService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.xml.processing.ProcessingException;
import org.apache.nifi.xml.processing.stream.StandardXMLStreamReaderProvider;
import org.apache.nifi.xml.processing.stream.XMLStreamReaderProvider;

import javax.xml.XMLConstants;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.ErrorListener;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Templates;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stax.StAXSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@SideEffectFree
@SupportsBatching
@Tags({"xml", "xslt", "transform"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Applies the provided XSLT file to the FlowFile XML payload. A new FlowFile is created "
        + "with transformed content and is routed to the 'success' relationship. If the XSL transform "
        + "fails, the original FlowFile is routed to the 'failure' relationship")
@DynamicProperty(name = "An XSLT transform parameter name", value = "An XSLT transform parameter value",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        description = "These XSLT parameters are passed to the transformer")
public class TransformXml extends AbstractProcessor {

    public static final PropertyDescriptor XSLT_FILE_NAME = new PropertyDescriptor.Builder()
            .name("XSLT file name")
            .description("Provides the name (including full path) of the XSLT file to apply to the FlowFile XML content."
                    + "One of the 'XSLT file name' and 'XSLT Lookup' properties must be defined.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
            .build();

    public static final PropertyDescriptor XSLT_CONTROLLER = new PropertyDescriptor.Builder()
            .name("xslt-controller")
            .displayName("XSLT Lookup")
            .description("Controller lookup used to store XSLT definitions. One of the 'XSLT file name' and "
                    + "'XSLT Lookup' properties must be defined. WARNING: note that the lookup controller service "
                    + "should not be used to store large XSLT files.")
            .required(false)
            .identifiesControllerService(StringLookupService.class)
            .build();

    public static final PropertyDescriptor XSLT_CONTROLLER_KEY = new PropertyDescriptor.Builder()
            .name("xslt-controller-key")
            .displayName("XSLT Lookup key")
            .description("Key used to retrieve the XSLT definition from the XSLT lookup controller. This property must be "
                    + "set when using the XSLT controller property.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    public static final PropertyDescriptor INDENT_OUTPUT = new PropertyDescriptor.Builder()
            .name("indent-output")
            .displayName("Indent")
            .description("Whether or not to indent the output.")
            .required(true)
            .defaultValue("true")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor SECURE_PROCESSING = new PropertyDescriptor.Builder()
            .name("secure-processing")
            .displayName("Secure processing")
            .description("Whether or not to mitigate various XML-related attacks like XXE (XML External Entity) attacks.")
            .required(true)
            .defaultValue("true")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("cache-size")
            .displayName("Cache size")
            .description("Maximum number of stylesheets to cache. Zero disables the cache.")
            .required(true)
            .defaultValue("10")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor CACHE_TTL_AFTER_LAST_ACCESS = new PropertyDescriptor.Builder()
            .name("cache-ttl-after-last-access")
            .displayName("Cache TTL after last access")
            .description("The cache TTL (time-to-live) or how long to keep stylesheets in the cache after last access.")
            .required(true)
            .defaultValue("60 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            XSLT_FILE_NAME,
            XSLT_CONTROLLER,
            XSLT_CONTROLLER_KEY,
            INDENT_OUTPUT,
            SECURE_PROCESSING,
            CACHE_SIZE,
            CACHE_TTL_AFTER_LAST_ACCESS
    );

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("The FlowFile with transformed content will be routed to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a FlowFile fails processing for any reason (for example, the FlowFile is not valid XML), it will be routed to this relationship")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    private LoadingCache<String, Templates> cache;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));

        PropertyValue filename = validationContext.getProperty(XSLT_FILE_NAME);
        PropertyValue controller = validationContext.getProperty(XSLT_CONTROLLER);
        PropertyValue key = validationContext.getProperty(XSLT_CONTROLLER_KEY);

        if ((filename.isSet() && controller.isSet())
                || (!filename.isSet() && !controller.isSet())) {
            results.add(new ValidationResult.Builder()
                    .valid(false)
                    .subject(this.getClass().getSimpleName())
                    .explanation("Exactly one of the \"XSLT file name\" and \"XSLT controller\" properties must be defined.")
                    .build());
        }

        if (controller.isSet() && !key.isSet()) {
            results.add(new ValidationResult.Builder()
                    .valid(false)
                    .subject(XSLT_CONTROLLER_KEY.getDisplayName())
                    .explanation("If using \"XSLT controller\", the XSLT controller key property must be defined.")
                    .build());
        }

        if (controller.isSet()) {
            final LookupService<String> lookupService = validationContext.getProperty(XSLT_CONTROLLER).asControllerService(StringLookupService.class);
            final Set<String> requiredKeys = lookupService.getRequiredKeys();
            if (requiredKeys == null || requiredKeys.size() != 1) {
                results.add(new ValidationResult.Builder()
                        .valid(false)
                        .subject(XSLT_CONTROLLER.getDisplayName())
                        .explanation("This processor requires a key-value lookup service supporting exactly one required key, was: " +
                                (requiredKeys == null ? "null" : String.valueOf(requiredKeys.size())))
                        .build());
            }
        }

        return results;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .required(false)
                .dynamic(true)
                .build();
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final ComponentLog logger = getLogger();
        final Integer cacheSize = context.getProperty(CACHE_SIZE).asInteger();
        final Long cacheTTL = context.getProperty(CACHE_TTL_AFTER_LAST_ACCESS).asTimePeriod(TimeUnit.SECONDS);

        if (cacheSize > 0) {
            final Caffeine<Object, Object> cacheBuilder = Caffeine.newBuilder().maximumSize(cacheSize);
            if (cacheTTL > 0) {
                cacheBuilder.expireAfterAccess(cacheTTL, TimeUnit.SECONDS);
            }

            cache = cacheBuilder.build(path -> newTemplates(context, path));
        } else {
            cache = null;
            logger.info("Stylesheet cache disabled because cache size is set to 0");
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }

        final StopWatch stopWatch = new StopWatch(true);
        final String path = context.getProperty(XSLT_FILE_NAME).isSet()
                ? context.getProperty(XSLT_FILE_NAME).evaluateAttributeExpressions(original).getValue()
                : context.getProperty(XSLT_CONTROLLER_KEY).evaluateAttributeExpressions(original).getValue();

        try {
            final FlowFile transformed = session.write(original, (inputStream, outputStream) -> {
                try (final InputStream bufferedInputStream = new BufferedInputStream(inputStream)) {
                    final Templates templates;
                    if (cache == null) {
                        templates = newTemplates(context, path);
                    } else {
                        templates = cache.get(path);
                    }

                    final Transformer transformer = templates.newTransformer();
                    final String indentProperty = context.getProperty(INDENT_OUTPUT).asBoolean() ? "yes" : "no";
                    transformer.setOutputProperty(OutputKeys.INDENT, indentProperty);
                    transformer.setErrorListener(getErrorListenerLogger());

                    // pass all dynamic properties to the transformer
                    for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
                        if (entry.getKey().isDynamic()) {
                            String value = context.newPropertyValue(entry.getValue()).evaluateAttributeExpressions(original).getValue();
                            transformer.setParameter(entry.getKey().getName(), value);
                        }
                    }

                    final Source source = new StreamSource(bufferedInputStream);
                    final Result result = new StreamResult(outputStream);
                    transformer.transform(source, result);
                } catch (final Exception e) {
                    throw new IOException(String.format("XSLT Source Path [%s] Transform Failed", path), e);
                }
            });
            session.transfer(transformed, REL_SUCCESS);
            session.getProvenanceReporter().modifyContent(transformed, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            getLogger().info("Transformation Completed {}", original);
        } catch (final ProcessException e) {
            getLogger().error("Transformation Failed", original, e);
            session.transfer(original, REL_FAILURE);
        }
    }

    private ErrorListenerLogger getErrorListenerLogger() {
        return new ErrorListenerLogger(getLogger());
    }

    @SuppressWarnings("unchecked")
    private Templates newTemplates(final ProcessContext context, final String path) throws TransformerConfigurationException, LookupFailureException {
        final boolean secureProcessing = context.getProperty(SECURE_PROCESSING).asBoolean();
        final TransformerFactory transformerFactory = getTransformerFactory(secureProcessing);
        final LookupService<String> lookupService = context.getProperty(XSLT_CONTROLLER).asControllerService(LookupService.class);
        final boolean filePath = context.getProperty(XSLT_FILE_NAME).isSet();
        final StreamSource templateSource = getTemplateSource(lookupService, path, filePath);
        final Source configuredTemplateSource = secureProcessing ? getSecureSource(templateSource) : templateSource;
        return transformerFactory.newTemplates(configuredTemplateSource);
    }

    private TransformerFactory getTransformerFactory(final boolean secureProcessing) throws TransformerConfigurationException {
        final TransformerFactory factory = TransformerFactory.newInstance();
        if (secureProcessing) {
            factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            factory.setFeature("http://saxon.sf.net/feature/parserFeature?uri=http://xml.org/sax/features/external-parameter-entities", false);
            factory.setFeature("http://saxon.sf.net/feature/parserFeature?uri=http://xml.org/sax/features/external-general-entities", false);
        }
        factory.setErrorListener(getErrorListenerLogger());

        return factory;
    }

    private StreamSource getTemplateSource(final LookupService<String> lookupService, final String path, final boolean filePath) throws LookupFailureException {
        final StreamSource streamSource;
        if (filePath) {
            streamSource = new StreamSource(path);
        } else {
            final String coordinateKey = lookupService.getRequiredKeys().iterator().next();
            final Map<String, Object> coordinates = Collections.singletonMap(coordinateKey, path);
            final Optional<String> foundSource = lookupService.lookup(coordinates);
            if (foundSource.isPresent() && StringUtils.isNotBlank(foundSource.get())) {
                final String source = foundSource.get();
                final Reader reader = new StringReader(source);
                streamSource = new StreamSource(reader);
            } else {
                throw new LookupFailureException(String.format("XSLT Source Path [%s] not found in Lookup Service", path));
            }
        }
        return streamSource;
    }

    private Source getSecureSource(final StreamSource streamSource) throws TransformerConfigurationException {
        final XMLStreamReaderProvider provider = new StandardXMLStreamReaderProvider();
        try {
            final XMLStreamReader streamReader = provider.getStreamReader(streamSource);
            return new StAXSource(streamReader);
        } catch (final ProcessingException e) {
            throw new TransformerConfigurationException("XSLT Source Stream Reader creation failed", e);
        }
    }

    private static class ErrorListenerLogger implements ErrorListener {
        private final ComponentLog logger;

        ErrorListenerLogger(ComponentLog logger) {
            this.logger = logger;
        }

        @Override
        public void warning(TransformerException exception) {
            logger.warn(exception.getMessageAndLocation(), exception);
        }

        @Override
        public void error(TransformerException exception) {
            logger.error(exception.getMessageAndLocation(), exception);
        }

        @Override
        public void fatalError(TransformerException exception) throws TransformerException {
            logger.log(LogLevel.FATAL, exception.getMessageAndLocation(), exception);
            throw exception;
        }
    }
}