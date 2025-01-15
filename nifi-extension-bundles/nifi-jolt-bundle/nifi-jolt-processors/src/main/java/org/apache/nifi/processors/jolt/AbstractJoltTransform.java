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
package org.apache.nifi.processors.jolt;

import com.bazaarvoice.jolt.JoltTransform;
import com.bazaarvoice.jolt.JsonUtils;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceReference;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.jolt.util.JoltTransformStrategy;
import org.apache.nifi.jolt.util.TransformFactory;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.util.file.classloader.ClassLoaderUtils;

import java.io.BufferedReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public abstract class AbstractJoltTransform extends AbstractProcessor {
    public static final PropertyDescriptor JOLT_TRANSFORM = new PropertyDescriptor.Builder()
            .name("Jolt Transform")
            .description("Specifies the Jolt Transformation that should be used with the provided specification.")
            .required(true)
            .allowableValues(JoltTransformStrategy.class)
            .defaultValue(JoltTransformStrategy.CHAINR.getValue())
            .build();

    public static final PropertyDescriptor JOLT_SPEC = new PropertyDescriptor.Builder()
            .name("Jolt Specification")
            .description("Jolt Specification for transformation of JSON data. The value for this property may be the text of a Jolt specification "
                    + "or the path to a file containing a Jolt specification. 'Jolt Specification' must be set, or "
                    + "the value is ignored if the Jolt Sort Transformation is selected.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE, ResourceType.TEXT)
            .required(false)
            .build();

    public static final PropertyDescriptor CUSTOM_CLASS = new PropertyDescriptor.Builder()
            .name("Custom Transformation Class Name")
            .description("Fully Qualified Class Name for Custom Transformation")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dependsOn(JOLT_TRANSFORM, JoltTransformStrategy.CUSTOMR)
            .build();

    public static final PropertyDescriptor MODULES = new PropertyDescriptor.Builder()
            .name("Custom Module Directory")
            .description("Comma-separated list of paths to files and/or directories which contain modules containing custom transformations (that are not included on NiFi's classpath).")
            .required(false)
            .identifiesExternalResource(ResourceCardinality.MULTIPLE, ResourceType.FILE, ResourceType.DIRECTORY)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .dynamicallyModifiesClasspath(true)
            .dependsOn(JOLT_TRANSFORM, JoltTransformStrategy.CUSTOMR)
            .build();

    static final PropertyDescriptor TRANSFORM_CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("Transform Cache Size")
            .description("Compiling a Jolt Transform can be fairly expensive. Ideally, this will be done only once. However, if the Expression Language is used in the transform, we may need "
                    + "a new Transform for each FlowFile. This value controls how many of those Transforms we cache in memory in order to avoid having to compile the Transform each time.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1")
            .required(true)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            JOLT_TRANSFORM,
            JOLT_SPEC,
            CUSTOM_CLASS,
            MODULES,
            TRANSFORM_CACHE_SIZE
    );

    /**
     * It is a cache for transform objects. It keeps values indexed by jolt specification string.
     * For some cases the key could be empty. It means that it represents default transform (e.g. for custom transform
     * when there is no jolt-record-spec specified).
     */
    private Cache<Optional<String>, JoltTransform> transformCache;

    protected static List<PropertyDescriptor> getCommonPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnScheduled
    public void setup(final ProcessContext context) {
        int maxTransformsToCache = context.getProperty(TRANSFORM_CACHE_SIZE).asInteger();
        transformCache = Caffeine.newBuilder()
                .maximumSize(maxTransformsToCache)
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));
        final String transform = validationContext.getProperty(JOLT_TRANSFORM).getValue();
        final String customTransform = validationContext.getProperty(CUSTOM_CLASS).getValue();
        final String modulePath = validationContext.getProperty(MODULES).isSet() ? validationContext.getProperty(MODULES).getValue() : null;
        final String joltSpecValue = validationContext.getProperty(JOLT_SPEC).getValue();

        if (StringUtils.isEmpty(joltSpecValue) && !JoltTransformStrategy.SORTR.getValue().equals(transform)) {
            results.add(new ValidationResult.Builder().subject(JOLT_SPEC.getDisplayName()).valid(false).explanation(
                    "'Jolt Specification' must be set, or the Transformation must be 'Sort'").build());
        } else {
            final ClassLoader customClassLoader;

            try {
                if (modulePath != null && !validationContext.isExpressionLanguagePresent(modulePath)) {
                    customClassLoader = ClassLoaderUtils.getCustomClassLoader(modulePath, this.getClass().getClassLoader(), getJarFilenameFilter());
                } else {
                    customClassLoader = this.getClass().getClassLoader();
                }

                final boolean elPresent = validationContext.isExpressionLanguagePresent(joltSpecValue);

                if (elPresent) {
                    final String invalidExpressionMsg = validationContext.newExpressionLanguageCompiler().validateExpression(joltSpecValue, true);
                    if (!StringUtils.isEmpty(invalidExpressionMsg)) {
                        results.add(new ValidationResult.Builder().valid(false)
                                .subject(JOLT_SPEC.getDisplayName())
                                .explanation("Invalid Expression Language: " + invalidExpressionMsg)
                                .build());
                    }
                } else if (validationContext.isExpressionLanguagePresent(customTransform)) {
                    final String invalidExpressionMsg = validationContext.newExpressionLanguageCompiler().validateExpression(customTransform, true);
                    if (!StringUtils.isEmpty(invalidExpressionMsg)) {
                        results.add(new ValidationResult.Builder().valid(false)
                                .subject(CUSTOM_CLASS.getDisplayName())
                                .explanation("Invalid Expression Language: " + invalidExpressionMsg)
                                .build());
                    }
                } else {
                    if (!JoltTransformStrategy.SORTR.getValue().equals(transform)) {

                        //for validation we want to be able to ensure the spec is syntactically correct and not try to resolve variables since they may not exist yet
                        final String content = readTransform(validationContext.getProperty(JOLT_SPEC));
                        final Object specJson = JsonUtils.jsonToObject(content.replaceAll("\\$\\{", "\\\\\\\\\\$\\{"), StandardCharsets.UTF_8.toString());

                        if (JoltTransformStrategy.CUSTOMR.getValue().equals(transform)) {
                            if (StringUtils.isEmpty(customTransform)) {
                                final String customMessage = "A custom transformation class should be provided. ";
                                results.add(new ValidationResult.Builder().valid(false)
                                        .explanation(customMessage)
                                        .build());
                            } else {
                                TransformFactory.getCustomTransform(customClassLoader, customTransform, specJson);
                            }
                        } else {
                            TransformFactory.getTransform(customClassLoader, transform, specJson);
                        }
                    }
                }
            } catch (final Exception e) {
                String message = String.format("Specification not valid for the selected transformation: %s", e.getMessage());
                results.add(new ValidationResult.Builder()
                        .valid(false)
                        .subject(JOLT_SPEC.getDisplayName())
                        .explanation(message)
                        .build());
            }
        }

        return results;
    }


    JoltTransform getTransform(final ProcessContext context, final FlowFile flowFile) {
        final Optional<String> specString;
        if (context.getProperty(JOLT_SPEC).isSet()) {
            specString = Optional.of(context.getProperty(JOLT_SPEC).evaluateAttributeExpressions(flowFile).getValue());
        } else if (JoltTransformStrategy.SORTR.getValue().equals(context.getProperty(JOLT_TRANSFORM).getValue())) {
            specString = Optional.empty();
        } else {
            throw new IllegalArgumentException("'Jolt Specification' must be set, or the Transformation must be Sort.");
        }

        return transformCache.get(specString, currString -> {
            try {
                return createTransform(context, flowFile);
            } catch (Exception e) {
                getLogger().error("Transform creation failed", e);
            }
            return null;
        });
    }

    JoltTransform createTransform(final ProcessContext context, final FlowFile flowFile) throws Exception {
        final Object specJson;
        if ((context.getProperty(JOLT_SPEC).isSet() && !JoltTransformStrategy.SORTR.getValue().equals(context.getProperty(JOLT_TRANSFORM).getValue()))) {
            final String resolvedSpec = readTransform(context.getProperty(JOLT_SPEC), flowFile);
            specJson = JsonUtils.jsonToObject(resolvedSpec, StandardCharsets.UTF_8.toString());
        } else {
            specJson = null;
        }

        if (JoltTransformStrategy.CUSTOMR.getValue().equals(context.getProperty(JOLT_TRANSFORM).getValue())) {
            return TransformFactory.getCustomTransform(Thread.currentThread().getContextClassLoader(), context.getProperty(CUSTOM_CLASS).evaluateAttributeExpressions(flowFile).getValue(), specJson);
        } else {
            return TransformFactory.getTransform(Thread.currentThread().getContextClassLoader(), context.getProperty(JOLT_TRANSFORM).getValue(), specJson);
        }
    }

    private String readTransform(final PropertyValue propertyValue, final FlowFile flowFile) {
        final String transform;

        if (propertyValue.isExpressionLanguagePresent()) {
            transform = propertyValue.evaluateAttributeExpressions(flowFile).getValue();
        } else {
            transform = readTransform(propertyValue);
        }

        return transform;
    }

    String readTransform(final PropertyValue propertyValue) {
        final ResourceReference resourceReference = propertyValue.asResource();
        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(resourceReference.read(), StandardCharsets.UTF_8))) {
            return reader.lines().collect(Collectors.joining(System.lineSeparator()));
        } catch (final IOException e) {
            throw new UncheckedIOException("Read JOLT Transform failed", e);
        }
    }

    protected FilenameFilter getJarFilenameFilter() {
        return (dir, name) -> (name != null && name.endsWith(".jar"));
    }
}
