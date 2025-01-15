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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.expression.AttributeValueDecorator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StopWatch;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"Text", "Regular Expression", "Update", "Change", "Replace", "Modify", "Regex", "Mapping"})
@CapabilityDescription("Updates the content of a FlowFile by evaluating a Regular Expression against it and replacing the section of the content that "
        + "matches the Regular Expression with some alternate value provided in a mapping file.")
public class ReplaceTextWithMapping extends AbstractProcessor {

    public static final PropertyDescriptor REGEX = new PropertyDescriptor.Builder()
            .name("Regular Expression")
            .description("The Regular Expression to search for in the FlowFile content")
            .required(true)
            .addValidator(StandardValidators.createRegexValidator(0, Integer.MAX_VALUE, true))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("\\S+")
            .build();
    public static final PropertyDescriptor MATCHING_GROUP_FOR_LOOKUP_KEY = new PropertyDescriptor.Builder()
            .name("Matching Group")
            .description("The number of the matching group of the provided regex to replace with the corresponding value from the mapping file (if it exists).")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .defaultValue("0")
            .build();
    public static final PropertyDescriptor MAPPING_FILE = new PropertyDescriptor.Builder()
            .name("Mapping File")
            .description("The name of the file (including the full path) containing the Mappings.")
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
            .required(true)
            .build();
    public static final PropertyDescriptor MAPPING_FILE_REFRESH_INTERVAL = new PropertyDescriptor.Builder()
            .name("Mapping File Refresh Interval")
            .description("The polling interval to check for updates to the mapping file. The default is 60s.")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .required(true)
            .defaultValue("60s")
            .build();
    public static final PropertyDescriptor CHARACTER_SET = new PropertyDescriptor.Builder()
            .name("Character Set")
            .description("The Character Set in which the file is encoded")
            .required(true)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue("UTF-8")
            .build();
    public static final PropertyDescriptor MAX_BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("Maximum Buffer Size")
            .description("Specifies the maximum amount of data to buffer (per file) in order to apply the regular expressions. If a FlowFile is larger "
                    + "than this value, the FlowFile will be routed to 'failure'")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("1 MB")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            REGEX,
            MATCHING_GROUP_FOR_LOOKUP_KEY,
            MAPPING_FILE,
            MAPPING_FILE_REFRESH_INTERVAL,
            CHARACTER_SET,
            MAX_BUFFER_SIZE
    );

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that have been successfully updated are routed to this relationship, as well as FlowFiles whose content does not match the given Regular Expression")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that could not be updated are routed to this relationship")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    private final Pattern backReferencePattern = Pattern.compile("[^\\\\]\\$(\\d+)");

    private final ReentrantLock processorLock = new ReentrantLock();
    private final AtomicLong lastModified = new AtomicLong(0L);
    final AtomicLong mappingTestTime = new AtomicLong(0);
    private final AtomicReference<ConfigurationState> configurationStateRef = new AtomicReference<>(new ConfigurationState(null));

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> errors = new ArrayList<>(super.customValidate(context));

        final String regexValue = context.getProperty(REGEX).evaluateAttributeExpressions().getValue();
        final int numCapturingGroups = Pattern.compile(regexValue).matcher("").groupCount();
        final int groupToMatch = context.getProperty(MATCHING_GROUP_FOR_LOOKUP_KEY).evaluateAttributeExpressions().asInteger();

        if (groupToMatch > numCapturingGroups) {
            errors.add(
                    new ValidationResult.Builder()
                    .subject("Insufficient Matching Groups")
                    .valid(false)
                    .explanation("The specified matching group does not exist for the regular expression provided")
                    .build());
        }
        return errors;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        updateMapping(context);
        final List<FlowFile> flowFiles = session.get(5);
        if (flowFiles.isEmpty()) {
            return;
        }

        final ComponentLog logger = getLogger();

        final int maxBufferSize = context.getProperty(MAX_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();

        for (FlowFile flowFile : flowFiles) {
            if (flowFile.getSize() > maxBufferSize) {
                session.transfer(flowFile, REL_FAILURE);
                continue;
            }

            final StopWatch stopWatch = new StopWatch(true);

            flowFile = session.write(flowFile, new ReplaceTextCallback(context, flowFile, maxBufferSize));

            logger.info("Transferred {} to 'success'", flowFile);
            session.getProvenanceReporter().modifyContent(flowFile, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(flowFile, REL_SUCCESS);
        }
    }

    protected String fillReplacementValueBackReferences(String rawReplacementValue, int numCapturingGroups) {
        String replacement = rawReplacementValue;
        final Matcher backRefMatcher = backReferencePattern.matcher(replacement);
        int replacementCount = 0;
        while (backRefMatcher.find()) {
            final int backRefIndex = Integer.parseInt(backRefMatcher.group(1));
            if (backRefIndex > numCapturingGroups || backRefIndex < 0) {
                final StringBuilder sb = new StringBuilder(replacement.length() + 1);
                final int groupStart = backRefMatcher.start(1) + replacementCount++;

                sb.append(replacement.substring(0, groupStart - 1));
                sb.append("\\");
                sb.append(replacement.substring(groupStart - 1));
                replacement = sb.toString();
            }
        }

        replacement = replacement.replaceAll("(\\$\\D)", "\\\\$1");

        return replacement;
    }

    private void updateMapping(final ProcessContext context) {
        if (processorLock.tryLock()) {
            final ComponentLog logger = getLogger();
            try {
                // if not queried mapping file lastUpdate time in
                // mapppingRefreshPeriodSecs, do so.
                long currentTimeSecs = System.currentTimeMillis() / 1000;
                long mappingRefreshPeriodSecs = context.getProperty(MAPPING_FILE_REFRESH_INTERVAL).asTimePeriod(TimeUnit.SECONDS);

                boolean retry = (currentTimeSecs > (mappingTestTime.get() + mappingRefreshPeriodSecs));
                if (retry) {
                    mappingTestTime.set(System.currentTimeMillis() / 1000);
                    // see if the mapping file needs to be reloaded
                    final String fileName = context.getProperty(MAPPING_FILE).getValue();
                    final File file = new File(fileName);
                    if (file.exists() && file.isFile() && file.canRead()) {
                        if (file.lastModified() > lastModified.get()) {
                            lastModified.getAndSet(file.lastModified());
                            try (FileInputStream is = new FileInputStream(file)) {
                                logger.info("Reloading mapping file: {}", fileName);

                                final Map<String, String> mapping = loadMappingFile(is);
                                final ConfigurationState newState = new ConfigurationState(mapping);
                                configurationStateRef.set(newState);
                            } catch (IOException e) {
                                logger.error("Error reading mapping file", e);
                            }
                        }
                    } else {
                        logger.error("Mapping file does not exist or is not readable: {}", fileName);
                    }
                }
            } catch (Exception e) {
                logger.error("Error loading mapping file", e);
            } finally {
                processorLock.unlock();
            }
        }
    }

    protected Map<String, String> loadMappingFile(InputStream is) throws IOException {
        Map<String, String> mapping = new HashMap<>();
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        String line = null;
        while ((line = reader.readLine()) != null) {
            final String[] splits = StringUtils.split(line, "\t ", 2);
            if (splits.length == 1) {
                mapping.put(splits[0].trim(), ""); // support key with empty value
            } else if (splits.length == 2) {
                final String key = splits[0].trim();
                final String value = splits[1].trim();
                mapping.put(key, value);
            }
        }
        return mapping;
    }

    public static class ConfigurationState {

        final Map<String, String> mapping = new HashMap<>();

        public ConfigurationState(final Map<String, String> mapping) {
            if (mapping != null) {
                this.mapping.putAll(mapping);
            }
        }

        public Map<String, String> getMapping() {
            return Map.copyOf(mapping);
        }

        public boolean isConfigured() {
            return !mapping.isEmpty();
        }
    }

    private final class ReplaceTextCallback implements StreamCallback {

        private final Charset charset;
        private final byte[] buffer;
        private final String regex;
        private final FlowFile flowFile;
        private final int numCapturingGroups;
        private final int groupToMatch;

        private final AttributeValueDecorator quotedAttributeDecorator = new AttributeValueDecorator() {
            @Override
            public String decorate(final String attributeValue) {
                return Pattern.quote(attributeValue);
            }
        };

        private ReplaceTextCallback(ProcessContext context, FlowFile flowFile, int maxBufferSize) {
            this.regex = context.getProperty(REGEX).evaluateAttributeExpressions(flowFile, quotedAttributeDecorator).getValue();
            this.flowFile = flowFile;

            this.charset = Charset.forName(context.getProperty(CHARACTER_SET).getValue());

            final String regexValue = context.getProperty(REGEX).evaluateAttributeExpressions().getValue();
            this.numCapturingGroups = Pattern.compile(regexValue).matcher("").groupCount();

            this.buffer = new byte[maxBufferSize];

            this.groupToMatch = context.getProperty(MATCHING_GROUP_FOR_LOOKUP_KEY).evaluateAttributeExpressions().asInteger();
        }

        @Override
        public void process(final InputStream in, final OutputStream out) throws IOException {

            final Map<String, String> mapping = configurationStateRef.get().getMapping();

            StreamUtils.fillBuffer(in, buffer, false);

            final int flowFileSize = (int) flowFile.getSize();

            final String contentString = new String(buffer, 0, flowFileSize, charset);

            final Matcher matcher = Pattern.compile(regex).matcher(contentString);

            matcher.reset();
            boolean result = matcher.find();
            if (result) {
                StringBuffer sb = new StringBuffer();
                do {
                    String matched = matcher.group(groupToMatch);
                    String rv = mapping.get(matched);

                    if (rv == null) {
                        String replacement = matcher.group().replace("$", "\\$");
                        matcher.appendReplacement(sb, replacement);
                    } else {
                        String allRegexMatched = matcher.group(); //this is everything that matched the regex

                        int scaledStart = matcher.start(groupToMatch) - matcher.start();
                        int scaledEnd = scaledStart + matcher.group(groupToMatch).length();

                        StringBuilder replacementBuilder = new StringBuilder();

                        replacementBuilder.append(allRegexMatched.substring(0, scaledStart).replace("$", "\\$"));
                        replacementBuilder.append(fillReplacementValueBackReferences(rv, numCapturingGroups));
                        replacementBuilder.append(allRegexMatched.substring(scaledEnd).replace("$", "\\$"));

                        matcher.appendReplacement(sb, replacementBuilder.toString());
                    }
                    result = matcher.find();
                } while (result);
                matcher.appendTail(sb);
                out.write(sb.toString().getBytes(charset));
                return;
            }
            out.write(contentString.getBytes(charset));
        }
    }
}
