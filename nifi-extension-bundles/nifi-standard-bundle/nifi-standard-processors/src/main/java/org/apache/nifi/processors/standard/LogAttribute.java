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

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DefaultRunDuration;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.eclipse.jetty.util.StringUtil;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@SideEffectFree
@SupportsBatching(defaultDuration = DefaultRunDuration.TWENTY_FIVE_MILLIS)
@Tags({"attributes", "logging"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Emits attributes of the FlowFile at the specified log level")
public class LogAttribute extends AbstractProcessor {
    private static final AllowableValue OUTPUT_FORMAT_LINE_PER_ATTRIBUTE = new AllowableValue("Line per Attribute", "Line per Attribute", "Each FlowFile attribute will be logged using a single line" +
        " for the attribute name and another line for the attribute value. This format is often most advantageous when looking at the attributes of a single FlowFile.");
    private static final AllowableValue OUTPUT_FORMAT_SINGLE_LINE = new AllowableValue("Single Line", "Single Line", "All FlowFile attribute names and values will be logged on a single line. This " +
        "format is often most advantageous when comparing logs from multiple FlowFiles.");

    public static final PropertyDescriptor LOG_LEVEL = new PropertyDescriptor.Builder()
            .name("Log Level")
            .required(true)
            .description("The Log Level to use when logging the Attributes")
            .allowableValues(DebugLevels.values())
            .defaultValue("info")
            .build();
    public static final PropertyDescriptor ATTRIBUTES_TO_LOG_CSV = new PropertyDescriptor.Builder()
            .name("Attributes to Log")
            .required(false)
            .description("A comma-separated list of Attributes to Log. If not specified, all attributes will be logged unless `Attributes to Log by Regular Expression` is modified." +
                    " There's an AND relationship between the two properties.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor ATTRIBUTES_TO_LOG_REGEX = new PropertyDescriptor.Builder()
            .name("attributes-to-log-regex")
            .displayName("Attributes to Log by Regular Expression")
            .required(false)
            .defaultValue(".*")
            .description("A regular expression indicating the Attributes to Log. If not specified, all attributes will be logged unless `Attributes to Log` is modified." +
                    " There's an AND relationship between the two properties.")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();
    public static final PropertyDescriptor ATTRIBUTES_TO_IGNORE_CSV = new PropertyDescriptor.Builder()
            .name("Attributes to Ignore")
            .description("A comma-separated list of Attributes to ignore. If not specified, no attributes will be ignored unless `Attributes to Ignore by Regular Expression` is modified." +
                    " There's an OR relationship between the two properties.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor ATTRIBUTES_TO_IGNORE_REGEX = new PropertyDescriptor.Builder()
            .name("attributes-to-ignore-regex")
            .displayName("Attributes to Ignore by Regular Expression")
            .required(false)
            .description("A regular expression indicating the Attributes to Ignore. If not specified, no attributes will be ignored unless `Attributes to Ignore` is modified." +
                    " There's an OR relationship between the two properties.")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();
    public static final PropertyDescriptor OUTPUT_FORMAT = new PropertyDescriptor.Builder()
            .name("Output Format")
            .displayName("Output Format")
            .description("Specifies the format to use for logging FlowFile attributes")
            .required(true)
            .allowableValues(OUTPUT_FORMAT_LINE_PER_ATTRIBUTE, OUTPUT_FORMAT_SINGLE_LINE)
            .defaultValue(OUTPUT_FORMAT_LINE_PER_ATTRIBUTE.getValue())
            .build();
    public static final PropertyDescriptor LOG_PAYLOAD = new PropertyDescriptor.Builder()
            .name("Log Payload")
            .required(true)
            .description("If true, the FlowFile's payload will be logged, in addition to its attributes; otherwise, just the Attributes will be logged.")
            .defaultValue("false")
            .allowableValues("true", "false")
            .build();
    static final PropertyDescriptor LOG_FLOWFILE_PROPERTIES = new PropertyDescriptor.Builder()
            .name("Log FlowFile Properties")
            .displayName("Log FlowFile Properties")
            .description("Specifies whether or not to log FlowFile \"properties\", such as Entry Date, Lineage Start Date, and content size")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();
    public static final PropertyDescriptor LOG_PREFIX = new PropertyDescriptor.Builder()
            .name("Log prefix")
            .required(false)
            .description("Log prefix appended to the log lines. It helps to distinguish the output of multiple LogAttribute processors.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("character-set")
            .displayName("Character Set")
            .description("The name of the CharacterSet to use")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue(Charset.defaultCharset().name())
            .required(true)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            LOG_LEVEL,
            LOG_PAYLOAD,
            ATTRIBUTES_TO_LOG_CSV,
            ATTRIBUTES_TO_LOG_REGEX,
            ATTRIBUTES_TO_IGNORE_CSV,
            ATTRIBUTES_TO_IGNORE_REGEX,
            LOG_FLOWFILE_PROPERTIES,
            OUTPUT_FORMAT,
            LOG_PREFIX,
            CHARSET
    );

    public static final String FIFTY_DASHES = "--------------------------------------------------";

    public static enum DebugLevels {
        trace, debug, info, warn, error
    }

    public static final long ONE_MB = 1024 * 1024;

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles are routed to this relationship")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS);

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    protected String processFlowFile(final ComponentLog logger, final DebugLevels logLevel, final FlowFile flowFile, final ProcessSession session, final ProcessContext context) {
        final Set<String> attributeKeys = getAttributesToLog(flowFile.getAttributes().keySet(), context);
        final String dashedLine;

        String logPrefix = context.getProperty(LOG_PREFIX).evaluateAttributeExpressions(flowFile).getValue();
        Charset charset = Charset.forName(context.getProperty(CHARSET).evaluateAttributeExpressions(flowFile).getValue());

        if (StringUtil.isBlank(logPrefix)) {
            dashedLine = StringUtils.repeat('-', 50);
        } else {
            // abbreviate long lines
            logPrefix = StringUtils.abbreviate(logPrefix, 40);
            // center the logPrefix and pad with dashes
            logPrefix = StringUtils.center(logPrefix, 40, '-');
            // five dashes on the left and right side, plus the dashed logPrefix
            dashedLine = StringUtils.repeat('-', 5) + logPrefix + StringUtils.repeat('-', 5);
        }

        final String outputFormat = context.getProperty(OUTPUT_FORMAT).getValue();
        final boolean logProperties = context.getProperty(LOG_FLOWFILE_PROPERTIES).asBoolean();

        // Pretty print metadata
        final StringBuilder message = new StringBuilder();
        message.append("logging for flow file ").append(flowFile);
        message.append("\n");

        if (OUTPUT_FORMAT_LINE_PER_ATTRIBUTE.getValue().equalsIgnoreCase(outputFormat)) {
            message.append(dashedLine);

            if (logProperties) {
                message.append("\nFlowFile Properties");
                message.append(String.format("\nKey: '%1$s'\n\tValue: '%2$s'", "entryDate", new Date(flowFile.getEntryDate())));
                message.append(String.format("\nKey: '%1$s'\n\tValue: '%2$s'", "lineageStartDate", new Date(flowFile.getLineageStartDate())));
                message.append(String.format("\nKey: '%1$s'\n\tValue: '%2$s'", "fileSize", flowFile.getSize()));
            }

            message.append("\nFlowFile Attribute Map Content");
            for (final String key : attributeKeys) {
                message.append(String.format("\nKey: '%1$s'\n\tValue: '%2$s'", key, flowFile.getAttribute(key)));
            }
            message.append("\n");
            message.append(dashedLine);
        } else {
            final Map<String, String> attributes = new TreeMap<>();
            for (final String key : attributeKeys) {
                attributes.put(key, flowFile.getAttribute(key));
            }

            if (logProperties) {
                attributes.put("entryDate", new Date(flowFile.getEntryDate()).toString());
                attributes.put("lineageStartDate", new Date(flowFile.getLineageStartDate()).toString());
                attributes.put("fileSize", String.valueOf(flowFile.getSize()));
            }

            message.append("FlowFile Properties: ").append(attributes);
        }

        // The user can request to log the payload
        final boolean logPayload = context.getProperty(LOG_PAYLOAD).asBoolean();
        if (logPayload) {
            message.append("\n");
            if (flowFile.getSize() < ONE_MB) {
                final FlowFilePayloadCallback callback = new FlowFilePayloadCallback(charset);
                session.read(flowFile, callback);
                message.append(callback.getContents());
            } else {
                message.append("\n Not including payload since it is larger than one mb.");
            }
        }
        final String outputMessage = message.toString().trim();
        // Uses optional property to specify logging level
        switch (logLevel) {
            case info:
                logger.info(outputMessage);
                break;
            case debug:
                logger.debug(outputMessage);
                break;
            case warn:
                logger.warn(outputMessage);
                break;
            case trace:
                logger.trace(outputMessage);
                break;
            case error:
                logger.error(outputMessage);
                break;
            default:
                logger.debug(outputMessage);
                break;
        }

        return outputMessage;

    }

    private Set<String> getAttributesToLog(final Set<String> flowFileAttrKeys, final ProcessContext context) {

        // collect properties
        final String attrsToLogValue = context.getProperty(ATTRIBUTES_TO_LOG_CSV).getValue();
        final String attrsToRemoveValue = context.getProperty(ATTRIBUTES_TO_IGNORE_CSV).getValue();
        final Set<String> attrsToLog = StringUtils.isBlank(attrsToLogValue) ? new HashSet<>(flowFileAttrKeys) : new HashSet<>(Arrays.asList(attrsToLogValue.split("\\s*,\\s*")));
        final Set<String> attrsToRemove = StringUtils.isBlank(attrsToRemoveValue) ? new HashSet<>() : new HashSet<>(Arrays.asList(attrsToRemoveValue.split("\\s*,\\s*")));
        final Pattern attrsToLogRegex = Pattern.compile(context.getProperty(ATTRIBUTES_TO_LOG_REGEX).getValue());
        final String attrsToRemoveRegexValue = context.getProperty(ATTRIBUTES_TO_IGNORE_REGEX).getValue();
        final Pattern attrsToRemoveRegex = attrsToRemoveRegexValue == null ? null : Pattern.compile(context.getProperty(ATTRIBUTES_TO_IGNORE_REGEX).getValue());
        return flowFileAttrKeys.stream()
                .filter(candidate -> {
                    // we'll consider logging an attribute if either no explicit attributes to log were configured,
                    // if this property was configured to be logged, or if the regular expression of properties to log matches
                    if ((attrsToLog.isEmpty() || attrsToLog.contains(candidate)) && attrsToLogRegex.matcher(candidate).matches()) {
                        // log properties we've _not_ configured either explicitly or by regular expression to be ignored.
                        if ((attrsToRemove.isEmpty() || !attrsToRemove.contains(candidate)) && (attrsToRemoveRegex == null || !attrsToRemoveRegex.matcher(candidate).matches())) {
                            return true;
                        }
                    }
                    return false;
                }).collect(Collectors.toCollection(TreeSet::new));
    }

    private void transferChunk(final ProcessSession session) {
        final List<FlowFile> flowFiles = session.get(50);
        if (!flowFiles.isEmpty()) {
            session.transfer(flowFiles, REL_SUCCESS);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final String logLevelValue = context.getProperty(LOG_LEVEL).getValue().toLowerCase();

        final DebugLevels logLevel;
        try {
            logLevel = DebugLevels.valueOf(logLevelValue);
        } catch (Exception e) {
            throw new ProcessException(e);
        }

        final ComponentLog LOG = getLogger();
        boolean isLogLevelEnabled = switch (logLevel) {
            case trace -> LOG.isTraceEnabled();
            case debug -> LOG.isDebugEnabled();
            case info -> LOG.isInfoEnabled();
            case warn -> LOG.isWarnEnabled();
            case error -> LOG.isErrorEnabled();
        };

        if (!isLogLevelEnabled) {
            transferChunk(session);
            return;
        }

        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        processFlowFile(LOG, logLevel, flowFile, session, context);
        session.transfer(flowFile, REL_SUCCESS);
    }

    protected static class FlowFilePayloadCallback implements InputStreamCallback {

        private String contents = "";
        private final Charset charset;

        public FlowFilePayloadCallback(Charset charset) {
            this.charset = charset;
        }

        @Override
        public void process(final InputStream in) throws IOException {
            contents = IOUtils.toString(in, charset);
        }

        public String getContents() {
            return contents;
        }
    }
}
