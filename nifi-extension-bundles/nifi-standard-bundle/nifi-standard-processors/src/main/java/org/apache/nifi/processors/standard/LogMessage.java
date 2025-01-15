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

import org.apache.nifi.annotation.behavior.DefaultRunDuration;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.eclipse.jetty.util.StringUtil;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

@SideEffectFree
@SupportsBatching(defaultDuration = DefaultRunDuration.TWENTY_FIVE_MILLIS)
@Tags({"attributes", "logging"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Emits a log message at the specified log level")
public class LogMessage extends AbstractProcessor {

    public static final PropertyDescriptor LOG_LEVEL = new PropertyDescriptor.Builder()
            .name("log-level")
            .displayName("Log Level")
            .required(true)
            .description("The Log Level to use when logging the message: " + Arrays.toString(MessageLogLevel.values()))
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(MessageLogLevel.info.toString())
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor LOG_PREFIX = new PropertyDescriptor.Builder()
            .name("log-prefix")
            .displayName("Log prefix")
            .required(false)
            .description("Log prefix appended to the log lines. " +
                    "It helps to distinguish the output of multiple LogMessage processors.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor LOG_MESSAGE = new PropertyDescriptor.Builder()
            .name("log-message")
            .displayName("Log message")
            .required(false)
            .description("The log message to emit")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        LOG_LEVEL,
        LOG_PREFIX,
        LOG_MESSAGE
    );

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles are routed to this relationship")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
        REL_SUCCESS
    );

    enum MessageLogLevel {
        trace, debug, info, warn, error
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
    public void onTrigger(final ProcessContext context, final ProcessSession session) {

        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String logLevelValue = context.getProperty(LOG_LEVEL).evaluateAttributeExpressions(flowFile).getValue().toLowerCase();

        final MessageLogLevel logLevel;
        try {
            logLevel = MessageLogLevel.valueOf(logLevelValue);
        } catch (Exception e) {
            throw new ProcessException(e);
        }

        final ComponentLog logger = getLogger();
        boolean isLogLevelEnabled = switch (logLevel) {
            case trace -> logger.isTraceEnabled();
            case debug -> logger.isDebugEnabled();
            case info -> logger.isInfoEnabled();
            case warn -> logger.isWarnEnabled();
            case error -> logger.isErrorEnabled();
        };

        if (isLogLevelEnabled) {
            processFlowFile(logger, logLevel, flowFile, context);
        }
        session.transfer(flowFile, REL_SUCCESS);
    }

    private void processFlowFile(
            final ComponentLog logger,
            final MessageLogLevel logLevel,
            final FlowFile flowFile,
            final ProcessContext context) {

        String logPrefix = context.getProperty(LOG_PREFIX).evaluateAttributeExpressions(flowFile).getValue();
        String logMessage = context.getProperty(LOG_MESSAGE).evaluateAttributeExpressions(flowFile).getValue();

        String messageToWrite;
        if (StringUtil.isBlank(logPrefix)) {
            messageToWrite = logMessage;
        } else {
            messageToWrite = String.format("%s%s", logPrefix, logMessage);
        }

        // Uses optional property to specify logging level
        switch (logLevel) {
            case info:
                logger.info(messageToWrite);
                break;
            case debug:
                logger.debug(messageToWrite);
                break;
            case warn:
                logger.warn(messageToWrite);
                break;
            case trace:
                logger.trace(messageToWrite);
                break;
            case error:
                logger.error(messageToWrite);
                break;
            default:
                logger.debug(messageToWrite);
        }
    }
}
