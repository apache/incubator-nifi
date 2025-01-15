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

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperties;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.SupportsSensitiveDynamicProperties;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.AttributeExpression.ResultType;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.util.ArgumentUtils;
import org.apache.nifi.processors.standard.util.SoftLimitBoundedByteArrayOutputStream;
import org.apache.nifi.stream.io.LimitingInputStream;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ProcessBuilder.Redirect;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <p>
 * This processor executes an external command on the contents of a flow file, and creates a new flow file with the results of the command.
 * </p>
 * <p>
 * <strong>Properties:</strong>
 * </p>
 * <ul>
 * <li><strong>Command Path</strong>
 * <ul>
 * <li>Specifies the command to be executed; if just the name of an executable is provided, it must be in the user's environment PATH.</li>
 * <li>Default value: none</li>
 * <li>Supports expression language: true</li>
 * </ul>
 * </li>
 * <li>Arguments Strategy
 * <ul>
 * <li>Selects the strategy to use for arguments to the executable</li>
 * <ul>
 * <li>Command Arguments Property: Use the delimited list of arguments from the Command Arguments Property.  Does not support quotations in parameters.</li>
 * <li>Dynamic Property Arguments: Use Dynamic Properties, with each property a separate argument.  Does support quotes.</li>
 * </ul>
 * </ul>
 * </li>
 * <li>Command Arguments
 * <ul>
 * <li>The arguments to supply to the executable delimited by the ';' character. Each argument may be an Expression Language statement.</li>
 * <li>Default value: none</li>
 * <li>Supports expression language: true</li>
 * </ul>
 * </li>
 * <li>Working Directory
 * <ul>
 * <li>The directory to use as the current working directory when executing the command</li>
 * <li>Default value: none (which means whatever NiFi's current working directory is...probably the root of the NiFi installation directory.)</li>
 * <li>Supports expression language: true</li>
 * </ul>
 * </li>
 * <li>Ignore STDIN
 * <ul>
 * <li>Indicates whether or not the flowfile's contents should be streamed as part of STDIN</li>
 * <li>Default value: false (this means that the contents of a flowfile will be sent as STDIN to your command</li>
 * <li>Supports expression language: false</li>
 * </ul>
 * </li>
 * </ul>
 *
 * <p>
 * <strong>Relationships:</strong>
 * </p>
 * <ul>
 * <li>original
 * <ul>
 * <li>The destination path for the original incoming flow file</li>
 * </ul>
 * </li>
 * <li>output-stream
 * <ul>
 * <li>The destination path for the flow file created from the command's output, if the exit code is zero</li>
 * </ul>
 * </li>
 * <li>nonzero-status
 * <ul>
 * <li>The destination path for the flow file created from the command's output, if the exit code is non-zero</li>
 * </ul>
 * </li>
 * </ul>
 * <p>
 */
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"command execution", "command", "stream", "execute"})
@CapabilityDescription("The ExecuteStreamCommand processor provides a flexible way to integrate external commands and scripts into NiFi data flows."
        + " ExecuteStreamCommand can pass the incoming FlowFile's content to the command that it executes similarly how piping works.")
@SupportsSensitiveDynamicProperties
@DynamicProperties({
        @DynamicProperty(name = "An environment variable name", value = "An environment variable value",
                description = "These environment variables are passed to the process spawned by this Processor"),
        @DynamicProperty(name = "command.argument.<commandIndex>", value = "Argument to be supplied to the command",
                description = "These arguments are supplied to the process spawned by this Processor when using the "
                        + "Command Arguments Strategy : Dynamic Property Arguments. <commandIndex> is a number and it will determine the order.")
})
@WritesAttributes({
        @WritesAttribute(attribute = "execution.command", description = "The name of the command executed"),
        @WritesAttribute(attribute = "execution.command.args", description = "The semi-colon delimited list of arguments. Sensitive properties will be masked"),
        @WritesAttribute(attribute = "execution.status", description = "The exit status code returned from executing the command"),
        @WritesAttribute(attribute = "execution.error", description = "Any error messages returned from executing the command"),
        @WritesAttribute(attribute = "mime.type", description = "Sets the MIME type of the output if the 'Output MIME Type' property is set and 'Output Destination Attribute' is not set")})
@Restricted(
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.EXECUTE_CODE,
                        explanation = "Provides operator the ability to execute arbitrary code assuming all permissions that NiFi has.")
        }
)
public class ExecuteStreamCommand extends AbstractProcessor {

    public static final Relationship ORIGINAL_RELATIONSHIP = new Relationship.Builder()
            .name("original")
            .description("The original FlowFile will be routed. It will have new attributes detailing the result of the script execution.")
            .build();
    public static final Relationship OUTPUT_STREAM_RELATIONSHIP = new Relationship.Builder()
            .name("output stream")
            .description("The destination path for the flow file created from the command's output, if the returned status code is zero.")
            .build();
    public static final Relationship NONZERO_STATUS_RELATIONSHIP = new Relationship.Builder()
            .name("nonzero status")
            .description("The destination path for the flow file created from the command's output, if the returned status code is non-zero. "
                    + "All flow files routed to this relationship will be penalized.")
            .build();
    private final AtomicReference<Set<Relationship>> relationships = new AtomicReference<>();

    private final static Set<Relationship> OUTPUT_STREAM_RELATIONSHIP_SET = Set.of(
            OUTPUT_STREAM_RELATIONSHIP,
            ORIGINAL_RELATIONSHIP,
            NONZERO_STATUS_RELATIONSHIP
    );
    private final static Set<Relationship> ATTRIBUTE_RELATIONSHIP_SET = Set.of(ORIGINAL_RELATIONSHIP);

    private static final Pattern COMMAND_ARGUMENT_PATTERN = Pattern.compile("command\\.argument\\.(?<commandIndex>[0-9]+)$");

    static final AllowableValue COMMAND_ARGUMENTS_PROPERTY_STRATEGY = new AllowableValue("Command Arguments Property", "Command Arguments Property",
            "Arguments to be supplied to the executable are taken from the Command Arguments property");

    static final AllowableValue DYNAMIC_PROPERTY_ARGUMENTS_STRATEGY = new AllowableValue("Dynamic Property Arguments", "Dynamic Property Arguments",
            "Arguments to be supplied to the executable are taken from dynamic properties with pattern of 'command.argument.<commandIndex>'");

   static final PropertyDescriptor WORKING_DIR = new PropertyDescriptor.Builder()
            .name("Working Directory")
            .description("The directory to use as the current working directory when executing the command")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.createDirectoryExistsValidator(true, true))
            .required(false)
            .build();

    private static final Validator ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR = StandardValidators.createAttributeExpressionLanguageValidator(ResultType.STRING, true);
    static final PropertyDescriptor EXECUTION_COMMAND = new PropertyDescriptor.Builder()
            .name("Command Path")
            .description("Specifies the command to be executed; if just the name of an executable is provided, it must be in the user's environment PATH.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor ARGUMENTS_STRATEGY = new PropertyDescriptor.Builder()
            .name("argumentsStrategy")
            .displayName("Command Arguments Strategy")
            .description("Strategy for configuring arguments to be supplied to the command.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(false)
            .allowableValues(COMMAND_ARGUMENTS_PROPERTY_STRATEGY, DYNAMIC_PROPERTY_ARGUMENTS_STRATEGY)
            .defaultValue(COMMAND_ARGUMENTS_PROPERTY_STRATEGY.getValue())
            .build();

    static final PropertyDescriptor EXECUTION_ARGUMENTS = new PropertyDescriptor.Builder()
            .name("Command Arguments")
            .description("The arguments to supply to the executable delimited by the ';' character.")
            .dependsOn(ARGUMENTS_STRATEGY, COMMAND_ARGUMENTS_PROPERTY_STRATEGY)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator((subject, input, context) -> {
                ValidationResult result = new ValidationResult.Builder()
                        .subject(subject).valid(true).input(input).build();
                List<String> args = ArgumentUtils.splitArgs(input, context.getProperty(ExecuteStreamCommand.ARG_DELIMITER).getValue().charAt(0));
                for (String arg : args) {
                    ValidationResult valResult = ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR.validate(subject, arg, context);
                    if (!valResult.isValid()) {
                        result = valResult;
                        break;
                    }
                }
                return result;
            }).build();

   static final PropertyDescriptor ARG_DELIMITER = new PropertyDescriptor.Builder()
            .name("Argument Delimiter")
            .description("Delimiter to use to separate arguments for a command [default: ;]. Must be a single character")
            .dependsOn(ARGUMENTS_STRATEGY, COMMAND_ARGUMENTS_PROPERTY_STRATEGY)
            .addValidator(StandardValidators.SINGLE_CHAR_VALIDATOR)
            .required(true)
            .defaultValue(";")
            .build();

    static final PropertyDescriptor IGNORE_STDIN = new PropertyDescriptor.Builder()
            .name("Ignore STDIN")
            .description("If true, the contents of the incoming flowfile will not be passed to the executing command")
            .addValidator(Validator.VALID)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    static final PropertyDescriptor PUT_OUTPUT_IN_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("Output Destination Attribute")
            .description("If set, the output of the stream command will be put into an attribute of the original FlowFile instead of a separate "
                    + "FlowFile. There will no longer be a relationship for 'output stream' or 'nonzero status'. The value of this property will be the key for the output attribute.")
            .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
            .build();

    static final PropertyDescriptor PUT_ATTRIBUTE_MAX_LENGTH = new PropertyDescriptor.Builder()
            .name("Max Attribute Length")
            .description("If routing the output of the stream command to an attribute, the number of characters put to the attribute value "
                    + "will be at most this amount. This is important because attributes are held in memory and large attributes will quickly "
                    + "cause out of memory issues. If the output goes longer than this value, it will truncated to fit. Consider making this smaller if able.")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("256")
            .build();

    static final PropertyDescriptor MIME_TYPE = new PropertyDescriptor.Builder()
            .name("Output MIME Type")
            .displayName("Output MIME Type")
            .description("Specifies the value to set for the \"mime.type\" attribute. This property is ignored if 'Output Destination Attribute' is set.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            WORKING_DIR,
            EXECUTION_COMMAND,
            ARGUMENTS_STRATEGY,
            EXECUTION_ARGUMENTS,
            ARG_DELIMITER,
            IGNORE_STDIN,
            PUT_OUTPUT_IN_ATTRIBUTE,
            PUT_ATTRIBUTE_MAX_LENGTH,
            MIME_TYPE
    );

    private static final String MASKED_ARGUMENT = "********";

    private ComponentLog logger;

    @Override
    public Set<Relationship> getRelationships() {
        return relationships.get();
    }

    @Override
    protected void init(ProcessorInitializationContext context) {
        logger = getLogger();

        relationships.set(OUTPUT_STREAM_RELATIONSHIP_SET);
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (descriptor.equals(PUT_OUTPUT_IN_ATTRIBUTE)) {
            if (newValue != null) {
                relationships.set(ATTRIBUTE_RELATIONSHIP_SET);
            } else {
                relationships.set(OUTPUT_STREAM_RELATIONSHIP_SET);
            }
        }
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        final Matcher matcher = COMMAND_ARGUMENT_PATTERN.matcher(propertyDescriptorName);
        if (matcher.matches()) {
            return new PropertyDescriptor.Builder()
                    .name(propertyDescriptorName)
                    .displayName(propertyDescriptorName)
                    .description("Argument passed to command")
                    .dynamic(true)
                    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                    .addValidator(ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
                    .build();
        } else {
            return new PropertyDescriptor.Builder()
                    .name(propertyDescriptorName)
                    .description("Sets the environment variable '" + propertyDescriptorName + "' for the process' environment")
                    .dynamic(true)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .build();
        }
    }

    @Override
    public void onTrigger(ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile inputFlowFile = session.get();
        if (null == inputFlowFile) {
            return;
        }

        final ArrayList<String> args = new ArrayList<>();
        final ArrayList<String> argumentAttributeValue = new ArrayList<>();
        final boolean putToAttribute = context.getProperty(PUT_OUTPUT_IN_ATTRIBUTE).isSet();
        final PropertyValue argumentsStrategyPropertyValue = context.getProperty(ARGUMENTS_STRATEGY);
        final boolean useDynamicPropertyArguments = argumentsStrategyPropertyValue.isSet() && argumentsStrategyPropertyValue.getValue().equals(DYNAMIC_PROPERTY_ARGUMENTS_STRATEGY.getValue());
        final Integer attributeSize = context.getProperty(PUT_ATTRIBUTE_MAX_LENGTH).asInteger();
        final String attributeName = context.getProperty(PUT_OUTPUT_IN_ATTRIBUTE).getValue();

        final String executeCommand = context.getProperty(EXECUTION_COMMAND).evaluateAttributeExpressions(inputFlowFile).getValue();
        args.add(executeCommand);
        final boolean ignoreStdin = Boolean.parseBoolean(context.getProperty(IGNORE_STDIN).getValue());
        final String commandArguments;
        if (!useDynamicPropertyArguments) {
            commandArguments = context.getProperty(EXECUTION_ARGUMENTS).evaluateAttributeExpressions(inputFlowFile).getValue();
            if (!StringUtils.isBlank(commandArguments)) {
                args.addAll(ArgumentUtils
                        .splitArgs(commandArguments, context.getProperty(ARG_DELIMITER).getValue().charAt(0)));
            }
        } else {
            List<PropertyDescriptor> propertyDescriptors = new ArrayList<>();
            for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
                Matcher matcher = COMMAND_ARGUMENT_PATTERN.matcher(entry.getKey().getName());
                if (matcher.matches()) {
                    propertyDescriptors.add(entry.getKey());
                }
            }
            propertyDescriptors.sort((p1, p2) -> {
                Matcher matcher = COMMAND_ARGUMENT_PATTERN.matcher(p1.getName());
                String indexString1 = null;
                while (matcher.find()) {
                    indexString1 = matcher.group("commandIndex");
                }
                matcher = COMMAND_ARGUMENT_PATTERN.matcher(p2.getName());
                String indexString2 = null;
                while (matcher.find()) {
                    indexString2 = matcher.group("commandIndex");
                }
                final int index1 = Integer.parseInt(indexString1);
                final int index2 = Integer.parseInt(indexString2);
                if (index1 > index2) {
                    return 1;
                } else if (index1 < index2) {
                    return -1;
                }
                return 0;
            });

            for (final PropertyDescriptor descriptor : propertyDescriptors) {
                String argValue = context.getProperty(descriptor.getName()).evaluateAttributeExpressions(inputFlowFile).getValue();
                if (descriptor.isSensitive()) {
                    argumentAttributeValue.add(MASKED_ARGUMENT);
                } else {
                    argumentAttributeValue.add(argValue);
                }
                args.add(argValue);

            }
            if (argumentAttributeValue.size() > 0) {
                final StringBuilder builder = new StringBuilder();
                for (String s : argumentAttributeValue) {
                    builder.append(s).append("\t");
                }
                commandArguments = builder.toString().trim();
            } else {
                commandArguments = "";
            }
        }
        final String workingDir = context.getProperty(WORKING_DIR).evaluateAttributeExpressions(inputFlowFile).getValue();

        final ProcessBuilder builder = new ProcessBuilder();

        // Avoid logging arguments that could contain sensitive values
        logger.debug("Executing and waiting for command: {}", executeCommand);
        File dir = null;
        if (!StringUtils.isBlank(workingDir)) {
            dir = new File(workingDir);
            if (!dir.exists() && !dir.mkdirs()) {
                logger.warn("Failed to create working directory {}, using current working directory {}", workingDir, System.getProperty("user.dir"));
            }
        }
        final Map<String, String> environment = new HashMap<>();
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (entry.getKey().isDynamic()) {
                environment.put(entry.getKey().getName(), entry.getValue());
            }
        }
        builder.environment().putAll(environment);
        builder.command(args);
        builder.directory(dir);
        builder.redirectInput(Redirect.PIPE);
        builder.redirectOutput(Redirect.PIPE);
        final File errorOut;
        try {
            errorOut = File.createTempFile("out", null);
            builder.redirectError(errorOut);
        } catch (IOException e) {
            logger.error("Could not create temporary file for error logging", e);
            throw new ProcessException(e);
        }

        final Process process;
        try {
            process = builder.start();
        } catch (IOException e) {
            try {
                if (!errorOut.delete()) {
                    logger.warn("Unable to delete file: {}", errorOut.getAbsolutePath());
                }
            } catch (SecurityException se) {
                logger.warn("Unable to delete file: '{}'", errorOut.getAbsolutePath(), se);
            }
            logger.error("Could not create external process to run command", e);
            throw new ProcessException(e);
        }
        try (final OutputStream pos = process.getOutputStream();
             final InputStream pis = process.getInputStream();
             final BufferedInputStream bis = new BufferedInputStream(pis)) {
            final BufferedOutputStream bos = new BufferedOutputStream(pos);
            FlowFile outputFlowFile = putToAttribute ? inputFlowFile : session.create(inputFlowFile);

            ProcessStreamWriterCallback callback = new ProcessStreamWriterCallback(ignoreStdin, bos, bis, logger,
                    attributeName, session, outputFlowFile, process, putToAttribute, attributeSize);
            session.read(inputFlowFile, callback);

            outputFlowFile = callback.outputFlowFile;
            if (putToAttribute) {
                outputFlowFile = session.putAttribute(outputFlowFile, attributeName, new String(callback.outputBuffer, 0, callback.size));
            }

            int exitCode = callback.exitCode;
            logger.debug("Execution complete for command: {}.  Exited with code: {}", executeCommand, exitCode);

            Map<String, String> attributes = new HashMap<>();

            String stdErr = "";
            try (final InputStream in = new BufferedInputStream(new LimitingInputStream(new FileInputStream(errorOut), 4000))) {
                stdErr = IOUtils.toString(in, Charset.defaultCharset());
            } catch (final Exception e) {
                stdErr = "Unknown...could not read Process's Std Error due to " + e.getClass().getName() + ": " + e.getMessage();
            }
            attributes.put("execution.error", stdErr);

            final Relationship outputFlowFileRelationship = putToAttribute ? ORIGINAL_RELATIONSHIP : (exitCode != 0) ? NONZERO_STATUS_RELATIONSHIP : OUTPUT_STREAM_RELATIONSHIP;
            if (exitCode == 0) {
                logger.info("Transferring {} to {}", outputFlowFile, outputFlowFileRelationship.getName());
            } else {
                logger.error("Transferring {} to {}. Executable command {} returned exitCode {} and error message: {}",
                        outputFlowFile, outputFlowFileRelationship.getName(), executeCommand, exitCode, stdErr);
            }

            attributes.put("execution.status", Integer.toString(exitCode));
            attributes.put("execution.command", executeCommand);
            attributes.put("execution.command.args", commandArguments);
            if (context.getProperty(MIME_TYPE).isSet() && !putToAttribute) {
                attributes.put(CoreAttributes.MIME_TYPE.key(), context.getProperty(MIME_TYPE).getValue());
            }
            outputFlowFile = session.putAllAttributes(outputFlowFile, attributes);

            if (NONZERO_STATUS_RELATIONSHIP.equals(outputFlowFileRelationship)) {
                outputFlowFile = session.penalize(outputFlowFile);
            }
            // This will transfer the FlowFile that received the stream output to its destined relationship.
            // In the event the stream is put to an attribute of the original, it will be transferred here.
            session.transfer(outputFlowFile, outputFlowFileRelationship);

            if (!putToAttribute) {
                logger.info("Transferring {} to original", inputFlowFile);
                inputFlowFile = session.putAllAttributes(inputFlowFile, attributes);
                session.transfer(inputFlowFile, ORIGINAL_RELATIONSHIP);
            }

        } catch (final IOException e) {
            // could not close Process related streams
            logger.warn("Problem terminating Process {}", process, e);
        } finally {
            FileUtils.deleteQuietly(errorOut);
            process.destroy(); // last ditch effort to clean up that process.
        }
    }

    static class ProcessStreamWriterCallback implements InputStreamCallback {

        final boolean ignoreStdin;
        final OutputStream stdinWritable;
        final InputStream stdoutReadable;
        final ComponentLog logger;
        final ProcessSession session;
        final Process process;
        FlowFile outputFlowFile;
        int exitCode;
        final boolean putToAttribute;
        final int attributeSize;

        byte[] outputBuffer;
        int size;

        public ProcessStreamWriterCallback(boolean ignoreStdin, OutputStream stdinWritable, InputStream stdoutReadable, ComponentLog logger, String attributeName,
                                           ProcessSession session, FlowFile outputFlowFile, Process process, boolean putToAttribute, int attributeSize) {
            this.ignoreStdin = ignoreStdin;
            this.stdinWritable = stdinWritable;
            this.stdoutReadable = stdoutReadable;
            this.logger = logger;
            this.session = session;
            this.outputFlowFile = outputFlowFile;
            this.process = process;
            this.putToAttribute = putToAttribute;
            this.attributeSize = attributeSize;
        }

        @Override
        public void process(final InputStream incomingFlowFileIS) throws IOException {
            if (putToAttribute) {
                try (SoftLimitBoundedByteArrayOutputStream softLimitBoundedBAOS = new SoftLimitBoundedByteArrayOutputStream(attributeSize)) {
                    readStdoutReadable(ignoreStdin, stdinWritable, logger, incomingFlowFileIS);
                    final long longSize = StreamUtils.copy(stdoutReadable, softLimitBoundedBAOS);

                    // Because the outputStream has a cap that the copy doesn't know about, adjust
                    // the actual size
                    if (longSize > attributeSize) { // Explicit cast for readability
                        size = attributeSize;
                    } else {
                        size = (int) longSize; // Note: safe cast, longSize is limited by attributeSize
                    }

                    outputBuffer = softLimitBoundedBAOS.getBuffer();
                    stdoutReadable.close();

                    try {
                        exitCode = process.waitFor();
                    } catch (InterruptedException e) {
                        logger.warn("Command Execution Process was interrupted", e);
                    }
                }
            } else {
                outputFlowFile = session.write(outputFlowFile, out -> {
                    readStdoutReadable(ignoreStdin, stdinWritable, logger, incomingFlowFileIS);
                    StreamUtils.copy(stdoutReadable, out);
                    try {
                        exitCode = process.waitFor();
                    } catch (InterruptedException e) {
                        logger.warn("Command Execution Process was interrupted", e);
                    }
                });
            }
        }
    }

    private static void readStdoutReadable(final boolean ignoreStdin, final OutputStream stdinWritable,
                                           final ComponentLog logger, final InputStream incomingFlowFileIS) {
        Thread writerThread = new Thread(() -> {
            if (!ignoreStdin) {
                try {
                    StreamUtils.copy(incomingFlowFileIS, stdinWritable);
                } catch (IOException e) {
                    // This is unlikely to occur, and isn't handled at the moment
                    // Bug captured in NIFI-1194
                    logger.error("Failed to write FlowFile to Standard Input Stream", e);
                }
            }
            // MUST close the output stream to the stdin so that whatever is reading knows
            // there is no more data.
            IOUtils.closeQuietly(stdinWritable);
        });
        writerThread.setDaemon(true);
        writerThread.start();
    }
}
