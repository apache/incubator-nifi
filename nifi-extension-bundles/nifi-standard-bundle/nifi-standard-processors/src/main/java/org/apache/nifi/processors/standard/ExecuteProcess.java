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
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.util.ArgumentUtils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"command", "process", "source", "external", "invoke", "script"})
@CapabilityDescription("Runs an operating system command specified by the user and writes the output of that command to a FlowFile. If the command is expected "
        + "to be long-running, the Processor can output the partial data on a specified interval. When this option is used, the output is expected to be in textual "
        + "format, as it typically does not make sense to split binary data on arbitrary time-based intervals.")
@DynamicProperty(name = "An environment variable name", value = "An environment variable value", description = "These environment variables are passed to the process spawned by this Processor")
@Restricted(
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.EXECUTE_CODE,
                        explanation = "Provides operator the ability to execute arbitrary code assuming all permissions that NiFi has.")
        }
)
@WritesAttributes({
    @WritesAttribute(attribute = "command", description = "Executed command"),
    @WritesAttribute(attribute = "command.arguments", description = "Arguments of the command"),
    @WritesAttribute(attribute = "mime.type", description = "Sets the MIME type of the output if the 'Output MIME Type' property is set and 'Batch Duration' is not set")
})
public class ExecuteProcess extends AbstractProcessor {

    final static String ATTRIBUTE_COMMAND = "command";
    final static String ATTRIBUTE_COMMAND_ARGS = "command.arguments";

    public static final PropertyDescriptor COMMAND = new PropertyDescriptor.Builder()
    .name("Command")
    .description("Specifies the command to be executed; if just the name of an executable is provided, it must be in the user's environment PATH.")
    .required(true)
    .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

    public static final PropertyDescriptor COMMAND_ARGUMENTS = new PropertyDescriptor.Builder()
    .name("Command Arguments")
    .description("The arguments to supply to the executable delimited by white space. White space can be escaped by enclosing it in double-quotes.")
    .required(false)
    .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

    public static final PropertyDescriptor WORKING_DIR = new PropertyDescriptor.Builder()
    .name("Working Directory")
    .description("The directory to use as the current working directory when executing the command")
    .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
    .addValidator(StandardValidators.createDirectoryExistsValidator(false, true))
    .required(false)
    .build();

    public static final PropertyDescriptor BATCH_DURATION = new PropertyDescriptor.Builder()
    .name("Batch Duration")
    .description("If the process is expected to be long-running and produce textual output, a batch duration can be specified so "
            + "that the output will be captured for this amount of time and a FlowFile will then be sent out with the results "
            + "and a new FlowFile will be started, rather than waiting for the process to finish before sending out the results")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor REDIRECT_ERROR_STREAM = new PropertyDescriptor.Builder()
    .name("Redirect Error Stream")
    .description("If true will redirect any error stream output of the process to the output stream. "
            + "This is particularly helpful for processes which write extensively to the error stream or for troubleshooting.")
            .required(false)
            .allowableValues("true", "false")
            .defaultValue("false")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    private static final Validator characterValidator = new StandardValidators.StringLengthValidator(1, 1);

    static final PropertyDescriptor ARG_DELIMITER = new PropertyDescriptor.Builder()
      .name("Argument Delimiter")
      .description("Delimiter to use to separate arguments for a command [default: space]. Must be a single character.")
      .addValidator(Validator.VALID)
      .addValidator(characterValidator)
      .required(true)
      .defaultValue(" ")
      .build();

    static final PropertyDescriptor MIME_TYPE = new PropertyDescriptor.Builder()
            .name("Output MIME type")
            .displayName("Output MIME Type")
            .description("Specifies the value to set for the \"mime.type\" attribute. This property is ignored if 'Batch Duration' is set.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            COMMAND,
            COMMAND_ARGUMENTS,
            BATCH_DURATION,
            REDIRECT_ERROR_STREAM,
            WORKING_DIR,
            ARG_DELIMITER,
            MIME_TYPE
    );

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
    .name("success")
    .description("All created FlowFiles are routed to this relationship")
    .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS);

    private volatile Process externalProcess;

    private volatile ExecutorService executor;
    private Future<?> longRunningProcess;
    private AtomicBoolean failure = new AtomicBoolean(false);
    private volatile ProxyOutputStream proxyOut;

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
        .description("Sets the environment variable '" + propertyDescriptorName + "' for the process' environment")
        .dynamic(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();
    }



    @OnScheduled
    public void setupExecutor(final ProcessContext context) {
        executor = Executors.newFixedThreadPool(context.getMaxConcurrentTasks() * 2, new ThreadFactory() {
            private final ThreadFactory defaultFactory = Executors.defaultThreadFactory();

            @Override
            public Thread newThread(final Runnable r) {
                final Thread t = defaultFactory.newThread(r);
                t.setName("ExecuteProcess " + getIdentifier() + " Task");
                return t;
            }
        });
    }

    @OnUnscheduled
    public void shutdownExecutor() {
        try {
            executor.shutdown();
        } finally {
            if ((this.externalProcess != null) && (this.externalProcess.isAlive())) {
                this.getLogger().info("Process hasn't terminated, forcing the interrupt");
                this.externalProcess.destroyForcibly();
            }
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        if (proxyOut == null) {
            proxyOut = new ProxyOutputStream(getLogger());
        }

        final Long batchNanos = context.getProperty(BATCH_DURATION).asTimePeriod(TimeUnit.NANOSECONDS);

        final String command = context.getProperty(COMMAND).evaluateAttributeExpressions().getValue();
        final String arguments = context.getProperty(COMMAND_ARGUMENTS).isSet()
          ? context.getProperty(COMMAND_ARGUMENTS).evaluateAttributeExpressions().getValue()
          : null;

        final List<String> commandStrings = createCommandStrings(context, command, arguments);
        final String commandString = StringUtils.join(commandStrings, " ");

        if (longRunningProcess == null || longRunningProcess.isDone()) {
            try {
                longRunningProcess = launchProcess(context, commandStrings, batchNanos, proxyOut);
            } catch (final IOException ioe) {
                getLogger().error("Failed to create process", ioe);
                context.yield();
                return;
            }
        } else {
            getLogger().info("Read from long running process");
        }

        if (!isScheduled()) {
            getLogger().info("User stopped processor; will terminate process immediately");
            longRunningProcess.cancel(true);
            return;
        }

        // Create a FlowFile that we can write to and set the OutputStream for the FlowFile
        // as the delegate for the ProxyOuptutStream, then wait until the process finishes
        // or until the specified amount of time
        FlowFile flowFile = session.create();
        flowFile = session.write(flowFile, flowFileOut -> {
            try (final OutputStream out = new BufferedOutputStream(flowFileOut)) {
                proxyOut.setDelegate(out);

                if (batchNanos == null) {
                    // we are not creating batches; wait until process terminates.
                    // NB!!! Maybe get(long timeout, TimeUnit unit) should
                    // be used to avoid waiting forever.
                    try {
                        longRunningProcess.get();
                    } catch (final InterruptedException ie) {
                        // Ignore
                    } catch (final ExecutionException ee) {
                        getLogger().error("Process execution failed", ee.getCause());
                    }
                } else {
                    // wait the allotted amount of time.
                    try {
                        TimeUnit.NANOSECONDS.sleep(batchNanos);
                    } catch (final InterruptedException ie) {
                        // Ignore
                    }
                }

                proxyOut.setDelegate(null); // prevent from writing to this
                // stream
            }
        });

        if (flowFile.getSize() == 0L) {
            // If no data was written to the file, remove it
            session.remove(flowFile);
        } else if (failure.get()) {
            // If there was a failure processing the output of the Process, remove the FlowFile
            session.remove(flowFile);
            getLogger().error("Failed to read data from Process, so will not generate FlowFile");
        } else {
            // add command, arguments, and MIME type as attributes
            Map<String, String> attributes = new HashMap<>();
            attributes.put(ATTRIBUTE_COMMAND, command);
            if (arguments != null) {
                attributes.put(ATTRIBUTE_COMMAND_ARGS, arguments);
            }
            if (batchNanos == null && context.getProperty(ExecuteProcess.MIME_TYPE).isSet()) {
                attributes.put(CoreAttributes.MIME_TYPE.key(), context.getProperty(ExecuteProcess.MIME_TYPE).getValue());
            }
            flowFile = session.putAllAttributes(flowFile, attributes);

            // All was good. Generate event and transfer FlowFile.
            session.getProvenanceReporter().create(flowFile, "Created from command: " + commandString);
            getLogger().info("Created {} and routed to success", flowFile);
            session.transfer(flowFile, REL_SUCCESS);
        }
    }

    protected List<String> createCommandStrings(final ProcessContext context, final String command, final String arguments) {
        final List<String> args = ArgumentUtils.splitArgs(arguments, context.getProperty(ARG_DELIMITER).getValue().charAt(0));
        final List<String> commandStrings = new ArrayList<>(args.size() + 1);
        commandStrings.add(command);
        commandStrings.addAll(args);
        return commandStrings;
    }

    protected Future<?> launchProcess(final ProcessContext context, final List<String> commandStrings, final Long batchNanos,
            final ProxyOutputStream proxyOut) throws IOException {

        final Boolean redirectErrorStream = context.getProperty(REDIRECT_ERROR_STREAM).asBoolean();

        final ProcessBuilder builder = new ProcessBuilder(commandStrings);
        final String workingDirName = context.getProperty(WORKING_DIR).evaluateAttributeExpressions().getValue();
        if (workingDirName != null) {
            builder.directory(new File(workingDirName));
        }

        final Map<String, String> environment = new HashMap<>();
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (entry.getKey().isDynamic()) {
                environment.put(entry.getKey().getName(), entry.getValue());
            }
        }

        if (!environment.isEmpty()) {
            builder.environment().putAll(environment);
        }

        getLogger().info("Start creating new Process > {} ", commandStrings);
        this.externalProcess = builder.redirectErrorStream(redirectErrorStream).start();

        // Submit task to read error stream from process
        if (!redirectErrorStream) {
            executor.submit(() -> {
                try (final BufferedReader reader = new BufferedReader(new InputStreamReader(externalProcess.getErrorStream()))) {
                    reader.lines().filter(line -> line != null && !line.isEmpty()).forEach(getLogger()::warn);
                } catch (final IOException ignored) {
                }
            });
        }

        // Submit task to read output of Process and write to FlowFile.
        failure = new AtomicBoolean(false);
        final Future<?> future = executor.submit(() -> {
            try {
                if (batchNanos == null) {
                    // if we aren't batching, just copy the stream from the
                    // process to the flowfile.
                    try (final BufferedInputStream bufferedIn = new BufferedInputStream(externalProcess.getInputStream())) {
                        final byte[] buffer = new byte[4096];
                        int len;
                        while ((len = bufferedIn.read(buffer)) > 0) {

                            // NB!!!! Maybe all data should be read from
                            // input stream in case of !isScheduled() to
                            // avoid subprocess deadlock?
                            // (we just don't write data to proxyOut)
                            // Or because we don't use this subprocess
                            // anymore anyway, we don't care?
                            if (!isScheduled()) {
                                return null;
                            }

                            proxyOut.write(buffer, 0, len);
                        }
                    }
                } else {
                    // we are batching, which means that the output of the
                    // process is text. It doesn't make sense to grab
                    // arbitrary batches of bytes from some process and send
                    // it along as a piece of data, so we assume that
                    // setting a batch during means text.
                    // Also, we don't want that text to get split up in the
                    // middle of a line, so we use BufferedReader
                    // to read lines of text and write them as lines of text.
                    try (final BufferedReader reader = new BufferedReader(new InputStreamReader(externalProcess.getInputStream()))) {
                        String line;

                        while ((line = reader.readLine()) != null) {
                            if (!isScheduled()) {
                                return null;
                            }

                            proxyOut.write((line + "\n").getBytes(StandardCharsets.UTF_8));
                        }
                    }
                }
            } catch (final IOException ioe) {
                failure.set(true);
                throw ioe;
            } finally {
                try {
                    // Since we are going to exit anyway, one sec gives it an extra chance to exit gracefully.
                    // In the future consider exposing it via configuration.
                    boolean terminated = externalProcess.waitFor(1000, TimeUnit.MILLISECONDS);
                    int exitCode = terminated ? externalProcess.exitValue() : -9999;
                    getLogger().info("Process finished with exit code {} ", exitCode);
                } catch (InterruptedException e1) {
                    Thread.currentThread().interrupt();
                }
            }

            return null;
        });

        return future;
    }


    /**
     * Output stream that is used to wrap another output stream in a way that the underlying output stream can be swapped out for a different one when needed
     */
    private static class ProxyOutputStream extends OutputStream {

        private final ComponentLog logger;

        private final Lock lock = new ReentrantLock();
        private OutputStream delegate;

        public ProxyOutputStream(final ComponentLog logger) {
            this.logger = logger;
        }

        public void setDelegate(final OutputStream delegate) {
            lock.lock();
            try {
                logger.trace("Switching delegate from {} to {}", this.delegate, delegate);
                this.delegate = delegate;
            } finally {
                lock.unlock();
            }
        }

        private void sleep(final long millis) {
            try {
                Thread.sleep(millis);
            } catch (final InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void write(final int b) throws IOException {
            lock.lock();
            try {
                while (true) {
                    if (delegate != null) {
                        logger.trace("Writing to {}", delegate);

                        delegate.write(b);
                        return;
                    } else {
                        lock.unlock();
                        sleep(1L);
                        lock.lock();
                    }
                }
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void write(final byte[] b, final int off, final int len) throws IOException {
            lock.lock();
            try {
                while (true) {
                    if (delegate != null) {
                        logger.trace("Writing to {}", delegate);
                        delegate.write(b, off, len);
                        return;
                    } else {
                        lock.unlock();
                        sleep(1L);
                        lock.lock();
                    }
                }
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void write(final byte[] b) throws IOException {
            write(b, 0, b.length);
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public void flush() throws IOException {
            lock.lock();
            try {
                while (true) {
                    if (delegate != null) {
                        delegate.flush();
                        return;
                    } else {
                        lock.unlock();
                        sleep(1L);
                        lock.lock();
                    }
                }
            } finally {
                lock.unlock();
            }
        }
    }
}
