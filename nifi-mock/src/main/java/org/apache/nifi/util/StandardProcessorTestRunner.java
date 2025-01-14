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
package org.apache.nifi.util;

import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.lifecycle.OnAdded;
import org.apache.nifi.annotation.lifecycle.OnConfigurationRestored;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.annotation.lifecycle.OnRemoved;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.annotation.notification.OnPrimaryNodeStateChange;
import org.apache.nifi.annotation.notification.PrimaryNodeState;
import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.kerberos.KerberosContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.registry.EnvironmentVariables;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.state.MockStateManager;
import org.junit.jupiter.api.Assertions;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StandardProcessorTestRunner implements TestRunner {

    private final Processor processor;
    private final MockProcessContext context;
    private final KerberosContext kerberosContext;
    private final MockFlowFileQueue flowFileQueue;
    private final SharedSessionState sharedState;
    private final AtomicLong idGenerator;
    private final boolean triggerSerially;
    private final MockStateManager processorStateManager;
    private final Map<String, MockStateManager> controllerServiceStateManagers = new HashMap<>();

    private int numThreads = 1;
    private MockSessionFactory sessionFactory;
    private boolean allowSynchronousSessionCommits = false;
    private long runSchedule = 0;
    private final AtomicInteger invocations = new AtomicInteger(0);

    private final Map<String, MockComponentLog> controllerServiceLoggers = new HashMap<>();
    private final MockComponentLog logger;
    private boolean enforceReadStreamsClosed = true;
    private boolean validateExpressionUsage = true;

    // This only for testing purposes as we don't want to set env/sys variables in the tests
    private final Map<String, String> environmentVariables = new HashMap<>();

    StandardProcessorTestRunner(final Processor processor) {
        this(processor, null);
    }

    StandardProcessorTestRunner(final Processor processor, String processorName) {
        this(processor, processorName, null, null);
    }

    StandardProcessorTestRunner(final Processor processor, String processorName, KerberosContext kerberosContext) {
        this(processor, processorName, null, kerberosContext);
    }

    StandardProcessorTestRunner(final Processor processor, String processorName, MockComponentLog logger) {
        this(processor, processorName, logger, null);
    }

    StandardProcessorTestRunner(final Processor processor, String processorName, MockComponentLog logger, KerberosContext kerberosContext) {
        this.processor = processor;
        this.idGenerator = new AtomicLong(0L);
        this.sharedState = new SharedSessionState(processor, idGenerator);
        this.flowFileQueue = sharedState.getFlowFileQueue();
        this.processorStateManager = new MockStateManager(processor);
        this.sessionFactory = new MockSessionFactory(sharedState, processor, enforceReadStreamsClosed, processorStateManager, allowSynchronousSessionCommits);

        this.context = new MockProcessContext(processor, processorName, processorStateManager, environmentVariables);
        this.kerberosContext = kerberosContext;

        final MockProcessorInitializationContext mockInitContext = new MockProcessorInitializationContext(processor, context, logger, kerberosContext);
        processor.initialize(mockInitContext);
        this.logger =  mockInitContext.getLogger();

        try {
            ReflectionUtils.invokeMethodsWithAnnotation(OnAdded.class, processor);
        } catch (final Exception e) {
            Assertions.fail("Could not invoke methods annotated with @OnAdded annotation due to: " + e);
        }

        triggerSerially = null != processor.getClass().getAnnotation(TriggerSerially.class);
    }

    @Override
    public void enforceReadStreamsClosed(final boolean enforce) {
        enforceReadStreamsClosed = enforce;
        this.sessionFactory = new MockSessionFactory(sharedState, processor, enforceReadStreamsClosed, processorStateManager, allowSynchronousSessionCommits);
    }

    @Override
    public void setValidateExpressionUsage(final boolean validate) {
        this.validateExpressionUsage = validate;
        context.setValidateExpressionUsage(validate);
    }

    @Override
    public void setAllowSynchronousSessionCommits(final boolean allowSynchronousSessionCommits) {
        this.allowSynchronousSessionCommits = allowSynchronousSessionCommits;
        this.sessionFactory = new MockSessionFactory(sharedState, processor, enforceReadStreamsClosed, processorStateManager, allowSynchronousSessionCommits);
    }

    @Override
    public Processor getProcessor() {
        return processor;
    }

    @Override
    public MockProcessContext getProcessContext() {
        return context;
    }

    @Override
    public boolean isYieldCalled() {
        return getProcessContext().isYieldCalled();
    }

    @Override
    public void run() {
        run(1);
    }

    @Override
    public void run(int iterations) {
        run(iterations, true);
    }

    @Override
    public void run(final int iterations, final boolean stopOnFinish) {
        run(iterations, stopOnFinish, true);
    }

    @Override
    public void run(final int iterations, final boolean stopOnFinish, final boolean initialize) {
        run(iterations, stopOnFinish, initialize, 5000 + iterations * runSchedule);
    }

    @Override
    public void run(final int iterations, final boolean stopOnFinish, final boolean initialize, final long runWait) {
        if (iterations < 0) {
            throw new IllegalArgumentException();
        }

        context.assertValid();

        // Call onConfigurationRestored here, right before the test run, as all properties should have been set byt this point.
        ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnConfigurationRestored.class, processor, this.context);

        if (initialize) {
            try {
                ReflectionUtils.invokeMethodsWithAnnotation(OnScheduled.class, processor, context);
            } catch (final Exception e) {
                Assertions.fail("Could not invoke methods annotated with @OnScheduled annotation due to: " + e, e);
            }
        }

        @SuppressWarnings("unchecked") final Future<Throwable>[] futures = new Future[iterations];
        try (final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(numThreads)) {
            for (int i = 0; i < iterations; i++) {
                final Future<Throwable> future = executorService.schedule(new RunProcessor(), i * runSchedule, TimeUnit.MILLISECONDS);
                futures[i] = future;
            }

            executorService.shutdown();
            try {
                executorService.awaitTermination(runWait, TimeUnit.MILLISECONDS);
            } catch (final InterruptedException ignored) {
            }
        }

        int finishedCount = 0;
        boolean unscheduledRun = false;
        for (final Future<Throwable> future : futures) {
            try {
                final Throwable thrown = future.get(); // wait for the result
                if (thrown != null) {
                    throw new AssertionError(thrown);
                }

                if (++finishedCount == 1 && stopOnFinish) {
                    unscheduledRun = true;
                    unSchedule();
                }
            } catch (final InterruptedException | ExecutionException ignored) {
            }
        }

        if (!unscheduledRun && stopOnFinish) {
            unSchedule();
        }

        if (stopOnFinish) {
            stop();
        }
    }

    @Override
    public void unSchedule() {
        try {
            ReflectionUtils.invokeMethodsWithAnnotation(OnUnscheduled.class, processor, context);
        } catch (final Exception e) {
            Assertions.fail("Could not invoke methods annotated with @OnUnscheduled annotation due to: " + e);
        }
    }

    @Override
    public void stop() {
        try {
            ReflectionUtils.invokeMethodsWithAnnotation(OnStopped.class, processor, context);
        } catch (final Exception e) {
            Assertions.fail("Could not invoke methods annotated with @OnStopped annotation due to: " + e);
        }
    }

    @Override
    public void shutdown() {
        try {
            ReflectionUtils.invokeMethodsWithAnnotation(OnShutdown.class, processor);
        } catch (final Exception e) {
            Assertions.fail("Could not invoke methods annotated with @OnShutdown annotation due to: " + e);
        }
    }

    private class RunProcessor implements Callable<Throwable> {

        @Override
        public Throwable call() throws Exception {
            invocations.incrementAndGet();
            try {
                processor.onTrigger(context, sessionFactory);
            } catch (final Throwable t) {
                return t;
            }

            return null;
        }
    }

    @Override
    public ProcessSessionFactory getProcessSessionFactory() {
        return sessionFactory;
    }

    @Override
    public void assertAllFlowFilesTransferred(final String relationship) {
        for (final MockProcessSession session : sessionFactory.getCreatedSessions()) {
            session.assertAllFlowFilesTransferred(relationship);
        }
    }

    @Override
    public void assertAllFlowFilesTransferred(final Relationship relationship) {
        for (final MockProcessSession session : sessionFactory.getCreatedSessions()) {
            session.assertAllFlowFilesTransferred(relationship);
        }
    }

    @Override
    public void assertAllFlowFilesTransferred(final String relationship, final int count) {
        assertAllFlowFilesTransferred(relationship);
        assertTransferCount(relationship, count);
    }

    @Override
    public void assertAllFlowFilesContainAttribute(final String attributeName) {
        assertAllFlowFiles(f -> Assertions.assertNotNull(f.getAttribute(attributeName)));
    }

    @Override
    public void assertAllFlowFilesContainAttribute(final Relationship relationship, final String attributeName) {
        assertAllFlowFiles(relationship, f -> Assertions.assertNotNull(f.getAttribute(attributeName)));
    }

    @Override
    public void assertAllFlowFiles(FlowFileValidator validator) {
        for (final MockProcessSession session : sessionFactory.getCreatedSessions()) {
            session.assertAllFlowFiles(validator);
        }
    }

    @Override
    public void assertAllFlowFiles(Relationship relationship, FlowFileValidator validator) {
        for (final MockProcessSession session : sessionFactory.getCreatedSessions()) {
            session.assertAllFlowFiles(relationship, validator);
        }
    }

    @Override
    public void assertAttributes(
        Relationship relationship,
        Set<String> checkedAttributeNames,
        Set<Map<String, String>> expectedAttributes
    ) {
        assertTransferCount(relationship, expectedAttributes.size());
        List<MockFlowFile> flowFiles = getFlowFilesForRelationship(relationship);

        Set<Map<String, String>> actualAttributes = flowFiles.stream()
            .map(flowFile -> flowFile.getAttributes().entrySet().stream()
                .filter(attributeNameAndValue -> checkedAttributeNames.contains(attributeNameAndValue.getKey()))
                .filter(entry -> entry.getKey() != null && entry.getValue() != null)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))

            )
            .collect(toSet());

        assertEquals(expectedAttributes, actualAttributes);
    }

    @Override
    public void assertContents(Relationship relationship, List<String> expectedContent) {
        assertTransferCount(relationship, expectedContent.size());
        List<MockFlowFile> flowFiles = getFlowFilesForRelationship(relationship);

        List<String> actualContent = flowFiles.stream()
            .map(MockFlowFile::getContent)
            .collect(Collectors.toList());

        assertEquals(expectedContent, actualContent);
    }

    @Override
    public void assertAllFlowFilesTransferred(final Relationship relationship, final int count) {
        assertAllFlowFilesTransferred(relationship);
        assertTransferCount(relationship, count);
    }

    @Override
    public void assertTransferCount(final Relationship relationship, final int count) {
        assertEquals(count, getFlowFilesForRelationship(relationship).size());
    }

    @Override
    public void assertTransferCount(final String relationship, final int count) {
        assertEquals(count, getFlowFilesForRelationship(relationship).size());
    }

    @Override
    public void assertPenalizeCount(final int count) {
        assertEquals(count, getPenalizedFlowFiles().size());
    }

    @Override
    public void assertValid() {
        context.assertValid();
    }

    @Override
    public boolean isValid() {
        return context.isValid();
    }

    @Override
    public void assertNotValid() {
        assertFalse(context.isValid(), "Processor appears to be valid but expected it to be invalid");
    }

    @Override
    public boolean isQueueEmpty() {
        return flowFileQueue.isEmpty();
    }

    @Override
    public void assertQueueEmpty() {
        assertTrue(flowFileQueue.isEmpty());
    }

    @Override
    public void assertQueueNotEmpty() {
        assertFalse(flowFileQueue.isEmpty());
    }

    @Override
    public void clearTransferState() {
        for (final MockProcessSession session : sessionFactory.getCreatedSessions()) {
            session.clearTransferState();
        }
    }

    @Override
    public void enqueue(final FlowFile... flowFiles) {
        for (final FlowFile flowFile : flowFiles) {
            flowFileQueue.offer((MockFlowFile) flowFile);
        }
    }

    @Override
    public MockFlowFile enqueue(final Path path) throws IOException {
        return enqueue(path, new HashMap<>());
    }

    @Override
    public MockFlowFile enqueue(final Path path, final Map<String, String> attributes) throws IOException {
        final Map<String, String> modifiedAttributes = new HashMap<>(attributes);
        if (!modifiedAttributes.containsKey(CoreAttributes.FILENAME.key())) {
            modifiedAttributes.put(CoreAttributes.FILENAME.key(), path.toFile().getName());
        }
        try (final InputStream in = Files.newInputStream(path)) {
            return enqueue(in, modifiedAttributes);
        }
    }

    @Override
    public MockFlowFile enqueue(final byte[] data) {
        return enqueue(data, new HashMap<>());
    }

    @Override
    public MockFlowFile enqueue(final String data) {
        return enqueue(data.getBytes(StandardCharsets.UTF_8), Collections.emptyMap());
    }

    @Override
    public MockFlowFile enqueue(final byte[] data, final Map<String, String> attributes) {
        return enqueue(new ByteArrayInputStream(data), attributes);
    }

    @Override
    public MockFlowFile enqueue(final String data, final Map<String, String> attributes) {
        return enqueue(data.getBytes(StandardCharsets.UTF_8), attributes);
    }


    @Override
    public MockFlowFile enqueue(final InputStream data) {
        return enqueue(data, new HashMap<>());
    }

    @Override
    public MockFlowFile enqueue(final InputStream data, final Map<String, String> attributes) {
        final MockProcessSession session = new MockProcessSession(new SharedSessionState(processor, idGenerator), processor, enforceReadStreamsClosed, processorStateManager);
        MockFlowFile flowFile = session.create();
        flowFile = session.importFrom(data, flowFile);
        flowFile = session.putAllAttributes(flowFile, attributes);
        enqueue(flowFile);
        return flowFile;
    }

    @Override
    public byte[] getContentAsByteArray(final MockFlowFile flowFile) {
        return flowFile.getData();
    }

    @Override
    public List<MockFlowFile> getFlowFilesForRelationship(final String relationship) {
        final Relationship rel = new Relationship.Builder().name(relationship).build();
        return getFlowFilesForRelationship(rel);
    }

    @Override
    public List<MockFlowFile> getFlowFilesForRelationship(final Relationship relationship) {
        final List<MockFlowFile> flowFiles = new ArrayList<>();
        for (final MockProcessSession session : sessionFactory.getCreatedSessions()) {
            flowFiles.addAll(session.getFlowFilesForRelationship(relationship));
        }

        return flowFiles;
    }

    @Override
    public List<MockFlowFile> getPenalizedFlowFiles() {
        final List<MockFlowFile> flowFiles = new ArrayList<>();
        for (final MockProcessSession session : sessionFactory.getCreatedSessions()) {
            flowFiles.addAll(session.getPenalizedFlowFiles());
        }

        return flowFiles;
    }

    @Override
    public QueueSize getQueueSize() {
        return flowFileQueue.size();
    }

    public void clearQueue() {
        // TODO: Add #clear to MockFlowFileQueue or just point to new instance?
        while (!flowFileQueue.isEmpty()) {
            flowFileQueue.poll();
        }
    }

    @Override
    public Long getCounterValue(final String name) {
        return sharedState.getCounterValue(name);
    }

    @Override
    public int getRemovedCount() {
        int count = 0;
        for (final MockProcessSession session : sessionFactory.getCreatedSessions()) {
            count += session.getRemovedCount();
        }

        return count;
    }

    @Override
    public void setAnnotationData(final String annotationData) {
        context.setAnnotationData(annotationData);
    }

    @Override
    public ValidationResult setProperty(final String propertyName, final String propertyValue) {
        return context.setProperty(propertyName, propertyValue);
    }

    @Override
    public ValidationResult setProperty(final PropertyDescriptor descriptor, final String value) {
        return context.setProperty(descriptor, value);
    }

    @Override
    public ValidationResult setProperty(final PropertyDescriptor descriptor, final DescribedValue value) {
        return context.setProperty(descriptor, value.getValue());
    }

    @Override
    public void setThreadCount(final int threadCount) {
        if (threadCount > 1 && triggerSerially) {
            Assertions.fail("Cannot set thread-count higher than 1 because the processor is triggered serially");
        }

        this.numThreads = threadCount;
        this.context.setMaxConcurrentTasks(threadCount);
    }

    @Override
    public int getThreadCount() {
        return numThreads;
    }

    @Override
    public void setRelationshipAvailable(final Relationship relationship) {
        final Set<Relationship> unavailable = new HashSet<>(context.getUnavailableRelationships());
        unavailable.remove(relationship);
        context.setUnavailableRelationships(unavailable);
    }

    @Override
    public void setRelationshipAvailable(final String relationshipName) {
        setRelationshipAvailable(new Relationship.Builder().name(relationshipName).build());
    }

    @Override
    public void setRelationshipUnavailable(final Relationship relationship) {
        final Set<Relationship> unavailable = new HashSet<>(context.getUnavailableRelationships());
        unavailable.add(relationship);
        context.setUnavailableRelationships(unavailable);
    }

    @Override
    public void setRelationshipUnavailable(final String relationshipName) {
        setRelationshipUnavailable(new Relationship.Builder().name(relationshipName).build());
    }

    @Override
    public void setIncomingConnection(boolean hasIncomingConnection) {
        context.setIncomingConnection(hasIncomingConnection);
    }

    @Override
    public void setNonLoopConnection(final boolean hasNonLoopConnection) {
        context.setNonLoopConnection(hasNonLoopConnection);
    }

    @Override
    public void addConnection(Relationship relationship) {
        context.addConnection(relationship);
    }

    @Override
    public void addConnection(String relationshipName) {
        addConnection(new Relationship.Builder().name(relationshipName).build());
    }

    @Override
    public void removeConnection(Relationship relationship) {
        context.removeConnection(relationship);
    }

    @Override
    public void removeConnection(String relationshipName) {
        removeConnection(new Relationship.Builder().name(relationshipName).build());
    }

    @Override
    public void addControllerService(final String identifier, final ControllerService service) throws InitializationException {
        addControllerService(identifier, service, new HashMap<>());
    }

    @Override
    public void addControllerService(final String identifier, final ControllerService service, final Map<String, String> properties) throws InitializationException {
        final MockComponentLog mockComponentLog = new MockComponentLog(identifier, service);
        controllerServiceLoggers.put(identifier, mockComponentLog);
        final MockStateManager serviceStateManager = new MockStateManager(service);
        final MockControllerServiceInitializationContext initContext = new MockControllerServiceInitializationContext(
                Objects.requireNonNull(service), Objects.requireNonNull(identifier), mockComponentLog, serviceStateManager, kerberosContext);
        controllerServiceStateManagers.put(identifier, serviceStateManager);
        initContext.addControllerServices(context);
        service.initialize(initContext);

        final Map<PropertyDescriptor, String> resolvedProps = new HashMap<>();
        for (final Map.Entry<String, String> entry : properties.entrySet()) {
            resolvedProps.put(service.getPropertyDescriptor(entry.getKey()), entry.getValue());
        }

        try {
            ReflectionUtils.invokeMethodsWithAnnotation(OnAdded.class, service);
        } catch (final InvocationTargetException | IllegalAccessException | IllegalArgumentException e) {
            throw new InitializationException(e);
        }

        context.addControllerService(service, resolvedProps, null);
    }

    @Override
    public void assertNotValid(final ControllerService service) {
        final StateManager serviceStateManager = controllerServiceStateManagers.get(service.getIdentifier());
        if (serviceStateManager == null) {
            throw new IllegalStateException("Controller Service has not been added to this TestRunner via the #addControllerService method");
        }

        final ValidationContext validationContext = new MockValidationContext(context, serviceStateManager).getControllerServiceValidationContext(service);
        final ControllerService canonicalService = context.getControllerService(service.getIdentifier());
        final Collection<ValidationResult> results = canonicalService.validate(validationContext);

        for (final ValidationResult result : results) {
            if (!result.isValid()) {
                return;
            }
        }

        Assertions.fail("Expected Controller Service " + service + " to be invalid but it is valid");
    }

    @Override
    public void assertValid(final ControllerService service) {
        final StateManager serviceStateManager = controllerServiceStateManagers.get(service.getIdentifier());
        if (serviceStateManager == null) {
            throw new IllegalStateException("Controller Service has not been added to this TestRunner via the #addControllerService method");
        }

        final ValidationContext validationContext = new MockValidationContext(context, serviceStateManager).getControllerServiceValidationContext(service);
        final Collection<ValidationResult> results = context.getControllerService(service.getIdentifier()).validate(validationContext);

        for (final ValidationResult result : results) {
            if (!result.isValid()) {
                Assertions.fail("Expected Controller Service to be valid but it is invalid due to: " + result);
            }
        }
    }

    @Override
    public void disableControllerService(final ControllerService service) {
        final ControllerServiceConfiguration configuration = context.getConfiguration(service.getIdentifier());
        if (configuration == null) {
            throw new IllegalArgumentException("Controller Service " + service + " is not known");
        }

        if (!configuration.isEnabled()) {
            throw new IllegalStateException("Controller service " + service + " cannot be disabled because it is not enabled");
        }

        try {
            // Create a config context to pass into the controller service's OnDisabled method (it will be ignored if the controller service has no arguments)
            final MockConfigurationContext configContext = new MockConfigurationContext(service, configuration.getProperties(), context, environmentVariables);
            configContext.setValidateExpressions(validateExpressionUsage);
            ReflectionUtils.invokeMethodsWithAnnotation(OnDisabled.class, service, configContext);
        } catch (final Exception e) {
            e.printStackTrace();
            Assertions.fail("Failed to disable Controller Service " + service + " due to " + e);
        }

        configuration.setEnabled(false);
    }

    @Override
    public void enableControllerService(final ControllerService service) {
        final ControllerServiceConfiguration configuration = context.getConfiguration(service.getIdentifier());
        if (configuration == null) {
            throw new IllegalArgumentException("Controller Service " + service + " is not known");
        }

        if (configuration.isEnabled()) {
            throw new IllegalStateException("Cannot enable Controller Service " + service + " because it is not disabled");
        }

        // ensure controller service is valid before enabling
        final MockValidationContext mockValidationContext = new MockValidationContext(context, null);
        mockValidationContext.setValidateExpressions(validateExpressionUsage);
        final ValidationContext serviceValidationContext = mockValidationContext.getControllerServiceValidationContext(service);

        final Collection<ValidationResult> results = context.getControllerService(service.getIdentifier()).validate(serviceValidationContext);

        for (final ValidationResult result : results) {
            if (!result.isValid()) {
                throw new IllegalStateException("Cannot enable Controller Service " + service + " because it is in an invalid state: " + result);
            }
        }

        try {
            final MockConfigurationContext configContext = new MockConfigurationContext(service, configuration.getProperties(), context, environmentVariables);
            configContext.setValidateExpressions(validateExpressionUsage);
            ReflectionUtils.invokeMethodsWithAnnotation(OnEnabled.class, service, configContext);
        } catch (final InvocationTargetException ite) {
            ite.getCause().printStackTrace();
            Assertions.fail("Failed to enable Controller Service " + service + " due to " + ite.getCause());
        } catch (final Exception e) {
            e.printStackTrace();
            Assertions.fail("Failed to enable Controller Service " + service + " due to " + e);
        }

        configuration.setEnabled(true);
    }

    @Override
    public boolean isControllerServiceEnabled(final ControllerService service) {
        final ControllerServiceConfiguration configuration = context.getConfiguration(service.getIdentifier());
        if (configuration == null) {
            throw new IllegalArgumentException("Controller Service " + service + " is not known");
        }

        return configuration.isEnabled();
    }

    @Override
    public void removeControllerService(final ControllerService service) {
        if (context.getControllerServiceLookup().isControllerServiceEnabled(service)) {
            disableControllerService(service);
        }

        try {
            ReflectionUtils.invokeMethodsWithAnnotation(OnRemoved.class, service);
        } catch (final Exception e) {
            e.printStackTrace();
            Assertions.fail("Failed to remove Controller Service " + service + " due to " + e);
        }

        context.removeControllerService(service);
    }

    @Override
    public void setAnnotationData(final ControllerService service, final String annotationData) {
        final ControllerServiceConfiguration configuration = getConfigToUpdate(service);
        configuration.setAnnotationData(annotationData);
    }

    private ControllerServiceConfiguration getConfigToUpdate(final ControllerService service) {
        final ControllerServiceConfiguration configuration = context.getConfiguration(service.getIdentifier());
        if (configuration == null) {
            throw new IllegalArgumentException("Controller Service " + service + " is not known");
        }

        if (configuration.isEnabled()) {
            throw new IllegalStateException("Controller service " + service + " cannot be modified because it is not disabled");
        }

        return configuration;
    }

    @Override
    public ValidationResult setProperty(final ControllerService service, final PropertyDescriptor property, final DescribedValue value) {
        return setProperty(service, property, value.getValue());
    }

    @Override
    public ValidationResult setProperty(final ControllerService service, final PropertyDescriptor property, final String value) {
        final MockStateManager serviceStateManager = controllerServiceStateManagers.get(service.getIdentifier());
        if (serviceStateManager == null) {
            throw new IllegalStateException("Controller service " + service + " has not been added to this TestRunner via the #addControllerService method");
        }

        final ControllerServiceConfiguration configuration = getConfigToUpdate(service);
        final Map<PropertyDescriptor, String> curProps = configuration.getProperties();
        final Map<PropertyDescriptor, String> updatedProps = new HashMap<>(curProps);

        final ValidationContext validationContext = new MockValidationContext(context, serviceStateManager).getControllerServiceValidationContext(service);
        final boolean dependencySatisfied = validationContext.isDependencySatisfied(property, processor::getPropertyDescriptor);

        final ValidationResult validationResult;
        if (dependencySatisfied) {
            validationResult = property.validate(value, validationContext);
        } else {
            validationResult = new ValidationResult.Builder()
                .valid(true)
                .input(value)
                .subject(property.getDisplayName())
                .explanation("Property is dependent upon another property, and this dependency is not satisfied, so value is considered valid")
                .build();
        }

        final String oldValue = updatedProps.get(property);
        updatedProps.put(property, value);
        configuration.setProperties(updatedProps);

        if ((value == null && oldValue != null) || (value != null && !value.equals(oldValue))) {
            service.onPropertyModified(property, oldValue, value);
        }

        return validationResult;
    }

    @Override
    public ValidationResult setProperty(final ControllerService service, final String propertyName, final String value) {
        final PropertyDescriptor descriptor = service.getPropertyDescriptor(propertyName);
        if (descriptor == null) {
            return new ValidationResult.Builder()
                .input(propertyName)
                .explanation(propertyName + " is not a known Property for Controller Service " + service)
                .subject("Invalid property")
                .valid(false)
                .build();
        }
        return setProperty(service, descriptor, value);
    }

    @Override
    public ControllerService getControllerService(final String identifier) {
        return context.getControllerService(identifier);
    }

    @Override
    public <T extends ControllerService> T getControllerService(final String identifier, final Class<T> serviceType) {
        final ControllerService service = context.getControllerService(identifier);
        return serviceType.cast(service);
    }

    @Override
    public boolean removeProperty(PropertyDescriptor descriptor) {
        return context.removeProperty(descriptor);
    }

    @Override
    public boolean removeProperty(String property) {
        return context.removeProperty(property);
    }

    @Override
    public boolean removeProperty(final ControllerService service, final PropertyDescriptor property) {
        final MockStateManager serviceStateManager = controllerServiceStateManagers.get(service.getIdentifier());
        if (serviceStateManager == null) {
            throw new IllegalStateException("Controller service " + service + " has not been added to this TestRunner via the #addControllerService method");
        }

        final ControllerServiceConfiguration configuration = getConfigToUpdate(service);
        final Map<PropertyDescriptor, String> curProps = configuration.getProperties();
        final Map<PropertyDescriptor, String> updatedProps = new HashMap<>(curProps);

        final String oldValue = updatedProps.remove(property);
        if (oldValue == null) {
            return false;
        }

        configuration.setProperties(updatedProps);
        service.onPropertyModified(property, oldValue, null);
        return true;
    }

    @Override
    public boolean removeProperty(ControllerService service, String propertyName) {
        final PropertyDescriptor descriptor = service.getPropertyDescriptor(propertyName);
        if (descriptor == null) {
            return false;
        }
        return removeProperty(service, descriptor);
    }

    @Override
    public void clearProperties() {
        context.clearProperties();
    }

    @Override
    public List<ProvenanceEventRecord> getProvenanceEvents() {
        return sharedState.getProvenanceEvents();
    }

    @Override
    public void clearProvenanceEvents() {
        sharedState.clearProvenanceEvents();
    }

    @Override
    public MockStateManager getStateManager() {
        return processorStateManager;
    }

    /**
     * Returns the State Manager for the given Controller Service.
     *
     * @param controllerService the Controller Service whose State Manager should be returned
     * @return the State Manager for the given Controller Service
     */
    @Override
    public MockStateManager getStateManager(final ControllerService controllerService) {
        return controllerServiceStateManagers.get(controllerService.getIdentifier());
    }

    @Override
    public MockComponentLog getLogger() {
        return logger;
    }

    @Override
    public MockComponentLog getControllerServiceLogger(final String identifier) {
        return controllerServiceLoggers.get(identifier);
    }

    @Override
    public void setClustered(boolean clustered) {
        context.setClustered(clustered);
    }

    @Override
    public void setIsConfiguredForClustering(final boolean isConfiguredForClustering) {
        context.setIsConfiguredForClustering(isConfiguredForClustering);
    }

    @Override
    public void setPrimaryNode(boolean primaryNode) {
        if (context.isPrimary() != primaryNode) {
            try {
                ReflectionUtils.invokeMethodsWithAnnotation(OnPrimaryNodeStateChange.class, processor,
                        primaryNode ? PrimaryNodeState.ELECTED_PRIMARY_NODE : PrimaryNodeState.PRIMARY_NODE_REVOKED);
            } catch (final Exception e) {
                Assertions.fail("Could not invoke methods annotated with @OnPrimaryNodeStateChange annotation due to: " + e);
            }
        }
        context.setPrimaryNode(primaryNode);
    }

    @Override
    public void setConnected(final boolean isConnected) {
        context.setConnected(isConnected);
    }

    @Override
    public String getEnvironmentVariableValue(final String name) {
        Objects.requireNonNull(name);
        if (environmentVariables.containsKey(name)) {
            return environmentVariables.get(name);
        } else {
            return EnvironmentVariables.ENVIRONMENT_VARIABLES.getEnvironmentVariableValue(name);
        }
    }

    @Override
    public void setEnvironmentVariableValue(String name, String value) {
        environmentVariables.put(name, value);
    }

    /**
     * Asserts that all FlowFiles meet all conditions.
     *
     * @param relationshipName relationship name
     * @param predicate conditions
     */
    @Override
    public void assertAllConditionsMet(final String relationshipName, Predicate<MockFlowFile> predicate) {
        assertAllConditionsMet(new Relationship.Builder().name(relationshipName).build(), predicate);
    }

    /**
     * Asserts that all FlowFiles meet all conditions.
     *
     * @param relationship relationship
     * @param predicate conditions
     */
    @Override
    public void assertAllConditionsMet(final Relationship relationship, Predicate<MockFlowFile> predicate) {

        if (predicate == null) {
            Assertions.fail("predicate cannot be null");
        }

        final List<MockFlowFile> flowFiles = getFlowFilesForRelationship(relationship);

        if (flowFiles.isEmpty()) {
            Assertions.fail("Relationship " + relationship.getName() + " does not contain any FlowFile");
        }

        for (MockFlowFile flowFile : flowFiles) {
            if (!predicate.test(flowFile)) {
                Assertions.fail("FlowFile " + flowFile + " does not meet all condition");
            }
        }
    }


    /**
     * Set the Run Schedule parameter (in milliseconds). If set, this will be the duration
     * between two calls of the onTrigger method.
     *
     * @param runSchedule Run schedule duration in milliseconds.
     */
    @Override
    public void setRunSchedule(long runSchedule) {
        this.runSchedule = runSchedule;
    }

    @Override
    public void assertProvenanceEvent(final ProvenanceEventType eventType) {
        Set<ProvenanceEventType> expectedEventTypes = Collections.singleton(eventType);
        Set<ProvenanceEventType> actualEventTypes = getProvenanceEvents().stream()
                        .map(ProvenanceEventRecord::getEventType)
                        .collect(toSet());
        assertEquals(expectedEventTypes, actualEventTypes);
    }

    @Override
    public PropertyMigrationResult migrateProperties() {
        final MockPropertyConfiguration mockPropertyConfiguration = new MockPropertyConfiguration(context.getAllProperties());
        getProcessor().migrateProperties(mockPropertyConfiguration);

        final PropertyMigrationResult migrationResult = mockPropertyConfiguration.toPropertyMigrationResult();
        final Set<MockPropertyConfiguration.CreatedControllerService> services = migrationResult.getCreatedControllerServices();

        RuntimeException serviceCreationException = null;
        for (final MockPropertyConfiguration.CreatedControllerService service : services) {
            final ControllerService serviceImpl;
            try {
                final Class<?> clazz = Class.forName(service.implementationClassName());
                final Object newInstance = clazz.getDeclaredConstructor().newInstance();
                if (!(newInstance instanceof ControllerService)) {
                    throw new RuntimeException(clazz + " is not a Controller Service");
                }

                serviceImpl = (ControllerService) newInstance;
                addControllerService(service.id(), serviceImpl, service.serviceProperties());
                enableControllerService(serviceImpl);
            } catch (final Exception e) {
                if (serviceCreationException == null) {
                    if (e instanceof RuntimeException) {
                        serviceCreationException = (RuntimeException) e;
                    } else {
                        serviceCreationException = new RuntimeException(e);
                    }
                } else {
                    serviceCreationException.addSuppressed(e);
                }
            }
        }

        if (serviceCreationException != null) {
            throw serviceCreationException;
        }

        final Map<String, String> updatedProperties = mockPropertyConfiguration.getRawProperties();
        clearProperties();
        updatedProperties.forEach((propertyName, propertyValue) -> {
            if (propertyValue == null) {
                removeProperty(propertyName);
            } else {
                setProperty(propertyName, propertyValue);
            }
        });

        return migrationResult;
    }

    @Override
    public RelationshipMigrationResult migrateRelationships() {
        final MockRelationshipConfiguration mockRelationshipConfiguration = new MockRelationshipConfiguration(context.getAllRelationships());
        getProcessor().migrateRelationships(mockRelationshipConfiguration);

        final Set<Relationship> updatedRelationships = mockRelationshipConfiguration.getRawRelationships();
        context.clearConnections();
        updatedRelationships.forEach(context::addConnection);

        return mockRelationshipConfiguration.toRelationshipMigrationResult();
    }
}
