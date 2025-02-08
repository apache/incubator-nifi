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

import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.migration.RelationshipConfiguration;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.state.MockStateManager;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

public interface TestRunner {

    /**
     * @return the {@link Processor} for which this <code>TestRunner</code> is
     *         configured
     */
    Processor getProcessor();

    /**
     * @return the {@link ProcessSessionFactory} that this
     *         <code>TestRunner</code> will use to invoke the
     *         {@link Processor#onTrigger(ProcessContext, ProcessSessionFactory)} method
     */
    ProcessSessionFactory getProcessSessionFactory();

    /**
     * @return the {@link ProcessContext} that this <code>TestRunner</code> will
     *         use to invoke the
     *         {@link Processor#onTrigger(ProcessContext, ProcessSessionFactory) onTrigger}
     *         method
     */
    ProcessContext getProcessContext();

    boolean isYieldCalled();

    /**
     * Performs exactly the same operation as calling {@link #run(int)} with a
     * value of 1.
     */
    void run();

    /**
     * Performs the same operation as calling {@link #run(int, boolean)} with a
     * value of <code>true</code>
     *
     * @param iterations number of iterations
     */
    void run(int iterations);

    /**
     * performs the same operation as calling {@link #run(int, boolean, boolean)}
     * with a value of {@code iterations}, {@code stopOnFinish}, {@code true}
     *
     * @param iterations number of iterations
     * @param stopOnFinish flag to stop when finished
     */
    void run(int iterations, boolean stopOnFinish);

    /**
     * This method runs the {@link Processor} <code>iterations</code> times,
     * using the sequence of steps below:
     * <ul>
     * <li>
     * If {@code initialize} is true, run all methods on the Processor that are
     * annotated with the
     * {@link org.apache.nifi.annotation.lifecycle.OnScheduled @OnScheduled} annotation. If
     * any of these methods throws an Exception, the Unit Test will fail.
     * </li>
     * <li>
     * Schedule the
     * {@link Processor#onTrigger(ProcessContext, ProcessSessionFactory) onTrigger}
     * method to be invoked <code>iterations</code> times. The number of threads
     * used to run these iterations is determined by the ThreadCount of this
     * <code>TestRunner</code>. By default, the value is set to 1, but it can be
     * modified by calling the {@link #setThreadCount(int)} method.
     * </li>
     * <li>
     * As soon as the first thread finishes its execution of
     * {@link Processor#onTrigger(ProcessContext, ProcessSessionFactory) onTrigger},
     * all methods on the Processor that are annotated with the
     * {@link org.apache.nifi.annotation.lifecycle.OnUnscheduled @OnUnscheduled} annotation
     * are invoked. If any of these methods throws an Exception, the Unit Test
     * will fail.
     * </li>
     * <li>
     * Waits for all threads to finish execution.
     * </li>
     * <li>
     * If and only if the value of <code>shutdown</code> is true: Call all
     * methods on the Processor that is annotated with the
     * {@link org.apache.nifi.annotation.lifecycle.OnStopped @OnStopped} annotation.
     * </li>
     * </ul>
     *
     * @param iterations number of iterations
     * @param stopOnFinish whether or not to run the Processor methods that are
     *            annotated with {@link org.apache.nifi.annotation.lifecycle.OnStopped @OnStopped}
     * @param initialize true if must initialize
     */
    void run(int iterations, boolean stopOnFinish, final boolean initialize);

    /**
     * This method runs the {@link Processor} <code>iterations</code> times,
     * using the sequence of steps below:
     * <ul>
     * <li>
     * If {@code initialize} is true, run all methods on the Processor that are
     * annotated with the
     * {@link org.apache.nifi.annotation.lifecycle.OnScheduled @OnScheduled} annotation. If
     * any of these methods throws an Exception, the Unit Test will fail.
     * </li>
     * <li>
     * Schedule the
     * {@link Processor#onTrigger(ProcessContext, ProcessSessionFactory) onTrigger}
     * method to be invoked <code>iterations</code> times. The number of threads
     * used to run these iterations is determined by the ThreadCount of this
     * <code>TestRunner</code>. By default, the value is set to 1, but it can be
     * modified by calling the {@link #setThreadCount(int)} method.
     * </li>
     * <li>
     * As soon as the first thread finishes its execution of
     * {@link Processor#onTrigger(ProcessContext, ProcessSessionFactory) onTrigger},
     * all methods on the Processor that are annotated with the
     * {@link org.apache.nifi.annotation.lifecycle.OnUnscheduled @OnUnscheduled} annotation
     * are invoked. If any of these methods throws an Exception, the Unit Test
     * will fail.
     * </li>
     * <li>
     * Waits for all threads to finish execution.
     * </li>
     * <li>
     * If and only if the value of <code>shutdown</code> is true: Call all
     * methods on the Processor that is annotated with the
     * {@link org.apache.nifi.annotation.lifecycle.OnStopped @OnStopped} annotation.
     * </li>
     * </ul>
     *
     * @param iterations number of iterations
     * @param stopOnFinish whether or not to run the Processor methods that are
     *            annotated with {@link org.apache.nifi.annotation.lifecycle.OnStopped @OnStopped}
     * @param initialize true if must initialize
     * @param runWait indicates the amount of time in milliseconds that the framework should wait for
     *            processors to stop running before calling the {@link org.apache.nifi.annotation.lifecycle.OnUnscheduled @OnUnscheduled} annotation
     */
    void run(int iterations, boolean stopOnFinish, final boolean initialize, final long runWait);

    /**
     * Invokes all methods on the Processor that are annotated with the
     * {@link org.apache.nifi.annotation.lifecycle.OnUnscheduled @OnUnscheduled} annotation. If
     * any of these methods throws an Exception, the Unit Test will fail
     */
    void unSchedule();

    /**
     * Invokes all methods on the Processor that are annotated with the
     * {@link org.apache.nifi.annotation.lifecycle.OnStopped @OnStopped} annotation. If
     * any of these methods throws an Exception, the Unit Test will fail
     */
    void stop();

    /**
     * Invokes all methods on the Processor that are annotated with the
     * {@link org.apache.nifi.annotation.lifecycle.OnShutdown @OnShutdown} annotation. If
     * any of these methods throws an Exception, the Unit Test will fail
     */
    void shutdown();

    /**
     * Updates the number of threads that will be used to run the Processor when
     * calling the {@link #run()} or {@link #run(int)} methods.
     *
     * @param threadCount num threads
     */
    void setThreadCount(int threadCount);

    /**
     * @return the currently configured number of threads that will be used to
     *         runt he Processor when calling the {@link #run()} or {@link #run(int)}
     *         methods
     */
    int getThreadCount();

    /**
     * Updates the value of the property with the given PropertyDescriptor to
     * the specified value IF and ONLY IF the value is valid according to the
     * descriptor's validator. Otherwise, Assertions.fail() is called, causing the
     * unit test to fail
     *
     * @param propertyName name
     * @param propertyValue value
     * @return result
     */
    ValidationResult setProperty(String propertyName, String propertyValue);

    /**
     * Updates the value of the property with the given PropertyDescriptor to
     * the specified value IF and ONLY IF the value is valid according to the
     * descriptor's validator. Otherwise, Assertions.fail() is called, causing the
     * unit test to fail
     *
     * @param descriptor descriptor
     * @param value value
     * @return result
     */
    ValidationResult setProperty(PropertyDescriptor descriptor, String value);

    /**
     * Updates the value of the property with the given PropertyDescriptor to
     * the specified value IF and ONLY IF the value is valid according to the
     * descriptor's validator. Otherwise, Assertions.fail() is called, causing the
     * unit test to fail
     *
     * @param descriptor descriptor
     * @param value allowable valu
     * @return result
     */
    ValidationResult setProperty(PropertyDescriptor descriptor, DescribedValue value);

    /**
     * Sets the annotation data.
     *
     * @param annotationData data
     */
    void setAnnotationData(String annotationData);

    /**
     * Asserts that all FlowFiles that were transferred were transferred to the
     * given relationship
     *
     * @param relationship to verify
     */
    void assertAllFlowFilesTransferred(String relationship);

    /**
     * Asserts that all FlowFiles that were transferred were transferred to the
     * given relationship
     *
     * @param relationship to verify
     */
    void assertAllFlowFilesTransferred(Relationship relationship);

    /**
     * Asserts that all FlowFiles that were transferred were transferred to the
     * given relationship and that the number of FlowFiles transferred is equal
     * to <code>count</code>
     *
     * @param relationship to verify
     * @param count number of expected transfers
     */
    void assertAllFlowFilesTransferred(String relationship, int count);

    /**
     * Asserts that all FlowFiles that were transferred were transferred to the
     * given relationship and that the number of FlowFiles transferred is equal
     * to <code>count</code>
     *
     * @param relationship to verify
     * @param count number of expected transfers
     */
    void assertAllFlowFilesTransferred(Relationship relationship, int count);

    /**
     * Asserts that all FlowFiles that were transferred contain the given
     * attribute.
     *
     * @param attributeName attribute to look for
     */
    void assertAllFlowFilesContainAttribute(String attributeName);

    /**
     * Asserts that all FlowFiles that were transferred to the given
     * relationship contain the given attribute.
     *
     * @param relationship relationship to check
     * @param attributeName attribute to look for
     */
    void assertAllFlowFilesContainAttribute(Relationship relationship, String attributeName);

    /**
     * Asserts that all FlowFiles that were transferred are compliant with the
     * given validator.
     *
     * @param validator validator to use
     */
    void assertAllFlowFiles(FlowFileValidator validator);

    /**
     * Asserts that all FlowFiles that were transferred in the given relationship
     * are compliant with the given validator.
     *
     * @param validator validator to use
     */
    void assertAllFlowFiles(Relationship relationship, FlowFileValidator validator);

    /**
     * Asserts that flowfiles on a given relationship have a certain set of attributes.
     *
     * @param relationship The relationship on which to check the attributes of flowfiles
     * @param checkedAttributeNames The names of attributes that should be checked
     * @param expectedAttributes The expected attributes of all flowfiles
     */
    void assertAttributes(
        Relationship relationship,
        Set<String> checkedAttributeNames,
        Set<Map<String, String>> expectedAttributes
    );

    /**
     * Asserts that flowfiles on a given relationship have certain contents.
     *
     * @param relationship The relationship on which to check the contents of flowfiles
     * @param expectedContent The expected contents of all flowfiles
     */
    void assertContents(Relationship relationship, List<String> expectedContent);

    /**
     * Assert that the number of FlowFiles transferred to the given relationship
     * is equal to the given count
     *
     * @param relationship to verify
     * @param count number of expected transfers
     */
    void assertTransferCount(Relationship relationship, int count);

    /**
     * Assert that the number of FlowFiles transferred to the given relationship
     * is equal to the given count
     *
     * @param relationship to verify
     * @param count number of expected transfers
     */
    void assertTransferCount(String relationship, int count);

    /**
     * Assert that the number of FlowFiles that were penalized is equal to the given count
     *
     * @param count
     *            number of expected penalized
     */
    void assertPenalizeCount(int count);

    /**
     * Assert that there are no FlowFiles left on the input queue.
     */
    void assertQueueEmpty();

    /**
     * @return <code>true</code> if the Input Queue to the Processor is empty,
     *         <code>false</code> otherwise
     */
    boolean isQueueEmpty();

    /**
     * Assert that there is at least one FlowFile left on the input queue.
     */
    void assertQueueNotEmpty();

    /**
     * Assert that the currently configured set of properties/annotation data
     * are valid
     */
    void assertValid();

    boolean isValid();

    /**
     * Assert that the currently configured set of properties/annotation data
     * are NOT valid
     */
    void assertNotValid();

    /**
     * Resets the Transfer Counts that indicate how many FlowFiles have been
     * transferred to each Relationship and removes from memory any FlowFiles
     * that have been transferred to this Relationships. This method should be
     * called between successive calls to {@link #run(int) run} if the output is
     * to be examined after each run.
     */
    void clearTransferState();

    /**
     * Enqueues the given FlowFiles into the Processor's input queue
     *
     * @param flowFiles to enqueue
     */
    void enqueue(FlowFile... flowFiles);

    /**
     * Reads the content from the given {@link Path} into memory and creates a
     * FlowFile from this content with no attributes and adds this FlowFile to
     * the Processor's Input Queue
     *
     * @param path to read content from
     * @throws IOException if unable to read content
     */
    MockFlowFile enqueue(Path path) throws IOException;

    /**
     * Reads the content from the given {@link Path} into memory and creates a
     * FlowFile from this content with the given attributes and adds this
     * FlowFile to the Processor's Input Queue
     *
     * @param path to read content from
     * @param attributes attributes to use for new flow file
     * @throws IOException if unable to read content
     */
    MockFlowFile enqueue(Path path, Map<String, String> attributes) throws IOException;

    /**
     * Copies the content from the given byte array into memory and creates a
     * FlowFile from this content with no attributes and adds this FlowFile to
     * the Processor's Input Queue
     *
     * @param data to enqueue
     */
    MockFlowFile enqueue(byte[] data);

    /**
     * Creates a FlowFile with the content set to the given string (in UTF-8 format), with no attributes,
     * and adds this FlowFile to the Processor's Input Queue
     *
     * @param data to enqueue
     */
    MockFlowFile enqueue(String data);

    /**
     * Copies the content from the given byte array into memory and creates a
     * FlowFile from this content with the given attributes and adds this
     * FlowFile to the Processor's Input Queue
     *
     * @param data to enqueue
     * @param attributes to use for enqueued item
     */
    MockFlowFile enqueue(byte[] data, Map<String, String> attributes);

    /**
     * Creates a FlowFile with the content set to the given string (in UTF-8 format), with the given attributes,
     * and adds this FlowFile to the Processor's Input Queue
     *
     * @param data to enqueue
     * @param attributes to use for enqueued item
     */
    MockFlowFile enqueue(String data, Map<String, String> attributes);

    /**
     * Reads the content from the given {@link InputStream} into memory and
     * creates a FlowFile from this content with no attributes and adds this
     * FlowFile to the Processor's Input Queue
     *
     * @param data to source data from
     */
    MockFlowFile enqueue(InputStream data);

    /**
     * Reads the content from the given {@link InputStream} into memory and
     * creates a FlowFile from this content with the given attributes and adds
     * this FlowFile to the Processor's Input Queue
     *
     * @param data source of data
     * @param attributes to use for flow files
     */
    MockFlowFile enqueue(InputStream data, Map<String, String> attributes);

    /**
     * Copies the contents of the given {@link MockFlowFile} into a byte array
     * and returns that byte array.
     *
     * @param flowFile to get content for
     * @return byte array of flowfile content
     */
    byte[] getContentAsByteArray(MockFlowFile flowFile);

    /**
     * Returns a List of FlowFiles in the order in which they were transferred
     * to the given relationship
     *
     * @param relationship to get flowfiles for
     * @return flowfiles transferred to given relationship
     */
    List<MockFlowFile> getFlowFilesForRelationship(String relationship);

    /**
     * Returns a List of FlowFiles in the order in which they were transferred
     * to the given relationship
     *
     * @param relationship to get flowfiles for
     * @return flowfiles transferred to given relationship
     */
    List<MockFlowFile> getFlowFilesForRelationship(Relationship relationship);

    /**
     * Returns a List of FlowFiles in the order in which they were transferred that were penalized
     *
     * @return flowfiles that were penalized
     */
    List<MockFlowFile> getPenalizedFlowFiles();

    /**
     * @return the current size of the Processor's Input Queue
     */
    QueueSize getQueueSize();

    /**
     * @param name of counter
     * @return the current value of the counter with the specified name, or null
     *         if no counter exists with the specified name
     */
    Long getCounterValue(String name);

    /**
     * @return the number of FlowFiles that have been removed from the system
     */
    int getRemovedCount();

    /**
     * Indicates to the Framework that the given Relationship should be
     * considered "available", meaning that the queues of all Connections that
     * contain this Relationship are not full. This is generally used only when
     * dealing with Processors that use the
     * {@link org.apache.nifi.annotation.behavior.TriggerWhenAnyDestinationAvailable}
     * annotation.
     *
     * @param relationship to mark as available
     */
    void setRelationshipAvailable(Relationship relationship);

    /**
     * Indicates to the Framework that the given Relationship with the given
     * name should be considered "available", meaning that the queues of all
     * Connections that contain this Relationship are not full. This is
     * generally used only when dealing with Processors that use the
     * {@link org.apache.nifi.annotation.behavior.TriggerWhenAnyDestinationAvailable}
     *
     * @param relationshipName relationship name
     */
    void setRelationshipAvailable(String relationshipName);

    /**
     * Indicates to the Framework that the given Relationship should NOT be
     * considered "available", meaning that the queue of at least one Connection
     * that contain this Relationship is full. This is generally used only when
     * dealing with Processors that use the
     * {@link org.apache.nifi.annotation.behavior.TriggerWhenAnyDestinationAvailable}
     * annotation.
     *
     * @param relationship to mark as unavailable
     */
    void setRelationshipUnavailable(Relationship relationship);

    /**
     * Indicates to the Framework that the Relationship with the given name
     * should NOT be considered "available", meaning that the queue of at least
     * one Connection that contain this Relationship is full. This is generally
     * used only when dealing with Processors that use the
     * {@link org.apache.nifi.annotation.behavior.TriggerWhenAnyDestinationAvailable}
     *
     * @param relationshipName name of relationship.
     */
    void setRelationshipUnavailable(String relationshipName);

    /**
     * Indicates to the framework that the configured processor has one or more
     * incoming connections.
     *
     * @param hasIncomingConnection whether or not the configured processor should behave as though it has an incoming connection
     */
    void setIncomingConnection(boolean hasIncomingConnection);

    /**
     * Indicates to the framework that the configured processor has one or more incoming
     * connections for which the processor is not also the source.
     *
     * @param hasNonLoopConnection whether or not the configured processor should behave as though it has a non-looping incoming connection
     */
    void setNonLoopConnection(boolean hasNonLoopConnection);

    /**
     * Indicates to the Framework that the configured processor has a connection for the given Relationship.
     *
     * @param relationship that has a connection
     */
    void addConnection(Relationship relationship);

    /**
     * Indicates to the Framework that the configured processor has a connection for the
     * Relationship with the given name.
     *
     * @param relationshipName name of relationship that has a connection
     */
    void addConnection(String relationshipName);

    /**
     * Removes the connection for the given Relationship from the configured processor.
     *
     * @param relationship to remove
     */
    void removeConnection(Relationship relationship);

    /**
     * Removes the connection for the relationship with the given name from the configured processor.
     *
     * @param relationshipName name of the relationship to remove
     */
    void removeConnection(String relationshipName);

    /**
     * Adds the given {@link ControllerService} to this TestRunner so that the
     * configured Processor can access it using the given
     * <code>identifier</code>. The ControllerService is not expected to be
     * initialized, as the framework will create the appropriate
     * {@link org.apache.nifi.controller.ControllerServiceInitializationContext ControllerServiceInitializationContext}
     * and initialize the ControllerService with no specified properties.
     *
     * This will call any method on the given Controller Service that is
     * annotated with the
     * {@link org.apache.nifi.annotation.lifecycle.OnAdded @OnAdded} annotation.
     *
     * @param identifier of service
     * @param service the service
     * @throws InitializationException ie
     */
    void addControllerService(String identifier, ControllerService service) throws InitializationException;

    /**
     * Adds the given {@link ControllerService} to this TestRunner so that the
     * configured Processor can access it using the given
     * <code>identifier</code>. The ControllerService is not expected to be
     * initialized, as the framework will create the appropriate
     * {@link org.apache.nifi.controller.ControllerServiceInitializationContext ControllerServiceInitializationContext}
     * and initialize the ControllerService with the given properties.
     *
     * This will call any method on the given Controller Service that is
     * annotated with the
     * {@link org.apache.nifi.annotation.lifecycle.OnAdded @OnAdded} annotation.
     *
     * @param identifier of service
     * @param service the service
     * @param properties service properties
     * @throws InitializationException ie
     */
    void addControllerService(String identifier, ControllerService service, Map<String, String> properties) throws InitializationException;

    /**
     * <p>
     * Marks the Controller Service as enabled so that it can be used by other
     * components.
     * </p>
     *
     * <p>
     * This method will result in calling any method in the Controller Service
     * that is annotated with the
     * {@link org.apache.nifi.annotation.lifecycle.OnEnabled @OnEnabled}
     * annotation.
     * </p>
     *
     * @param service the service to enable
     */
    void enableControllerService(ControllerService service);

    /**
     * <p>
     * Marks the Controller Service as disabled so that it cannot be used by
     * other components.
     * </p>
     *
     * <p>
     * This method will result in calling any method in the Controller Service
     * that is annotated with the
     * {@link org.apache.nifi.annotation.lifecycle.OnDisabled @OnDisabled}
     * annotation.
     * </p>
     *
     * @param service the service to disable
     */
    void disableControllerService(ControllerService service);

    /**
     * @param service the service
     * @return {@code true} if the given Controller Service is enabled,
     *         {@code false} if it is disabled
     *
     * @throws IllegalArgumentException if the given ControllerService is not
     *             known by this TestRunner (i.e., it has not been added via the
     *             {@link #addControllerService(String, ControllerService)} or
     *             {@link #addControllerService(String, ControllerService, Map)} method or
     *             if the Controller Service has been removed via the
     *             {@link #removeControllerService(ControllerService)} method.
     */
    boolean isControllerServiceEnabled(ControllerService service);

    /**
     * <p>
     * Removes the Controller Service from the TestRunner. This will call any
     * method on the ControllerService that is annotated with the
     * {@link org.apache.nifi.annotation.lifecycle.OnRemoved @OnRemoved}
     * annotation.
     * </p>
     *
     * @param service the service
     *
     * @throws IllegalStateException if the ControllerService is not disabled
     * @throws IllegalArgumentException if the given ControllerService is not
     *             known by this TestRunner (i.e., it has not been added via the
     *             {@link #addControllerService(String, ControllerService)} or
     *             {@link #addControllerService(String, ControllerService, Map)} method or
     *             if the Controller Service has been removed via the
     *             {@link #removeControllerService(ControllerService)} method.
     *
     */
    void removeControllerService(ControllerService service);

    /**
     * Sets the given property on the given ControllerService
     *
     * @param service to modify
     * @param property to modify
     * @param value value to use
     * @return result
     *
     * @throws IllegalStateException if the ControllerService is not disabled
     * @throws IllegalArgumentException if the given ControllerService is not
     *             known by this TestRunner (i.e., it has not been added via the
     *             {@link #addControllerService(String, ControllerService)} or
     *             {@link #addControllerService(String, ControllerService, Map)} method or
     *             if the Controller Service has been removed via the
     *             {@link #removeControllerService(ControllerService)} method.
     *
     */
    ValidationResult setProperty(ControllerService service, PropertyDescriptor property, String value);

    /**
     * Sets the given property on the given ControllerService
     *
     * @param service to modify
     * @param property to modify
     * @param value value to use
     * @return result
     *
     * @throws IllegalStateException if the ControllerService is not disabled
     * @throws IllegalArgumentException if the given ControllerService is not
     *             known by this TestRunner (i.e., it has not been added via the
     *             {@link #addControllerService(String, ControllerService)} or
     *             {@link #addControllerService(String, ControllerService, Map)} method or
     *             if the Controller Service has been removed via the
     *             {@link #removeControllerService(ControllerService)} method.
     *
     */
    ValidationResult setProperty(ControllerService service, PropertyDescriptor property, DescribedValue value);

    /**
     * Sets the property with the given name on the given ControllerService
     *
     * @param service to modify
     * @param propertyName to modify
     * @param value value to use
     * @return result
     *
     * @throws IllegalStateException if the ControllerService is not disabled
     * @throws IllegalArgumentException if the given ControllerService is not
     *             known by this TestRunner (i.e., it has not been added via the
     *             {@link #addControllerService(String, ControllerService)} or
     *             {@link #addControllerService(String, ControllerService, Map)} method or
     *             if the Controller Service has been removed via the
     *             {@link #removeControllerService(ControllerService)} method.
     *
     */
    ValidationResult setProperty(ControllerService service, String propertyName, String value);

    /**
     * Sets the annotation data of the given service to the provided annotation
     * data.
     *
     * @param service to modify
     * @param annotationData the data
     *
     * @throws IllegalStateException if the Controller Service is not disabled
     *
     * @throws IllegalArgumentException if the given ControllerService is not
     *             known by this TestRunner (i.e., it has not been added via the
     *             {@link #addControllerService(String, ControllerService)} or
     *             {@link #addControllerService(String, ControllerService, Map)} method or
     *             if the Controller Service has been removed via the
     *             {@link #removeControllerService(ControllerService)} method.
     */
    void setAnnotationData(ControllerService service, String annotationData);

    /**
     * @param identifier of controller service
     * @return the {@link ControllerService} that is registered with the given
     *         identifier, or <code>null</code> if no Controller Service exists with the
     *         given identifier
     */
    ControllerService getControllerService(String identifier);

    /**
     * Assert that the currently configured set of properties/annotation data
     * are valid for the given Controller Service.
     *
     * @param service the service to validate
     * @throws IllegalArgumentException if the given ControllerService is not
     *             known by this TestRunner (i.e., it has not been added via the
     *             {@link #addControllerService(String, ControllerService)} or
     *             {@link #addControllerService(String, ControllerService, Map)} method or
     *             if the Controller Service has been removed via the
     *             {@link #removeControllerService(ControllerService)} method.
     */
    void assertValid(ControllerService service);

    /**
     * Assert that the currently configured set of properties/annotation data
     * are NOT valid for the given Controller Service.
     *
     * @param service the service to validate
     * @throws IllegalArgumentException if the given ControllerService is not
     *             known by this TestRunner (i.e., it has not been added via the
     *             {@link #addControllerService(String, ControllerService)} or
     *             {@link #addControllerService(String, ControllerService, Map)} method or
     *             if the Controller Service has been removed via the
     *             {@link #removeControllerService(ControllerService)} method.
     *
     */
    void assertNotValid(ControllerService service);

    /**
     * @param <T> type of service
     * @param identifier identifier of service
     * @param serviceType type of service
     * @return the {@link ControllerService} that is registered with the given
     *         identifier, cast as the provided service type, or <code>null</code> if no
     *         Controller Service exists with the given identifier
     *
     * @throws ClassCastException if the identifier given is registered for a
     *             Controller Service but that Controller Service is not of the type
     *             specified
     */
    <T extends ControllerService> T getControllerService(String identifier, Class<T> serviceType);

    /**
     * Specifies whether or not the TestRunner will validate the use of
     * Expression Language. By default, the value is <code>true</code>, which
     * means that an Exception will be thrown if the Processor attempts to
     * obtain the configured value of a Property without calling
     * {@link org.apache.nifi.components.PropertyValue#evaluateAttributeExpressions evaluateAttributeExpressions}
     * on the Property Value or if
     * {@link org.apache.nifi.components.PropertyValue#evaluateAttributeExpressions evaluateAttributeExpressions}
     * is called but the PropertyDescriptor indicates that the Expression
     * Language is not supported.
     *
     * <p>
     * <b>See Also:
     * </b>{@link PropertyDescriptor.Builder#expressionLanguageSupported(ExpressionLanguageScope)}
     * </p>
     *
     * @param validate whether there is any need to validate the EL was used
     */
    void setValidateExpressionUsage(boolean validate);

    /**
     * Specifies whether the TestRunner will allow ProcessSession.commit() to be called.
     * By default, the value is <code>false</code>, meaning that any call to ProcessSession.commit() will throw
     * an Exception. See JavaDocs for {@link ProcessSession#commit()} for more information
     * @param allow whether to allow asynchronous session commits (i.e., calls to ProcessSession.commit())
     */
    void setAllowSynchronousSessionCommits(boolean allow);

    /**
     * Specifies whether the TestRunner will allow ProcessSession.read() multiple times for the same FlowFile while an InputStream is already open.
     * By default, the value is <code>false</code>, meaning that any call to ProcessSession.read() for a FlowFile already being read will throw
     * an Exception. See JavaDocs for {@link ProcessSession#read(FlowFile)} for more information
     * @param allow whether to allow recursive reads of a FlowFile (i.e., calls to ProcessSession.read())
     */
    void setAllowRecursiveReads(boolean allow);

    /**
     * Removes the {@link PropertyDescriptor} from the {@link ProcessContext},
     * effectively setting its value to null, or the property's default value, if it has one.
     *
     * @param descriptor of property to remove
     * @return <code>true</code> if removed, <code>false</code> if the property was not set
     */
    boolean removeProperty(PropertyDescriptor descriptor);

    /**
     * Removes the {@link PropertyDescriptor} from the ControllerService,
     * effectively setting its value to null, or the property's default value, if it has one.
     *
     * @param controllerService the controller service to remove the property from
     * @param descriptor of property to remove
     * @return <code>true</code> if removed, <code>false</code> if the property was not set
     */
    boolean removeProperty(ControllerService controllerService, PropertyDescriptor descriptor);

    /**
     * Removes the property from the {@link ProcessContext},
     * effectively setting its value to null, or the property's default value, if it has one.
     *
     * @param property name of the property to remove
     * @return <code>true</code> if removed, <code>false</code> if the property was not set
     */
    boolean removeProperty(String property);

    /**
     * Removes the {@link PropertyDescriptor} from the ControllerService,
     * effectively setting its value to null, or the property's default value, if it has one.
     *
     * @param controllerService the controller service to remove the property from
     * @param property name of the property to remove
     * @return <code>true</code> if removed, <code>false</code> if the property was not set
     */
    boolean removeProperty(ControllerService controllerService, String property);

    /**
     * Clears all set properties from the {@link ProcessContext}.
     */
    void clearProperties();

    /**
     * Returns a {@link List} of all {@link ProvenanceEventRecord}s that were
     * emitted by the Processor
     *
     * @return a List of all Provenance Events that were emitted by the
     *         Processor
     */
    List<ProvenanceEventRecord> getProvenanceEvents();

    /**
     * Clears the Provenance Events that have been emitted by the Processor
     */
    void clearProvenanceEvents();

    /**
     * Returns the {@link MockComponentLog} that is used by the Processor under test.
     * @return the logger
     */
    MockComponentLog getLogger();

    /**
     * Returns the {@link MockComponentLog} that is used by the specified controller service.
     *
     * @param identifier a controller service identifier
     * @return the logger
     */
    MockComponentLog getControllerServiceLogger(final String identifier);

    /**
     * @return the State Manager that is used to stored and retrieve state
     */
    MockStateManager getStateManager();

    /**
     * @param service the controller service of interest
     * @return the State Manager that is used to store and retrieve state for the given controller service
     */
    MockStateManager getStateManager(ControllerService service);

    /**
     * @param clustered Specify if this test emulates running in a clustered environment
     */
    void setClustered(boolean clustered);

    /**
     * @param isConfiguredForClustering Specify if this test emulates running in an environment where the expected
     *        cluster state equals with the argument.
     */
    void setIsConfiguredForClustering(boolean isConfiguredForClustering);

    /**
     * @param primaryNode Specify if this test emulates running as a primary node
     */
    void setPrimaryNode(boolean primaryNode);

    /**
     * @param isConnected Specify if this test emulates ongoing cluster connection
     */
    void setConnected(boolean isConnected);

    /**
     * Returns the current value of the variable with the given name
     *
     * @param name the name of the variable whose value should be returned.
     * @return the current value of the variable with the given name or <code>null</code> if no value is currently set
     *
     * @throws NullPointerException if the name is null
     */
    String getEnvironmentVariableValue(String name);

    /**
     * Sets the value of the variable with the given name to be the given value. This exposes the variable
     * for use by the Expression Language.
     *
     * NOTE - this method is only for testing purposes as we only support variables coming from environment
     * and system variables. This usage is to avoid the need to set env/sys variables in the tests.
     *
     * @param name the name of the variable to set
     * @param value the value of the variable
     *
     * @throws NullPointerException if either the name or the value is null
     */
    void setEnvironmentVariableValue(String name, String value);

    /**
     * Asserts that all FlowFiles meet all conditions.
     *
     * @param relationshipName relationship name
     * @param predicate conditions
     */
    void assertAllConditionsMet(final String relationshipName, Predicate<MockFlowFile> predicate);

    /**
     * Asserts that all FlowFiles meet all conditions.
     *
     * @param relationship relationship
     * @param predicate conditions
     */
    void assertAllConditionsMet(final Relationship relationship, Predicate<MockFlowFile> predicate);

    /**
     * By default, if {@link ProcessSession#read(FlowFile)} is called, the InputStream that is returned MUST be closed by
     * the processor under test or calls to {@link ProcessSession#commit()} will throw an Exception. This method allows
     * the developer to indicate explicitly that they do or do not want this functionality. The ProcessSession that is used
     * in the framework when running NiFi does not enforce this, as the framework itself tracks the InputStreams that it returns
     * and ensures that they are properly closed on session commit or rollback. However, it is considered a best practice for
     * Processors to close the streams themselves whenever they are no longer needed. There may be cases, however, where this
     * is not feasible or easy and this method provides developers the ability to indicate that by disabling enforcement so that
     * the framework will handle this.
     *
     * @param enforce <code>true</code> if calls to session.commit() should fail if the read streams are not properly closed.
     */
    void enforceReadStreamsClosed(boolean enforce);

    /**
     * Set the Run Schedule parameter (in milliseconds). If set, this will be the duration
     * between two calls of the onTrigger method.
     *
     * @param runSchedule Run schedule duration in milliseconds.
     */
     void setRunSchedule(long runSchedule);

    /**
     * Assert that provenance event was created with the specified event type.
     *
     * @param eventType Provenance event type
     */
     void assertProvenanceEvent(ProvenanceEventType eventType);

    /**
     * Causes the TestRunner to call the Processor's {@link Processor#migrateProperties(PropertyConfiguration)} method. The effects that are
     * caused by calling the method are applied, as they would be in a running NiFi instance. Unlike in a running NiFi instance, though, the
     * operations that were performed are captured so that they can be examined and assertions made about the migration that occurred.
     *
     * @return the results of migrating properties
     */
    PropertyMigrationResult migrateProperties();

    /**
     * Causes the TestRunner to call the Processor's {@link Processor#migrateRelationships(RelationshipConfiguration)} method. The effects that are
     * caused by calling the method are applied, as they would be in a running NiFi instance. Unlike in a running NiFi instance, though, the
     * operations that were performed are captured so that they can be examined and assertions made about the migration that occurred.
     *
     * @return the results of migrating relationships
     */
    RelationshipMigrationResult migrateRelationships();
}
