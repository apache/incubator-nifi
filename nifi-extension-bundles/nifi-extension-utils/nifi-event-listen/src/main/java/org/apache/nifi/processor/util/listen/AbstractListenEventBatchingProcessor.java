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
package org.apache.nifi.processor.util.listen;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.listen.event.Event;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.nifi.processor.util.listen.ListenerProperties.NETWORK_INTF_NAME;

/**
 * An abstract processor that extends from AbstractListenEventProcessor and adds common functionality for
 * batching events into a single FlowFile.
 *
 * @param <E> the type of Event
 */
public abstract class AbstractListenEventBatchingProcessor<E extends Event> extends AbstractListenEventProcessor<E> {

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            NETWORK_INTF_NAME,
            PORT,
            RECV_BUFFER_SIZE,
            MAX_MESSAGE_QUEUE_SIZE,
            MAX_SOCKET_BUFFER_SIZE,
            CHARSET,
            ListenerProperties.MAX_BATCH_SIZE,
            ListenerProperties.MESSAGE_DELIMITER
    );

    // it is only the array reference that is volatile - not the contents.
    protected volatile byte[] messageDemarcatorBytes;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        this.descriptors = Stream.concat(
                PROPERTY_DESCRIPTORS.stream(),
                getAdditionalProperties().stream()
        ).toList();

        relationships = Stream.concat(
                Stream.of(REL_SUCCESS),
                getAdditionalRelationships().stream()
        ).collect(Collectors.toUnmodifiableSet());
    }

    @Override
    @OnScheduled
    public void onScheduled(ProcessContext context) throws IOException {
        super.onScheduled(context);
        final String msgDemarcator = context.getProperty(ListenerProperties.MESSAGE_DELIMITER).getValue().replace("\\n", "\n").replace("\\r", "\r").replace("\\t", "\t");
        messageDemarcatorBytes = msgDemarcator.getBytes(charset);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final int maxBatchSize = context.getProperty(ListenerProperties.MAX_BATCH_SIZE).asInteger();
        final Map<String, FlowFileEventBatch> batches = getBatches(session, maxBatchSize, messageDemarcatorBytes);

        // if the size is 0 then there was nothing to process so return
        // we don't need to yield here because we have a long poll in side of getBatches
        if (batches.size() == 0) {
            return;
        }

        final List<E> allEvents = new ArrayList<>();

        for (Map.Entry<String, FlowFileEventBatch> entry : batches.entrySet()) {
            FlowFile flowFile = entry.getValue().getFlowFile();
            final List<E> events = entry.getValue().getEvents();

            if (flowFile.getSize() == 0L || events.size() == 0) {
                session.remove(flowFile);
                getLogger().debug("No data written to FlowFile from batch {}; removing FlowFile", entry.getKey());
                continue;
            }

            final Map<String, String> attributes = getAttributes(entry.getValue());
            flowFile = session.putAllAttributes(flowFile, attributes);

            getLogger().debug("Transferring {} to success", flowFile);
            session.transfer(flowFile, REL_SUCCESS);
            session.adjustCounter("FlowFiles Transferred to Success", 1L, false);

            // the sender and command will be the same for all events based on the batch key
            final String transitUri = getTransitUri(entry.getValue());
            session.getProvenanceReporter().receive(flowFile, transitUri);

            allEvents.addAll(events);
        }

        // let sub-classes take any additional actions
        postProcess(context, session, allEvents);
    }

    /**
     * Creates the attributes for the FlowFile of the given batch.
     *
     * @param batch the current batch
     * @return the Map of FlowFile attributes
     */
    protected abstract Map<String, String> getAttributes(final FlowFileEventBatch batch);

    /**
     * Creates the transit uri to be used when reporting a provenance receive event for the given batch.
     *
     * @param batch the current batch
     * @return the transit uri string
     */
    protected abstract String getTransitUri(final FlowFileEventBatch batch);

    /**
     * Called at the end of onTrigger to allow sub-classes to take post processing action on the events
     *
     * @param context the current context
     * @param session the current session
     * @param events the list of all events processed by the current execution of onTrigger
     */
    protected void postProcess(ProcessContext context, ProcessSession session, final List<E> events) {
        // empty implementation so sub-classes only have to override if necessary
    }

    /**
     * Batches together up to the batchSize events. Events are grouped together based on a batch key which
     * by default is the sender of the event, but can be overriden by sub-classes.
     *
     * This method will return when batchSize has been reached, or when no more events are available on the queue.
     *
     * @param session the current session
     * @param totalBatchSize the total number of events to process
     * @param messageDemarcatorBytes the demarcator to put between messages when writing to a FlowFile
     *
     * @return a Map from the batch key to the FlowFile and events for that batch, the size of events in all
     *              the batches will be <= batchSize
     */
    protected Map<String, FlowFileEventBatch> getBatches(final ProcessSession session, final int totalBatchSize,
                                                        final byte[] messageDemarcatorBytes) {

        final Map<String, FlowFileEventBatch> batches = new HashMap<>();
        for (int i = 0; i < totalBatchSize; i++) {
            final E event = getMessage(true, true, session);
            if (event == null) {
                break;
            }

            final String batchKey = getBatchKey(event);
            FlowFileEventBatch batch = batches.get(batchKey);

            // if we don't have a batch for this key then create a new one
            if (batch == null) {
                batch = new FlowFileEventBatch(session.create(), new ArrayList<>());
                batches.put(batchKey, batch);
            }

            // add the current event to the batch
            batch.getEvents().add(event);

            // append the event's data to the FlowFile, write the demarcator first if not on the first event
            final boolean writeDemarcator = (i > 0);
            try {
                final byte[] rawMessage = event.getData();
                FlowFile appendedFlowFile = session.append(batch.getFlowFile(), out -> {
                    if (writeDemarcator) {
                        out.write(messageDemarcatorBytes);
                    }

                    out.write(rawMessage);
                });

                // update the FlowFile reference in the batch object
                batch.setFlowFile(appendedFlowFile);

            } catch (final Exception e) {
                getLogger().error("Failed to write contents of the message to FlowFile due to {}; will re-queue message and try again", e.getMessage(), e);
                errorEvents.offer(event);
                break;
            }
        }

        return batches;
    }

    /**
     * @param event an event that was pulled off the queue
     *
     * @return a key to use for batching events together, by default this uses the sender of the
     *              event, but sub-classes should override this to batch by something else
     */
    protected String getBatchKey(final E event) {
        return event.getSender();
    }

    /**
     * Wrapper to hold a FlowFile and the events that have been appended to it.
     */
    protected final class FlowFileEventBatch {

        private FlowFile flowFile;
        private List<E> events;

        public FlowFileEventBatch(final FlowFile flowFile, final List<E> events) {
            this.flowFile = flowFile;
            this.events = events;
        }

        public FlowFile getFlowFile() {
            return flowFile;
        }

        public List<E> getEvents() {
            return events;
        }

        public void setFlowFile(FlowFile flowFile) {
            this.flowFile = flowFile;
        }
    }

}
