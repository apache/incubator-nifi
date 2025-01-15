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
package org.apache.nifi.processors.splunk;

import com.splunk.RequestMessage;
import com.splunk.ResponseMessage;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dto.splunk.EventIndexStatusRequest;
import org.apache.nifi.dto.splunk.EventIndexStatusResponse;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.nifi.scheduling.SchedulingStrategy;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"splunk", "logs", "http", "acknowledgement"})
@CapabilityDescription("Queries Splunk server in order to acquire the status of indexing acknowledgement.")
@ReadsAttributes({
        @ReadsAttribute(attribute = "splunk.acknowledgement.id", description = "The indexing acknowledgement id provided by Splunk."),
        @ReadsAttribute(attribute = "splunk.responded.at", description = "The time of the response of put request for Splunk.")})
@SeeAlso(PutSplunkHTTP.class)
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 min")
public class QuerySplunkIndexingStatus extends SplunkAPICall {
    private static final String ENDPOINT = "/services/collector/ack";

    static final Relationship RELATIONSHIP_ACKNOWLEDGED = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is transferred to this relationship when the acknowledgement was successful.")
            .build();

    static final Relationship RELATIONSHIP_UNACKNOWLEDGED = new Relationship.Builder()
            .name("unacknowledged")
            .description(
                    "A FlowFile is transferred to this relationship when the acknowledgement was not successful. " +
                    "This can happen when the acknowledgement did not happened within the time period set for Maximum Waiting Time. " +
                    "FlowFiles with acknowledgement id unknown for the Splunk server will be transferred to this relationship after the Maximum Waiting Time is reached.")
            .build();

    static final Relationship RELATIONSHIP_UNDETERMINED = new Relationship.Builder()
            .name("undetermined")
            .description(
                    "A FlowFile is transferred to this relationship when the acknowledgement state is not determined. " +
                    "FlowFiles transferred to this relationship might be penalized. " +
                    "This happens when Splunk returns with HTTP 200 but with false response for the acknowledgement id in the flow file attribute.")
            .build();

    static final Relationship RELATIONSHIP_FAILURE = new Relationship.Builder()
            .name("failure")
            .description(
                    "A FlowFile is transferred to this relationship when the acknowledgement was not successful due to errors during the communication. " +
                    "FlowFiles are timing out or unknown by the Splunk server will transferred to \"undetermined\" relationship.")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            RELATIONSHIP_ACKNOWLEDGED,
            RELATIONSHIP_UNACKNOWLEDGED,
            RELATIONSHIP_UNDETERMINED,
            RELATIONSHIP_FAILURE
    );

    static final PropertyDescriptor TTL = new PropertyDescriptor.Builder()
            .name("ttl")
            .displayName("Maximum Waiting Time")
            .description(
                    "The maximum time the processor tries to acquire acknowledgement confirmation for an index, from the point of registration. " +
                    "After the given amount of time, the processor considers the index as not acknowledged and transfers the FlowFile to the \"unacknowledged\" relationship.")
            .defaultValue("1 hour")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final PropertyDescriptor MAX_QUERY_SIZE = new PropertyDescriptor.Builder()
            .name("max-query-size")
            .displayName("Maximum Query Size")
            .description(
                    "The maximum number of acknowledgement identifiers the outgoing query contains in one batch. " +
                    "It is recommended not to set it too low in order to reduce network communication.")
            .defaultValue("10000")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Stream.concat(
            getCommonPropertyDescriptors().stream(),
            Stream.of(
                TTL,
                MAX_QUERY_SIZE
            )
    ).toList();

    private volatile Integer maxQuerySize;
    private volatile Integer ttl;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        super.onScheduled(context);
        maxQuerySize = context.getProperty(MAX_QUERY_SIZE).asInteger();
        ttl = context.getProperty(TTL).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
    }

    @OnStopped
    public void onStopped() {
        super.onStopped();
        maxQuerySize = null;
        ttl = null;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final RequestMessage requestMessage;
        final List<FlowFile> flowFiles = session.get(maxQuerySize);

        if (flowFiles.isEmpty()) {
            return;
        }

        final long currentTime = System.currentTimeMillis();
        final Map<Long, FlowFile> undetermined = new HashMap<>();

        for (final FlowFile flowFile : flowFiles)  {
            final Optional<Long> sentAt = extractLong(flowFile.getAttribute(SplunkAPICall.RESPONDED_AT_ATTRIBUTE));
            final Optional<Long> ackId = extractLong(flowFile.getAttribute(SplunkAPICall.ACKNOWLEDGEMENT_ID_ATTRIBUTE));

            if (!sentAt.isPresent() || !ackId.isPresent()) {
                getLogger().error("Flow file ({}) attributes {} and {} are expected to be set using 64-bit integer values!",
                        flowFile.getId(), SplunkAPICall.RESPONDED_AT_ATTRIBUTE, SplunkAPICall.ACKNOWLEDGEMENT_ID_ATTRIBUTE);
                session.transfer(flowFile, RELATIONSHIP_FAILURE);
            } else {
                undetermined.put(ackId.get(), flowFile);
            }
        }

        if (undetermined.isEmpty()) {
            getLogger().debug("There was no eligible flow file to send request to Splunk.");
            return;
        }

        try {
            requestMessage = createRequestMessage(undetermined);
        } catch (final IOException e) {
            getLogger().error("Could not prepare Splunk request!", e);
            session.transfer(undetermined.values(), RELATIONSHIP_FAILURE);
            return;
        }

        try {
            final ResponseMessage responseMessage = call(ENDPOINT, requestMessage);

            if (responseMessage.getStatus() == 200) {
                final EventIndexStatusResponse splunkResponse = unmarshallResult(responseMessage.getContent(), EventIndexStatusResponse.class);

                splunkResponse.getAcks().forEach((flowFileId, isAcknowledged) -> {
                    final FlowFile toTransfer = undetermined.get(flowFileId);
                    if (isAcknowledged) {
                        session.transfer(toTransfer, RELATIONSHIP_ACKNOWLEDGED);
                    } else {
                        final Long sentAt = extractLong(toTransfer.getAttribute(SplunkAPICall.RESPONDED_AT_ATTRIBUTE)).get();
                        if (sentAt + ttl < currentTime) {
                            session.transfer(toTransfer, RELATIONSHIP_UNACKNOWLEDGED);
                        } else {
                            session.penalize(toTransfer);
                            session.transfer(toTransfer, RELATIONSHIP_UNDETERMINED);
                        }
                    }
                });
            } else {
                getLogger().error("Query index status was not successful because of ({}) {}", responseMessage.getStatus(), responseMessage.getContent());
                context.yield();
                session.transfer(undetermined.values(), RELATIONSHIP_UNDETERMINED);
            }
        } catch (final Exception e) {
            getLogger().error("Error during communication with Splunk server", e);
            session.transfer(undetermined.values(), RELATIONSHIP_FAILURE);
        }
    }

    private RequestMessage createRequestMessage(Map<Long, FlowFile> undetermined) throws IOException {
        final RequestMessage requestMessage = new RequestMessage("POST");
        requestMessage.getHeader().put("Content-Type", "application/json");
        requestMessage.setContent(generateContent(undetermined));
        return requestMessage;
    }

    private String generateContent(final Map<Long, FlowFile> undetermined) throws IOException {
        final EventIndexStatusRequest splunkRequest = new EventIndexStatusRequest();
        splunkRequest.setAcks(new ArrayList<>(undetermined.keySet()));
        return marshalRequest(splunkRequest);
    }

    private static Optional<Long> extractLong(final String value) {
        try {
            return Optional.ofNullable(value).map(Long::valueOf);
        } catch (final NumberFormatException e) {
            return Optional.empty();
        }
    }
}
