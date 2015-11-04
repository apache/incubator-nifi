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
package org.apache.nifi.processors.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.BufferedInputStream;
import org.apache.nifi.stream.io.ByteArrayOutputStream;
import org.apache.nifi.stream.io.ByteCountingInputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.stream.io.util.NonThreadSafeCircularBuffer;
import org.apache.nifi.util.LongHolder;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import scala.actors.threadpool.Arrays;

@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({ "Apache", "Kafka", "Put", "Send", "Message", "PubSub" })
@CapabilityDescription("Sends the contents of a FlowFile as a message to Apache Kafka")
public class PutKafka extends AbstractProcessor {

    private static final String SINGLE_BROKER_REGEX = ".*?\\:\\d{3,5}";
    private static final String BROKER_REGEX = SINGLE_BROKER_REGEX + "(?:,\\s*" + SINGLE_BROKER_REGEX + ")*";

    public static final AllowableValue DELIVERY_REPLICATED = new AllowableValue("-1", "Guarantee Replicated Delivery", "FlowFile will be routed to"
        + " failure unless the message is replicated to the appropriate number of Kafka Nodes according to the Topic configuration");
    public static final AllowableValue DELIVERY_ONE_NODE = new AllowableValue("1", "Guarantee Single Node Delivery", "FlowFile will be routed"
        + " to success if the message is received by a single Kafka node, whether or not it is replicated. This is faster than"
        + " <Guarantee Replicated Delivery> but can result in data loss if a Kafka node crashes");
    public static final AllowableValue DELIVERY_BEST_EFFORT = new AllowableValue("0", "Best Effort", "FlowFile will be routed to success after"
        + " successfully writing the content to a Kafka node, without waiting for a response. This provides the best performance but may result"
        + " in data loss.");

    /**
     * AllowableValue for a Producer Type that synchronously sends messages to Kafka
     */
    public static final AllowableValue PRODUCTER_TYPE_SYNCHRONOUS = new AllowableValue("sync", "Synchronous", "Send FlowFiles to Kafka immediately.");

    /**
     * AllowableValue for a Producer Type that asynchronously sends messages to Kafka
     */
    public static final AllowableValue PRODUCTER_TYPE_ASYNCHRONOUS = new AllowableValue("async", "Asynchronous", "Batch messages before sending them to Kafka."
        + " While this will improve throughput, it opens the possibility that a failure on the client machine will drop unsent data.");

    /**
     * AllowableValue for sending messages to Kafka without compression
     */
    public static final AllowableValue COMPRESSION_CODEC_NONE = new AllowableValue("none", "None", "Compression will not be used for any topic.");

    /**
     * AllowableValue for sending messages to Kafka with GZIP compression
     */
    public static final AllowableValue COMPRESSION_CODEC_GZIP = new AllowableValue("gzip", "GZIP", "Compress messages using GZIP");

    /**
     * AllowableValue for sending messages to Kafka with Snappy compression
     */
    public static final AllowableValue COMPRESSION_CODEC_SNAPPY = new AllowableValue("snappy", "Snappy", "Compress messages using Snappy");


    public static final PropertyDescriptor SEED_BROKERS = new PropertyDescriptor.Builder()
        .name("Known Brokers")
        .description("A comma-separated list of known Kafka Brokers in the format <host>:<port>")
        .required(true)
        .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile(BROKER_REGEX)))
        .expressionLanguageSupported(false)
        .build();
    public static final PropertyDescriptor TOPIC = new PropertyDescriptor.Builder()
        .name("Topic Name")
        .description("The Kafka Topic of interest")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(true)
        .build();
    public static final PropertyDescriptor KEY = new PropertyDescriptor.Builder()
        .name("Kafka Key")
        .description("The Key to use for the Message")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(true)
        .build();
    public static final PropertyDescriptor DELIVERY_GUARANTEE = new PropertyDescriptor.Builder()
        .name("Delivery Guarantee")
        .description("Specifies the requirement for guaranteeing that a message is sent to Kafka")
        .required(true)
        .expressionLanguageSupported(false)
        .allowableValues(DELIVERY_BEST_EFFORT, DELIVERY_ONE_NODE, DELIVERY_REPLICATED)
        .defaultValue(DELIVERY_BEST_EFFORT.getValue())
        .build();
    public static final PropertyDescriptor MESSAGE_DELIMITER = new PropertyDescriptor.Builder()
        .name("Message Delimiter")
        .description("Specifies the delimiter to use for splitting apart multiple messages within a single FlowFile. "
            + "If not specified, the entire content of the FlowFile will be used as a single message. "
            + "If specified, the contents of the FlowFile will be split on this delimiter and each section "
            + "sent as a separate Kafka message.")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(true)
        .build();
    public static final PropertyDescriptor MAX_BUFFER_SIZE = new PropertyDescriptor.Builder()
        .name("Max Buffer Size")
        .description("The maximum amount of data to buffer in memory before sending to Kafka")
        .required(true)
        .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
        .expressionLanguageSupported(false)
        .defaultValue("1 MB")
        .build();
    public static final PropertyDescriptor TIMEOUT = new PropertyDescriptor.Builder()
        .name("Communications Timeout")
        .description("The amount of time to wait for a response from Kafka before determining that there is a communications error")
        .required(true)
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .expressionLanguageSupported(false)
        .defaultValue("30 secs")
        .build();
    public static final PropertyDescriptor CLIENT_NAME = new PropertyDescriptor.Builder()
        .name("Client Name")
        .description("Client Name to use when communicating with Kafka")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(false)
        .build();
    public static final PropertyDescriptor PRODUCER_TYPE = new PropertyDescriptor.Builder()
        .name("Producer Type")
        .description("This parameter specifies whether the messages are sent asynchronously in a background thread.")
        .required(true)
        .allowableValues(PRODUCTER_TYPE_SYNCHRONOUS, PRODUCTER_TYPE_ASYNCHRONOUS)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(false)
        .defaultValue(PRODUCTER_TYPE_SYNCHRONOUS.getValue())
        .build();
    public static final PropertyDescriptor BATCH_NUM_MESSAGES = new PropertyDescriptor.Builder()
        .name("Async Batch Size")
        .description("Used only if Producer Type is set to \"" + PRODUCTER_TYPE_ASYNCHRONOUS.getDisplayName() + "\"."
            + " The number of messages to send in one batch when using " + PRODUCTER_TYPE_ASYNCHRONOUS.getDisplayName() + " mode."
            + " The producer will wait until either this number of messages are ready"
            + " to send or \"Queue Buffering Max Time\" is reached.")
        .required(true)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .defaultValue("200")
        .build();
    public static final PropertyDescriptor QUEUE_BUFFERING_MAX = new PropertyDescriptor.Builder()
        .name("Queue Buffering Max Time")
        .description("Used only if Producer Type is set to \"" + PRODUCTER_TYPE_ASYNCHRONOUS.getDisplayName() + "\"."
            + " Maximum time to buffer data when using " + PRODUCTER_TYPE_ASYNCHRONOUS.getDisplayName() + " mode. For example a setting of 100 ms"
            + " will try to batch together 100ms of messages to send at once. This will improve"
            + " throughput but adds message delivery latency due to the buffering.")
        .required(true)
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .defaultValue("5 secs")
        .build();
    public static final PropertyDescriptor QUEUE_BUFFERING_MAX_MESSAGES = new PropertyDescriptor.Builder()
        .name("Queue Buffer Max Count")
        .description("Used only if Producer Type is set to \"" + PRODUCTER_TYPE_ASYNCHRONOUS.getDisplayName() + "\"."
            + " The maximum number of unsent messages that can be queued up in the producer when"
            + " using " + PRODUCTER_TYPE_ASYNCHRONOUS.getDisplayName() + " mode before either the producer must be blocked or data must be dropped.")
        .required(true)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .defaultValue("10000")
        .build();
    public static final PropertyDescriptor QUEUE_ENQUEUE_TIMEOUT = new PropertyDescriptor.Builder()
        .name("Queue Enqueue Timeout")
        .description("Used only if Producer Type is set to \"" + PRODUCTER_TYPE_ASYNCHRONOUS.getDisplayName() + "\"."
            + " The amount of time to block before dropping messages when running in "
            + PRODUCTER_TYPE_ASYNCHRONOUS.getDisplayName() + " mode"
            + " and the buffer has reached the \"Queue Buffer Max Count\". If set to 0, events will"
            + " be enqueued immediately or dropped if the queue is full (the producer send call will"
            + " never block). If not set, the producer will block indefinitely and never willingly"
            + " drop a send.")
        .required(false)
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .build();
    public static final PropertyDescriptor COMPRESSION_CODEC = new PropertyDescriptor.Builder()
        .name("Compression Codec")
        .description("This parameter allows you to specify the compression codec for all"
            + " data generated by this producer.")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .allowableValues(COMPRESSION_CODEC_NONE, COMPRESSION_CODEC_GZIP, COMPRESSION_CODEC_SNAPPY)
        .defaultValue(COMPRESSION_CODEC_NONE.getValue())
        .build();
    public static final PropertyDescriptor COMPRESSED_TOPICS = new PropertyDescriptor.Builder()
        .name("Compressed Topics")
        .description("This parameter allows you to set whether compression should be turned on"
            + " for particular topics. If the compression codec is anything other than"
            + " \"" + COMPRESSION_CODEC_NONE.getDisplayName() + "\", enable compression only for specified topics if any."
            + " If the list of compressed topics is empty, then enable the specified"
            + " compression codec for all topics. If the compression codec is " + COMPRESSION_CODEC_NONE.getDisplayName() + ","
            + " compression is disabled for all topics")
        .required(false)
        .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("Any FlowFile that is successfully sent to Kafka will be routed to this Relationship")
        .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("Any FlowFile that cannot be sent to Kafka will be routed to this Relationship")
        .build();

    private final BlockingQueue<Producer<byte[], byte[]>> producers = new LinkedBlockingQueue<>();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final PropertyDescriptor clientName = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(CLIENT_NAME)
            .defaultValue("NiFi-" + getIdentifier())
            .build();

        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(SEED_BROKERS);
        props.add(TOPIC);
        props.add(KEY);
        props.add(DELIVERY_GUARANTEE);
        props.add(MESSAGE_DELIMITER);
        props.add(MAX_BUFFER_SIZE);
        props.add(TIMEOUT);
        props.add(PRODUCER_TYPE);
        props.add(BATCH_NUM_MESSAGES);
        props.add(QUEUE_BUFFERING_MAX_MESSAGES);
        props.add(QUEUE_BUFFERING_MAX);
        props.add(QUEUE_ENQUEUE_TIMEOUT);
        props.add(COMPRESSION_CODEC);
        props.add(COMPRESSED_TOPICS);
        props.add(clientName);
        return props;
    }

    @Override
    public Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> errors = new ArrayList<>(super.customValidate(context));

        final Integer batchMessages = context.getProperty(BATCH_NUM_MESSAGES).asInteger();
        final Integer bufferMaxMessages = context.getProperty(QUEUE_BUFFERING_MAX_MESSAGES).asInteger();

        if (batchMessages > bufferMaxMessages) {
            errors.add(new ValidationResult.Builder().subject("Batch Size, Queue Buffer").valid(false)
                .explanation("Batch Size (" + batchMessages + ") must be equal to or less than the Queue Buffer Max Count (" + bufferMaxMessages + ")").build());
        }

        return errors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>(1);
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        return relationships;
    }

    @OnStopped
    public void closeProducers() {
        Producer<byte[], byte[]> producer;

        while ((producer = producers.poll()) != null) {
            producer.close();
        }
    }

    protected ProducerConfig createConfig(final ProcessContext context) {
        final String brokers = context.getProperty(SEED_BROKERS).getValue();

        final Properties properties = new Properties();
        properties.setProperty("metadata.broker.list", brokers);
        properties.setProperty("request.required.acks", context.getProperty(DELIVERY_GUARANTEE).getValue());
        properties.setProperty("client.id", context.getProperty(CLIENT_NAME).getValue());
        properties.setProperty("request.timeout.ms", String.valueOf(context.getProperty(TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).longValue()));

        properties.setProperty("message.send.max.retries", "1");
        properties.setProperty("producer.type", context.getProperty(PRODUCER_TYPE).getValue());
        properties.setProperty("batch.num.messages", context.getProperty(BATCH_NUM_MESSAGES).getValue());

        final Long queueBufferingMillis = context.getProperty(QUEUE_BUFFERING_MAX).asTimePeriod(TimeUnit.MILLISECONDS);
        if (queueBufferingMillis != null) {
            properties.setProperty("queue.buffering.max.ms", String.valueOf(queueBufferingMillis));
        }
        properties.setProperty("queue.buffering.max.messages", context.getProperty(QUEUE_BUFFERING_MAX_MESSAGES).getValue());

        final Long queueEnqueueTimeoutMillis = context.getProperty(QUEUE_ENQUEUE_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS);
        if (queueEnqueueTimeoutMillis != null) {
            properties.setProperty("queue.enqueue.timeout.ms", String.valueOf(queueEnqueueTimeoutMillis));
        }

        final String compressionCodec = context.getProperty(COMPRESSION_CODEC).getValue();
        properties.setProperty("compression.codec", compressionCodec);

        final String compressedTopics = context.getProperty(COMPRESSED_TOPICS).getValue();
        if (compressedTopics != null) {
            properties.setProperty("compressed.topics", compressedTopics);
        }

        return new ProducerConfig(properties);
    }

    protected Producer<byte[], byte[]> createProducer(final ProcessContext context) {
        return new Producer<>(createConfig(context));
    }

    private Producer<byte[], byte[]> borrowProducer(final ProcessContext context) {
        final Producer<byte[], byte[]> producer = producers.poll();
        return producer == null ? createProducer(context) : producer;
    }

    private void returnProducer(final Producer<byte[], byte[]> producer) {
        producers.offer(producer);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final long start = System.nanoTime();
        final String topic = context.getProperty(TOPIC).evaluateAttributeExpressions(flowFile).getValue();
        final String key = context.getProperty(KEY).evaluateAttributeExpressions(flowFile).getValue();
        final byte[] keyBytes = key == null ? null : key.getBytes(StandardCharsets.UTF_8);
        String delimiter = context.getProperty(MESSAGE_DELIMITER).evaluateAttributeExpressions(flowFile).getValue();
        if (delimiter != null) {
            delimiter = delimiter.replace("\\n", "\n").replace("\\r", "\r").replace("\\t", "\t");
        }

        final long maxBufferSize = context.getProperty(MAX_BUFFER_SIZE).asDataSize(DataUnit.B).longValue();
        final Producer<byte[], byte[]> producer = borrowProducer(context);

        if (delimiter == null) {
            // Send the entire FlowFile as a single message.
            final byte[] value = new byte[(int) flowFile.getSize()];
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    StreamUtils.fillBuffer(in, value);
                }
            });

            boolean error = false;
            try {
                final KeyedMessage<byte[], byte[]> message;
                if (key == null) {
                    message = new KeyedMessage<>(topic, value);
                } else {
                    message = new KeyedMessage<>(topic, keyBytes, value);
                }

                producer.send(message);
                final long nanos = System.nanoTime() - start;

                session.getProvenanceReporter().send(flowFile, "kafka://" + topic);
                session.transfer(flowFile, REL_SUCCESS);
                getLogger().info("Successfully sent {} to Kafka in {} millis", new Object[] { flowFile, TimeUnit.NANOSECONDS.toMillis(nanos) });
            } catch (final Exception e) {
                getLogger().error("Failed to send {} to Kafka due to {}; routing to failure", new Object[] { flowFile, e });
                session.transfer(session.penalize(flowFile), REL_FAILURE);
                error = true;
            } finally {
                if (error) {
                    producer.close();
                } else {
                    returnProducer(producer);
                }
            }
        } else {
            final byte[] delimiterBytes = delimiter.getBytes(StandardCharsets.UTF_8);

            // The NonThreadSafeCircularBuffer allows us to add a byte from the stream one at a time and see
            // if it matches some pattern. We can use this to search for the delimiter as we read through
            // the stream of bytes in the FlowFile
            final NonThreadSafeCircularBuffer buffer = new NonThreadSafeCircularBuffer(delimiterBytes);

            boolean error = false;
            final LongHolder lastMessageOffset = new LongHolder(0L);
            final LongHolder messagesSent = new LongHolder(0L);

            try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                session.read(flowFile, new InputStreamCallback() {
                    @Override
                    public void process(final InputStream rawIn) throws IOException {
                        byte[] data = null; // contents of a single message

                        boolean streamFinished = false;

                        final List<KeyedMessage<byte[], byte[]>> messages = new ArrayList<>(); // batch to send
                        long messageBytes = 0L; // size of messages in the 'messages' list

                        int nextByte;
                        try (final InputStream bufferedIn = new BufferedInputStream(rawIn);
                            final ByteCountingInputStream in = new ByteCountingInputStream(bufferedIn)) {

                            // read until we're out of data.
                            while (!streamFinished) {
                                nextByte = in.read();

                                if (nextByte > -1) {
                                    baos.write(nextByte);
                                }

                                if (nextByte == -1) {
                                    // we ran out of data. This message is complete.
                                    data = baos.toByteArray();
                                    streamFinished = true;
                                } else if (buffer.addAndCompare((byte) nextByte)) {
                                    // we matched our delimiter. This message is complete. We want all of the bytes from the
                                    // underlying BAOS exception for the last 'delimiterBytes.length' bytes because we don't want
                                    // the delimiter itself to be sent.
                                    data = Arrays.copyOfRange(baos.getUnderlyingBuffer(), 0, baos.size() - delimiterBytes.length);
                                }

                                if (data != null) {
                                    // If the message has no data, ignore it.
                                    if (data.length != 0) {
                                        // either we ran out of data or we reached the end of the message.
                                        // Either way, create the message because it's ready to send.
                                        final KeyedMessage<byte[], byte[]> message;
                                        if (key == null) {
                                            message = new KeyedMessage<>(topic, data);
                                        } else {
                                            message = new KeyedMessage<>(topic, keyBytes, data);
                                        }

                                        // Add the message to the list of messages ready to send. If we've reached our
                                        // threshold of how many we're willing to send (or if we're out of data), go ahead
                                        // and send the whole List.
                                        messages.add(message);
                                        messageBytes += data.length;
                                        if (messageBytes >= maxBufferSize || streamFinished) {
                                            // send the messages, then reset our state.
                                            try {
                                                producer.send(messages);
                                            } catch (final Exception e) {
                                                // we wrap the general exception in ProcessException because we want to separate
                                                // failures in sending messages from general Exceptions that would indicate bugs
                                                // in the Processor. Failure to send a message should be handled appropriately, but
                                                // we don't want to catch the general Exception or RuntimeException in order to catch
                                                // failures from Kafka's Producer.
                                                throw new ProcessException("Failed to send messages to Kafka", e);
                                            }

                                            messagesSent.addAndGet(messages.size());    // count number of messages sent

                                            // reset state
                                            messages.clear();
                                            messageBytes = 0;

                                            // We've successfully sent a batch of messages. Keep track of the byte offset in the
                                            // FlowFile of the last successfully sent message. This way, if the messages cannot
                                            // all be successfully sent, we know where to split off the data. This allows us to then
                                            // split off the first X number of bytes and send to 'success' and then split off the rest
                                            // and send them to 'failure'.
                                            lastMessageOffset.set(in.getBytesConsumed());
                                        }
                                    }
                                    // reset BAOS so that we can start a new message.
                                    baos.reset();
                                    data = null;

                                }
                            }

                            // If there are messages left, send them
                            if (!messages.isEmpty()) {
                                try {
                                    messagesSent.addAndGet(messages.size());    // add count of messages
                                    producer.send(messages);
                                } catch (final Exception e) {
                                    throw new ProcessException("Failed to send messages to Kafka", e);
                                }
                            }
                        }
                    }
                });

                final long nanos = System.nanoTime() - start;
                session.getProvenanceReporter().send(flowFile, "kafka://" + topic, "Sent " + messagesSent.get() + " messages");
                session.transfer(flowFile, REL_SUCCESS);
                getLogger().info("Successfully sent {} messages to Kafka for {} in {} millis", new Object[] { messagesSent.get(), flowFile, TimeUnit.NANOSECONDS.toMillis(nanos) });
            } catch (final ProcessException pe) {
                error = true;

                // There was a failure sending messages to Kafka. Iff the lastMessageOffset is 0, then all of them failed and we can
                // just route the FlowFile to failure. Otherwise, some messages were successful, so split them off and send them to
                // 'success' while we send the others to 'failure'.
                final long offset = lastMessageOffset.get();
                if (offset == 0L) {
                    // all of the messages failed to send. Route FlowFile to failure
                    getLogger().error("Failed to send {} to Kafka due to {}; routing to fialure", new Object[] { flowFile, pe.getCause() });
                    session.transfer(session.penalize(flowFile), REL_FAILURE);
                } else {
                    // Some of the messages were sent successfully. We want to split off the successful messages from the failed messages.
                    final FlowFile successfulMessages = session.clone(flowFile, 0L, offset);
                    final FlowFile failedMessages = session.clone(flowFile, offset, flowFile.getSize() - offset);

                    getLogger().error("Successfully sent {} of the messages from {} but then failed to send the rest. Original FlowFile split into"
                        + " two: {} routed to 'success', {} routed to 'failure'. Failure was due to {}", new Object[] {
                        messagesSent.get(), flowFile, successfulMessages, failedMessages, pe.getCause() });

                    session.transfer(successfulMessages, REL_SUCCESS);
                    session.transfer(session.penalize(failedMessages), REL_FAILURE);
                    session.remove(flowFile);
                    session.getProvenanceReporter().send(successfulMessages, "kafka://" + topic);
                }
            } finally {
                if (error) {
                    producer.close();
                } else {
                    returnProducer(producer);
                }
            }

        }
    }

}
