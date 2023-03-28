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
package org.apache.nifi.jms.processors.strategy.consumer.record;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.jms.processors.strategy.consumer.AttributeSupplier;
import org.apache.nifi.jms.processors.strategy.consumer.FlowFileWriter;
import org.apache.nifi.jms.processors.strategy.consumer.FlowFileWriterCallback;
import org.apache.nifi.jms.processors.strategy.consumer.Serializer;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SchemaValidationException;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.nifi.jms.processors.strategy.consumer.record.OutputStrategy.USE_APPENDER;
import static org.apache.nifi.jms.processors.strategy.consumer.record.OutputStrategy.USE_VALUE;
import static org.apache.nifi.jms.processors.strategy.consumer.record.OutputStrategy.USE_WRAPPER;

public class RecordWriter<T> implements FlowFileWriter<T> {

    private final static String RECORD_COUNT_KEY = "record.count";

    private final RecordReaderFactory readerFactory;
    private final RecordSetWriterFactory writerFactory;
    private final Serializer<T> serializer;
    private final AttributeSupplier<T> attributeSupplier;
    private final OutputStrategy outputStrategy;
    private final ComponentLog logger;

    public RecordWriter(RecordReaderFactory readerFactory,
                        RecordSetWriterFactory writerFactory,
                        Serializer<T> serializer,
                        AttributeSupplier<T> attributeSupplier,
                        OutputStrategy outputStrategy,
                        ComponentLog logger) {
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
        this.serializer = serializer;
        this.attributeSupplier = attributeSupplier;
        this.outputStrategy = outputStrategy;
        this.logger = logger;
    }

    @Override
    public void write(ProcessSession session, List<T> messages, FlowFileWriterCallback<T> flowFileWriterCallback) {
        FlowFile flowFile = session.create();

        final Map<String, String> attributes = new HashMap<>();
        final AtomicInteger recordCount = new AtomicInteger();

        final List<T> processedMessages = new ArrayList<>();
        final List<T> failedMessages = new ArrayList<>();

        RecordSetWriter writer = null;
        boolean isWriterInitialized = false;

        try {
            for (T message : messages) {
                if (message == null) {
                    break;
                }

                final byte[] recordBytes = serializer.serialize(message);
                try (final InputStream in = new ByteArrayInputStream(recordBytes)) {
                    final RecordReader reader;

                    // parse incoming message which may contain multiple messages
                    try {
                        reader = readerFactory.createRecordReader(attributes, in, recordBytes.length, logger);
                    } catch (final IOException ioe) {
                        logger.error("Failed to parse message due to comms failure. Will roll back session and try again momentarily.");
                        flowFileWriterCallback.onFailure(flowFile, processedMessages, failedMessages, ioe);
                        closeWriter(writer);
                        return;
                    } catch (final Exception e) {
                        logger.error("Failed to parse message, sending to the parse failure relationship", e);
                        failedMessages.add(message);
                        flowFileWriterCallback.onParseFailure(flowFile, message, e);
                        continue;
                    }

                    // write messages as records into FlowFile
                    try {
                        Record record;
                        while ((record = reader.nextRecord()) != null) {

                            if (attributeSupplier != null && !outputStrategy.equals(USE_VALUE)) {
                                final Map<String, String> additionalAttributes = attributeSupplier.supply(message);
                                if (outputStrategy.equals(USE_APPENDER)) {
                                    record = RecordUtils.append(record, additionalAttributes, "_");
                                } else if (outputStrategy.equals(USE_WRAPPER)){
                                    record = RecordUtils.wrap(record, additionalAttributes, "_");
                                }
                            }

                            if (!isWriterInitialized) {
                                final RecordSchema recordSchema = record.getSchema();
                                final OutputStream rawOut = session.write(flowFile);

                                RecordSchema writeSchema;
                                try {
                                    writeSchema = writerFactory.getSchema(flowFile.getAttributes(), recordSchema);
                                } catch (final Exception e) {
                                    logger.error("Failed to obtain Schema for FlowFile, sending to the parse failure relationship", e);
                                    failedMessages.add(message);
                                    flowFileWriterCallback.onParseFailure(flowFile, message, e);
                                    continue;
                                }

                                writer = writerFactory.createWriter(logger, writeSchema, rawOut, flowFile);
                                writer.beginRecordSet();
                            }

                            try {
                                writer.write(record);
                                isWriterInitialized = true;
                                processedMessages.add(message);
                            } catch (final RuntimeException re) {
                                logger.error("Failed to write message using the configured Record Writer, sending to the parse failure relationship", re);
                                failedMessages.add(message);
                                flowFileWriterCallback.onParseFailure(flowFile, message, re);
                            }
                        }
                    } catch (final IOException | MalformedRecordException | SchemaValidationException e) {
                        logger.error("Failed to write message, sending to the parse failure relationship", e);
                        failedMessages.add(message);
                        flowFileWriterCallback.onParseFailure(flowFile, message, e);
                    }
                } catch (Exception e) {
                    logger.error("Failed to write message, sending to the parse failure relationship", e);
                    failedMessages.add(message);
                    flowFileWriterCallback.onParseFailure(flowFile, message, e);
                }
            }

            if (writer != null) {
                final WriteResult writeResult = writer.finishRecordSet();
                attributes.put(RECORD_COUNT_KEY, String.valueOf(writeResult.getRecordCount()));
                attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                attributes.putAll(writeResult.getAttributes());
                recordCount.set(writeResult.getRecordCount());
            }

        } catch (final Exception e) {
            flowFileWriterCallback.onFailure(flowFile, processedMessages, failedMessages, e);
        } finally {
            closeWriter(writer);
        }

        if (recordCount.get() == 0) {
            session.remove(flowFile);
            return;
        }

        session.putAllAttributes(flowFile, attributes);
        flowFileWriterCallback.onSuccess(flowFile, processedMessages, failedMessages);

        final int count = recordCount.get();
        logger.info("Successfully processed {} records for {}", count, flowFile);
    }

    private void closeWriter(final RecordSetWriter writer) {
        try {
            if (writer != null) {
                writer.close();
            }
        } catch (final Exception ioe) {
            logger.warn("Failed to close Record Writer", ioe);
        }
    }

    public static final class Builder<T> {
        private RecordReaderFactory readerFactory;
        private RecordSetWriterFactory writerFactory;
        private Serializer<T> serializer;
        private AttributeSupplier<T> attributeSupplier;
        private OutputStrategy outputStrategy;
        private ComponentLog logger;

        public Builder<T> withReaderFactory(RecordReaderFactory readerFactory) {
            this.readerFactory = readerFactory;
            return this;
        }

        public Builder<T> withWriterFactory(RecordSetWriterFactory writerFactory) {
            this.writerFactory = writerFactory;
            return this;
        }

        public Builder<T> withSerializer(Serializer<T> serializer) {
            this.serializer = serializer;
            return this;
        }

        public Builder<T> withAttributeSupplier(AttributeSupplier<T> attributeSupplier) {
            this.attributeSupplier = attributeSupplier;
            return this;
        }

        public Builder<T> withOutputStrategy(OutputStrategy outputStrategy) {
            this.outputStrategy = outputStrategy;
            return this;
        }

        public Builder<T> withLogger(ComponentLog logger) {
            this.logger = logger;
            return this;
        }

        public RecordWriter<T> build() {
            return new RecordWriter<>(readerFactory, writerFactory, serializer, attributeSupplier, outputStrategy, logger);
        }
    }
}
