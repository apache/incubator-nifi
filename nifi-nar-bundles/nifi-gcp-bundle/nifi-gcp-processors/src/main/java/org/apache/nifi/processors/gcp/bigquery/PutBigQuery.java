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

package org.apache.nifi.processors.gcp.bigquery;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsRequest;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsResponse;
import com.google.cloud.bigquery.storage.v1.BQTableSchemaToProtoDescriptor;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.CivilTimeEncoder;
import com.google.cloud.bigquery.storage.v1.CreateWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.google.cloud.bigquery.storage.v1.ProtoSchemaConverter;
import com.google.cloud.bigquery.storage.v1.StorageError;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.grpc.Status;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.gcp.bigquery.proto.ProtoUtils;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"google", "google cloud", "bq", "bigquery"})
@CapabilityDescription("Unified processor for batch and stream flow files content to a Google BigQuery table via the Storage Write API." +
    "The processor is record based so the used schema is driven by the RecordReader. Attributes that are not matched to the target schema" +
    "are skipped. Exactly once delivery semantics are achieved via stream offsets. The Storage Write API is more efficient than the older " +
    "insertAll method because it uses gRPC streaming rather than REST over HTTP")
@SeeAlso({PutBigQueryBatch.class, PutBigQueryStreaming.class})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttributes({
    @WritesAttribute(attribute = BigQueryAttributes.JOB_NB_RECORDS_ATTR, description = BigQueryAttributes.JOB_NB_RECORDS_DESC)
})
public class PutBigQuery extends AbstractBigQueryProcessor {

    static final String STREAM = "STREAM";
    static final String BATCH = "BATCH";
    static final AllowableValue STREAM_TYPE = new AllowableValue(STREAM, STREAM, "Define streaming approach.");
    static final AllowableValue BATCH_TYPE = new AllowableValue(BATCH, BATCH, "Define batching approach.");

    private static final String APPEND_RECORD_COUNT_NAME = "bq.append.record.count";
    private static final String APPEND_RECORD_COUNT_DESC = "The number of records to be appended to the write stream at once. Applicable for both batch and stream types.";
    private static final String TRANSFER_TYPE_NAME = "bq.transfer.type";
    private static final String TRANSFER_TYPE_DESC = "Defines the preferred transfer type streaming or batching.";

    private static final List<Status.Code> RETRYABLE_ERROR_CODES = Arrays.asList(Status.Code.INTERNAL, Status.Code.ABORTED, Status.Code.CANCELLED);

    private final AtomicReference<Exception> setupException = new AtomicReference<>();
    private final AtomicReference<RuntimeException> error = new AtomicReference<>();
    private final AtomicInteger appendSuccessCount = new AtomicInteger(0);
    private final Phaser inflightRequestCount = new Phaser(1);
    private TableName tableName = null;
    private BigQueryWriteClient writeClient = null;
    private StreamWriter streamWriter = null;
    private String transferType;
    private int maxRetryCount;
    private int recordBatchCount;
    private boolean skipInvalidRows;

    static final PropertyDescriptor TRANSFER_TYPE = new PropertyDescriptor.Builder()
        .name(TRANSFER_TYPE_NAME)
        .displayName("Transfer Type")
        .description(TRANSFER_TYPE_DESC)
        .required(true)
        .defaultValue(STREAM_TYPE.getValue())
        .allowableValues(STREAM_TYPE, BATCH_TYPE)
        .build();

    static final PropertyDescriptor APPEND_RECORD_COUNT = new PropertyDescriptor.Builder()
        .name(APPEND_RECORD_COUNT_NAME)
        .displayName("Append Record Count")
        .description(APPEND_RECORD_COUNT_DESC)
        .required(true)
        .defaultValue("20")
        .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
        .build();

    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
        .name(BigQueryAttributes.RECORD_READER_ATTR)
        .displayName("Record Reader")
        .description(BigQueryAttributes.RECORD_READER_DESC)
        .identifiesControllerService(RecordReaderFactory.class)
        .required(true)
        .build();

    public static final PropertyDescriptor SKIP_INVALID_ROWS = new PropertyDescriptor.Builder()
        .name(BigQueryAttributes.SKIP_INVALID_ROWS_ATTR)
        .displayName("Skip Invalid Rows")
        .description(BigQueryAttributes.SKIP_INVALID_ROWS_DESC)
        .required(true)
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue("false")
        .build();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> descriptors = new ArrayList<>(super.getSupportedPropertyDescriptors());
        descriptors.add(TRANSFER_TYPE);
        descriptors.add(RECORD_READER);
        descriptors.add(APPEND_RECORD_COUNT);
        descriptors.add(SKIP_INVALID_ROWS);
        descriptors.remove(IGNORE_UNKNOWN);

        return Collections.unmodifiableList(descriptors);
    }

    @Override
    @OnScheduled
    public void onScheduled(ProcessContext context) {
        super.onScheduled(context);

        transferType = context.getProperty(TRANSFER_TYPE).getValue();
        maxRetryCount = context.getProperty(RETRY_COUNT).asInteger();
        skipInvalidRows = context.getProperty(SKIP_INVALID_ROWS).asBoolean();
        recordBatchCount = context.getProperty(APPEND_RECORD_COUNT).asInteger();
        tableName = TableName.of(context.getProperty(PROJECT_ID).getValue(), context.getProperty(DATASET).getValue(), context.getProperty(TABLE_NAME).getValue());
        writeClient = createWriteClient(getGoogleCredentials(context));
    }

    @OnUnscheduled
    public void onUnScheduled() {
        writeClient.shutdown();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        if (writeClient == null) {
            if (setupException.get() != null) {
                getLogger().error("Failed to create Big Query Writer Client due to {}", setupException.get());
            } else {
                getLogger().error("Big Query Writer Client was not properly created");
            }

            context.yield();
            return;
        }

        WriteStream writeStream;
        Descriptors.Descriptor protoDescriptor;
        try {
            writeStream = createWriteStream();
            protoDescriptor = BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(writeStream.getTableSchema());
            streamWriter = createStreamWriter(writeStream.getName(), protoDescriptor, getGoogleCredentials(context));
        } catch (Descriptors.DescriptorValidationException | IOException e) {
            getLogger().error("Failed to create Big Query Write Client for writing due to {}", e);
            context.yield();
            return;
        }

        int recordNumWritten;
        try {
            try (InputStream in = session.read(flowFile)) {
                RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
                try (RecordReader reader = readerFactory.createRecordReader(flowFile, in, getLogger())) {
                    recordNumWritten = writeRecordsToStream(reader, protoDescriptor);
                }
            }

            flowFile = session.putAttribute(flowFile, BigQueryAttributes.JOB_NB_RECORDS_ATTR, Integer.toString(recordNumWritten));
        } catch (Exception e) {
            getLogger().error(e.getMessage(), e);
            error.set(new RuntimeException(e));
        } finally {
            finishProcessing(session, flowFile, streamWriter, writeStream.getName(), tableName.toString());
        }
    }

    private int writeRecordsToStream(RecordReader reader, Descriptors.Descriptor descriptor) throws IOException, MalformedRecordException {
        Record currentRecord;
        int offset = 0;
        int recordNum = 0;
        ProtoRows.Builder rowsBuilder = ProtoRows.newBuilder();
        while ((currentRecord = reader.nextRecord()) != null) {
            DynamicMessage message = recordToProtoMessage(currentRecord, descriptor);

            if (message == null) {
                continue;
            }

            rowsBuilder.addSerializedRows(message.toByteString());

            if (++recordNum % recordBatchCount == 0) {
                append(new AppendContext(rowsBuilder.build(), offset));
                rowsBuilder = ProtoRows.newBuilder();
                offset = recordNum;
            }
        }

        if (recordNum > offset) {
            append(new AppendContext(rowsBuilder.build(), offset));
        }

        return recordNum;
    }

    private DynamicMessage recordToProtoMessage(Record record, Descriptors.Descriptor descriptor) {
        Map<String, Object> valueMap = convertMapRecord(record.toMap());
        DynamicMessage message = null;
        try {
            message = ProtoUtils.createMessage(descriptor, valueMap);
        } catch (RuntimeException e) {
            getLogger().error("Can't create message from input", e);
            if (!skipInvalidRows) {
                throw e;
            }
        }

        return message;
    }

    private void append(AppendContext appendContext) {
        if (error.get() != null) {
            throw error.get();
        }

        ApiFuture<AppendRowsResponse> future = streamWriter.append(appendContext.getData(), appendContext.getOffset());
        ApiFutures.addCallback(future, new AppendCompleteCallback(appendContext), Runnable::run);

        inflightRequestCount.register();
    }

    private void finishProcessing(ProcessSession session, FlowFile flowFile, StreamWriter streamWriter, String streamName, String parentTable) {
        // Wait for all in-flight requests to complete.
        inflightRequestCount.arriveAndAwaitAdvance();

        // Close the connection to the server.
        streamWriter.close();

        // Verify that no error occurred in the stream.
        if (error.get() != null) {
            getLogger().error("Error occurred in the stream: ", error.get());
            flowFile = session.putAttribute(flowFile, BigQueryAttributes.JOB_NB_RECORDS_ATTR, isBatch() ? "0" : String.valueOf(appendSuccessCount.get() * recordBatchCount));
            session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        } else {
            if (isBatch()) {
                writeClient.finalizeWriteStream(streamName);

                BatchCommitWriteStreamsRequest commitRequest =
                    BatchCommitWriteStreamsRequest.newBuilder()
                        .setParent(parentTable)
                        .addWriteStreams(streamName)
                        .build();

                BatchCommitWriteStreamsResponse commitResponse = writeClient.batchCommitWriteStreams(commitRequest);

                // If the response does not have a commit time, it means the commit operation failed.
                if (!commitResponse.hasCommitTime()) {
                    for (StorageError err : commitResponse.getStreamErrorsList()) {
                        getLogger().error("Commit operation error: ", err.getErrorMessage());
                    }
                    session.penalize(flowFile);
                    session.transfer(flowFile, REL_FAILURE);
                }
                getLogger().info("Appended and committed all records successfully.");
            }

            session.transfer(flowFile, REL_SUCCESS);
        }
    }

    class AppendCompleteCallback implements ApiFutureCallback<AppendRowsResponse> {

        private final AppendContext appendContext;

        public AppendCompleteCallback(AppendContext appendContext) {
            this.appendContext = appendContext;
        }

        public void onSuccess(AppendRowsResponse response) {
            getLogger().info("Append success with offset: " + appendContext.getOffset());
            appendSuccessCount.incrementAndGet();
            inflightRequestCount.arriveAndDeregister();
        }

        public void onFailure(Throwable throwable) {
            // If the state is INTERNAL, CANCELLED, or ABORTED, you can retry. For more information,
            // see: https://grpc.github.io/grpc-java/javadoc/io/grpc/StatusRuntimeException.html
            Status status = Status.fromThrowable(throwable);
            if (appendContext.getRetryCount() < maxRetryCount && RETRYABLE_ERROR_CODES.contains(status.getCode())) {
                appendContext.incrementRetryCount();
                try {
                    append(appendContext);
                    inflightRequestCount.arriveAndDeregister();
                    return;
                } catch (Exception e) {
                    getLogger().error("Failed to retry append: ", e);
                }
            }

            error.compareAndSet(null, Optional.ofNullable(Exceptions.toStorageException(throwable))
                .map(RuntimeException.class::cast)
                .orElse(new RuntimeException(throwable)));

            getLogger().error("Failure during appending data: ", throwable);
            inflightRequestCount.arriveAndDeregister();
        }
    }

    private WriteStream createWriteStream() {
        WriteStream.Type type = isBatch() ? WriteStream.Type.PENDING : WriteStream.Type.COMMITTED;
        CreateWriteStreamRequest createWriteStreamRequest = CreateWriteStreamRequest.newBuilder()
            .setParent(tableName.toString())
            .setWriteStream(WriteStream.newBuilder().setType(type).build())
            .build();

        return writeClient.createWriteStream(createWriteStreamRequest);
    }

    protected BigQueryWriteClient createWriteClient(GoogleCredentials credentials) {
        BigQueryWriteClient client = null;
        try {
            client = BigQueryWriteClient.create(BigQueryWriteSettings.newBuilder().setCredentialsProvider(FixedCredentialsProvider.create(credentials)).build());
        } catch (IOException e) {
            getLogger().error("Failed to create Big Query Write Client for writing due to {}", new Object[] {e});
            setupException.set(e);
        }

        return client;
    }

    protected StreamWriter createStreamWriter(String streamName, Descriptors.Descriptor descriptor, GoogleCredentials credentials) throws IOException {
        ProtoSchema protoSchema = ProtoSchemaConverter.convert(descriptor);
        return StreamWriter.newBuilder(streamName)
            .setWriterSchema(protoSchema)
            .setCredentialsProvider(FixedCredentialsProvider.create(credentials)).build();
    }

    private boolean isBatch() {
        return BATCH_TYPE.getValue().equals(transferType);
    }

    private static class AppendContext {
        private final ProtoRows data;
        private final long offset;
        private int retryCount;

        AppendContext(ProtoRows data, long offset) {
            this.data = data;
            this.offset = offset;
            this.retryCount = 0;
        }

        public ProtoRows getData() {
            return data;
        }

        public int getRetryCount() {
            return retryCount;
        }

        public void incrementRetryCount() {
            retryCount++;
        }

        public long getOffset() {
            return offset;
        }
    }

    private static Map<String, Object> convertMapRecord(Map<String, Object> map) {
        Map<String, Object> result = new HashMap<>();
        for (String key : map.keySet()) {
            Object obj = map.get(key);
            if (obj instanceof MapRecord) {
                result.put(key, convertMapRecord(((MapRecord) obj).toMap()));
            } else if (obj instanceof Object[]
                && ((Object[]) obj).length > 0
                && ((Object[]) obj)[0] instanceof MapRecord) {
                List<Map<String, Object>> lmapr = new ArrayList<>();
                for (Object mapr : ((Object[]) obj)) {
                    lmapr.add(convertMapRecord(((MapRecord) mapr).toMap()));
                }
                result.put(key, lmapr);
            } else if (obj instanceof Timestamp) {
                // ZoneOffset.UTC time zone is necessary due to implicit time zone conversion in Record Readers from
                // the local system time zone to the GMT time zone
                LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(((Timestamp) obj).getTime()), ZoneOffset.UTC);
                result.put(key, dateTime.toEpochSecond(ZoneOffset.UTC) * 1000 * 1000);
            } else if (obj instanceof Time) {
                // ZoneOffset.UTC time zone is necessary due to implicit time zone conversion in Record Readers from
                // the local system time zone to the GMT time zone
                LocalDateTime time = LocalDateTime.ofInstant(Instant.ofEpochMilli(((Time) obj).getTime()), ZoneOffset.UTC);
                org.threeten.bp.LocalTime localTime = org.threeten.bp.LocalTime.of(
                    time.getHour(),
                    time.getMinute(),
                    time.getSecond());
                result.put(key, CivilTimeEncoder.encodePacked64TimeMicros(localTime));
            } else if (obj instanceof Date) {
                result.put(key, (int) ((Date) obj).toLocalDate().toEpochDay());
            } else {
                result.put(key, obj);
            }
        }

        return result;
    }
}
