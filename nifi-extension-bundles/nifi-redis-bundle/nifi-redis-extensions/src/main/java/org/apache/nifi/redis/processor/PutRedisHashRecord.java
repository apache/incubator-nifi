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
package org.apache.nifi.redis.processor;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.validation.RecordPathValidator;
import org.apache.nifi.redis.RedisConnectionPool;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.springframework.data.redis.connection.RedisConnection;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.apache.nifi.expression.ExpressionLanguageScope.NONE;
import static org.apache.nifi.redis.util.RedisUtils.REDIS_CONNECTION_POOL;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"put", "redis", "hash", "record"})
@CapabilityDescription("Puts record field data into Redis using a specified hash value, which is determined by a RecordPath to a field in each record containing the hash value. The record fields "
        + "and values are stored as key/value pairs associated by the hash value. NOTE: Neither the evaluated hash value nor any of the field values can be null. If the hash value is null, "
        + "the FlowFile will be routed to failure. For each of the field values, if the value is null that field will be not set in Redis.")
@WritesAttributes({
        @WritesAttribute(
                attribute = PutRedisHashRecord.SUCCESS_RECORD_COUNT,
                description = "Number of records written to Redis")
})
public class PutRedisHashRecord extends AbstractProcessor {

    public static final String SUCCESS_RECORD_COUNT = "redis.success.record.count";

    protected static final PropertyDescriptor RECORD_READER_FACTORY = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for parsing incoming data and determining the data's schema")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor HASH_VALUE_RECORD_PATH = new PropertyDescriptor.Builder()
            .name("hash-value-record-path")
            .displayName("Hash Value Record Path")
            .description("Specifies a RecordPath to evaluate against each Record in order to determine the hash value associated with all the record fields/values "
                    + "(see 'hset' in Redis documentation for more details). The RecordPath must point at exactly one field or an error will occur.")
            .required(true)
            .addValidator(new RecordPathValidator())
            .expressionLanguageSupported(NONE)
            .build();

    static final PropertyDescriptor DATA_RECORD_PATH = new PropertyDescriptor.Builder()
            .name("data-record-path")
            .displayName("Data Record Path")
            .description("This property denotes a RecordPath that will be evaluated against each incoming Record and the Record that results from evaluating the RecordPath will be sent to" +
                    " Redis instead of sending the entire incoming Record. The property defaults to the root '/' which corresponds to a 'flat' record (all fields/values at the top level of " +
                    " the Record.")
            .required(true)
            .addValidator(new RecordPathValidator())
            .defaultValue("/")
            .expressionLanguageSupported(NONE)
            .build();

    static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("charset")
            .displayName("Character Set")
            .description("Specifies the character set to use when storing record field values as strings. All fields will be converted to strings using this character set "
                    + "before being stored in Redis.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles having all Records stored in Redis will be routed to this relationship")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles containing Records with processing errors will be routed to this relationship")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            RECORD_READER_FACTORY,
            REDIS_CONNECTION_POOL,
            HASH_VALUE_RECORD_PATH,
            DATA_RECORD_PATH,
            CHARSET
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    private volatile RedisConnectionPool redisConnectionPool;

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        this.redisConnectionPool = context.getProperty(REDIS_CONNECTION_POOL).asControllerService(RedisConnectionPool.class);
    }

    @OnStopped
    public void onStopped() {
        this.redisConnectionPool = null;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER_FACTORY).asControllerService(RecordReaderFactory.class);
        long count = 0;

        try (InputStream in = session.read(flowFile);
             RecordReader reader = readerFactory.createRecordReader(flowFile, in, getLogger());
             RedisConnection redisConnection = redisConnectionPool.getConnection()) {

            final String hashValueRecordPathValue = context.getProperty(HASH_VALUE_RECORD_PATH).getValue();
            final RecordPath hashValueRecordPath = RecordPath.compile(hashValueRecordPathValue);

            final String dataRecordPathValue = context.getProperty(DATA_RECORD_PATH).getValue();
            final RecordPath dataRecordPath = RecordPath.compile(dataRecordPathValue);

            final Charset charset = Charset.forName(context.getProperty(CHARSET).evaluateAttributeExpressions(flowFile).getValue());
            Record record;

            while ((record = reader.nextRecord()) != null) {
                final RecordPathResult recordPathResult = hashValueRecordPath.evaluate(record);
                final List<FieldValue> resultList = recordPathResult.getSelectedFields().distinct().toList();
                if (resultList.isEmpty()) {
                    throw new ProcessException(String.format("No results found for Record [%d] Hash Value Record Path: %s", count, hashValueRecordPath.getPath()));
                }

                if (resultList.size() > 1) {
                    throw new ProcessException(String.format("Multiple results [%d] found for Record [%d] Hash Value Record Path: %s", resultList.size(), count, hashValueRecordPath.getPath()));
                }

                final FieldValue hashValueFieldValue = resultList.getFirst();
                final Object hashValueObject = hashValueFieldValue.getValue();
                if (hashValueObject == null) {
                    throw new ProcessException(String.format("Null value found for Record [%d] Hash Value Record Path: %s", count, hashValueRecordPath.getPath()));
                }
                final String hashValue = (String) DataTypeUtils.convertType(hashValueObject, RecordFieldType.STRING.getDataType(), charset.name());

                List<Record> dataRecords = getDataRecords(dataRecordPath, record);

                count = putDataRecordsToRedis(dataRecords, redisConnection, hashValue, charset, count);
            }

        } catch (MalformedRecordException e) {
            getLogger().error("Read Records failed {}", flowFile, e);
            flowFile = session.putAttribute(flowFile, SUCCESS_RECORD_COUNT, String.valueOf(count));
            session.transfer(flowFile, REL_FAILURE);
            return;
        } catch (SchemaNotFoundException e) {
            getLogger().error("Record Schema not found {}", flowFile, e);
            flowFile = session.putAttribute(flowFile, SUCCESS_RECORD_COUNT, String.valueOf(count));
            session.transfer(flowFile, REL_FAILURE);
            return;
        } catch (Exception e) {
            getLogger().error("Put Records failed {}", flowFile, e);
            flowFile = session.putAttribute(flowFile, SUCCESS_RECORD_COUNT, String.valueOf(count));
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        flowFile = session.putAttribute(flowFile, SUCCESS_RECORD_COUNT, String.valueOf(count));
        session.transfer(flowFile, REL_SUCCESS);
    }

    private List<Record> getDataRecords(final RecordPath dataRecordPath, final Record outerRecord) {
        if (dataRecordPath == null) {
            return Collections.singletonList(outerRecord);
        }

        final RecordPathResult result = dataRecordPath.evaluate(outerRecord);
        final List<FieldValue> fieldValues = result.getSelectedFields().toList();
        if (fieldValues.isEmpty()) {
            throw new ProcessException("RecordPath " + dataRecordPath.getPath() + " evaluated against Record yielded no results.");
        }

        for (final FieldValue fieldValue : fieldValues) {
            final RecordFieldType fieldType = fieldValue.getField().getDataType().getFieldType();
            if (fieldType != RecordFieldType.RECORD) {
                throw new ProcessException("RecordPath " + dataRecordPath.getPath() + " evaluated against Record expected to return one or more Records but encountered field of type" +
                        " " + fieldType);
            }
        }

        final List<Record> dataRecords = new ArrayList<>(fieldValues.size());
        for (final FieldValue fieldValue : fieldValues) {
            dataRecords.add((Record) fieldValue.getValue());
        }

        return dataRecords;
    }

    private long putDataRecordsToRedis(final List<Record> dataRecords, final RedisConnection redisConnection, final String hashValue, final Charset charset, final long originalCount) {
        long count = originalCount;
        for (Record dataRecord : dataRecords) {
            RecordSchema dataRecordSchema = dataRecord.getSchema();

            for (RecordField recordField : dataRecordSchema.getFields()) {
                final String fieldName = recordField.getFieldName();
                final Object value = dataRecord.getValue(fieldName);
                if (fieldName == null || value == null) {
                    getLogger().debug("Record field missing required elements: name [{}] value [{}]", fieldName, value);
                } else {
                    final String stringValue = (String) DataTypeUtils.convertType(value, RecordFieldType.STRING.getDataType(), charset.name());
                    redisConnection.hashCommands().hSet(hashValue.getBytes(charset), fieldName.getBytes(charset), stringValue.getBytes(charset));
                }
            }
            count++;
        }
        return count;
    }
}
