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
package org.apache.nifi.processors.gcp.pubsub.consume;

import com.google.pubsub.v1.ReceivedMessage;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.ACK_ID_ATTRIBUTE;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.MESSAGE_ID_ATTRIBUTE;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.MSG_ATTRIBUTES_COUNT_ATTRIBUTE;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.MSG_PUBLISH_TIME_ATTRIBUTE;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.SERIALIZED_SIZE_ATTRIBUTE;

public class WrapperRecord extends MapRecord {

    public static final String METADATA = "metadata";
    public static final String ATTRIBUTES = "attributes";
    public static final String VALUE = "value";

    private static final RecordField FIELD_ACK_ID_ATTRIBUTE = new RecordField(ACK_ID_ATTRIBUTE, RecordFieldType.STRING.getDataType());
    private static final RecordField FIELD_SERIALIZED_SIZE_ATTRIBUTE = new RecordField(SERIALIZED_SIZE_ATTRIBUTE, RecordFieldType.INT.getDataType());
    private static final RecordField FIELD_MESSAGE_ID_ATTRIBUTE = new RecordField(MESSAGE_ID_ATTRIBUTE, RecordFieldType.STRING.getDataType());
    private static final RecordField FIELD_MSG_ATTRIBUTES_COUNT_ATTRIBUTE = new RecordField(MSG_ATTRIBUTES_COUNT_ATTRIBUTE, RecordFieldType.INT.getDataType());
    private static final RecordField FIELD_MSG_PUBLISH_TIME_ATTRIBUTE = new RecordField(MSG_PUBLISH_TIME_ATTRIBUTE, RecordFieldType.LONG.getDataType());
    public static final RecordSchema SCHEMA_METADATA = new SimpleRecordSchema(
            Arrays.asList(FIELD_ACK_ID_ATTRIBUTE, FIELD_SERIALIZED_SIZE_ATTRIBUTE, FIELD_MESSAGE_ID_ATTRIBUTE, FIELD_MSG_ATTRIBUTES_COUNT_ATTRIBUTE, FIELD_MSG_PUBLISH_TIME_ATTRIBUTE));

    public static final RecordField FIELD_METADATA = new RecordField(METADATA, RecordFieldType.RECORD.getRecordDataType(SCHEMA_METADATA));
    public static final RecordField FIELD_ATTRIBUTES = new RecordField(ATTRIBUTES, RecordFieldType.MAP.getMapDataType(RecordFieldType.STRING.getDataType()));

    private static RecordSchema toRecordSchema(final Record record) {
        final RecordField fieldValue = new RecordField(VALUE, RecordFieldType.RECORD.getRecordDataType(record.getSchema()));
        return new SimpleRecordSchema(Arrays.asList(FIELD_METADATA, FIELD_ATTRIBUTES, fieldValue));
    }

    private static Map<String, Object> toValues(final Record record, ReceivedMessage message) {
        final Map<String, Object> valuesMetadata = new HashMap<>();
        valuesMetadata.put(ACK_ID_ATTRIBUTE, message.getAckId());
        valuesMetadata.put(SERIALIZED_SIZE_ATTRIBUTE, message.getSerializedSize());
        valuesMetadata.put(MESSAGE_ID_ATTRIBUTE, message.getMessage().getMessageId());
        valuesMetadata.put(MSG_ATTRIBUTES_COUNT_ATTRIBUTE, message.getMessage().getAttributesCount());
        valuesMetadata.put(MSG_PUBLISH_TIME_ATTRIBUTE, message.getMessage().getPublishTime().getSeconds());
        final Record recordMetadata = new MapRecord(SCHEMA_METADATA, valuesMetadata);

        final Map<String, Object> valuesWrapper = new HashMap<>();
        valuesWrapper.put(METADATA, recordMetadata);
        valuesWrapper.put(ATTRIBUTES, message.getMessage().getAttributesMap());
        valuesWrapper.put(VALUE, record);
        return valuesWrapper;
    }

    public WrapperRecord(final Record record, ReceivedMessage message) {
        super(toRecordSchema(record), toValues(record, message));
    }

    public static RecordSchema toWrapperSchema(final RecordSchema recordSchema) {
        final RecordField fieldValue = new RecordField(VALUE, RecordFieldType.RECORD.getRecordDataType(recordSchema));
        return new SimpleRecordSchema(Arrays.asList(FIELD_METADATA, FIELD_ATTRIBUTES, fieldValue));
    }

}
