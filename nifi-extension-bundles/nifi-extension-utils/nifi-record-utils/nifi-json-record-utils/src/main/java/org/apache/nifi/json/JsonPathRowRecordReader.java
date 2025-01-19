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

package org.apache.nifi.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.json.JsonProvider;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.serialization.record.util.IllegalTypeConversionException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class JsonPathRowRecordReader extends AbstractJsonRowRecordReader {

    private final ComponentLog logger;
    private final LinkedHashMap<String, JsonPath> jsonPaths;
    private final InputStream in;
    private final RecordSchema schema;
    private final Configuration providerConfiguration;

    public JsonPathRowRecordReader(
            final LinkedHashMap<String, JsonPath> jsonPaths,
            final RecordSchema schema,
            final InputStream in,
            final ComponentLog logger,
            final String dateFormat,
            final String timeFormat,
            final String timestampFormat,
            final ObjectMapper objectMapper,
            final TokenParserFactory tokenParserFactory
    ) throws MalformedRecordException, IOException {
        super(in, logger, dateFormat, timeFormat, timestampFormat, null, null, null, tokenParserFactory);

        this.schema = schema;
        this.jsonPaths = jsonPaths;
        this.in = in;
        this.logger = logger;

        final JsonProvider jsonProvider = new JacksonJsonProvider(objectMapper);
        providerConfiguration = Configuration.builder().jsonProvider(jsonProvider).build();
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    @Override
    public RecordSchema getSchema() {
        return schema;
    }

    @Override
    protected Record convertJsonNodeToRecord(final JsonNode jsonNode, final RecordSchema schema, final boolean coerceTypes, final boolean dropUnknownFields) {
        if (jsonNode == null) {
            return null;
        }

        final DocumentContext ctx = JsonPath.using(providerConfiguration).parse(jsonNode.toString());
        final Map<String, Object> values = new HashMap<>(schema.getFieldCount());

        for (final Map.Entry<String, JsonPath> entry : jsonPaths.entrySet()) {
            final String fieldName = entry.getKey();
            final DataType desiredType = schema.getDataType(fieldName).orElse(null);

            if (desiredType == null && dropUnknownFields) {
                continue;
            }

            final JsonPath jsonPath = entry.getValue();

            Object value;
            try {
                value = ctx.read(jsonPath);
            } catch (final PathNotFoundException pnfe) {
                logger.debug("Evaluated JSONPath Expression {} but the path was not found; will use a null value", entry.getValue());
                value = null;
            }

            final Optional<RecordField> field = schema.getField(fieldName);
            final Object defaultValue = field.map(RecordField::getDefaultValue).orElse(null);

            if (coerceTypes && desiredType != null) {
                value = convert(value, desiredType, fieldName, defaultValue);
            } else {
                final DataType dataType = field.map(RecordField::getDataType).orElse(null);
                value = convert(value, dataType, fieldName);
            }

            values.put(fieldName, value);
        }

        return new MapRecord(schema, values);
    }


    @SuppressWarnings("unchecked")
    protected Object convert(final Object value, final DataType dataType, final String fieldName) {
        if (value == null) {
            return null;
        }

        if (value instanceof List) {
            final List<?> list = (List<?>) value;
            final Object[] array = new Object[list.size()];

            final DataType elementDataType;
            if (dataType != null && dataType.getFieldType() == RecordFieldType.ARRAY) {
                elementDataType = ((ArrayDataType) dataType).getElementType();
            } else {
                elementDataType = null;
            }

            int i = 0;
            for (final Object val : list) {
                array[i++] = convert(val, elementDataType, fieldName);
            }

            return array;
        }

        if (value instanceof Map) {
            final Map<String, ?> map = (Map<String, ?>) value;

            boolean record = false;
            for (final Object obj : map.values()) {
                if (obj instanceof JsonNode) {
                    record = true;
                }
            }

            if (!record) {
                return value;
            }

            RecordSchema childSchema = null;
            if (dataType != null && dataType.getFieldType() == RecordFieldType.RECORD) {
                childSchema = ((RecordDataType) dataType).getChildSchema();
            }
            if (childSchema == null) {
                childSchema = new SimpleRecordSchema(Collections.emptyList());
            }

            final Map<String, Object> values = new HashMap<>();
            for (final Map.Entry<String, ?> entry : map.entrySet()) {
                final String key = entry.getKey();
                final Object childValue = entry.getValue();

                final RecordField recordField = childSchema.getField(key).orElse(null);
                final DataType childDataType = recordField == null ? null : recordField.getDataType();

                values.put(key, convert(childValue, childDataType, fieldName));
            }

            return new MapRecord(childSchema, values);
        }

        if (dataType != null && value instanceof String) {
            switch (dataType.getFieldType()) {
                case DATE:
                case TIME:
                case UUID:
                case TIMESTAMP:
                    try {
                        return DataTypeUtils.convertType(value, dataType, getDateFormat(), getTimeFormat(), getTimestampFormat(), fieldName);
                    } catch (final Exception e) {
                        return value;
                    }
            }
        }

        return value;
    }

    @SuppressWarnings("unchecked")
    protected Object convert(final Object value, final DataType dataType, final String fieldName, final Object defaultValue) {
        if (value == null) {
            return defaultValue;
        }

        if (value instanceof List) {
            if (dataType.getFieldType() != RecordFieldType.ARRAY) {
                throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type Array to " + dataType);
            }

            final ArrayDataType arrayType = (ArrayDataType) dataType;

            final List<?> list = (List<?>) value;
            final Object[] coercedValues = new Object[list.size()];
            int i = 0;
            for (final Object rawValue : list) {
                coercedValues[i++] = convert(rawValue, arrayType.getElementType(), fieldName, null);
            }
            return coercedValues;
        }

        if (dataType.getFieldType() == RecordFieldType.RECORD && value instanceof Map) {
            final RecordDataType recordDataType = (RecordDataType) dataType;
            final RecordSchema childSchema = recordDataType.getChildSchema();

            final Map<String, Object> rawValues = (Map<String, Object>) value;
            final Map<String, Object> coercedValues = new HashMap<>();

            for (final Map.Entry<String, Object> entry : rawValues.entrySet()) {
                final String key = entry.getKey();
                final Optional<DataType> desiredTypeOption = childSchema.getDataType(key);
                if (desiredTypeOption.isPresent()) {
                    final Optional<RecordField> field = childSchema.getField(key);
                    final Object defaultFieldValue = field.map(RecordField::getDefaultValue).orElse(null);

                    final Object coercedValue = convert(entry.getValue(), desiredTypeOption.get(), fieldName + "." + key, defaultFieldValue);
                    coercedValues.put(key, coercedValue);
                }
            }

            return new MapRecord(childSchema, coercedValues);
        } else {
            return DataTypeUtils.convertType(value, dataType, getDateFormat(), getTimeFormat(), getTimestampFormat(), fieldName);
        }
    }

}
