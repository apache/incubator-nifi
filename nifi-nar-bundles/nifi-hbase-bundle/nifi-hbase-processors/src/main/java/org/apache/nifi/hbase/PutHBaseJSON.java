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
package org.apache.nifi.hbase;


import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.hbase.put.PutColumn;
import org.apache.nifi.hbase.put.PutFlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.ObjectHolder;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

@EventDriven
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"hadoop", "hbase", "put", "json"})
@CapabilityDescription("Adds rows to HBase based on the contents of incoming JSON documents. Each FlowFile must contain a single " +
        "UTF-8 encoded JSON document, and any FlowFiles where the root element is not a single document will be routed to failure. " +
        "Each JSON field name and value will become a column qualifier and value of the HBase row. Any fields with a null value " +
        "will be skipped, and fields with a complex value will be handled according to the Complex Field Strategy. " +
        "The row id can be specified either directly on the processor through the Row Identifier property, or can be extracted from the JSON " +
        "document by specifying the Row Identifier Field Name property. This processor will hold the contents of all FlowFiles for the given batch " +
        "in memory at one time.")
public class PutHBaseJSON extends AbstractPutHBase {

    protected static final PropertyDescriptor ROW_FIELD_NAME = new PropertyDescriptor.Builder()
            .name("Row Identifier Field Name")
            .description("Specifies the name of a JSON element whose value should be used as the row id for the given JSON document.")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final String FAIL_VALUE = "Fail";
    protected static final String WARN_VALUE = "Warn";
    protected static final String IGNORE_VALUE = "Ignore";
    protected static final String TEXT_VALUE = "Text";

    protected static final AllowableValue COMPLEX_FIELD_FAIL = new AllowableValue(FAIL_VALUE, FAIL_VALUE, "Route entire FlowFile to failure if any elements contain complex values.");
    protected static final AllowableValue COMPLEX_FIELD_WARN = new AllowableValue(WARN_VALUE, WARN_VALUE, "Provide a warning and do not include field in row sent to HBase.");
    protected static final AllowableValue COMPLEX_FIELD_IGNORE = new AllowableValue(IGNORE_VALUE, IGNORE_VALUE, "Silently ignore and do not include in row sent to HBase.");
    protected static final AllowableValue COMPLEX_FIELD_TEXT = new AllowableValue(TEXT_VALUE, TEXT_VALUE, "Use the string representation of the complex field as the value of the given column.");

    protected static final PropertyDescriptor COMPLEX_FIELD_STRATEGY = new PropertyDescriptor.Builder()
            .name("Complex Field Strategy")
            .description("Indicates how to handle complex fields, i.e. fields that do not have a single text value.")
            .expressionLanguageSupported(false)
            .required(true)
            .allowableValues(COMPLEX_FIELD_FAIL, COMPLEX_FIELD_WARN, COMPLEX_FIELD_IGNORE, COMPLEX_FIELD_TEXT)
            .defaultValue(COMPLEX_FIELD_TEXT.getValue())
            .build();

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(HBASE_CLIENT_SERVICE);
        properties.add(TABLE_NAME);
        properties.add(ROW_ID);
        properties.add(ROW_FIELD_NAME);
        properties.add(COLUMN_FAMILY);
        properties.add(BATCH_SIZE);
        properties.add(COMPLEX_FIELD_STRATEGY);
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        return rels;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final Collection<ValidationResult> results = new ArrayList<>();

        final String rowId = validationContext.getProperty(ROW_ID).getValue();
        final String rowFieldName = validationContext.getProperty(ROW_FIELD_NAME).getValue();

        if (StringUtils.isBlank(rowId) && StringUtils.isBlank(rowFieldName)) {
            results.add(new ValidationResult.Builder()
                    .subject(this.getClass().getSimpleName())
                    .explanation("Row Identifier or Row Identifier Field Name is required")
                    .valid(false)
                    .build());
        } else if (!StringUtils.isBlank(rowId) && !StringUtils.isBlank(rowFieldName)) {
            results.add(new ValidationResult.Builder()
                    .subject(this.getClass().getSimpleName())
                    .explanation("Row Identifier and Row Identifier Field Name can not be used together")
                    .valid(false)
                    .build());
        }

        return results;
    }

    @Override
    protected PutFlowFile createPut(final ProcessSession session, final ProcessContext context, final FlowFile flowFile) {
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String rowId = context.getProperty(ROW_ID).evaluateAttributeExpressions(flowFile).getValue();
        final String rowFieldName = context.getProperty(ROW_FIELD_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String columnFamily = context.getProperty(COLUMN_FAMILY).evaluateAttributeExpressions(flowFile).getValue();
        final boolean extractRowId = !StringUtils.isBlank(rowFieldName);
        final String complexFieldStrategy = context.getProperty(COMPLEX_FIELD_STRATEGY).getValue();

        // Parse the JSON document
        final ObjectMapper mapper = new ObjectMapper();
        final ObjectHolder<JsonNode> rootNodeRef = new ObjectHolder<>(null);
        try {
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    try (final InputStream bufferedIn = new BufferedInputStream(in)) {
                        rootNodeRef.set(mapper.readTree(bufferedIn));
                    }
                }
            });
        } catch (final ProcessException pe) {
            getLogger().error("Failed to parse {} as JSON due to {}; routing to failure", new Object[]{flowFile, pe.toString()}, pe);
            return null;
        }

        final JsonNode rootNode = rootNodeRef.get();

        if (rootNode.isArray()) {
            getLogger().error("Root node of JSON must be a single document, found array for {}; routing to failure", new Object[]{flowFile});
            return null;
        }

        final Collection<PutColumn> columns = new ArrayList<>();
        final ObjectHolder<String> rowIdHolder = new ObjectHolder<>(null);

        // convert each field/value to a column for the put, skip over nulls and arrays
        final Iterator<String> fieldNames = rootNode.getFieldNames();
        while (fieldNames.hasNext()) {
            final String fieldName = fieldNames.next();
            final ObjectHolder<byte[]> fieldValueHolder = new ObjectHolder<>(null);

            final JsonNode fieldNode = rootNode.get(fieldName);
            if (fieldNode.isNull()) {
                getLogger().debug("Skipping {} because value was null", new Object[]{fieldName});
            } else if (fieldNode.isValueNode()) {
                fieldValueHolder.set(extractJNodeValue(fieldNode));
            } else {
                // for non-null, non-value nodes, determine what to do based on the handling strategy
                switch (complexFieldStrategy) {
                    case FAIL_VALUE:
                        getLogger().error("Complex value found for {}; routing to failure", new Object[]{fieldName});
                        return null;
                    case WARN_VALUE:
                        getLogger().warn("Complex value found for {}; skipping", new Object[]{fieldName});
                        break;
                    case TEXT_VALUE:
                        // use toString() here because asText() is only guaranteed to be supported on value nodes
                        // some other types of nodes, like ArrayNode, provide toString implementations
                        fieldValueHolder.set(Bytes.toBytes(fieldNode.toString()));
                        break;
                    case IGNORE_VALUE:
                        // silently skip
                        break;
                    default:
                        break;
                }
            }

            // if we have a field value, then see if this is the row id field, if so store the value for later
            // otherwise add a new column where the fieldName and fieldValue are the column qualifier and value
            if (fieldValueHolder.get() != null) {
                if (extractRowId && fieldName.equals(rowFieldName)) {
                    rowIdHolder.set(fieldNode.asText());
                } else {
                    columns.add(new PutColumn(columnFamily, fieldName, fieldValueHolder.get()));
                }
            }
        }

        // if we are expecting a field name to use for the row id and the incoming document doesn't have it
        // log an error message so the user can see what the field names were and return null so it gets routed to failure
        if (extractRowId && rowIdHolder.get() == null) {
            final String fieldNameStr = StringUtils.join(rootNode.getFieldNames(), ",");
            getLogger().error("Row ID field named '{}' not found in field names '{}'; routing to failure", new Object[] {rowFieldName, fieldNameStr});
            return null;
        }

        final String putRowId = (extractRowId ? rowIdHolder.get() : rowId);
        return new PutFlowFile(tableName, putRowId, columns, flowFile);
    }
    
    /*
     *Handles the conversion of the JsonNode value into it correct underlying data type in the form of a byte array as expected by the columns.add function 
     */
    private byte[] extractJNodeValue(JsonNode n){
    	if (n.isBoolean()){
    		//boolean
    		return Bytes.toBytes(n.asBoolean());
    	}else if(n.isNumber()){
    		if(n.isIntegralNumber()){
    			//interpret as Long
    			return Bytes.toBytes(n.asLong());
    		}else{
    			//interpret as Double
    			return Bytes.toBytes(n.asDouble());
    		}
    	}else{
    		//if all else fails, interpret as String
    		return Bytes.toBytes(n.asText());
    	}
    }

}
