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

package org.apache.nifi.processors.standard;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.record.path.validation.RecordPathValidator;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

@Tags({ "record", "stats", "metrics" })
@CapabilityDescription("Counts the number of Records in a record set, optionally counting the number of elements per category, where the categories are " +
        "defined by user-defined properties.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@DynamicProperty(
    name = "The name of the category. For example, sport",
    value = "The RecordPath that points to the value of the category. For example /sport",
    expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
    description = "Specifies a category that should be counted. For example, if the property name is 'sport' and the value is '/sport', " +
            "the processor will count how many records have a value of 'soccer' for the /sport field, how many have a value of 'baseball' for the /sport, " +
            "and so on. These counts be added as attributes named recordStats.sport.soccer, recordStats.sport.baseball.")

@WritesAttributes({
    @WritesAttribute(attribute = CalculateRecordStats.RECORD_COUNT_ATTR, description = "A count of the records in the record set in the FlowFile."),
    @WritesAttribute(attribute = "recordStats.<User Defined Property Name>.count", description = "A count of the records that contain a value for the user defined property."),
    @WritesAttribute(attribute = "recordStats.<User Defined Property Name>.<value>.count",
            description = "Each value discovered for the user defined property will have its own count attribute. " +
                    "Total number of top N value counts to be added is defined by the limit configuration.")
})
public class CalculateRecordStats extends AbstractProcessor {
    static final String RECORD_COUNT_ATTR = "record.count";

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
        .name("record-stats-reader")
        .displayName("Record Reader")
        .description("A record reader to use for reading the records.")
        .identifiesControllerService(RecordReaderFactory.class)
        .required(true)
        .build();

    static final PropertyDescriptor LIMIT = new PropertyDescriptor.Builder()
        .name("record-stats-limit")
        .description("Limit the number of individual stats that are returned for each record path to the top N results.")
        .required(true)
        .defaultValue("10")
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();

    static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            RECORD_READER,
            LIMIT
    );

    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("All FlowFiles that are successfully processed, are routed to this Relationship.")
        .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("If a FlowFile cannot be processed for any reason, it is routed to this Relationship.")
        .build();

    static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    private RecordPathCache cache;

    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
            .name(propertyDescriptorName)
            .displayName(propertyDescriptorName)
            .dynamic(true)
            .addValidator(new RecordPathValidator())
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnScheduled
    public void onEnabled(ProcessContext context) {
        cache = new RecordPathCache(25);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        try {
            final Map<String, RecordPath> recordPaths = getRecordPaths(context, flowFile);
            final Map<String, String> stats = calculateStats(flowFile, recordPaths, context, session);

            flowFile = session.putAllAttributes(flowFile, stats);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final Exception e) {
            getLogger().error("Failed to process stats for {}", flowFile, e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    protected Map<String, RecordPath> getRecordPaths(final ProcessContext context, final FlowFile flowFile) {
        return context.getProperties().keySet()
            .stream().filter(PropertyDescriptor::isDynamic)
            .collect(Collectors.toMap(
                PropertyDescriptor::getName,
                propertyName ->  {
                    final String val = context.getProperty(propertyName).evaluateAttributeExpressions(flowFile).getValue();
                    return cache.getCompiled(val);
                })
            );
    }

    protected Map<String, String> calculateStats(final FlowFile flowFile, final Map<String, RecordPath> paths, final ProcessContext context, final ProcessSession session)
                throws IOException, SchemaNotFoundException, MalformedRecordException {

        try (final InputStream is = session.read(flowFile)) {
            final RecordReaderFactory factory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
            final int limit = context.getProperty(LIMIT).evaluateAttributeExpressions(flowFile).asInteger();
            final RecordReader reader = factory.createRecordReader(flowFile, is, getLogger());

            final Map<String, Integer> stats = new HashMap<>();
            Record record;

            int recordCount = 0;
            final Set<String> baseKeys = new LinkedHashSet<>();
            while ((record = reader.nextRecord()) != null) {
                for (final Map.Entry<String, RecordPath> entry : paths.entrySet()) {
                    final RecordPathResult result = entry.getValue().evaluate(record);

                    result.getSelectedFields().forEach(selectedField -> {
                        final Object selectedValue = selectedField.getValue();
                        final String approxValue = selectedValue == null ? "<null>" : selectedValue.toString();
                        final String baseKey = "recordStats." + entry.getKey();
                        final String key = baseKey + "." + approxValue;
                        final int stat = stats.getOrDefault(key, 0);
                        final int baseStat = stats.getOrDefault(baseKey, 0);

                        stats.put(key, stat + 1);
                        if (selectedValue != null) {
                            stats.put(baseKey, baseStat + 1);
                        }

                        baseKeys.add(baseKey);
                    });
                }

                recordCount++;
            }

            final Map<String, Integer> limited = filterBySize(stats, limit, baseKeys);
            limited.put(RECORD_COUNT_ATTR, recordCount);

            return limited.entrySet().stream()
                .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().toString()));
        }
    }

    protected Map<String, Integer> filterBySize(final Map<String, Integer> values, final int limit, final Collection<String> baseKeys) {
        if (values.size() <= limit) {
            return values;
        }

        final Map<String, Integer> toFilter = values.entrySet().stream()
            .filter(e -> !baseKeys.contains(e.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        List<Map.Entry<String, Integer>> entryList = new ArrayList<>(toFilter.entrySet());
        entryList.sort(Map.Entry.comparingByValue());
        entryList = entryList.reversed();
        final List<Map.Entry<String, Integer>> topEntries = entryList.subList(0, limit);

        final Map<String, Integer> limitedValues = new HashMap<>();
        // Add any element that is in the baseKeys
        values.forEach((k, v) -> {
            if (baseKeys.contains(k)) {
                limitedValues.put(k, v);
            }
        });

        for (final Map.Entry<String, Integer> entry : topEntries) {
            limitedValues.put(entry.getKey(), entry.getValue());
        }

        return limitedValues;
    }
}
