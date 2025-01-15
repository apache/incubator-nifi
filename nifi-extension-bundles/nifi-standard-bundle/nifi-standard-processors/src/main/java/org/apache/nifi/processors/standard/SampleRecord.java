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

import org.apache.commons.lang3.Range;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SideEffectFree
@SupportsBatching
@Tags({"record", "sample", "reservoir", "range", "interval"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Samples the records of a FlowFile based on a specified sampling strategy (such as Reservoir Sampling). The resulting "
        + "FlowFile may be of a fixed number of records (in the case of reservoir-based algorithms) or some subset of the total number of records "
        + "(in the case of probabilistic sampling), or a deterministic number of records (in the case of interval sampling).")
@SystemResourceConsideration(resource = SystemResource.MEMORY)
@WritesAttributes({
        @WritesAttribute(attribute = "mime.type", description = "The MIME type indicated by the record writer"),
        @WritesAttribute(attribute = "record.count", description = "The number of records in the resulting flow file")
})
public class SampleRecord extends AbstractProcessor {

    static final String INTERVAL_SAMPLING_KEY = "interval";
    static final String RANGE_SAMPLING_KEY = "range";
    static final String PROBABILISTIC_SAMPLING_KEY = "probabilistic";
    static final String RESERVOIR_SAMPLING_KEY = "reservoir";

    static final AllowableValue INTERVAL_SAMPLING = new AllowableValue(INTERVAL_SAMPLING_KEY, "Interval Sampling",
            "Selects every Nth record where N is the value of the 'Interval Value' property");
    static final AllowableValue RANGE_SAMPLING = new AllowableValue(RANGE_SAMPLING_KEY, "Range Sampling",
            "Creates a sample of records based on the index (i.e. record number) of the records using the specified range. An example is '3,6-8,20-' which includes the third record, "
                    + "the sixth, seventh and eighth record, and all records from the twentieth record on. Commas separate intervals that don't overlap, and an interval can be between two numbers "
                    + "(i.e. 6-8) or up to a given number (i.e. -5), or from a number to the number of the last record (i.e. 20-).");
    static final AllowableValue PROBABILISTIC_SAMPLING = new AllowableValue(PROBABILISTIC_SAMPLING_KEY, "Probabilistic Sampling",
            "Selects each record with probability P where P is the value of the 'Selection Probability' property");
    static final AllowableValue RESERVOIR_SAMPLING = new AllowableValue(RESERVOIR_SAMPLING_KEY, "Reservoir Sampling",
            "Creates a sample of K records where each record has equal probability of being included, where K is "
                    + "the value of the 'Reservoir Size' property. Note that if the value is very large it may cause memory issues as "
                    + "the reservoir is kept in-memory.");

    private static final String RANGE_SEPARATOR = ",";
    private static final Pattern RANGE_PATTERN = Pattern.compile("^([0-9]+)?(-)?([0-9]+)?");
    private static final Pattern INTERVAL_PATTERN = Pattern.compile("([0-9]+)?(-)?([0-9]+)?(?:,|$)");

    static final PropertyDescriptor RECORD_READER_FACTORY = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for parsing incoming data and determining the data's schema")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();
    static final PropertyDescriptor RECORD_WRITER_FACTORY = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("Specifies the Controller Service to use for writing results to a FlowFile")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();
    static final PropertyDescriptor SAMPLING_STRATEGY = new PropertyDescriptor.Builder()
            .name("sample-record-sampling-strategy")
            .displayName("Sampling Strategy")
            .description("Specifies which method to use for sampling records from the incoming FlowFile")
            .allowableValues(INTERVAL_SAMPLING, RANGE_SAMPLING, PROBABILISTIC_SAMPLING, RESERVOIR_SAMPLING)
            .required(true)
            .defaultValue(RESERVOIR_SAMPLING.getValue())
            .addValidator(Validator.VALID)
            .build();
    static final PropertyDescriptor SAMPLING_INTERVAL = new PropertyDescriptor.Builder()
            .name("sample-record-interval")
            .displayName("Sampling Interval")
            .description("Specifies the number of records to skip before writing a record to the outgoing FlowFile. This property is only "
                    + "used if Sampling Strategy is set to Interval Sampling. A value of zero (0) will cause no records to be included in the"
                    + "outgoing FlowFile, a value of one (1) will cause all records to be included, and a value of two (2) will cause half the "
                    + "records to be included, and so on.")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .dependsOn(SAMPLING_STRATEGY, INTERVAL_SAMPLING)
            .build();
    static final PropertyDescriptor SAMPLING_RANGE = new PropertyDescriptor.Builder()
            .name("sample-record-range")
            .displayName("Sampling Range")
            .description("Specifies the range of records to include in the sample, from 1 to the total number of records. An example is '3,6-8,20-' which includes the third record, the sixth, "
                    + "seventh and eighth records, and all records from the twentieth record on. Commas separate intervals that don't overlap, and an interval can be between two numbers "
                    + "(i.e. 6-8) or up to a given number (i.e. -5), or from a number to the number of the last record (i.e. 20-). If this property is unset, all records will be included.")
            .required(true)
            .addValidator(Validator.VALID)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .dependsOn(SAMPLING_STRATEGY, RANGE_SAMPLING)
            .build();
    static final PropertyDescriptor SAMPLING_PROBABILITY = new PropertyDescriptor.Builder()
            .name("sample-record-probability")
            .displayName("Sampling Probability")
            .description("Specifies the probability (as a percent from 0-100) of a record being included in the outgoing FlowFile. This property is only "
                    + "used if Sampling Strategy is set to Probabilistic Sampling. A value of zero (0) will cause no records to be included in the"
                    + "outgoing FlowFile, and a value of 100 will cause all records to be included in the outgoing FlowFile..")
            .required(true)
            .addValidator(StandardValidators.createLongValidator(0, 100, true))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .dependsOn(SAMPLING_STRATEGY, PROBABILISTIC_SAMPLING)
            .build();
    static final PropertyDescriptor RESERVOIR_SIZE = new PropertyDescriptor.Builder()
            .name("sample-record-reservoir")
            .displayName("Reservoir Size")
            .description("Specifies the number of records to write to the outgoing FlowFile. This property is only used if Sampling Strategy is set to "
                    + "reservoir-based strategies such as Reservoir Sampling.")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .dependsOn(SAMPLING_STRATEGY, RESERVOIR_SAMPLING)
            .build();

    static final PropertyDescriptor RANDOM_SEED = new PropertyDescriptor.Builder()
            .name("sample-record-random-seed")
            .displayName("Random Seed")
            .description("Specifies a particular number to use as the seed for the random number generator (used by probabilistic strategies). "
                    + "Setting this property will ensure the same records are selected even when using probabilistic strategies.")
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .dependsOn(SAMPLING_STRATEGY, PROBABILISTIC_SAMPLING, RESERVOIR_SAMPLING)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            RECORD_READER_FACTORY,
            RECORD_WRITER_FACTORY,
            SAMPLING_STRATEGY,
            SAMPLING_INTERVAL,
            SAMPLING_RANGE,
            SAMPLING_PROBABILITY,
            RESERVOIR_SIZE,
            RANDOM_SEED
    );

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original FlowFile is routed to this relationship if sampling is successful")
            .autoTerminateDefault(true)
            .build();
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("The FlowFile is routed to this relationship if the sampling completed successfully")
            .autoTerminateDefault(true)
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a FlowFile fails processing for any reason (for example, any record "
                    + "is not valid), the original FlowFile will be routed to this relationship")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE,
            REL_ORIGINAL
    );

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        FlowFile sampledFlowFile = session.create(flowFile);
        final FlowFile outFlowFile = sampledFlowFile;
        final Map<String, String> attributes = new HashMap<>();
        try (final InputStream inputStream = session.read(flowFile);
             final OutputStream outputStream = session.write(sampledFlowFile)) {

            final RecordReaderFactory recordParserFactory = context.getProperty(RECORD_READER_FACTORY)
                    .asControllerService(RecordReaderFactory.class);
            final RecordReader reader = recordParserFactory.createRecordReader(flowFile, inputStream, getLogger());
            final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER_FACTORY)
                    .asControllerService(RecordSetWriterFactory.class);
            final RecordSchema writeSchema = writerFactory.getSchema(flowFile.getAttributes(), reader.getSchema());
            final RecordSetWriter recordSetWriter = writerFactory.createWriter(getLogger(), writeSchema, outputStream, outFlowFile);

            final String samplingStrategyValue = context.getProperty(SAMPLING_STRATEGY).getValue();
            final SamplingStrategy samplingStrategy;
            if (INTERVAL_SAMPLING_KEY.equals(samplingStrategyValue)) {
                final int intervalValue = context.getProperty(SAMPLING_INTERVAL).evaluateAttributeExpressions(outFlowFile).asInteger();
                samplingStrategy = new IntervalSamplingStrategy(recordSetWriter, intervalValue);
            } else if (RANGE_SAMPLING_KEY.equals(samplingStrategyValue)) {
                final String rangeExpression = context.getProperty(SAMPLING_RANGE).evaluateAttributeExpressions(outFlowFile).getValue();
                samplingStrategy = new RangeSamplingStrategy(recordSetWriter, rangeExpression);
            } else if (PROBABILISTIC_SAMPLING_KEY.equals(samplingStrategyValue)) {
                final int probabilityValue = context.getProperty(SAMPLING_PROBABILITY).evaluateAttributeExpressions(outFlowFile).asInteger();
                final Long randomSeed = context.getProperty(RANDOM_SEED).isSet()
                        ? context.getProperty(RANDOM_SEED).evaluateAttributeExpressions(outFlowFile).asLong()
                        : null;
                samplingStrategy = new ProbabilisticSamplingStrategy(recordSetWriter, probabilityValue, randomSeed);
            } else {
                final int reservoirSize = context.getProperty(RESERVOIR_SIZE).evaluateAttributeExpressions(outFlowFile).asInteger();
                final Long randomSeed = context.getProperty(RANDOM_SEED).isSet()
                        ? context.getProperty(RANDOM_SEED).evaluateAttributeExpressions(outFlowFile).asLong()
                        : null;
                samplingStrategy = new ReservoirSamplingStrategy(recordSetWriter, reservoirSize, randomSeed);
            }
            samplingStrategy.init();

            Record record;
            while ((record = reader.nextRecord()) != null) {
                samplingStrategy.sample(record);
            }

            WriteResult writeResult = samplingStrategy.finish();
            try {
                recordSetWriter.flush();
                recordSetWriter.close();
            } catch (final IOException ioe) {
                getLogger().warn("Failed to close Writer for {}", outFlowFile);
            }

            attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
            attributes.put(CoreAttributes.MIME_TYPE.key(), recordSetWriter.getMimeType());
            attributes.putAll(writeResult.getAttributes());
        } catch (Exception e) {
            getLogger().error("Error during transmission of records, routing to failure", e);
            session.transfer(flowFile, REL_FAILURE);
            session.remove(sampledFlowFile);
            return;
        }
        session.transfer(flowFile, REL_ORIGINAL);
        sampledFlowFile = session.putAllAttributes(sampledFlowFile, attributes);
        session.transfer(sampledFlowFile, REL_SUCCESS);
    }

    interface SamplingStrategy {
        void init() throws IOException;

        void sample(Record record) throws IOException;

        WriteResult finish() throws IOException;
    }

    static class IntervalSamplingStrategy implements SamplingStrategy {
        final RecordSetWriter writer;
        final int interval;
        int currentCount = 0;

        IntervalSamplingStrategy(RecordSetWriter writer, int interval) {
            this.writer = writer;
            this.interval = interval;
        }

        @Override
        public void init() throws IOException {
            currentCount = 0;
            writer.beginRecordSet();
        }

        @Override
        public void sample(Record record) throws IOException {
            if (++currentCount >= interval && interval > 0) {
                writer.write(record);
                currentCount = 0;
            }
        }

        @Override
        public WriteResult finish() throws IOException {
            return writer.finishRecordSet();
        }
    }

    static class RangeSamplingStrategy implements SamplingStrategy {

        final RecordSetWriter writer;
        final String rangeExpression;
        int currentCount = 1;
        final List<Range<Integer>> ranges = new ArrayList<>();

        RangeSamplingStrategy(final RecordSetWriter writer, final String rangeExpression) {
            this.writer = writer;
            this.rangeExpression = rangeExpression;
        }

        private boolean isRangeExpressionInvalid() {
            boolean invalid = false;
            final String[] ranges = rangeExpression.split(RANGE_SEPARATOR);
            for (final String range : ranges) {
                final Matcher matcher = RANGE_PATTERN.matcher(range);
                if (!matcher.matches()) {
                    invalid = true;
                    break;
                }
            }
            return invalid;
        }

        @Override
        public void init() throws IOException {
            currentCount = 1;
            ranges.clear();
            writer.beginRecordSet();
            if (isRangeExpressionInvalid()) {
                throw new IOException(rangeExpression + " is not a valid range expression");
            }
            Integer startRange, endRange;
            if (StringUtils.isEmpty(rangeExpression)) {
                startRange = 0;
                endRange = Integer.MAX_VALUE;
                ranges.add(Range.of(startRange, endRange));
            } else {
                Matcher m = INTERVAL_PATTERN.matcher(rangeExpression);
                while (m.find()) {
                    // groupCount will be 3, need to check nulls to see if it's a range or single number. Groups that are all null are ignored
                    if (m.group(1) == null && m.group(2) == null && m.group(3) == null) {
                        continue;
                    }

                    if (m.group(1) != null) {
                        startRange = Integer.parseInt(m.group(1));
                    } else if ("-".equals(m.group(2))) {
                        startRange = 0;
                    } else {
                        startRange = null;
                    }
                    if (m.group(3) != null) {
                        endRange = Integer.parseInt(m.group(3));
                    } else if ("-".equals(m.group(2))) {
                        endRange = Integer.MAX_VALUE;
                    } else {
                        endRange = null;
                    }

                    final Range<Integer> range;

                    if (startRange != null && endRange == null) {
                        // Single value
                        range = Range.of(startRange, startRange);
                    } else {
                        range = Range.of(startRange, endRange);
                    }
                    ranges.add(range);
                }
            }
        }

        @Override
        public void sample(Record record) throws IOException {
            // Check the current record number against the specified ranges
            for (Range<Integer> range : ranges) {
                if (range.contains(currentCount)) {
                    writer.write(record);
                    break;
                }
            }
            currentCount++;
        }

        @Override
        public WriteResult finish() throws IOException {
            return writer.finishRecordSet();
        }
    }

    static class ProbabilisticSamplingStrategy implements SamplingStrategy {
        final RecordSetWriter writer;
        final int probabilityValue;
        final Random randomNumberGenerator;

        ProbabilisticSamplingStrategy(RecordSetWriter writer, int probabilityValue, Long randomSeed) {
            this.writer = writer;
            this.probabilityValue = probabilityValue;
            this.randomNumberGenerator = randomSeed == null ? new Random() : new Random(randomSeed);
        }

        @Override
        public void init() throws IOException {
            writer.beginRecordSet();
        }

        @Override
        public void sample(Record record) throws IOException {
            final int random = randomNumberGenerator.nextInt(100);
            if (random < probabilityValue) {
                writer.write(record);
            }
        }

        @Override
        public WriteResult finish() throws IOException {
            return writer.finishRecordSet();
        }
    }

    static class ReservoirSamplingStrategy implements SamplingStrategy {
        final RecordSetWriter writer;
        final int reservoirSize;
        final ArrayList<Record> reservoir;
        int currentCount = 0;
        final Random randomNumberGenerator;

        ReservoirSamplingStrategy(RecordSetWriter writer, int reservoirSize, Long randomSeed) {
            this.writer = writer;
            this.reservoirSize = reservoirSize;
            this.reservoir = new ArrayList<>(reservoirSize);
            this.randomNumberGenerator = randomSeed == null ? new Random() : new Random(randomSeed);
        }

        @Override
        public void init() throws IOException {
            currentCount = 0;
            writer.beginRecordSet();
        }

        @Override
        public void sample(Record record) {
            if (currentCount < reservoirSize) {
                reservoir.add(record);
            } else {
                // Use Algorithm R as we can't randomly access records (otherwise would use optimal Algorithm L).
                int j = randomNumberGenerator.nextInt(currentCount + 1);
                if (j < reservoirSize) {
                    reservoir.set(j, record);
                }
            }
            currentCount++;
        }

        @Override
        public WriteResult finish() throws IOException {
            for (Record record : reservoir) {
                writer.write(record);
            }
            return writer.finishRecordSet();
        }
    }
}
