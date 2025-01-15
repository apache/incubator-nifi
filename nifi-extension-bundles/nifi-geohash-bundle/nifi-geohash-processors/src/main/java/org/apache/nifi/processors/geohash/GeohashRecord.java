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
package org.apache.nifi.processors.geohash;

import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.record.path.validation.RecordPathValidator;

import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.WriteResult;

import ch.hsr.geohash.GeoHash;
import ch.hsr.geohash.WGS84Point;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.Optional;

@SideEffectFree
@SupportsBatching
@Tags({"geo", "geohash", "record"})
@CapabilityDescription("A record-based processor that encodes and decodes Geohashes from and to latitude/longitude coordinates.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttributes({
        @WritesAttribute(attribute = "mime.type", description = "The MIME type indicated by the record writer"),
        @WritesAttribute(attribute = "record.count", description = "The number of records in the resulting flow file")
})
public class GeohashRecord extends AbstractProcessor {

    public enum ProcessingMode {
        ENCODE, DECODE
    }

    public enum GeohashFormat {
        BASE32, BINARY, LONG
    }

    public enum RoutingStrategy {
        SKIP,
        SPLIT,
        REQUIRE
    }

    public static final PropertyDescriptor MODE = new PropertyDescriptor.Builder()
            .name("mode")
            .displayName("Mode")
            .description("Specifies whether to encode latitude/longitude to geohash or decode geohash to latitude/longitude")
            .required(true)
            .allowableValues(ProcessingMode.values())
            .defaultValue(ProcessingMode.ENCODE.name())
            .build();

    public static final PropertyDescriptor ROUTING_STRATEGY = new PropertyDescriptor.Builder()
            .name("routing-strategy")
            .displayName("Routing Strategy")
            .description("Specifies how to route flowfiles after encoding or decoding being performed. "
                    + "SKIP will enrich those records that can be enriched and skip the rest. "
                    + "The SKIP strategy will route a flowfile to failure only if unable to parse the data. "
                    + "Otherwise, it will route the enriched flowfile to success, and the original input to original. "
                    + "SPLIT will separate the records that have been enriched from those that have not and send them to matched, while unenriched records will be sent to unmatched; "
                    + "the original input flowfile will be sent to original. The SPLIT strategy will route a flowfile to failure only if unable to parse the data. "
                    + "REQUIRE will route a flowfile to success only if all of its records are enriched, and the original input will be sent to original. "
                    + "The REQUIRE strategy will route the original input flowfile to failure if any of its records cannot be enriched or unable to be parsed")
            .required(true)
            .allowableValues(RoutingStrategy.values())
            .defaultValue(RoutingStrategy.SKIP.name())
            .build();

    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("Specifies the record reader service to use for reading incoming data")
            .required(true)
            .identifiesControllerService(RecordReaderFactory.class)
            .build();

    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("Specifies the record writer service to use for writing data")
            .required(true)
            .identifiesControllerService(RecordSetWriterFactory.class)
            .build();

    public static final PropertyDescriptor LATITUDE_RECORD_PATH = new PropertyDescriptor.Builder()
            .name("latitude-record-path")
            .displayName("Latitude Record Path")
            .description("In the ENCODE mode, this property specifies the record path to retrieve the latitude values. "
                    + "Latitude values should be in the range of [-90, 90]; invalid values will be logged at warn level. "
                    + "In the DECODE mode, this property specifies the record path to put the latitude value")
            .required(true)
            .addValidator(new RecordPathValidator())
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor LONGITUDE_RECORD_PATH = new PropertyDescriptor.Builder()
            .name("longitude-record-path")
            .displayName("Longitude Record Path")
            .description("In the ENCODE mode, this property specifies the record path to retrieve the longitude values; "
                    + "Longitude values should be in the range of [-180, 180]; invalid values will be logged at warn level. "
                    + "In the DECODE mode, this property specifies the record path to put the longitude value")
            .required(true)
            .addValidator(new RecordPathValidator())
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor GEOHASH_RECORD_PATH = new PropertyDescriptor.Builder()
            .name("geohash-record-path")
            .displayName("Geohash Record Path")
            .description("In the ENCODE mode, this property specifies the record path to put the geohash value; "
                    + "in the DECODE mode, this property specifies the record path to retrieve the geohash value")
            .required(true)
            .addValidator(new RecordPathValidator())
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor GEOHASH_FORMAT = new PropertyDescriptor.Builder()
            .name("geohash-format")
            .displayName("Geohash Format")
            .description("In the ENCODE mode, this property specifies the desired format for encoding geohash; "
                    + "in the DECODE mode, this property specifies the format of geohash provided")
            .required(true)
            .allowableValues(GeohashFormat.values())
            .defaultValue(GeohashFormat.BASE32.name())
            .build();

    public static final PropertyDescriptor GEOHASH_LEVEL = new PropertyDescriptor.Builder()
            .name("geohash-level")
            .displayName("Geohash Level")
            .description("The integer precision level(1-12) desired for encoding geohash")
            .required(true)
            .addValidator(StandardValidators.createLongValidator(1, 12, true))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .dependsOn(MODE, ProcessingMode.ENCODE.name())
            .build();

    public static final Relationship REL_NOT_MATCHED = new Relationship.Builder()
            .name("not matched")
            .description("Using the SPLIT strategy, flowfiles that cannot be encoded or decoded due to the lack of lat/lon or geohashes will be routed to not matched")
            .build();

    public static final Relationship REL_MATCHED = new Relationship.Builder()
            .name("matched")
            .description("Using the SPLIT strategy, flowfiles with lat/lon or geohashes provided that are successfully encoded or decoded will be routed to matched")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Flowfiles that cannot be encoded or decoded will be routed to failure")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Flowfiles that are successfully encoded or decoded will be routed to success")
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original input flowfile will be sent to this relationship")
            .build();

    private static final List<PropertyDescriptor> RECORD_PATH_PROPERTIES = List.of(
            LATITUDE_RECORD_PATH,
            LONGITUDE_RECORD_PATH,
            GEOHASH_RECORD_PATH
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_ORIGINAL,
            REL_FAILURE
    );

    private static final Set<Relationship> SPLIT_RELATIONSHIPS = Set.of(
            REL_MATCHED,
            REL_NOT_MATCHED,
            REL_ORIGINAL,
            REL_FAILURE
    );

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            MODE,
            RECORD_READER,
            RECORD_WRITER,
            ROUTING_STRATEGY,
            LATITUDE_RECORD_PATH,
            LONGITUDE_RECORD_PATH,
            GEOHASH_RECORD_PATH,
            GEOHASH_FORMAT,
            GEOHASH_LEVEL
    );

    private RoutingStrategyExecutor routingStrategyExecutor;
    private static boolean isSplit;
    private static Integer enrichedCount, unenrichedCount;

    private final RecordPathCache cache = new RecordPathCache(100);

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (descriptor.equals(ROUTING_STRATEGY)) {
            isSplit = RoutingStrategy.SPLIT.name().equals(newValue);
        }
    }

    @Override
    public Set<Relationship> getRelationships() {
        return isSplit ? SPLIT_RELATIONSHIPS : RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnScheduled
    public void setup(ProcessContext context) {
        final RoutingStrategy routingStrategy = RoutingStrategy.valueOf(context.getProperty(ROUTING_STRATEGY).getValue());
        switch (routingStrategy) {
            case REQUIRE:
                routingStrategyExecutor = new RequireRoutingStrategyExecutor();
                break;
            case SKIP:
                routingStrategyExecutor = new SkipRoutingStrategyExecutor();
                break;
            case SPLIT:
                routingStrategyExecutor = new SplitRoutingStrategyExecutor();
                break;
            default:
                throw new AssertionError();
        }
        enrichedCount = 0;
        unenrichedCount = 0;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile input = session.get();
        if (input == null) {
            return;
        }

        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        final boolean encode = context.getProperty(MODE).getValue().equalsIgnoreCase(ProcessingMode.ENCODE.toString());
        final RoutingStrategy routingStrategy = RoutingStrategy.valueOf(context.getProperty(ROUTING_STRATEGY).getValue());
        final GeohashFormat format = GeohashFormat.valueOf(context.getProperty(GEOHASH_FORMAT).getValue());

        FlowFile output = session.create(input);
        FlowFile notMatched = routingStrategy == RoutingStrategy.SPLIT ? session.create(input) : null;

        try (final InputStream is = session.read(input);
             final RecordReader reader = readerFactory.createRecordReader(input, is, getLogger());
             final OutputStream os = session.write(output);
             final OutputStream osNotFound = routingStrategy == RoutingStrategy.SPLIT ? session.write(notMatched) : null) {

            final RecordSetWriter writer = writerFactory.createWriter(getLogger(), writerFactory.getSchema(input.getAttributes(), reader.getSchema()), os, output);
            final RecordSetWriter notMatchedWriter = routingStrategy == RoutingStrategy.SPLIT ? writerFactory.createWriter(getLogger(), reader.getSchema(), osNotFound, notMatched) : null;

            Map<PropertyDescriptor, RecordPath> paths = new HashMap<>();
            for (PropertyDescriptor descriptor : RECORD_PATH_PROPERTIES) {
                String rawRecordPath = context.getProperty(descriptor).evaluateAttributeExpressions(input).getValue();
                RecordPath compiled = cache.getCompiled(rawRecordPath);
                paths.put(descriptor, compiled);
            }

            Record record;

            writer.beginRecordSet();

            if (notMatchedWriter != null) {
                notMatchedWriter.beginRecordSet();
            }

            int level = context.getProperty(GEOHASH_LEVEL).evaluateAttributeExpressions(input).asInteger();
            final String rawLatitudePath = context.getProperty(LATITUDE_RECORD_PATH).evaluateAttributeExpressions(input).getValue();
            RecordPath latitudePath = cache.getCompiled(rawLatitudePath);
            final String rawLongitudePath = context.getProperty(LONGITUDE_RECORD_PATH).evaluateAttributeExpressions(input).getValue();
            RecordPath longitudePath = cache.getCompiled(rawLongitudePath);
            final String rawGeohashPath = context.getProperty(GEOHASH_RECORD_PATH).evaluateAttributeExpressions(input).getValue();
            RecordPath geohashPath = cache.getCompiled(rawGeohashPath);

            while ((record = reader.nextRecord()) != null) {
                boolean updated = false;

                try {
                    if (encode) {
                        Object encodedGeohash = getEncodedGeohash(latitudePath, longitudePath, record, format, level);
                        updated = updateRecord(GEOHASH_RECORD_PATH, encodedGeohash, record, paths);
                    } else {
                        WGS84Point decodedPoint = getDecodedPointFromGeohash(geohashPath, record, format);
                        if (decodedPoint != null) {
                            updated = updateRecord(LATITUDE_RECORD_PATH, String.valueOf(decodedPoint.getLatitude()), record, paths)
                                    && updateRecord(LONGITUDE_RECORD_PATH, String.valueOf(decodedPoint.getLongitude()), record, paths);
                        }
                    }
                } catch (IllegalArgumentException e) {
                    //lat/lon/geohash values out of range or is not valid
                    getLogger().warn("Unable to {}", (encode ? "encode" : "decode"), e);
                }

                routingStrategyExecutor.writeFlowFiles(record, writer, notMatchedWriter, updated);
            }

            final WriteResult writeResult = writer.finishRecordSet();
            writer.close();
            output = session.putAllAttributes(output, buildAttributes(writeResult.getRecordCount(), writer.getMimeType(), writeResult));

            WriteResult notMatchedWriterResult;

            if (notMatchedWriter != null) {
                notMatchedWriterResult = notMatchedWriter.finishRecordSet();
                notMatchedWriter.close();
                if (notMatchedWriterResult.getRecordCount() > 0) {
                    notMatched = session.putAllAttributes(notMatched, buildAttributes(notMatchedWriterResult.getRecordCount(), writer.getMimeType(), notMatchedWriterResult));
                }
            }
        } catch (IOException | SchemaNotFoundException | MalformedRecordException e) {
            //cannot parse incoming data
            getLogger().error("Cannot parse the incoming data", e);
            session.remove(output);
            if (notMatched != null) {
                session.remove(notMatched);
            }
            session.transfer(input, REL_FAILURE);
            return;
        }

        //Transfer Flowfiles by routing strategy
        routingStrategyExecutor.transferFlowFiles(session, input, output, notMatched);
    }

    private interface RoutingStrategyExecutor {
        void writeFlowFiles(Record record, RecordSetWriter writer, RecordSetWriter notMatchedWriter, boolean updated) throws IOException;

        void transferFlowFiles(final ProcessSession session, FlowFile input, FlowFile output, FlowFile notMatched);
    }

    private class SkipRoutingStrategyExecutor implements RoutingStrategyExecutor {
        @Override
        public void writeFlowFiles(Record record, RecordSetWriter writer, RecordSetWriter notMatchedWriter, boolean updated) throws IOException {
            writer.write(record);
        }

        @Override
        public void transferFlowFiles(final ProcessSession session, FlowFile input, FlowFile output, FlowFile notMatched) {
            session.transfer(output, REL_SUCCESS);
            session.transfer(input, REL_ORIGINAL);
        }
    }

    private class SplitRoutingStrategyExecutor implements RoutingStrategyExecutor {
        @Override
        public void writeFlowFiles(Record record, RecordSetWriter writer, RecordSetWriter notMatchedWriter, boolean updated) throws IOException {
            if (updated) {
                enrichedCount++;
                writer.write(record);
            } else {
                unenrichedCount++;
                notMatchedWriter.write(record);
            }
        }

        @Override
        public void transferFlowFiles(final ProcessSession session, FlowFile input, FlowFile output, FlowFile notMatched) {
            if (unenrichedCount > 0) {
                session.transfer(notMatched, REL_NOT_MATCHED);
            } else {
                session.remove(notMatched);
            }
            if (enrichedCount > 0) {
                session.transfer(output, REL_MATCHED);
            } else {
                session.remove(output);
            }
            session.transfer(input, REL_ORIGINAL);
        }
    }

    private class RequireRoutingStrategyExecutor implements RoutingStrategyExecutor {
        @Override
        public void writeFlowFiles(Record record, RecordSetWriter writer, RecordSetWriter notMatchedWriter, boolean updated) throws IOException {
            if (updated) {
                writer.write(record);
            } else {
                unenrichedCount++;
            }
        }

        @Override
        public void transferFlowFiles(final ProcessSession session, FlowFile input, FlowFile output, FlowFile notMatched) {
            if (unenrichedCount > 0) {
                session.remove(output);
                getLogger().error("There exists some records that cannot be enriched or parsed. The original input flowfile is routed to failure using the REQUIRE strategy");
                session.transfer(input, REL_FAILURE);
            } else {
                session.transfer(output, REL_SUCCESS);
                session.transfer(input, REL_ORIGINAL);
            }
        }
    }

    private Object getEncodedGeohash(RecordPath latitudePath, RecordPath longitudePath, Record record, GeohashFormat format, int level) {
        RecordPathResult latitudeResult = latitudePath.evaluate(record);
        RecordPathResult longitudeResult = longitudePath.evaluate(record);
        Optional<FieldValue> latitudeField = latitudeResult.getSelectedFields().findFirst();
        Optional<FieldValue> longitudeField = longitudeResult.getSelectedFields().findFirst();

        if (latitudeField.isEmpty() || longitudeField.isEmpty()) {
            return null;
        }

        FieldValue latitudeValue = latitudeField.get();
        FieldValue longitudeValue = longitudeField.get();
        Object latitudeVal = latitudeValue.getValue();
        Object longitudeVal = longitudeValue.getValue();

        if (latitudeVal == null || longitudeVal == null) {
            return null;
        }

        double realLatValue = Double.parseDouble(latitudeVal.toString());
        double realLongValue = Double.parseDouble(longitudeVal.toString());
        GeoHash gh = GeoHash.withCharacterPrecision(realLatValue, realLongValue, level);

        return switch (format) {
            case BINARY -> gh.toBinaryString();
            case LONG -> gh.longValue();
            default -> gh.toBase32();
        };
    }

    private WGS84Point getDecodedPointFromGeohash(RecordPath geohashPath, Record record, GeohashFormat format) {
        RecordPathResult geohashResult = geohashPath.evaluate(record);
        Optional<FieldValue> geohashField = geohashResult.getSelectedFields().findFirst();

        if (geohashField.isEmpty()) {
            return null;
        }

        FieldValue geohashFieldValue = geohashField.get();
        Object geohashVal = geohashFieldValue.getValue();
        if (geohashVal == null) {
            return null;
        }

        String geohashString = geohashVal.toString();
        GeoHash decodedHash = switch (format) {
            case BINARY -> GeoHash.fromBinaryString(geohashString);
            case LONG -> {
                String binaryString = Long.toBinaryString(Long.parseLong(geohashString));
                yield GeoHash.fromBinaryString(binaryString);
            }
            default -> GeoHash.fromGeohashString(geohashString);
        };

        return decodedHash.getBoundingBoxCenter();
    }

    private boolean updateRecord(PropertyDescriptor descriptor, Object newValue, Record record, Map<PropertyDescriptor, RecordPath> cached) {
        if (!cached.containsKey(descriptor) || newValue == null) {
            return false;
        }
        RecordPath path = cached.get(descriptor);
        RecordPathResult result = path.evaluate(record);

        final Optional<FieldValue> fieldValueOption = result.getSelectedFields().findFirst();
        if (fieldValueOption.isEmpty()) {
            return false;
        }

        final FieldValue fieldValue = fieldValueOption.get();

        if (fieldValue.getParent().isEmpty() || fieldValue.getParent().get().getValue() == null) {
            return false;
        }

        fieldValue.updateValue(newValue);
        return true;
    }

    private Map<String, String> buildAttributes(int recordCount, String mimeType, WriteResult writeResult) {
        Map<String, String> retVal = new HashMap<>();
        retVal.put(CoreAttributes.MIME_TYPE.key(), mimeType);
        retVal.put("record.count", String.valueOf(recordCount));
        retVal.putAll(writeResult.getAttributes());
        return retVal;
    }
}