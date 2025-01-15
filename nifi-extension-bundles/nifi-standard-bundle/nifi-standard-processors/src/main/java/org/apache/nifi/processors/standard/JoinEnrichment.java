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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.bin.Bin;
import org.apache.nifi.processor.util.bin.BinFiles;
import org.apache.nifi.processor.util.bin.BinManager;
import org.apache.nifi.processor.util.bin.BinProcessingResult;
import org.apache.nifi.processor.util.bin.EvictionReason;
import org.apache.nifi.processors.standard.enrichment.EnrichmentRole;
import org.apache.nifi.processors.standard.enrichment.InsertRecordFieldsJoinStrategy;
import org.apache.nifi.processors.standard.enrichment.RecordJoinInput;
import org.apache.nifi.processors.standard.enrichment.RecordJoinResult;
import org.apache.nifi.processors.standard.enrichment.RecordJoinStrategy;
import org.apache.nifi.processors.standard.enrichment.SqlJoinCache;
import org.apache.nifi.processors.standard.enrichment.SqlJoinStrategy;
import org.apache.nifi.processors.standard.enrichment.WrapperJoinStrategy;
import org.apache.nifi.record.path.validation.RecordPathValidator;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.util.db.JdbcProperties;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@TriggerWhenEmpty
@SideEffectFree
@Tags({"fork", "join", "enrichment", "record", "sql", "wrap", "recordpath", "merge", "combine", "streams"})
@CapabilityDescription("Joins together Records from two different FlowFiles where one FlowFile, the 'original' contains arbitrary records and the second FlowFile, the 'enrichment' contains " +
    "additional data that should be used to enrich the first. See Additional Details for more information on how to configure this processor and the different use cases that it aims to accomplish.")
@SeeAlso(ForkEnrichment.class)
@WritesAttributes({
    @WritesAttribute(attribute = "mime.type", description = "Sets the mime.type attribute to the MIME Type specified by the Record Writer"),
    @WritesAttribute(attribute = "record.count", description = "The number of records in the FlowFile")
})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SystemResourceConsideration(resource = SystemResource.MEMORY, description = "This Processor will load into heap all FlowFiles that are on its incoming queues. While it loads the FlowFiles " +
    "themselves, and not their content, the FlowFile attributes can be very memory intensive. Additionally, if the Join Strategy is set to SQL, the SQL engine may require buffering the entire " +
    "contents of the enrichment FlowFile for each concurrent task. See Processor's Additional Details for more details and for steps on how to mitigate these concerns.")
public class JoinEnrichment extends BinFiles {
    static final String GROUP_ID_ATTRIBUTE = "enrichment.group.id";
    static final String ENRICHMENT_ROLE_ATTRIBUTE = "enrichment.role";
    static final String RECORD_COUNT_ATTRIBUTE = "record.count";

    static final AllowableValue JOIN_WRAPPER = new AllowableValue("Wrapper", "Wrapper", "The output is a Record that contains two fields: (1) 'original', containing the Record from the original " +
        "FlowFile and (2) 'enrichment' containing the corresponding Record from the enrichment FlowFile. Records will be correlated based on their index in the FlowFile. If one FlowFile has more " +
        "Records than the other, a null value will be used.");
    static final AllowableValue JOIN_SQL = new AllowableValue("SQL", "SQL", "The output is derived by evaluating a SQL SELECT statement that allows for two tables: 'original' and 'enrichment'. This" +
        " allows for SQL JOIN statements to be used in order to correlate the Records of the two FlowFiles, so the index in which the Record is encountered in the FlowFile does not matter.");
    static final AllowableValue JOIN_INSERT_ENRICHMENT_FIELDS = new AllowableValue("Insert Enrichment Fields", "Insert Enrichment Fields",
        "The enrichment is joined together with the original FlowFile by placing all fields of the enrichment Record into the corresponding Record from the original FlowFile. " +
            "Records will be correlated based on their index in the FlowFile.");

    static final PropertyDescriptor ORIGINAL_RECORD_READER = new PropertyDescriptor.Builder()
        .name("Original Record Reader")
        .displayName("Original Record Reader")
        .description("The Record Reader for reading the 'original' FlowFile")
        .required(true)
        .identifiesControllerService(RecordReaderFactory.class)
        .build();
    static final PropertyDescriptor ENRICHMENT_RECORD_READER = new PropertyDescriptor.Builder()
        .name("Enrichment Record Reader")
        .displayName("Enrichment Record Reader")
        .description("The Record Reader for reading the 'enrichment' FlowFile")
        .required(true)
        .identifiesControllerService(RecordReaderFactory.class)
        .build();
    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
        .name("Record Writer")
        .displayName("Record Writer")
        .description("The Record Writer to use for writing the results. If the Record Writer is configured to inherit the schema from the Record, the schema that it will inherit will be the result " +
            "of merging both the 'original' record schema and the 'enrichment' record schema.")
        .required(true)
        .identifiesControllerService(RecordSetWriterFactory.class)
        .build();

    static final PropertyDescriptor JOIN_STRATEGY = new PropertyDescriptor.Builder()
        .name("Join Strategy")
        .displayName("Join Strategy")
        .description("Specifies how to join the two FlowFiles into a single FlowFile")
        .required(true)
        .allowableValues(JOIN_WRAPPER, JOIN_SQL, JOIN_INSERT_ENRICHMENT_FIELDS)
        .defaultValue(JOIN_WRAPPER.getValue())
        .build();
    static final PropertyDescriptor SQL = new PropertyDescriptor.Builder()
        .name("SQL")
        .displayName("SQL")
        .description("The SQL SELECT statement to evaluate. Expression Language may be provided, but doing so may result in poorer performance. Because this Processor is dealing with two FlowFiles " +
            "at a time, it's also important to understand how attributes will be referenced. If both FlowFiles have an attribute with the same name but different values, the Expression Language " +
            "will resolve to the value provided by the 'enrichment' FlowFile.")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .dependsOn(JOIN_STRATEGY, JOIN_SQL)
        .defaultValue("SELECT original.*, enrichment.* \nFROM original \nLEFT OUTER JOIN enrichment \nON original.id = enrichment.id")
        .build();
    static final PropertyDescriptor DEFAULT_PRECISION = new PropertyDescriptor.Builder()
        .fromPropertyDescriptor(JdbcProperties.DEFAULT_PRECISION)
        .required(false)
        .dependsOn(JOIN_STRATEGY, JOIN_SQL)
        .build();
    static final PropertyDescriptor DEFAULT_SCALE = new PropertyDescriptor.Builder()
        .fromPropertyDescriptor(JdbcProperties.DEFAULT_SCALE)
        .required(false)
        .dependsOn(JOIN_STRATEGY, JOIN_SQL)
        .build();
    static final PropertyDescriptor INSERTION_RECORD_PATH = new PropertyDescriptor.Builder()
        .name("Insertion Record Path")
        .displayName("Insertion Record Path")
        .description("Specifies where in the 'original' Record the 'enrichment' Record's fields should be inserted. Note that if the RecordPath " +
            "does not point to any existing field in the original Record, the enrichment will not be inserted.")
        .required(true)
        .addValidator(new RecordPathValidator())
        .defaultValue("/")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .dependsOn(JOIN_STRATEGY, JOIN_INSERT_ENRICHMENT_FIELDS)
        .build();

    static final PropertyDescriptor TIMEOUT = new PropertyDescriptor.Builder()
        .name("Timeout")
        .displayName("Timeout")
        .description("Specifies the maximum amount of time to wait for the second FlowFile once the first arrives at the processor, after which point the first " +
            "FlowFile will be routed to the 'timeout' relationship.")
        .required(true)
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .defaultValue("10 min")
        .build();
    static final PropertyDescriptor MAX_BIN_COUNT = new PropertyDescriptor.Builder()
        .fromPropertyDescriptor(BinFiles.MAX_BIN_COUNT)
        .defaultValue("10000")
        .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            ORIGINAL_RECORD_READER,
            ENRICHMENT_RECORD_READER,
            RECORD_WRITER,
            JOIN_STRATEGY,
            SQL,
            DEFAULT_PRECISION,
            DEFAULT_SCALE,
            INSERTION_RECORD_PATH,
            MAX_BIN_COUNT,
            TIMEOUT
    );

    // Relationships
    static final Relationship REL_JOINED = new Relationship.Builder()
        .name("joined")
        .description("The resultant FlowFile with Records joined together from both the original and enrichment FlowFiles will be routed to this relationship")
        .build();
    static final Relationship REL_ORIGINAL = new Relationship.Builder()
        .name("original")
        .description("Both of the incoming FlowFiles ('original' and 'enrichment') will be routed to this Relationship. I.e., this is the 'original' version of both of these FlowFiles.")
        .autoTerminateDefault(true)
        .build();
    static final Relationship REL_TIMEOUT = new Relationship.Builder()
        .name("timeout")
        .description("If one of the incoming FlowFiles (i.e., the 'original' FlowFile or the 'enrichment' FlowFile) arrives to this Processor but the other does not arrive within the configured " +
            "Timeout period, the FlowFile that did arrive is routed to this relationship.")
        .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("If both the 'original' and 'enrichment' FlowFiles arrive at the processor but there was a failure in joining the records, both of those FlowFiles will be routed to this " +
            "relationship.")
        .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_JOINED,
            REL_ORIGINAL,
            REL_TIMEOUT,
            REL_FAILURE
    );

    private final SqlJoinCache sqlJoinCache = new SqlJoinCache(getLogger());

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnStopped
    public synchronized void cleanup() throws Exception {
        sqlJoinCache.close();
    }

    // No-op
    @Override
    protected FlowFile preprocessFlowFile(final ProcessContext context, final ProcessSession session, final FlowFile flowFile) {
        return flowFile;
    }

    @Override
    protected String getGroupId(final ProcessContext context, final FlowFile flowFile, final ProcessSession session) {
        return flowFile.getAttribute(GROUP_ID_ATTRIBUTE);
    }

    // No-op
    @Override
    protected void setUpBinManager(final BinManager binManager, final ProcessContext context) {
    }

    // We expect to always have exactly 2 entries ('original' FlowFile and 'enrichment' FlowFile).
    @Override
    protected int getMinEntries(final PropertyContext context) {
        return 2;
    }

    // We expect to always have exactly 2 entries ('original' FlowFile and 'enrichment' FlowFile).
    @Override
    protected int getMaxEntries(final PropertyContext context) {
        return 2;
    }

    // We will not constrain bin sizes on byte counts.
    @Override
    protected long getMinBytes(final PropertyContext context) {
        return 0L;
    }

    // We will not constrain bin sizes on byte counts.
    @Override
    protected long getMaxBytes(final PropertyContext context) {
        return Long.MAX_VALUE;
    }

    @Override
    protected int getMaxBinAgeSeconds(final PropertyContext context) {
        return context.getProperty(TIMEOUT).asTimePeriod(TimeUnit.SECONDS).intValue();
    }

    @Override
    protected BinProcessingResult processBin(final Bin unmodifiableBin, final ProcessContext context) throws ProcessException {
        final ProcessSession session = unmodifiableBin.getSession();
        final List<FlowFile> flowFiles = unmodifiableBin.getContents();

        // We should receive either 1 FlowFile (in case of timeout) or 2 FlowFiles (max bin entries). Check for timeouts.
        if (flowFiles.size() != 2) {
            if (unmodifiableBin.getEvictionReason() == EvictionReason.BIN_MANAGER_FULL) {
                session.transfer(flowFiles);
                session.commitAsync();
                return new BinProcessingResult(true);
            }

            session.transfer(flowFiles, REL_TIMEOUT);

            final FlowFile flowFile = flowFiles.getFirst();
            final EnrichmentRole role = getEnrichmentRole(flowFile);
            final String missingType = (role == null) ? "other" : getOtherEnrichmentRole(role).name();
            getLogger().warn("Timed out waiting for the {} FlowFile to match {}; routing to {}", missingType, flowFiles.getFirst(), REL_TIMEOUT.getName());
            session.commitAsync();

            return new BinProcessingResult(true);
        }

        // Ensure that we receive both an 'original' and an 'enrichment' flowfile. This ensures that our attributes are properly aligned.
        final FlowFile original = getFlowFileWithRole(flowFiles, EnrichmentRole.ORIGINAL);
        final FlowFile enrichment = getFlowFileWithRole(flowFiles, EnrichmentRole.ENRICHMENT);

        if (original == null || enrichment == null) {
            getLogger().error("Received two FlowFiles {} but could not find both an 'original' and an 'enrichment' FlowFile. The FlowFiles do not appear to have the proper attributes necessary for " +
                "use with this Processor. Routing to {}", flowFiles, REL_FAILURE);
            return transferFailure(flowFiles, session);
        }

        // Combine FlowFile attributes into a single Map, preferring the 'enrichment' FlowFile's attributes over the 'original' attributes in case of conflicts.
        final Map<String, String> combinedAttributes = new HashMap<>(original.getAttributes());
        combinedAttributes.putAll(enrichment.getAttributes());

        // Create the Record Readers/Writers and necessary schemas
        final RecordReaderFactory originalRecordReaderFactory = context.getProperty(ORIGINAL_RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordReaderFactory enrichmentRecordReaderFactory = context.getProperty(ENRICHMENT_RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        final RecordSchema originalSchema;
        try {
            originalSchema = getReaderSchema(originalRecordReaderFactory, original, session);
        } catch (final Exception e) {
            getLogger().error("Failed to determine Record Schema from 'original' FlowFile {}; routing to failure", original, e);
            return transferFailure(flowFiles, session);
        }

        final RecordSchema enrichmentSchema;
        try {
            enrichmentSchema = getReaderSchema(enrichmentRecordReaderFactory, enrichment, session);
        } catch (final Exception e) {
            getLogger().error("Failed to determine Record Schema from 'enrichment' FlowFile {}; routing to failure", original, e);
            return transferFailure(flowFiles, session);
        }

        // Merge the 'original' and 'enrichment' schemas into a combined schema.
        final RecordSchema combinedSchema = DataTypeUtils.merge(originalSchema, enrichmentSchema);

        // Determine the RecordWriter schema
        final RecordSchema writerSchema;
        try {
            writerSchema = writerFactory.getSchema(combinedAttributes, combinedSchema);
        } catch (final Exception e) {
            getLogger().error("Failed to determine Record Schema for the Record Writer from 'original' FlowFile {} and 'enrichment' FLowFile {}; routing to failure", original, enrichment, e);
            return transferFailure(flowFiles, session);
        }

        final RecordJoinInput originalInput = new RecordJoinInput(original, originalRecordReaderFactory, originalSchema);
        final RecordJoinInput enrichmentInput = new RecordJoinInput(enrichment, enrichmentRecordReaderFactory, enrichmentSchema);

        // Get the appropriate strategy for joining the FlowFiles
        final RecordJoinStrategy joinStrategy = getJoinStrategy(context, combinedAttributes);

        // Create a RecordSet that will be used to produce the Records we need.
        final WriteResult writeResult;
        final String mimeType;
        FlowFile output;
        try (final RecordJoinResult result = joinStrategy.join(originalInput, enrichmentInput, combinedAttributes, session, writerSchema)) {
            // Create output FlowFile
            output = session.create(flowFiles);

            // Create the RecordSetWriter and write the RecordSet. This will allow us to iterate over the Records in the RecordSet in a 'pull'-like manner, in much the same
            // way that the Java Stream class works.
            try (final OutputStream out = session.write(output)) {
                final RecordSet resultRecordSet = result.getRecordSet();
                final RecordSchema rsSchema = resultRecordSet.getSchema();

                try (final RecordSetWriter writer = writerFactory.createWriter(getLogger(), rsSchema, out, combinedAttributes)) {
                    writeResult = writer.write(resultRecordSet);
                    mimeType = writer.getMimeType();
                }
            }
        } catch (final Exception e) {
            getLogger().error("Failed to join 'original' FlowFile {} and 'enrichment' FlowFile {}; routing to failure", original, enrichment, e);
            return transferFailure(flowFiles, session);
        }

        // Create attributes for outbound FlowFile
        final Map<String, String> writeAttributes = new HashMap<>(writeResult.getAttributes());
        final int joinedCount = writeResult.getRecordCount();
        writeAttributes.put(RECORD_COUNT_ATTRIBUTE, String.valueOf(joinedCount));
        writeAttributes.put(CoreAttributes.MIME_TYPE.key(), mimeType);
        session.putAllAttributes(output, writeAttributes);

        session.adjustCounter("Records Written", joinedCount, false);
        session.transfer(output, REL_JOINED);

        // We do not transfer original FlowFiles or commit the session - let parent class handle that, so return a BinProcessingResult with a 'false' for isCommitted.
        return new BinProcessingResult(false);
    }

    private RecordSchema getReaderSchema(final RecordReaderFactory recordReaderFactory, final FlowFile flowFile, final ProcessSession session)
            throws IOException, MalformedRecordException, SchemaNotFoundException {

        try (final InputStream rawIn = session.read(flowFile)) {
            final Map<String, String> enrichmentAttributes = flowFile.getAttributes();
            final RecordReader reader = recordReaderFactory.createRecordReader(enrichmentAttributes, rawIn, flowFile.getSize(), getLogger());
            return reader.getSchema();
        }
    }

    private BinProcessingResult transferFailure(final List<FlowFile> flowFiles, final ProcessSession session) {
        session.transfer(flowFiles, REL_FAILURE);
        session.commitAsync();
        return new BinProcessingResult(true);
    }

    private RecordJoinStrategy getJoinStrategy(final ProcessContext context, final Map<String, String> attributes) {
        final String strategyName = context.getProperty(JOIN_STRATEGY).getValue();
        if (strategyName.equalsIgnoreCase(JOIN_SQL.getValue())) {
            final PropertyValue sqlPropertyValue = context.getProperty(SQL);
            final int defaultPrecision = context.getProperty(DEFAULT_PRECISION).evaluateAttributeExpressions(attributes).asInteger();
            final int defaultScale = context.getProperty(DEFAULT_SCALE).evaluateAttributeExpressions(attributes).asInteger();
            return new SqlJoinStrategy(sqlJoinCache, sqlPropertyValue, getLogger(), defaultPrecision, defaultScale);
        } else if (strategyName.equalsIgnoreCase(JOIN_WRAPPER.getValue())) {
            return new WrapperJoinStrategy(getLogger());
        } else if (strategyName.equalsIgnoreCase(JOIN_INSERT_ENRICHMENT_FIELDS.getValue())) {
            final String recordPath = context.getProperty(INSERTION_RECORD_PATH).evaluateAttributeExpressions(attributes).getValue();
            return new InsertRecordFieldsJoinStrategy(getLogger(), recordPath);
        }

        throw new ProcessException("Invalid Join Strategy: " + strategyName);
    }

    private FlowFile getFlowFileWithRole(final Collection<FlowFile> flowFiles, final EnrichmentRole desiredRole) {
        for (final FlowFile flowFile : flowFiles) {
            final EnrichmentRole role = getEnrichmentRole(flowFile);
            if (role == desiredRole) {
                return flowFile;
            }
        }

        return null;
    }

    private EnrichmentRole getEnrichmentRole(final FlowFile flowFile) {
        final String role = flowFile.getAttribute(ENRICHMENT_ROLE_ATTRIBUTE);
        if (role == null) {
            return EnrichmentRole.UNKNOWN;
        }

        try {
            return EnrichmentRole.valueOf(role);
        } catch (final Exception e) {
            return EnrichmentRole.UNKNOWN;
        }
    }

    private EnrichmentRole getOtherEnrichmentRole(final EnrichmentRole role) {
        return switch (role) {
            case ENRICHMENT -> EnrichmentRole.ORIGINAL;
            case ORIGINAL -> EnrichmentRole.ENRICHMENT;
            case UNKNOWN -> EnrichmentRole.UNKNOWN;
            case null -> null;
        };
    }

    @Override
    protected int processBins(final ProcessContext context, final ProcessSessionFactory sessionFactory) {
        // If there is only a single ready bin, process in the same way that our parent class does.
        // However, because this Processor uses tiny bins, if we have many ready we can significantly benefit
        // by creating a new ProcessSession and batching together the processing of many bins into one.
        final Queue<Bin> readyBins = getReadyBins();
        if (readyBins.size() <= 1) {
            return super.processBins(context, sessionFactory);
        }

        final ComponentLog logger = getLogger();
        int binsProcessed = 0;
        long bytesProcessed = 0L;

        final ProcessSession batchSession = sessionFactory.createSession();

        Bin bin;
        while (isScheduled() && (bin = readyBins.poll()) != null) {
            long binBytes = 0L;
            for (final FlowFile flowFile : bin.getContents()) {
                binBytes += flowFile.getSize();
            }

            BinProcessingResult binProcessingResult;
            try {
                binProcessingResult = this.processBin(bin, context);
            } catch (final ProcessException e) {
                logger.error("Failed to process bundle of {} files", bin.getContents().size(), e);

                final ProcessSession binSession = bin.getSession();
                for (final FlowFile flowFile : bin.getContents()) {
                    binSession.transfer(flowFile, REL_FAILURE);
                }
                binSession.commitAsync();
                continue;
            } catch (final Exception e) {
                logger.error("Rolling back sessions since failed to process bundle of {} files", bin.getContents().size(), e);

                bin.getSession().rollback();
                continue;
            }

            // If this bin's session has been committed, move on.
            if (!binProcessingResult.isCommitted()) {
                final ProcessSession binSession = bin.getSession();
                if (!context.isAutoTerminated(REL_ORIGINAL)) {
                    bin.getContents().forEach(ff -> binSession.putAllAttributes(ff, binProcessingResult.getAttributes()));
                }
                binSession.transfer(bin.getContents(), REL_ORIGINAL);

                // Migrate FlowFiles to our batch session. Then commit the bin session to free up any resources
                binSession.migrate(batchSession);
                binSession.commitAsync();
            }

            binsProcessed++;
            bytesProcessed += binBytes;

            // Every 100 bins or 5 MB of input, commit the session
            if (binsProcessed % 100 == 0 || bytesProcessed > 5_000_000L) {
                batchSession.commitAsync();
                break;
            }
        }

        batchSession.commitAsync();
        return binsProcessed;
    }
}
