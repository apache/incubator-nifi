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
package org.apache.nifi.processors;

import com.maxmind.db.InvalidDatabaseException;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.AddressNotFoundException;
import com.maxmind.geoip2.model.CityResponse;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.Lock;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"geo", "enrich", "ip", "maxmind", "record"})
@CapabilityDescription("Looks up geolocation information for an IP address and adds the geo information to FlowFile attributes. The "
        + "geo data is provided as a MaxMind database. This version uses the NiFi Record API to allow large scale enrichment of record-oriented data sets. "
        + "Each field provided by the MaxMind database can be directed to a field of the user's choosing by providing a record path for that field configuration. ")
public class GeoEnrichIPRecord extends AbstractEnrichIP {
    public static final PropertyDescriptor READER = new PropertyDescriptor.Builder()
            .name("geo-enrich-ip-record-reader")
            .displayName("Record Reader")
            .description("Record reader service to use for reading the flowfile contents.")
            .required(true)
            .identifiesControllerService(RecordReaderFactory.class)
            .build();
    public static final PropertyDescriptor WRITER = new PropertyDescriptor.Builder()
            .name("geo-enrich-ip-record-writer")
            .displayName("Record Writer")
            .description("Record writer service to use for enriching the flowfile contents.")
            .required(true)
            .identifiesControllerService(RecordSetWriterFactory.class)
            .build();
    public static final PropertyDescriptor IP_RECORD_PATH = new PropertyDescriptor.Builder()
            .name("geo-enrich-ip-ip-record-path")
            .displayName("IP Address Record Path")
            .description("The record path to retrieve the IP address for doing the lookup.")
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();
    public static final PropertyDescriptor SPLIT_FOUND_NOT_FOUND = new PropertyDescriptor.Builder()
            .name("geo-enrich-ip-split-found-not-found")
            .displayName("Separate Enriched From Not Enriched")
            .description("Separate records that have been enriched from ones that have not. Default behavior is " +
                    "to send everything to the found relationship if even one record is enriched.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor GEO_CITY = new PropertyDescriptor.Builder()
            .name("geo-enrich-ip-city-record-path")
            .displayName("City Record Path")
            .description("Record path for putting the city identified for the IP address")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor GEO_LATITUDE = new PropertyDescriptor.Builder()
            .name("geo-enrich-ip-latitude-record-path")
            .displayName("Latitude Record Path")
            .description("Record path for putting the latitude identified for this IP address")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor GEO_LONGITUDE = new PropertyDescriptor.Builder()
            .name("geo-enrich-ip-longitude-record-path")
            .displayName("Longitude Record Path")
            .description("Record path for putting the longitude identified for this IP address")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor GEO_COUNTRY = new PropertyDescriptor.Builder()
            .name("geo-enrich-ip-country-record-path")
            .displayName("Country Record Path")
            .description("Record path for putting the country identified for this IP address")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor GEO_COUNTRY_ISO = new PropertyDescriptor.Builder()
            .name("geo-enrich-ip-country-iso-record-path")
            .displayName("Country ISO Code Record Path")
            .description("Record path for putting the ISO Code for the country identified")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor GEO_POSTAL_CODE = new PropertyDescriptor.Builder()
            .name("geo-enrich-ip-country-postal-record-path")
            .displayName("Country Postal Code Record Path")
            .description("Record path for putting the postal code for the country identified")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original input flowfile goes to this relationship regardless of whether the content was enriched or not.")
            .build();

    public static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_ORIGINAL,
            REL_FOUND,
            REL_NOT_FOUND
    );

    public static final List<PropertyDescriptor> GEO_PROPERTY_DESCRIPTORS = List.of(
            GEO_CITY,
            GEO_LATITUDE,
            GEO_LONGITUDE,
            GEO_COUNTRY,
            GEO_COUNTRY_ISO,
            GEO_POSTAL_CODE
    );

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            GEO_DATABASE_FILE,
            READER,
            WRITER,
            SPLIT_FOUND_NOT_FOUND,
            IP_RECORD_PATH,
            GEO_CITY,
            GEO_LATITUDE,
            GEO_LONGITUDE,
            GEO_COUNTRY,
            GEO_COUNTRY_ISO,
            GEO_POSTAL_CODE,
            LOG_LEVEL
    );

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    protected volatile RecordReaderFactory readerFactory;
    protected volatile RecordSetWriterFactory writerFactory;
    protected boolean splitOutput;

    @Override
    @OnScheduled
    public void onScheduled(ProcessContext context) throws IOException {
        super.onScheduled(context);

        readerFactory = context.getProperty(READER).asControllerService(RecordReaderFactory.class);
        writerFactory = context.getProperty(WRITER).asControllerService(RecordSetWriterFactory.class);
        splitOutput = context.getProperty(SPLIT_FOUND_NOT_FOUND).asBoolean();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile input = session.get();
        if (input == null) {
            return;
        }

        FlowFile output = session.create(input);
        FlowFile notFound = splitOutput ? session.create(input) : null;
        try {
            if (isNeedsReload() || getWatcher().checkAndReset()) {
                Lock dbWriteLock = getDbWriteLock();
                dbWriteLock.lock();
                try {
                    loadDatabaseFile();
                    setNeedsReload(false);
                } catch (InternalError | InvalidDatabaseException ie) {
                    // The database was likely changed out while being read, rollback and try again
                    setNeedsReload(true);
                    session.rollback();
                    return;
                } finally {
                    dbWriteLock.unlock();
                }
            }
        } catch (final IllegalStateException | IOException e) {
            throw new ProcessException(e.getMessage(), e);
        }
        DatabaseReader dbReader = databaseReaderRef.get();
        try (InputStream is = session.read(input);
             OutputStream os = session.write(output);
             OutputStream osNotFound = splitOutput ? session.write(notFound) : null) {
            RecordPathCache cache = new RecordPathCache(GEO_PROPERTY_DESCRIPTORS.size() + 1);
            Map<PropertyDescriptor, RecordPath> paths = new HashMap<>();
            for (PropertyDescriptor descriptor : GEO_PROPERTY_DESCRIPTORS) {
                if (!context.getProperty(descriptor).isSet()) {
                    continue;
                }
                String rawPath = context.getProperty(descriptor).evaluateAttributeExpressions(input).getValue();
                RecordPath compiled = cache.getCompiled(rawPath);
                paths.put(descriptor, compiled);
            }

            String rawIpPath = context.getProperty(IP_RECORD_PATH).evaluateAttributeExpressions(input).getValue();
            final MessageLogLevel logLevel = MessageLogLevel.valueOf(context.getProperty(LOG_LEVEL).evaluateAttributeExpressions(input).getValue().toUpperCase());

            RecordPath ipPath = cache.getCompiled(rawIpPath);

            RecordReader reader = readerFactory.createRecordReader(input, is, getLogger());
            RecordSchema schema = writerFactory.getSchema(input.getAttributes(), reader.getSchema());
            RecordSetWriter writer = writerFactory.createWriter(getLogger(), schema, os, input);
            RecordSetWriter notFoundWriter = splitOutput ? writerFactory.createWriter(getLogger(), schema, osNotFound, input) : null;
            Record record;
            Relationship targetRelationship = REL_NOT_FOUND;
            writer.beginRecordSet();

            if (notFoundWriter != null) {
                notFoundWriter.beginRecordSet();
            }

            int foundCount = 0;
            int notFoundCount = 0;
            while ((record = reader.nextRecord()) != null) {
                CityResponse response;
                response = geocode(ipPath, record, dbReader, logLevel);
                boolean wasEnriched = enrichRecord(response, record, paths);
                if (wasEnriched) {
                    targetRelationship = REL_FOUND;
                }
                if (!splitOutput || wasEnriched) {
                    writer.write(record);
                    foundCount++;
                } else {
                    notFoundWriter.write(record);
                    notFoundCount++;
                }
            }
            writer.finishRecordSet();
            writer.close();

            if (notFoundWriter != null) {
                notFoundWriter.finishRecordSet();
                notFoundWriter.close();
            }

            is.close();
            os.close();
            if (osNotFound != null) {
                osNotFound.close();
            }

            output = session.putAllAttributes(output, buildAttributes(foundCount, writer.getMimeType()));
            if (!splitOutput) {
                session.transfer(output, targetRelationship);
                session.remove(input);
            } else {
                if (notFoundCount > 0) {
                    notFound = session.putAllAttributes(notFound, buildAttributes(notFoundCount, writer.getMimeType()));
                    session.transfer(notFound, REL_NOT_FOUND);
                } else {
                    session.remove(notFound);
                }
                session.transfer(output, REL_FOUND);
                session.transfer(input, REL_ORIGINAL);
                session.getProvenanceReporter().modifyContent(notFound);
            }
            session.getProvenanceReporter().modifyContent(output);
        } catch (InvalidDatabaseException | InternalError idbe) {
            // The database was likely changed out while being read, rollback and try again
            setNeedsReload(true);
            getLogger().warn("Failure while trying to load enrichment data due to {}, rolling back session "
                    + "and will reload the database on the next run", idbe.getMessage());
            session.rollback();
            return;
        } catch (Exception ex) {
            getLogger().error("Error enriching records.", ex);
            session.rollback();
            context.yield();
        }
    }

    private Map<String, String> buildAttributes(int recordCount, String mimeType) {
        Map<String, String> retVal = new HashMap<>();
        retVal.put(CoreAttributes.MIME_TYPE.key(), mimeType);
        retVal.put("record.count", String.valueOf(recordCount));

        return retVal;
    }

    private CityResponse geocode(RecordPath ipPath, Record record, DatabaseReader reader, MessageLogLevel logLevel) throws Exception {
        RecordPathResult result = ipPath.evaluate(record);
        Optional<FieldValue> ipField = result.getSelectedFields().findFirst();
        if (ipField.isPresent()) {
            FieldValue value = ipField.get();
            Object val = value.getValue();
            if (val == null) {
                return null;
            }
            String realValue = val.toString();
            InetAddress address = InetAddress.getByName(realValue);

            try {
                return reader.city(address);
            } catch (AddressNotFoundException anfe) {

                switch (logLevel) {
                    case INFO:
                        getLogger().info("Address not found in the database", anfe);
                        break;
                    case WARN:
                        getLogger().warn("Address not found in the database", anfe);
                        break;
                    case ERROR:
                        getLogger().error("Address not found in the database", anfe);
                        break;
                    case DEBUG:
                    default:
                        getLogger().debug("Address not found in the database", anfe);
                        break;
                }

                return null;
            }
        } else {
            return null;
        }
    }

    private boolean enrichRecord(CityResponse response, Record record, Map<PropertyDescriptor, RecordPath> cached) {
        boolean retVal;

        if (response == null) {
            return false;
        } else if (response.getCity() == null) {
            return false;
        }

        boolean city = update(GEO_CITY, cached, record, response.getCity().getName());
        boolean country = update(GEO_COUNTRY, cached, record, response.getCountry().getName());
        boolean iso = update(GEO_COUNTRY_ISO, cached, record, response.getCountry().getIsoCode());
        boolean lat = update(GEO_LATITUDE, cached, record, response.getLocation().getLatitude());
        boolean lon = update(GEO_LONGITUDE, cached, record, response.getLocation().getLongitude());
        boolean postal = update(GEO_POSTAL_CODE, cached, record, response.getPostal().getCode());

        retVal = (city || country || iso || lat || lon || postal);

        return retVal;
    }

    private boolean update(PropertyDescriptor descriptor, Map<PropertyDescriptor, RecordPath> cached, Record record, Object fieldValue) {
        if (!cached.containsKey(descriptor) || fieldValue == null) {
            return false;
        }
        RecordPath cityPath = cached.get(descriptor);
        RecordPathResult result = cityPath.evaluate(record);
        FieldValue value = result.getSelectedFields().findFirst().get();

        if (value.getParent().get().getValue() == null) {
            return false;
        }

        value.updateValue(fieldValue.toString());

        return true;
    }
}
