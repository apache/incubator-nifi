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
package org.apache.nifi.reporting.sink;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.migration.ProxyServiceMigration;
import org.apache.nifi.record.sink.RecordSinkService;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.reporting.s2s.SiteToSiteUtils;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;

@Tags({ "db", "s2s", "site", "record"})
@CapabilityDescription("Provides a service to write records using a configured RecordSetWriter over a Site-to-Site connection.")
public class SiteToSiteReportingRecordSink extends AbstractControllerService implements RecordSinkService {

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            RECORD_WRITER_FACTORY,
            SiteToSiteUtils.DESTINATION_URL,
            SiteToSiteUtils.PORT_NAME,
            SiteToSiteUtils.SSL_CONTEXT,
            SiteToSiteUtils.INSTANCE_URL,
            SiteToSiteUtils.COMPRESS,
            SiteToSiteUtils.TIMEOUT,
            SiteToSiteUtils.BATCH_SIZE,
            SiteToSiteUtils.TRANSPORT_PROTOCOL,
            SiteToSiteUtils.PROXY_CONFIGURATION_SERVICE
    );

    private volatile SiteToSiteClient siteToSiteClient;
    private volatile RecordSetWriterFactory writerFactory;
    private volatile StateManager stateManager;

    @Override
    protected void init(final ControllerServiceInitializationContext context) {
        this.stateManager = context.getStateManager();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void migrateProperties(final PropertyConfiguration config) {
        ProxyServiceMigration.migrateProxyProperties(config, SiteToSiteUtils.PROXY_CONFIGURATION_SERVICE,
                SiteToSiteUtils.OBSOLETE_PROXY_HOST, SiteToSiteUtils.OBSOLETE_PROXY_PORT, SiteToSiteUtils.OBSOLETE_PROXY_USERNAME, SiteToSiteUtils.OBSOLETE_PROXY_PASSWORD);
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        try {
            final ComponentLog logger = getLogger();
            siteToSiteClient = SiteToSiteUtils.getClient(context, logger, stateManager);

            writerFactory = context.getProperty(RECORD_WRITER_FACTORY).asControllerService(RecordSetWriterFactory.class);
        } catch (Exception e) {
            throw new InitializationException(e);
        }
    }

    @Override
    public WriteResult sendData(final RecordSet recordSet, final Map<String, String> attributes, final boolean sendZeroResults) throws IOException {
        Transaction transaction = null;
        try {
            WriteResult writeResult = null;
            transaction = getClient().createTransaction(TransferDirection.SEND);
            if (transaction == null) {
                getLogger().info("All destination nodes are penalized; will attempt to send data later");
            } else {
                final RecordSchema writeSchema = getWriterFactory().getSchema(null, recordSet.getSchema());
                final ByteArrayOutputStream out = new ByteArrayOutputStream();
                int recordCount = 0;
                try (final RecordSetWriter writer = getWriterFactory().createWriter(getLogger(), writeSchema, out, attributes)) {
                    writer.beginRecordSet();
                    Record record;
                    while ((record = recordSet.next()) != null) {
                        writer.write(record);
                    }
                    writeResult = writer.finishRecordSet();
                    recordCount = writeResult.getRecordCount();

                    attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                    attributes.put("record.count", Integer.toString(recordCount));
                    attributes.putAll(writeResult.getAttributes());
                }

                if (recordCount > 0 || sendZeroResults) {
                    transaction.send(out.toByteArray(), attributes);
                    transaction.confirm();
                    transaction.complete();
                }
            }
            return writeResult;
        } catch (Exception e) {
            if (transaction != null) {
                transaction.error();
            }
            if (e instanceof IOException) {
                throw (IOException) e;
            } else {
                throw new IOException("Failed to write metrics using record writer: " + e.getMessage(), e);
            }
        }
    }

    @OnDisabled
    public void stop() throws IOException {
        final SiteToSiteClient client = getClient();
        if (client != null) {
            client.close();
            this.siteToSiteClient = null;
        }
    }

    // this getter is intended explicitly for testing purposes
    protected SiteToSiteClient getClient() {
        return this.siteToSiteClient;
    }

    // this getter is intended explicitly for testing purposes
    protected RecordSetWriterFactory getWriterFactory() {
        return this.writerFactory;
    }
}
