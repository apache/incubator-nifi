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
package org.apache.nifi.record.sink;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSet;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

@Tags({"record", "sink", "log"})
@CapabilityDescription("Provides a RecordSinkService that can be used to log records to the application log (nifi-app.log, e.g.) using the specified writer for formatting.")
public class LoggingRecordSink extends AbstractControllerService implements RecordSinkService {

    public static final PropertyDescriptor LOG_LEVEL = new PropertyDescriptor.Builder()
            .name("logsink-log-level")
            .displayName("Log Level")
            .required(true)
            .description("The Log Level at which to log records (INFO, DEBUG, e.g.)")
            .allowableValues(LogLevel.values())
            .defaultValue(LogLevel.INFO.name())
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            RecordSinkService.RECORD_WRITER_FACTORY,
            LOG_LEVEL
    );

    private volatile RecordSetWriterFactory writerFactory;
    private volatile LogLevel logLevel;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        writerFactory = context.getProperty(RecordSinkService.RECORD_WRITER_FACTORY).asControllerService(RecordSetWriterFactory.class);
        logLevel = LogLevel.valueOf(context.getProperty(LOG_LEVEL).getValue());
    }

    @Override
    public WriteResult sendData(RecordSet recordSet, Map<String, String> attributes, boolean sendZeroResults) throws IOException {
        WriteResult writeResult;
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            ComponentLog log = getLogger();
            try (final RecordSetWriter writer = writerFactory.createWriter(getLogger(), recordSet.getSchema(), baos, attributes)) {
                writer.beginRecordSet();
                Record r;
                while ((r = recordSet.next()) != null) {
                    baos.reset();
                    writer.write(r);
                    writer.flush();
                    log.log(logLevel, "{}", baos);
                }
                writeResult = writer.finishRecordSet();
                writer.flush();
            }

        } catch (SchemaNotFoundException e) {
            throw new IOException(e);
        }
        return writeResult;
    }
}
