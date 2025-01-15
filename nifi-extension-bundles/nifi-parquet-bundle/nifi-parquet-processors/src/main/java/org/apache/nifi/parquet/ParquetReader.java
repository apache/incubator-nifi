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
package org.apache.nifi.parquet;

import static org.apache.nifi.parquet.utils.ParquetUtils.applyCommonConfig;
import static org.apache.nifi.parquet.utils.ParquetUtils.createParquetConfig;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.parquet.record.ParquetRecordReader;
import org.apache.nifi.parquet.utils.ParquetConfig;
import org.apache.nifi.parquet.utils.ParquetUtils;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;

@Tags({"parquet", "parse", "record", "row", "reader"})
@CapabilityDescription("Parses Parquet data and returns each Parquet record as a separate Record object. " +
        "The schema will come from the Parquet data itself.")
public class ParquetReader extends AbstractControllerService implements RecordReaderFactory {

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            ParquetUtils.AVRO_READ_COMPATIBILITY
    );

    @Override
    public RecordReader createRecordReader(final Map<String, String> variables, final InputStream in, final long inputLength, final ComponentLog logger) throws IOException {
        final Configuration conf = new Configuration();
        final ParquetConfig parquetConfig = createParquetConfig(getConfigurationContext(), variables);
        applyCommonConfig(conf, parquetConfig);
        return new ParquetRecordReader(in, inputLength, conf, variables);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }
}
