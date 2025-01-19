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

package org.apache.nifi.yaml;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.json.JsonTreeRowRecordReader;
import org.apache.nifi.json.TokenParserFactory;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

@Tags({"yaml", "tree", "record", "reader", "parser"})
@CapabilityDescription("Parses YAML into individual Record objects. While the reader expects each record "
        + "to be well-formed YAML, the content of a FlowFile may consist of many records, each as a well-formed "
        + "YAML array or YAML object. "
        + "If an array is encountered, each element in that array will be treated as a separate record. "
        + "If the schema that is configured contains a field that is not present in the YAML, a null value will be used. If the YAML contains "
        + "a field that is not present in the schema, that field will be skipped. "
        + "Please note this controller service does not support resolving the use of YAML aliases. Any alias present will be treated as a string. "
        + "See the Usage of the Controller Service for more information and examples.")
public class YamlTreeReader extends JsonTreeReader {

    private static final boolean ALLOW_COMMENTS_DISABLED = false;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return new ArrayList<>(super.getSupportedPropertyDescriptors());
    }

    @Override
    protected TokenParserFactory createTokenParserFactory(final ConfigurationContext context) {
        return new YamlParserFactory(buildStreamReadConstraints(context), isAllowCommentsEnabled(context));
    }

    @Override
    protected JsonTreeRowRecordReader createJsonTreeRowRecordReader(InputStream in, ComponentLog logger, RecordSchema schema) throws IOException, MalformedRecordException {
        return new YamlTreeRowRecordReader(in, logger, schema, dateFormat, timeFormat, timestampFormat, startingFieldStrategy, startingFieldName,
                schemaApplicationStrategy, null);
    }

    @Override
    protected boolean isAllowCommentsEnabled(final ConfigurationContext context) {
        return ALLOW_COMMENTS_DISABLED;
    }
}
