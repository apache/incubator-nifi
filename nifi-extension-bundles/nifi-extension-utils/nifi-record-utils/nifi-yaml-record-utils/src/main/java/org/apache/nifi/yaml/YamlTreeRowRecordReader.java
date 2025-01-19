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

import org.apache.nifi.json.JsonTreeRowRecordReader;
import org.apache.nifi.json.SchemaApplicationStrategy;
import org.apache.nifi.json.StartingFieldStrategy;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.BiPredicate;

public class YamlTreeRowRecordReader extends JsonTreeRowRecordReader {

    private static final YamlParserFactory yamlParserFactory = new YamlParserFactory();

    public YamlTreeRowRecordReader(final InputStream in, final ComponentLog logger, final RecordSchema schema,
                                   final String dateFormat, final String timeFormat, final String timestampFormat) throws IOException, MalformedRecordException {
        this(in, logger, schema, dateFormat, timeFormat, timestampFormat, null, null, null, null);
    }

    public YamlTreeRowRecordReader(final InputStream in, final ComponentLog logger, final RecordSchema schema,
                                   final String dateFormat, final String timeFormat, final String timestampFormat,
                                   final StartingFieldStrategy startingFieldStrategy, final String startingFieldName,
                                   final SchemaApplicationStrategy schemaApplicationStrategy, final BiPredicate<String, String> captureFieldPredicate)
            throws IOException, MalformedRecordException {

        super(in, logger, schema, dateFormat, timeFormat, timestampFormat, startingFieldStrategy, startingFieldName, schemaApplicationStrategy,
                captureFieldPredicate, yamlParserFactory);
    }
}
