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
package org.apache.nifi.processors.gcp.pubsub.consume;

import com.google.pubsub.v1.ReceivedMessage;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

public class RecordStreamPubSubMessageConverter extends AbstractPubSubMessageConverter implements PubSubMessageConverter {

    public RecordStreamPubSubMessageConverter(
            final RecordReaderFactory readerFactory,
            final RecordSetWriterFactory writerFactory,
            final ComponentLog logger) {
        super(readerFactory, writerFactory, logger);
    }

    @Override
    protected Record getRecord(Record record, ReceivedMessage message) {
        return record;
    }

    @Override
    protected RecordSchema getRecordSchema(RecordSchema recordSchema) {
        return recordSchema;
    }
}
