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
package org.apache.nifi.processors.document;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.tika.Tika;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Tags({"extract, document, text"})
@CapabilityDescription("Extract text contents from supported binary document formats using Apache Tika")
public class ExtractDocumentText extends AbstractProcessor {
    private static final String TEXT_PLAIN = "text/plain";

    public static final Relationship REL_ORIGINAL = new Relationship.Builder().name("original")
            .description("Success for original input FlowFiles").build();

    public static final Relationship REL_EXTRACTED = new Relationship.Builder().name("extracted")
            .description("Success for extracted text FlowFiles").build();

    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("Content extraction failed").build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_ORIGINAL,
            REL_EXTRACTED,
            REL_FAILURE
    );

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        FlowFile extracted = session.create(flowFile);
        boolean error = false;
        try (InputStream is = session.read(flowFile);
             Reader tikaReader = new Tika().parse(is);
             OutputStream os = session.write(extracted);
             OutputStreamWriter writer = new OutputStreamWriter(os)) {
            IOUtils.copy(tikaReader, writer);
        } catch (final Throwable t) {
            error = true;
            getLogger().error("Extraction Failed {}", flowFile, t);
            session.remove(extracted);
            session.transfer(flowFile, REL_FAILURE);
        } finally {
            if (!error) {
                final Map<String, String> attributes = new HashMap<>();
                attributes.put(CoreAttributes.MIME_TYPE.key(), TEXT_PLAIN);
                extracted = session.putAllAttributes(extracted, attributes);
                session.transfer(extracted, REL_EXTRACTED);
                session.transfer(flowFile, REL_ORIGINAL);
            }
        }
    }
}
