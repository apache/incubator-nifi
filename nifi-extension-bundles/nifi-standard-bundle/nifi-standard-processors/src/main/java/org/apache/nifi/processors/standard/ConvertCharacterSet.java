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
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * This processor reads files in as text according to the specified character set and it outputs another text file according to the given character set. The character sets supported depend on the
 * version of the JRE and is platform specific. In addition, the JVM can be expanded with additional character sets to support. More information on which character sets are supported can be found in
 * the JDK documentation under the docs directory in the following path: ....\technotes\guides\intl\encoding.doc.html</p>
 *
 * <p>
 * The conversion process is very passive. For conversions that do not map perfectly the conversion will replace unmappable or unrecognized input using the '?' character.
 *
 * <p>
 * The following properties are required: <ul> <li><b>input.charset</b> - The character set of the original file contents</li> <li><b>output.charset</b> - The character set of the resulting file</li>
 * </ul> </p>
 *
 * <p>
 * The following properties are optional: <ul> <li><b>N/A</b> - </li> </ul>
 * </p>
 *
 * <p>
 * The following relationships are required: <ul> <li><b>success</b> - the id of the processor to transfer successfully converted files</li>
 * <li><b>failure</b> - the id of the processor to transfer unsuccessfully converted files</li> </ul> </p>
 */
@SideEffectFree
@InputRequirement(Requirement.INPUT_REQUIRED)
@SupportsBatching
@Tags({"text", "convert", "characterset", "character set"})
@CapabilityDescription("Converts a FlowFile's content from one character set to another")
public class ConvertCharacterSet extends AbstractProcessor {

    public static final PropertyDescriptor INPUT_CHARSET = new PropertyDescriptor.Builder()
            .name("Input Character Set")
            .description("The name of the CharacterSet to expect for Input")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .required(true)
            .build();
    public static final PropertyDescriptor OUTPUT_CHARSET = new PropertyDescriptor.Builder()
            .name("Output Character Set")
            .description("The name of the CharacterSet to convert to")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .required(true)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            INPUT_CHARSET,
            OUTPUT_CHARSET
    );

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("").build();

    private final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS
    );

    public static final int MAX_BUFFER_SIZE = 512 * 1024;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();

        final Charset inputCharset = Charset.forName(context.getProperty(INPUT_CHARSET).evaluateAttributeExpressions(flowFile).getValue());
        final Charset outputCharset = Charset.forName(context.getProperty(OUTPUT_CHARSET).evaluateAttributeExpressions(flowFile).getValue());
        final CharBuffer charBuffer = CharBuffer.allocate(MAX_BUFFER_SIZE);

        final CharsetDecoder decoder = inputCharset.newDecoder();
        decoder.onMalformedInput(CodingErrorAction.REPLACE);
        decoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
        decoder.replaceWith("?");

        final CharsetEncoder encoder = outputCharset.newEncoder();
        encoder.onMalformedInput(CodingErrorAction.REPLACE);
        encoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
        encoder.replaceWith("?".getBytes(outputCharset));

        try {
            final StopWatch stopWatch = new StopWatch(true);
            flowFile = session.write(flowFile, (rawIn, rawOut) -> {
                try (final BufferedReader reader = new BufferedReader(new InputStreamReader(rawIn, decoder), MAX_BUFFER_SIZE);
                        final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(rawOut, encoder), MAX_BUFFER_SIZE)) {
                    int charsRead;
                    while ((charsRead = reader.read(charBuffer)) != -1) {
                        charBuffer.flip();
                        writer.write(charBuffer.array(), 0, charsRead);
                    }

                    writer.flush();
                }
            });

            session.getProvenanceReporter().modifyContent(flowFile, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            logger.info("successfully converted characters from {} to {} for {}",
                    inputCharset, outputCharset, flowFile);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final Exception e) {
            throw new ProcessException(e);
        }
    }

}
