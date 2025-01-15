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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.krakens.grok.api.Grok;
import io.krakens.grok.api.GrokCompiler;
import io.krakens.grok.api.Match;
import io.krakens.grok.api.exception.GrokException;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StopWatch;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@SupportsBatching
@SideEffectFree
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"grok", "log", "text", "parse", "delimit", "extract"})
@CapabilityDescription("Evaluates one or more Grok Expressions against the content of a FlowFile, " +
    "adding the results as attributes or replacing the content of the FlowFile with a JSON " +
    "notation of the matched content")
@WritesAttributes({
    @WritesAttribute(attribute = "grok.XXX", description = "When operating in flowfile-attribute mode, each of the Grok identifier that is matched in the flowfile " +
        "will be added as an attribute, prefixed with \"grok.\" For example," +
        "if the grok identifier \"timestamp\" is matched, then the value will be added to an attribute named \"grok.timestamp\"")})
@Restricted(
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.REFERENCE_REMOTE_RESOURCES,
                        explanation = "Patterns can reference resources over HTTP"
                )
        }
)
public class ExtractGrok extends AbstractProcessor {

    public static final String FLOWFILE_ATTRIBUTE = "flowfile-attribute";
    public static final String FLOWFILE_CONTENT = "flowfile-content";
    private static final String APPLICATION_JSON = "application/json";
    private static final String DEFAULT_PATTERN_NAME = "/default-grok-patterns.txt";

    public static final PropertyDescriptor GROK_EXPRESSION = new PropertyDescriptor.Builder()
        .name("Grok Expression")
        .description("Grok expression. If other Grok expressions are referenced in this expression, they must be provided "
            + "in the Grok Pattern File if set or exist in the default Grok patterns")
        .required(true)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .build();

    public static final PropertyDescriptor GROK_PATTERNS = new PropertyDescriptor.Builder()
        .name("Grok Pattern file")
        .displayName("Grok Patterns")
        .description("Custom Grok pattern definitions. These definitions will be loaded after the default Grok "
            + "patterns. The Grok Parser will use the default Grok patterns when this property is not configured.")
        .required(false)
        .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE, ResourceType.TEXT, ResourceType.URL)
        .build();

    public static final PropertyDescriptor KEEP_EMPTY_CAPTURES = new PropertyDescriptor.Builder()
            .name("Keep Empty Captures")
            .description("If true, then empty capture values will be included in the returned capture map.")
            .required(true)
            .defaultValue("true")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor DESTINATION = new PropertyDescriptor.Builder()
        .name("Destination")
        .description("Control if Grok output value is written as a new flowfile attributes, in this case " +
            "each of the Grok identifier that is matched in the flowfile will be added as an attribute, " +
            "prefixed with \"grok.\" or written in the flowfile content. Writing to flowfile content " +
            "will overwrite any existing flowfile content.")
        .required(true)
        .allowableValues(FLOWFILE_ATTRIBUTE, FLOWFILE_CONTENT)
        .defaultValue(FLOWFILE_ATTRIBUTE)
        .build();

    public static final PropertyDescriptor CHARACTER_SET = new PropertyDescriptor.Builder()
        .name("Character Set")
        .description("The Character Set in which the file is encoded")
        .required(true)
        .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
        .defaultValue("UTF-8")
        .build();

    public static final PropertyDescriptor MAX_BUFFER_SIZE = new PropertyDescriptor.Builder()
        .name("Maximum Buffer Size")
        .description("Specifies the maximum amount of data to buffer (per file) in order to apply the Grok expressions. Files larger than the specified maximum will not be fully evaluated.")
        .required(true)
        .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
        .addValidator(StandardValidators.createDataSizeBoundsValidator(0, Integer.MAX_VALUE))
        .defaultValue("1 MB")
        .build();

    public static final PropertyDescriptor NAMED_CAPTURES_ONLY = new PropertyDescriptor.Builder()
        .name("Named captures only")
        .description("Only store named captures from grok")
        .required(true)
        .allowableValues("true", "false")
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .defaultValue("false")
        .build();

    private final static List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            GROK_EXPRESSION,
            GROK_PATTERNS,
            DESTINATION,
            CHARACTER_SET,
            MAX_BUFFER_SIZE,
            NAMED_CAPTURES_ONLY,
            KEEP_EMPTY_CAPTURES
    );

    public static final Relationship REL_MATCH = new Relationship.Builder()
            .name("matched")
            .description("FlowFiles are routed to this relationship when the Grok Expression is successfully evaluated and the FlowFile is modified as a result")
            .build();

    public static final Relationship REL_NO_MATCH = new Relationship.Builder()
            .name("unmatched")
            .description("FlowFiles are routed to this relationship when no provided Grok Expression matches the content of the FlowFile")
            .build();

    private final static Set<Relationship> RELATIONSHIPS = Set.of(
            REL_MATCH,
            REL_NO_MATCH
    );

    private volatile Grok grok;
    private final BlockingQueue<byte[]> bufferQueue = new LinkedBlockingQueue<>();

    private final AtomicBoolean keepEmptyCaputures = new AtomicBoolean(true);

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnStopped
    public void onStopped() {
        bufferQueue.clear();
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        Collection<ValidationResult> problems = new ArrayList<>();

        // validate the grok expression against configuration
        // if there is a GROK_PATTERN_FILE set we must be sure to register that so that it's
        // patterns will be available to compile()
        // we also have to make sure the default grok patterns are loaded
        boolean namedCaptures = false;
        if (validationContext.getProperty(NAMED_CAPTURES_ONLY).isSet()) {
            namedCaptures = validationContext.getProperty(NAMED_CAPTURES_ONLY).asBoolean();
        }

        GrokCompiler grokCompiler = GrokCompiler.newInstance();
        String subject = GROK_EXPRESSION.getName();
        String input = validationContext.getProperty(GROK_EXPRESSION).getValue();

        try {

            try (final InputStream defaultPatterns = getClass().getResourceAsStream(DEFAULT_PATTERN_NAME)) {
                grokCompiler.register(defaultPatterns);
            }

            if (validationContext.getProperty(GROK_PATTERNS).isSet()) {
                try (final InputStream patterns = validationContext.getProperty(GROK_PATTERNS).asResource().read()) {
                    grokCompiler.register(patterns);
                }
            }
            grok = grokCompiler.compile(input, namedCaptures);
        } catch (final Exception e) {
            problems.add(new ValidationResult.Builder()
                    .subject(subject)
                    .input(input)
                    .valid(false)
                    .explanation("Not a valid Grok Expression - " + e.getMessage())
                    .build());
            return problems;
        }

        problems.add(new ValidationResult.Builder().subject(subject).input(input).valid(true).build());
        return problems;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws GrokException, IOException {

        keepEmptyCaputures.set(context.getProperty(KEEP_EMPTY_CAPTURES).asBoolean());

        for (int i = 0; i < context.getMaxConcurrentTasks(); i++) {
            final int maxBufferSize = context.getProperty(MAX_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
            final byte[] buffer = new byte[maxBufferSize];
            bufferQueue.add(buffer);
        }

        final GrokCompiler grokCompiler = GrokCompiler.newInstance();

        try (final InputStream defaultPatterns = getClass().getResourceAsStream(DEFAULT_PATTERN_NAME)) {
            grokCompiler.register(defaultPatterns);
        }

        if (context.getProperty(GROK_PATTERNS).isSet()) {
            try (final InputStream patterns = context.getProperty(GROK_PATTERNS).asResource().read()) {
                grokCompiler.register(patterns);
            }
        }
        grok = grokCompiler.compile(context.getProperty(GROK_EXPRESSION).getValue(), context.getProperty(NAMED_CAPTURES_ONLY).asBoolean());

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        final StopWatch stopWatch = new StopWatch(true);
        final Charset charset = Charset.forName(context.getProperty(CHARACTER_SET).getValue());
        final String contentString;
        byte[] buffer = bufferQueue.poll();
        if (buffer == null) {
            final int maxBufferSize = context.getProperty(MAX_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
            buffer = new byte[maxBufferSize];
        }

        try {
            final byte[] byteBuffer = buffer;
            session.read(flowFile, in -> StreamUtils.fillBuffer(in, byteBuffer, false));
            final long len = Math.min(byteBuffer.length, flowFile.getSize());
            contentString = new String(byteBuffer, 0, (int) len, charset);
        } finally {
            bufferQueue.offer(buffer);
        }

        final Match gm = grok.match(contentString);
        gm.setKeepEmptyCaptures(keepEmptyCaputures.get());
        final Map<String, Object> captureMap = gm.capture();

        if (captureMap.isEmpty()) {
            session.transfer(flowFile, REL_NO_MATCH);
            getLogger().info("Did not match any Grok Expressions for FlowFile {}", flowFile);
            return;
        }

        final ObjectMapper objectMapper = new ObjectMapper();
        switch (context.getProperty(DESTINATION).getValue()) {
            case FLOWFILE_ATTRIBUTE:
                Map<String, String> grokResults = new HashMap<>();
                for (Map.Entry<String, Object> entry : captureMap.entrySet()) {
                    if (null != entry.getValue()) {
                        grokResults.put("grok." + entry.getKey(), entry.getValue().toString());
                    }
                }

                flowFile = session.putAllAttributes(flowFile, grokResults);
                session.getProvenanceReporter().modifyAttributes(flowFile);
                session.transfer(flowFile, REL_MATCH);
                getLogger().info("Matched {} Grok Expressions and added attributes to FlowFile {}", grokResults.size(), flowFile);

                break;
            case FLOWFILE_CONTENT:
                FlowFile conFlowfile = session.write(flowFile, outputStream -> objectMapper.writeValue(outputStream, captureMap));
                conFlowfile = session.putAttribute(conFlowfile, CoreAttributes.MIME_TYPE.key(), APPLICATION_JSON);
                session.getProvenanceReporter().modifyContent(conFlowfile, "Replaced content with parsed Grok fields and values", stopWatch.getElapsed(TimeUnit.MILLISECONDS));
                session.transfer(conFlowfile, REL_MATCH);

                break;
        }
    }
}
