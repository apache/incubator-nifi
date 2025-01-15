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

import org.apache.commons.io.IOUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.nifi.annotation.behavior.DefaultRunDuration;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.attribute.expression.language.exception.AttributeExpressionLanguageException;
import org.apache.nifi.attribute.expression.language.exception.IllegalAttributeException;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.AttributeValueDecorator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.stream.io.util.LineDemarcator;
import org.apache.nifi.util.StopWatch;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.BufferOverflowException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SideEffectFree
@SupportsBatching(defaultDuration = DefaultRunDuration.TWENTY_FIVE_MILLIS)
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"Text", "Regular Expression", "Update", "Change", "Replace", "Modify", "Regex"})
@CapabilityDescription("Updates the content of a FlowFile by searching for some textual value in the FlowFile content (via Regular Expression/regex, or literal value) and replacing the " +
    "section of the content that matches with some alternate value. It can also be used to append or prepend text to the contents of a FlowFile.")
@SystemResourceConsideration(resource = SystemResource.MEMORY)
@UseCase(
    description = "Append text to the end of every line in a FlowFile",
    keywords = {"raw text", "append", "line"},
    configuration = """
        "Evaluation Mode" = "Line-by-Line"
        "Replacement Strategy" = "Append"

        "Replacement Value" is set to whatever text should be appended to the line.
        For example, to insert the text `<fin>` at the end of every line, we would set "Replacement Value" to `<fin>`.
        We can also use Expression Language. So to insert the filename at the end of every line, we set "Replacement Value" to `${filename}`
        """
)
@UseCase(
    description = "Prepend text to the beginning of every line in a FlowFile",
    keywords = {"raw text", "prepend", "line"},
    configuration = """
        "Evaluation Mode" = "Line-by-Line"
        "Replacement Strategy" = "Prepend"

        "Replacement Value" is set to whatever text should be prepended to the line.
        For example, to insert the text `<start>` at the beginning of every line, we would set "Replacement Value" to `<start>`.
        We can also use Expression Language. So to insert the filename at the beginning of every line, we set "Replacement Value" to `${filename}`
        """
)
@UseCase(
    description = "Replace every occurrence of a literal string in the FlowFile with a different value",
    keywords = {"replace", "string", "text", "literal"},
    configuration = """
        "Evaluation Mode" = "Line-by-Line"
        "Replacement Strategy" = "Literal Replace"
        "Search Value" is set to whatever text is in the FlowFile that needs to be replaced.
        "Replacement Value" is set to the text that should replace the current text.

        For example, to replace the word "spider" with "arachnid" we set "Search Value" to `spider` and set "Replacement Value" to `arachnid`.
        """
)
@UseCase(
    description = "Transform every occurrence of a literal string in a FlowFile",
    keywords = {"replace", "transform", "raw text"},
    configuration = """
        "Evaluation Mode" = "Line-by-Line"
        "Replacement Strategy" = "Regex Replace"
        "Search Value" is set to a regular expression that matches the text that should be transformed in a capturing group.
        "Replacement Value" is set to a NiFi Expression Language expression that references `$1` (in quotes to escape the reference name).

        For example, if we wanted to lowercase any occurrence of WOLF, TIGER, or LION, we would use a "Search Value" of `(WOLF|TIGER|LION)` \
        and a "Replacement Value" of `${'$1':toLower()}`.
        If we want to replace any identifier with a hash of that identifier, we might use a "Search Value" of `identifier: (.*)` and a \
        "Replacement Value" of `identifier: ${'$1':hash('sha256')}`
        """
)
@UseCase(
    description = "Completely replace the contents of a FlowFile to a specific text",
    keywords = {"replace", "raw text"},
    configuration = """
        "Evaluation Mode" = "Entire text"
        "Replacement Strategy" = "Always Replace"

        "Replacement Value" is set to the new text that should be written to the FlowFile. \
        This text might include NiFi Expression Language to reference one or more attributes.
        """
)
public class ReplaceText extends AbstractProcessor {

    private static final Pattern REPLACEMENT_NORMALIZATION_PATTERN = Pattern.compile("(\\$\\D)");

    // Constants
    public static final String LINE_BY_LINE = "Line-by-Line";
    public static final String ALL = "All";
    public static final String FIRST_LINE = "First-Line";
    public static final String EXCEPT_FIRST_LINE = "Except-First-Line";
    public static final String LAST_LINE = "Last-Line";
    public static final String EXCEPT_LAST_LINE = "Except-Last-Line";
    public static final String ENTIRE_TEXT = "Entire text";
    public static final String prependValue = "Prepend";
    public static final String appendValue = "Append";
    public static final String surroundValue = "Surround";
    public static final String regexReplaceValue = "Regex Replace";
    public static final String literalReplaceValue = "Literal Replace";
    public static final String alwaysReplace = "Always Replace";
    public static final String SUBSTITUTE_VARIABLES_VALUE = "Substitute Variables";
    private static final Pattern unescapedBackReferencePattern = Pattern.compile("[^\\\\]\\$(\\d+)");
    private static final String DEFAULT_REGEX = "(?s)(^.*$)";
    private static final String DEFAULT_REPLACEMENT_VALUE = "$1";

    // Prepend and Append will just insert the replacement value at the beginning or end
    // Properties PREPEND, APPEND, REGEX_REPLACE, LITERAL_REPLACE
    static final AllowableValue PREPEND = new AllowableValue(prependValue, prependValue,
        "Insert the Replacement Value at the beginning of the FlowFile or the beginning of each line (depending on the Evaluation Mode). For \"Line-by-Line\" Evaluation Mode, "
            + "the value will be prepended to each line. Similarly, for \"First-Line\", \"Last-Line\", \"Except-Last-Line\" and \"Except-First-Line\" Evaluation Modes,"
            + "the value will be prepended to header alone, footer alone, all lines except header and all lines except footer respectively. For \"Entire Text\" evaluation mode,"
            + "the value will be prepended to the entire text.");
    static final AllowableValue APPEND = new AllowableValue(appendValue, appendValue,
        "Insert the Replacement Value at the end of the FlowFile or the end of each line (depending on the Evaluation Mode). For \"Line-by-Line\" Evaluation Mode, "
            + "the value will be appended to each line. Similarly, for \"First-Line\", \"Last-Line\", \"Except-Last-Line\" and \"Except-First-Line\" Evaluation Modes,"
            + "the value will be appended to header alone, footer alone, all lines except header and all lines except footer respectively. For \"Entire Text\" evaluation mode,"
            + "the value will be appended to the entire text.");
    static final AllowableValue SURROUND = new AllowableValue(surroundValue, surroundValue,
        "Prepends text before the start of the FlowFile (or the start of each line, depending on the configuration of the Evaluation Mode property) " +
            "as well as appending text to the end of the FlowFile (or the end of each line, depending on the configuration of the Evaluation Mode property)");
    static final AllowableValue LITERAL_REPLACE = new AllowableValue(literalReplaceValue, literalReplaceValue,
        "Search for all instances of the Search Value and replace the matches with the Replacement Value.");
    static final AllowableValue REGEX_REPLACE = new AllowableValue(regexReplaceValue, regexReplaceValue,
        "Interpret the Search Value as a Regular Expression and replace all matches with the Replacement Value. The Replacement Value may reference Capturing Groups used "
            + "in the Search Value by using a dollar-sign followed by the Capturing Group number, such as $1 or $2. If the Search Value is set to .* then everything is replaced without "
            + "even evaluating the Regular Expression.");
    static final AllowableValue ALWAYS_REPLACE = new AllowableValue(alwaysReplace, alwaysReplace,
        "Always replaces the entire line or the entire contents of the FlowFile (depending on the value of the <Evaluation Mode> property) and does not bother searching "
            + "for any value. When this strategy is chosen, the <Search Value> property is ignored.");
    static final AllowableValue SUBSTITUTE_VARIABLES = new AllowableValue(SUBSTITUTE_VARIABLES_VALUE, SUBSTITUTE_VARIABLES_VALUE,
            "Substitute variable references (specified in ${var} form) using FlowFile attributes for looking up the replacement value by variable name. "
                    + "When this strategy is chosen, both the <Search Value> and <Replacement Value> properties are ignored.");

    public static final PropertyDescriptor REPLACEMENT_STRATEGY = new PropertyDescriptor.Builder()
        .name("Replacement Strategy")
        .description("The strategy for how and what to replace within the FlowFile's text content.")
        .allowableValues(PREPEND, APPEND, SURROUND, REGEX_REPLACE, LITERAL_REPLACE, ALWAYS_REPLACE, SUBSTITUTE_VARIABLES)
        .defaultValue(REGEX_REPLACE.getValue())
        .required(true)
        .build();
    public static final PropertyDescriptor SEARCH_VALUE = new PropertyDescriptor.Builder()
        .name("Regular Expression")
        .displayName("Search Value")
        .description("The Search Value to search for in the FlowFile content. Only used for 'Literal Replace' and 'Regex Replace' matching strategies")
        .required(true)
        .addValidator(Validator.VALID)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .dependsOn(REPLACEMENT_STRATEGY, REGEX_REPLACE, LITERAL_REPLACE)
        .defaultValue(DEFAULT_REGEX)
        .build();
    public static final PropertyDescriptor REPLACEMENT_VALUE = new PropertyDescriptor.Builder()
        .name("Replacement Value")
        .description("The value to insert using the 'Replacement Strategy'. Using \"Regex Replace\" back-references to Regular Expression capturing groups "
            + "are supported, but back-references that reference capturing groups that do not exist in the regular expression will be treated as literal value. "
            + "Back References may also be referenced using the Expression Language, as '$1', '$2', etc. The single-tick marks MUST be included, as these variables are "
            + "not \"Standard\" attribute names (attribute names must be quoted unless they contain only numbers, letters, and _).")
        .required(true)
        .defaultValue(DEFAULT_REPLACEMENT_VALUE)
        .addValidator(Validator.VALID)
        .dependsOn(REPLACEMENT_STRATEGY, REGEX_REPLACE, LITERAL_REPLACE, ALWAYS_REPLACE, PREPEND, APPEND)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();
    static final PropertyDescriptor PREPEND_TEXT = new PropertyDescriptor.Builder()
        .name("Text to Prepend")
        .displayName("Text to Prepend")
        .description("The text to prepend to the start of the FlowFile, or each line, depending on the configured value of the Evaluation Mode property")
        .required(true)
        .addValidator(Validator.VALID)
        .dependsOn(REPLACEMENT_STRATEGY, SURROUND)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();
    static final PropertyDescriptor APPEND_TEXT = new PropertyDescriptor.Builder()
        .name("Text to Append")
        .displayName("Text to Append")
        .description("The text to append to the end of the FlowFile, or each line, depending on the configured value of the Evaluation Mode property")
        .required(true)
        .addValidator(Validator.VALID)
        .dependsOn(REPLACEMENT_STRATEGY, SURROUND)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
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
        .description("Specifies the maximum amount of data to buffer (per file or per line, depending on the Evaluation Mode) in order to "
            + "apply the replacement. If 'Entire Text' (in Evaluation Mode) is selected and the FlowFile is larger than this value, "
            + "the FlowFile will be routed to 'failure'. "
            + "In 'Line-by-Line' Mode, if a single line is larger than this value, the FlowFile will be routed to 'failure'. A default value "
            + "of 1 MB is provided, primarily for 'Entire Text' mode. In 'Line-by-Line' Mode, a value such as 8 KB or 16 KB is suggested. "
            + "This value is ignored if the <Replacement Strategy> property is set to one of: Append, Prepend, Always Replace")
        .required(true)
        .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
        .defaultValue("1 MB")
        .build();
    public static final PropertyDescriptor EVALUATION_MODE = new PropertyDescriptor.Builder()
        .name("Evaluation Mode")
        .description("Run the 'Replacement Strategy' against each line separately (Line-by-Line) or buffer the entire file "
            + "into memory (Entire Text) and run against that.")
        .allowableValues(LINE_BY_LINE, ENTIRE_TEXT)
        .defaultValue(LINE_BY_LINE)
        .required(true)
        .build();

    public static final PropertyDescriptor LINE_BY_LINE_EVALUATION_MODE = new PropertyDescriptor.Builder()
        .name("Line-by-Line Evaluation Mode")
        .description("Run the 'Replacement Strategy' against each line separately (Line-by-Line) for all lines in the FlowFile, First Line (Header) alone, "
            + "Last Line (Footer) alone, Except the First Line (Header) or Except the Last Line (Footer).")
        .allowableValues(ALL, FIRST_LINE, LAST_LINE, EXCEPT_FIRST_LINE, EXCEPT_LAST_LINE)
        .defaultValue(ALL)
        .required(false)
        .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            REPLACEMENT_STRATEGY,
            SEARCH_VALUE,
            REPLACEMENT_VALUE,
            PREPEND_TEXT,
            APPEND_TEXT,
            CHARACTER_SET,
            MAX_BUFFER_SIZE,
            EVALUATION_MODE,
            LINE_BY_LINE_EVALUATION_MODE
    );

    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("FlowFiles that have been successfully processed are routed to this relationship. This includes both FlowFiles that had text"
            + " replaced and those that did not.")
        .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("FlowFiles that could not be updated are routed to this relationship")
        .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    private ReplacementStrategyExecutor replacementStrategyExecutor;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> errors = new ArrayList<>(super.customValidate(validationContext));

        switch (validationContext.getProperty(REPLACEMENT_STRATEGY).getValue()) {
            case literalReplaceValue:
                errors.add(StandardValidators.NON_EMPTY_VALIDATOR
                        .validate(SEARCH_VALUE.getName(), validationContext.getProperty(SEARCH_VALUE).getValue(), validationContext));
                break;

            case regexReplaceValue:
                errors.add(StandardValidators.createRegexValidator(0, Integer.MAX_VALUE, true)
                        .validate(SEARCH_VALUE.getName(), validationContext.getProperty(SEARCH_VALUE).getValue(), validationContext));
                break;

            case appendValue:
            case prependValue:
            case alwaysReplace:
            case SUBSTITUTE_VARIABLES_VALUE:
            default:
                // nothing to check, search value is not used
                break;
        }

        return errors;
    }

    @OnScheduled
    public void setup(ProcessContext context) {
        final String replacementStrategy = context.getProperty(REPLACEMENT_STRATEGY).getValue();
        final String evaluateMode = context.getProperty(EVALUATION_MODE).getValue();

        switch (replacementStrategy) {
            case prependValue:
                replacementStrategyExecutor = new PrependReplace();
                break;
            case appendValue:
                replacementStrategyExecutor = new SurroundReplace(null, REPLACEMENT_VALUE);
                break;
            case surroundValue:
                replacementStrategyExecutor = new SurroundReplace(PREPEND_TEXT, APPEND_TEXT);
                break;
            case regexReplaceValue:
                // for backward compatibility - if replacement regex is ".*" then we will simply always replace the content.
                if (context.getProperty(SEARCH_VALUE).getValue().equals(".*")) {
                    replacementStrategyExecutor = new AlwaysReplace();
                } else if (context.getProperty(SEARCH_VALUE).getValue().equals(DEFAULT_REGEX)
                        && evaluateMode.equalsIgnoreCase(ENTIRE_TEXT)
                        && context.getProperty(REPLACEMENT_VALUE).getValue().isEmpty()) {
                    replacementStrategyExecutor = new AlwaysReplace();
                } else {
                    final String regex = context.getProperty(SEARCH_VALUE).evaluateAttributeExpressions().getValue();
                    replacementStrategyExecutor = new RegexReplace(regex);
                }

                break;
            case literalReplaceValue:
                replacementStrategyExecutor = new LiteralReplace();
                break;
            case alwaysReplace:
                replacementStrategyExecutor = new AlwaysReplace();
                break;
            case SUBSTITUTE_VARIABLES_VALUE:
                replacementStrategyExecutor = new SubstituteVariablesReplace();
                break;
            default:
                throw new AssertionError();
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();

        final Charset charset = Charset.forName(context.getProperty(CHARACTER_SET).getValue());
        final int maxBufferSize = context.getProperty(MAX_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
        final String evaluateMode = context.getProperty(EVALUATION_MODE).getValue();

        if (evaluateMode.equalsIgnoreCase(ENTIRE_TEXT)) {
            if (flowFile.getSize() > maxBufferSize && replacementStrategyExecutor.isAllDataBufferedForEntireText()) {
                logger.warn("Transferred {} to 'failure' because it was larger than the buffer size");
                session.transfer(flowFile, REL_FAILURE);
                return;
            }
        }

        final StopWatch stopWatch = new StopWatch(true);

        try {
            flowFile = replacementStrategyExecutor.replace(flowFile, session, context, evaluateMode, charset, maxBufferSize);
        } catch (StackOverflowError e) {
            // Some regular expressions can produce many matches on large input data size using recursive code
            // do not log the StackOverflowError stack trace
            logger.info("Transferred {} to 'failure'", flowFile, e);
            session.transfer(flowFile, REL_FAILURE);
            return;
        } catch (BufferOverflowException | IllegalAttributeException | AttributeExpressionLanguageException e) {
            logger.warn("Transferred {} to 'failure'", flowFile, e);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        logger.info("Transferred {} to 'success'", flowFile);
        session.getProvenanceReporter().modifyContent(flowFile, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
        session.transfer(flowFile, REL_SUCCESS);
    }

    // If we find a back reference that is not valid, then we will treat it as a literal string. For example, if we have 3 capturing
    // groups and the Replacement Value has the value is "I owe $8 to him", then we want to treat the $8 as a literal "$8", rather
    // than attempting to use it as a back reference.
    private static String escapeLiteralBackReferences(final String unescaped, final int numCapturingGroups) {
        if (numCapturingGroups == 0) {
            return unescaped;
        }

        String value = unescaped;
        final Matcher backRefMatcher = unescapedBackReferencePattern.matcher(value); // consider unescaped back references
        while (backRefMatcher.find()) {
            final String backRefNum = backRefMatcher.group(1);
            if (backRefNum.startsWith("0")) {
                continue;
            }
            final int originalBackRefIndex = Integer.parseInt(backRefNum);
            int backRefIndex = originalBackRefIndex;

            // if we have a replacement value like $123, and we have less than 123 capturing groups, then
            // we want to truncate the 3 and use capturing group 12; if we have less than 12 capturing groups,
            // then we want to truncate the 2 and use capturing group 1; if we don't have a capturing group then
            // we want to truncate the 1 and get 0.
            while (backRefIndex > numCapturingGroups && backRefIndex >= 10) {
                backRefIndex /= 10;
            }

            if (backRefIndex > numCapturingGroups) {
                final StringBuilder sb = new StringBuilder(value.length() + 1);
                final int groupStart = backRefMatcher.start(1);

                sb.append(value, 0, groupStart - 1);
                sb.append("\\");
                sb.append(value.substring(groupStart - 1));
                value = sb.toString();
            }
        }

        return value;
    }

    private static class AlwaysReplace implements ReplacementStrategyExecutor {
        @Override
        public FlowFile replace(FlowFile flowFile, final ProcessSession session, final ProcessContext context, final String evaluateMode, final Charset charset, final int maxBufferSize) {

            final String replacementValue = context.getProperty(REPLACEMENT_VALUE).evaluateAttributeExpressions(flowFile).getValue();
            final StringBuilder lineEndingBuilder = new StringBuilder(2);

            if (evaluateMode.equalsIgnoreCase(ENTIRE_TEXT)) {
                flowFile = session.write(flowFile, out -> out.write(replacementValue.getBytes(charset)));
            } else {
                flowFile = session.write(flowFile, new StreamReplaceCallback(charset, maxBufferSize, context.getProperty(LINE_BY_LINE_EVALUATION_MODE).getValue(),
                    ((bw, oneLine) -> {
                        // We need to determine what line ending was used and use that after our replacement value.
                        lineEndingBuilder.setLength(0);
                        for (int i = oneLine.length() - 1; i >= 0; i--) {
                            final char c = oneLine.charAt(i);
                            if (c == '\r' || c == '\n') {
                                lineEndingBuilder.append(c);
                            } else {
                                break;
                            }
                        }

                        bw.write(replacementValue);

                        // Preserve original line endings. Reverse string because we iterated over original line ending in reverse order, appending to builder.
                        // So if builder has multiple characters, they are now reversed from the original string's ordering.
                        bw.write(lineEndingBuilder.reverse().toString());
                    })));
            }

            return flowFile;
        }

        @Override
        public boolean isAllDataBufferedForEntireText() {
            return false;
        }
    }

    private static class PrependReplace implements ReplacementStrategyExecutor {
        @Override
        public FlowFile replace(FlowFile flowFile, final ProcessSession session, final ProcessContext context, final String evaluateMode, final Charset charset, final int maxBufferSize) {
            final String replacementValue = context.getProperty(REPLACEMENT_VALUE).evaluateAttributeExpressions(flowFile).getValue();

            if (evaluateMode.equalsIgnoreCase(ENTIRE_TEXT)) {
                flowFile = session.write(flowFile, (in, out) -> {
                    out.write(replacementValue.getBytes(charset));
                    IOUtils.copy(in, out);
                });
            } else {
                flowFile = session.write(flowFile, new StreamReplaceCallback(charset, maxBufferSize, context.getProperty(LINE_BY_LINE_EVALUATION_MODE).getValue(),
                    (bw, oneLine) -> bw.write(replacementValue.concat(oneLine))));
            }
            return flowFile;
        }

        @Override
        public boolean isAllDataBufferedForEntireText() {
            return false;
        }

    }

    private static class SurroundReplace implements ReplacementStrategyExecutor {
        private final PropertyDescriptor prependValueDescriptor;
        private final PropertyDescriptor appendValueDescriptor;

        public SurroundReplace(final PropertyDescriptor prependValueDescriptor, final PropertyDescriptor appendValueDescriptor) {
            this.prependValueDescriptor = prependValueDescriptor;
            this.appendValueDescriptor = appendValueDescriptor;
        }

        @Override
        public FlowFile replace(FlowFile flowFile, final ProcessSession session, final ProcessContext context, final String evaluateMode, final Charset charset, final int maxBufferSize) {
            final String prependValue = (prependValueDescriptor == null) ? null : context.getProperty(prependValueDescriptor).evaluateAttributeExpressions(flowFile).getValue();
            final String appendValue = context.getProperty(appendValueDescriptor).evaluateAttributeExpressions(flowFile).getValue();

            if (evaluateMode.equalsIgnoreCase(ENTIRE_TEXT)) {
                flowFile = session.write(flowFile, (in, out) -> {
                    if (prependValue != null && !prependValue.isEmpty()) {
                        out.write(prependValue.getBytes(charset));
                    }

                    IOUtils.copy(in, out);
                    out.write(appendValue.getBytes(charset));
                });
            } else {
                flowFile = session.write(flowFile, new StreamReplaceCallback(charset, maxBufferSize, context.getProperty(LINE_BY_LINE_EVALUATION_MODE).getValue(),
                    (bw, oneLine) -> {
                        if (prependValue != null && !prependValue.isEmpty()) {
                            bw.write(prependValue);
                        }

                        // we need to find the first carriage return or new-line so that we can append the new value
                        // before the line separate. However, we don't want to do this using a regular expression due
                        // to performance concerns. So we will find the first occurrence of either \r or \n and use
                        // that to insert the replacement value.
                        boolean foundNewLine = false;
                        for (int i = 0; i < oneLine.length(); i++) {
                            final char c = oneLine.charAt(i);
                            if (foundNewLine) {
                                bw.write(c);
                                continue;
                            }

                            if (c == '\r' || c == '\n') {
                                bw.write(appendValue);
                                foundNewLine = true;
                            }

                            bw.write(c);
                        }

                        if (!foundNewLine) {
                            bw.write(appendValue);
                        }
                    }));
            }
            return flowFile;
        }

        @Override
        public boolean isAllDataBufferedForEntireText() {
            return false;
        }
    }


    private static class RegexReplace implements ReplacementStrategyExecutor {
        private final int numCapturingGroups;

        // back references are not supported in the evaluated expression
        private final AttributeValueDecorator escapeBackRefDecorator = attributeValue -> {
            // when we encounter a '$[0-9+]'  replace it with '\$[0-9+]'
            return attributeValue.replaceAll("(\\$\\d+?)", "\\\\$1");
        };

        public RegexReplace(final String regex) {
            numCapturingGroups = Pattern.compile(regex).matcher("").groupCount();
        }

        @Override
        public FlowFile replace(final FlowFile flowFile, final ProcessSession session, final ProcessContext context, final String evaluateMode, final Charset charset, final int maxBufferSize) {
            final AttributeValueDecorator quotedAttributeDecorator = Pattern::quote;

            final String searchRegex = context.getProperty(SEARCH_VALUE).evaluateAttributeExpressions(flowFile, quotedAttributeDecorator).getValue();
            final Pattern searchPattern = Pattern.compile(searchRegex);
            final Map<String, String> additionalAttrs = new HashMap<>(numCapturingGroups);

            FlowFile updatedFlowFile;
            if (evaluateMode.equalsIgnoreCase(ENTIRE_TEXT)) {
                final int flowFileSize = (int) flowFile.getSize();
                final int bufferSize = Math.min(maxBufferSize, flowFileSize);
                final byte[] buffer = new byte[bufferSize];

                session.read(flowFile, in -> StreamUtils.fillBuffer(in, buffer, false));

                final String contentString = new String(buffer, 0, flowFileSize, charset);
                final Matcher matcher = searchPattern.matcher(contentString);

                final PropertyValue replacementValueProperty = context.getProperty(REPLACEMENT_VALUE);

                int matches = 0;
                final StringBuffer sb = new StringBuffer();
                while (matcher.find()) {
                    matches++;

                    for (int i = 0; i <= matcher.groupCount(); i++) {
                        additionalAttrs.put("$" + i, matcher.group(i));
                    }

                    String replacement = replacementValueProperty.evaluateAttributeExpressions(flowFile, additionalAttrs, escapeBackRefDecorator).getValue();
                    replacement = escapeLiteralBackReferences(replacement, numCapturingGroups);
                    String replacementFinal = normalizeReplacementString(replacement);

                    matcher.appendReplacement(sb, replacementFinal);
                }

                if (matches > 0) {
                    matcher.appendTail(sb);

                    final String updatedValue = sb.toString();
                    updatedFlowFile = session.write(flowFile, out -> out.write(updatedValue.getBytes(charset)));
                } else {
                    return flowFile;
                }

            } else {
                final Matcher matcher = searchPattern.matcher("");
                updatedFlowFile = session.write(flowFile, new StreamReplaceCallback(charset, maxBufferSize, context.getProperty(LINE_BY_LINE_EVALUATION_MODE).getValue(),
                    (bw, oneLine) -> {
                        matcher.reset(oneLine);

                        int matches = 0;
                        StringBuffer sb = new StringBuffer();

                        while (matcher.find()) {
                            matches++;

                            for (int i = 0; i <= matcher.groupCount(); i++) {
                                additionalAttrs.put("$" + i, matcher.group(i));
                            }

                            String replacement = context.getProperty(REPLACEMENT_VALUE).evaluateAttributeExpressions(flowFile, additionalAttrs, escapeBackRefDecorator).getValue();
                            replacement = escapeLiteralBackReferences(replacement, numCapturingGroups);
                            String replacementFinal = normalizeReplacementString(replacement);

                            matcher.appendReplacement(sb, replacementFinal);
                        }

                        if (matches > 0) {
                            matcher.appendTail(sb);

                            final String updatedValue = sb.toString();
                            bw.write(updatedValue);
                        } else {
                            // No match. Just write out the line as it was.
                            bw.write(oneLine);
                        }
                    }));
            }

            return updatedFlowFile;
        }

        @Override
        public boolean isAllDataBufferedForEntireText() {
            return true;
        }
    }

    private static class LiteralReplace implements ReplacementStrategyExecutor {
        @Override
        public FlowFile replace(FlowFile flowFile, final ProcessSession session, final ProcessContext context, final String evaluateMode, final Charset charset, final int maxBufferSize) {
            final String replacementValue = context.getProperty(REPLACEMENT_VALUE).evaluateAttributeExpressions(flowFile).getValue();
            final String searchValue = context.getProperty(SEARCH_VALUE).evaluateAttributeExpressions(flowFile).getValue();

            if (evaluateMode.equalsIgnoreCase(ENTIRE_TEXT)) {
                final int flowFileSize = (int) flowFile.getSize();
                final int bufferSize = Math.min(maxBufferSize, flowFileSize);
                final byte[] buffer = new byte[bufferSize];

                flowFile = session.write(flowFile, (in, out) -> {
                    StreamUtils.fillBuffer(in, buffer, false);
                    final String contentString = new String(buffer, 0, flowFileSize, charset);
                    // Interpreting the search and replacement values as char sequences
                    final String updatedValue = contentString.replace(searchValue, replacementValue);
                    out.write(updatedValue.getBytes(charset));
                });
            } else {
                final Pattern searchPattern = Pattern.compile(searchValue, Pattern.LITERAL);

                flowFile = session.write(flowFile, new StreamReplaceCallback(charset, maxBufferSize, context.getProperty(LINE_BY_LINE_EVALUATION_MODE).getValue(),
                    (bw, oneLine) -> {
                        int matches = 0;
                        int lastEnd = 0;

                        int index = oneLine.indexOf(searchValue, lastEnd);
                        while (index >= 0) {
                            bw.write(oneLine, lastEnd, index - lastEnd);
                            bw.write(replacementValue);
                            matches++;

                            lastEnd = index + searchValue.length();
                            index = oneLine.indexOf(searchValue, lastEnd);
                        }

                        if (matches > 0) {
                            bw.write(oneLine, lastEnd, oneLine.length() - lastEnd);
                        } else {
                            bw.write(oneLine);
                        }
                    }));
            }
            return flowFile;
        }

        @Override
        public boolean isAllDataBufferedForEntireText() {
            return true;
        }
    }

    private static class SubstituteVariablesReplace implements ReplacementStrategyExecutor {
        @Override
        public FlowFile replace(FlowFile flowFile, final ProcessSession session, final ProcessContext context, final String evaluateMode, final Charset charset, final int maxBufferSize) {
            final Map<String, String> flowFileAttributes = flowFile.getAttributes();

            if (evaluateMode.equalsIgnoreCase(ENTIRE_TEXT)) {
                final int flowFileSize = (int) flowFile.getSize();
                final int bufferSize = Math.min(maxBufferSize, flowFileSize);
                final byte[] buffer = new byte[bufferSize];

                flowFile = session.write(flowFile, (in, out) -> {
                    StreamUtils.fillBuffer(in, buffer, false);
                    final String originalContent = new String(buffer, 0, flowFileSize, charset);
                    final String substitutedContent = StringSubstitutor.replace(originalContent, flowFileAttributes);
                    out.write(substitutedContent.getBytes(charset));
                });
            } else {
                flowFile = session.write(flowFile, new StreamReplaceCallback(charset, maxBufferSize, context.getProperty(LINE_BY_LINE_EVALUATION_MODE).getValue(),
                        (bw, oneLine) -> {
                            final String substitutedLine = StringSubstitutor.replace(oneLine, flowFileAttributes);
                            bw.write(substitutedLine);
                        }));
            }
            return flowFile;
        }

        @Override
        public boolean isAllDataBufferedForEntireText() {
            return true;
        }
    }

    /**
     * If we have a '$' followed by anything other than a number, then escape
     * it. E.g., '$d' becomes '\$d' so that it can be used as a literal in a
     * regex.
     */
    private static String normalizeReplacementString(String replacement) {
        String replacementFinal = replacement;
        if (REPLACEMENT_NORMALIZATION_PATTERN.matcher(replacement).find()) {
            replacementFinal = Matcher.quoteReplacement(replacement);
        }
        return replacementFinal;
    }

    private interface ReplacementStrategyExecutor {
        FlowFile replace(FlowFile flowFile, ProcessSession session, ProcessContext context, String evaluateMode, Charset charset, int maxBufferSize);

        boolean isAllDataBufferedForEntireText();
    }

    @FunctionalInterface
    private interface ReplaceLine {
        void apply(BufferedWriter bw, String oneLine) throws IOException;
    }


    private static class StreamReplaceCallback implements StreamCallback {
        private final Charset charset;
        private final int maxBufferSize;
        private final String lineByLineEvaluationMode;
        private final ReplaceLine replaceLine;

        private StreamReplaceCallback(Charset charset,
                                     int maxBufferSize,
                                     String lineByLineEvaluationMode,
                                     ReplaceLine replaceLine) {
            this.charset = charset;
            this.maxBufferSize = maxBufferSize;
            this.lineByLineEvaluationMode = lineByLineEvaluationMode;
            this.replaceLine = replaceLine;
        }

        @Override
        public void process(final InputStream in, final OutputStream out) throws IOException {
            try (final LineDemarcator demarcator = new LineDemarcator(in, charset, maxBufferSize, 8192);
                 final BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out, charset))) {

                String precedingLine = demarcator.nextLine();
                String succeedingLine;

                boolean firstLine = true;

                while (null != (succeedingLine = demarcator.nextLine())) {
                    if (firstLine && lineByLineEvaluationMode.equalsIgnoreCase(FIRST_LINE)) {
                        replaceLine.apply(bw, precedingLine);
                        firstLine = false;
                    } else if (firstLine && lineByLineEvaluationMode.equalsIgnoreCase(EXCEPT_FIRST_LINE)) {
                        firstLine = false;
                        bw.write(precedingLine);
                    } else if (lineByLineEvaluationMode.equalsIgnoreCase(LINE_BY_LINE)
                        || lineByLineEvaluationMode.equalsIgnoreCase(EXCEPT_LAST_LINE)
                        || lineByLineEvaluationMode.equalsIgnoreCase(ALL)
                        || (!firstLine && lineByLineEvaluationMode.equalsIgnoreCase(EXCEPT_FIRST_LINE))) {
                        replaceLine.apply(bw, precedingLine);
                    } else {
                        bw.write(precedingLine);
                    }
                    precedingLine = succeedingLine;
                }

                // 0 byte empty FlowFIles are left untouched
                if (null != precedingLine) {
                    if (lineByLineEvaluationMode.equalsIgnoreCase(EXCEPT_LAST_LINE)
                        || (!firstLine && lineByLineEvaluationMode.equalsIgnoreCase(FIRST_LINE))
                        || (firstLine && lineByLineEvaluationMode.equalsIgnoreCase(EXCEPT_FIRST_LINE))) {
                        bw.write(precedingLine);
                    } else {
                        replaceLine.apply(bw, precedingLine);
                    }
                }
            }
        }
    }
}