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
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.ParseBigDecimal;
import org.supercsv.cellprocessor.ParseBool;
import org.supercsv.cellprocessor.ParseChar;
import org.supercsv.cellprocessor.ParseDate;
import org.supercsv.cellprocessor.ParseDouble;
import org.supercsv.cellprocessor.ParseInt;
import org.supercsv.cellprocessor.ParseLong;
import org.supercsv.cellprocessor.constraint.DMinMax;
import org.supercsv.cellprocessor.constraint.Equals;
import org.supercsv.cellprocessor.constraint.ForbidSubStr;
import org.supercsv.cellprocessor.constraint.IsIncludedIn;
import org.supercsv.cellprocessor.constraint.LMinMax;
import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.constraint.RequireHashCode;
import org.supercsv.cellprocessor.constraint.RequireSubStr;
import org.supercsv.cellprocessor.constraint.StrMinMax;
import org.supercsv.cellprocessor.constraint.StrNotNullOrEmpty;
import org.supercsv.cellprocessor.constraint.StrRegEx;
import org.supercsv.cellprocessor.constraint.Strlen;
import org.supercsv.cellprocessor.constraint.Unique;
import org.supercsv.cellprocessor.constraint.UniqueHashCode;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.exception.SuperCsvException;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;
import org.supercsv.util.CsvContext;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"csv", "schema", "validation"})
@CapabilityDescription("Validates the contents of FlowFiles or a FlowFile attribute value against a user-specified CSV schema. " +
        "Take a look at the additional documentation of this processor for some schema examples.")
@WritesAttributes({
    @WritesAttribute(attribute = "count.valid.lines", description = "If line by line validation, number of valid lines extracted from the source data"),
    @WritesAttribute(attribute = "count.invalid.lines", description = "If line by line validation, number of invalid lines extracted from the source data"),
    @WritesAttribute(attribute = "count.total.lines", description = "If line by line validation, total number of lines in the source data"),
    @WritesAttribute(attribute = "validation.error.message", description = "For flow files routed to invalid, message of the first validation error")
})
public class ValidateCsv extends AbstractProcessor {

    private final static List<String> ALLOWED_OPERATORS = List.of(
            "ParseBigDecimal", "ParseBool", "ParseChar", "ParseDate", "ParseDouble", "ParseInt", "ParseLong",
            "Optional", "DMinMax", "Equals", "ForbidSubStr", "LMinMax", "NotNull", "Null", "RequireHashCode", "RequireSubStr",
            "Strlen", "StrMinMax", "StrNotNullOrEmpty", "StrRegEx", "Unique", "UniqueHashCode", "IsIncludedIn"
    );

    private static final String ROUTE_WHOLE_FLOW_FILE = "FlowFile validation";
    private static final String ROUTE_LINES_INDIVIDUALLY = "Line by line validation";

    public static final AllowableValue VALIDATE_WHOLE_FLOWFILE = new AllowableValue(ROUTE_WHOLE_FLOW_FILE, ROUTE_WHOLE_FLOW_FILE,
            "As soon as an error is found in the CSV file, the validation will stop and the whole flow file will be routed to the 'invalid'"
                    + " relationship. This option offers best performances.");

    public static final AllowableValue VALIDATE_LINES_INDIVIDUALLY = new AllowableValue(ROUTE_LINES_INDIVIDUALLY, ROUTE_LINES_INDIVIDUALLY,
            "In case an error is found, the input CSV file will be split into two FlowFiles: one routed to the 'valid' "
                    + "relationship containing all the correct lines and one routed to the 'invalid' relationship containing all "
                    + "the incorrect lines. Take care if choosing this option while using Unique cell processors in schema definition:"
                    + "the first occurrence will be considered valid and the next ones as invalid.");

    public static final PropertyDescriptor SCHEMA = new PropertyDescriptor.Builder()
            .name("validate-csv-schema")
            .displayName("Schema")
            .description("The schema to be used for validation. Is expected a comma-delimited string representing the cell "
                    + "processors to apply. The following cell processors are allowed in the schema definition: "
                    + ALLOWED_OPERATORS + ". Note: cell processors cannot be nested except with Optional. Schema is required if Header is false.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    public static final PropertyDescriptor HEADER = new PropertyDescriptor.Builder()
            .name("validate-csv-header")
            .displayName("Header")
            .description("True if the incoming flow file contains a header to ignore, false otherwise.")
            .required(true)
            .defaultValue("true")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor QUOTE_CHARACTER = new PropertyDescriptor.Builder()
            .name("validate-csv-quote")
            .displayName("Quote character")
            .description("Character used as 'quote' in the incoming data. Example: \"")
            .required(true)
            .defaultValue("\"")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DELIMITER_CHARACTER = new PropertyDescriptor.Builder()
            .name("validate-csv-delimiter")
            .displayName("Delimiter character")
            .description("Character used as 'delimiter' in the incoming data. Example: ,")
            .required(true)
            .defaultValue(",")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor END_OF_LINE_CHARACTER = new PropertyDescriptor.Builder()
            .name("validate-csv-eol")
            .displayName("End of line symbols")
            .description("Symbols used as 'end of line' in the incoming data. Example: \\n")
            .required(true)
            .defaultValue("\\n")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor VALIDATION_STRATEGY = new PropertyDescriptor.Builder()
            .name("validate-csv-strategy")
            .displayName("Validation strategy")
            .description("Strategy to apply when routing input files to output relationships.")
            .required(true)
            .defaultValue(VALIDATE_WHOLE_FLOWFILE.getValue())
            .allowableValues(VALIDATE_LINES_INDIVIDUALLY, VALIDATE_WHOLE_FLOWFILE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CSV_SOURCE_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("CSV Source Attribute")
            .displayName("CSV Source Attribute")
            .description("The name of the attribute containing CSV data to be validated. If this property is blank, the FlowFile content will be validated.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.ATTRIBUTE_KEY_VALIDATOR)
            .dependsOn(VALIDATION_STRATEGY, VALIDATE_WHOLE_FLOWFILE.getValue())
            .build();

    public static final PropertyDescriptor INCLUDE_ALL_VIOLATIONS = new PropertyDescriptor.Builder()
            .name("validate-csv-violations")
            .displayName("Include all violations")
            .description("If true, the validation.error.message attribute would include the list of all the violations"
                    + " for the first invalid line. Note that setting this property to true would slightly decrease"
                    + " the performances as all columns would be validated. If false, a line is invalid as soon as a"
                    + " column is found violating the specified constraint and only this violation for the first invalid"
                    + " line will be included in the validation.error.message attribute.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            SCHEMA,
            CSV_SOURCE_ATTRIBUTE,
            HEADER,
            DELIMITER_CHARACTER,
            QUOTE_CHARACTER,
            END_OF_LINE_CHARACTER,
            VALIDATION_STRATEGY,
            INCLUDE_ALL_VIOLATIONS
    );

    public static final Relationship REL_VALID = new Relationship.Builder()
            .name("valid")
            .description("FlowFiles that are successfully validated against the schema are routed to this relationship")
            .build();
    public static final Relationship REL_INVALID = new Relationship.Builder()
            .name("invalid")
            .description("FlowFiles that are not valid according to the specified schema,"
                    + " or no schema or CSV header can be identified, are routed to this relationship")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_VALID,
            REL_INVALID
    );

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {

        PropertyValue schemaProp = context.getProperty(SCHEMA);
        PropertyValue headerProp = context.getProperty(HEADER);
        String schema = schemaProp.getValue();
        String subject = SCHEMA.getName();

        if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(schema)) {
            return List.of(new ValidationResult.Builder().subject(subject).input(schema).explanation("Expression Language Present").valid(true).build());
        }
        // If no Expression Language is present, try parsing the schema
        try {
            if (schema != null) {
                this.parseSchema(schema);
            } else if (!headerProp.asBoolean()) {
                throw(new Exception("Schema cannot be empty if Header property is false."));
            }
        } catch (Exception e) {
            final List<ValidationResult> problems = new ArrayList<>(1);
            problems.add(new ValidationResult.Builder().subject(subject)
                    .input(schema)
                    .valid(false)
                    .explanation("Error while parsing the schema: " + e.getMessage())
                    .build());
            return problems;
        }
        return super.customValidate(context);
    }

    public CsvPreference getPreference(final ProcessContext context, final FlowFile flowFile) {
        // When going from the UI to Java, the characters are escaped so that what you
        // input is transferred over to Java as is. So when you type the characters "\"
        // and "n" into the UI the Java string will end up being those two characters
        // not the interpreted value "\n".
        final String msgDemarcator = context.getProperty(END_OF_LINE_CHARACTER).evaluateAttributeExpressions(flowFile).getValue().replace("\\n", "\n").replace("\\r", "\r").replace("\\t", "\t");
        return new CsvPreference.Builder(context.getProperty(QUOTE_CHARACTER).evaluateAttributeExpressions(flowFile).getValue().charAt(0),
                context.getProperty(DELIMITER_CHARACTER).evaluateAttributeExpressions(flowFile).getValue().charAt(0), msgDemarcator).build();
    }

    /**
     * Method used to parse the string supplied by the user. The string is converted
     * to a list of cell processors used to validate the CSV data.
     * @param schema Schema to parse
     */
    private CellProcessor[] parseSchema(String schema) {
        List<CellProcessor> processorsList = new ArrayList<>();

        String remaining = schema;
        while (!remaining.isEmpty()) {
            remaining = setProcessor(remaining, processorsList);
        }

        return processorsList.toArray(new CellProcessor[0]);
    }

    private String setProcessor(String remaining, List<CellProcessor> processorsList) {
        StringBuilder buffer = new StringBuilder();
        String inputString = remaining;
        int i = 0;
        int opening = 0;
        int closing = 0;
        while (buffer.length() != inputString.length()) {
            char c = remaining.charAt(i);
            i++;

            if (opening == 0 && c == ',') {
                if (i == 1) {
                    inputString = inputString.substring(1);
                    continue;
                }
                break;
            }

            buffer.append(c);

            if (c == '(') {
                opening++;
            } else if (c == ')') {
                closing++;
            }

            if (opening > 0 && opening == closing) {
                break;
            }
        }

        final String procString = buffer.toString().trim();
        opening = procString.indexOf('(');
        String method = procString;
        String argument = null;
        if (opening != -1) {
            argument = method.substring(opening + 1, method.length() - 1);
            method = method.substring(0, opening);
        }

        processorsList.add(getProcessor(method.toLowerCase(), argument));

        return remaining.substring(i);
    }

    private CellProcessor getProcessor(String method, String argument) {
        switch (method) {

            case "optional":
                int opening = argument.indexOf('(');
                String subMethod = argument;
                String subArgument = null;
                if (opening != -1) {
                    subArgument = subMethod.substring(opening + 1, subMethod.length() - 1);
                    subMethod = subMethod.substring(0, opening);
                }
                return new Optional(getProcessor(subMethod.toLowerCase(), subArgument));

            case "parsedate":
                return new ParseDate(argument.substring(1, argument.length() - 1));

            case "parsedouble":
                if (argument != null && !argument.isEmpty())
                    throw new IllegalArgumentException("ParseDouble does not expect any argument but has " + argument);
                return new ParseDouble();

            case "parsebigdecimal":
                if (argument != null && !argument.isEmpty())
                    throw new IllegalArgumentException("ParseBigDecimal does not expect any argument but has " + argument);
                return new ParseBigDecimal();

            case "parsebool":
                if (argument != null && !argument.isEmpty())
                    throw new IllegalArgumentException("ParseBool does not expect any argument but has " + argument);
                return new ParseBool();

            case "parsechar":
                if (argument != null && !argument.isEmpty())
                    throw new IllegalArgumentException("ParseChar does not expect any argument but has " + argument);
                return new ParseChar();

            case "parseint":
                if (argument != null && !argument.isEmpty())
                    throw new IllegalArgumentException("ParseInt does not expect any argument but has " + argument);
                return new ParseInt();

            case "parselong":
                if (argument != null && !argument.isEmpty())
                    throw new IllegalArgumentException("ParseLong does not expect any argument but has " + argument);
                return new ParseLong();

            case "notnull":
                if (argument != null && !argument.isEmpty())
                    throw new IllegalArgumentException("NotNull does not expect any argument but has " + argument);
                return new NotNull();

            case "strregex":
                return new StrRegEx(argument.substring(1, argument.length() - 1));

            case "unique":
                if (argument != null && !argument.isEmpty())
                    throw new IllegalArgumentException("Unique does not expect any argument but has " + argument);
                return new Unique();

            case "uniquehashcode":
                if (argument != null && !argument.isEmpty())
                    throw new IllegalArgumentException("UniqueHashCode does not expect any argument but has " + argument);
                return new UniqueHashCode();

            case "strlen":
                String[] splts = argument.split(",");
                int[] requiredLengths = new int[splts.length];
                for (int i = 0; i < splts.length; i++) {
                    requiredLengths[i] = Integer.parseInt(splts[i]);
                }
                return new Strlen(requiredLengths);

            case "strminmax":
                String[] splits = argument.split(",");
                return new StrMinMax(Long.parseLong(splits[0]), Long.parseLong(splits[1]));

            case "lminmax":
                String[] args = argument.split(",");
                return new LMinMax(Long.parseLong(args[0]), Long.parseLong(args[1]));

            case "dminmax":
                String[] doubles = argument.split(",");
                return new DMinMax(Double.parseDouble(doubles[0]), Double.parseDouble(doubles[1]));

            case "equals":
                if (argument != null && !argument.isEmpty())
                    throw new IllegalArgumentException("Equals does not expect any argument but has " + argument);
                return new Equals();

            case "forbidsubstr":
                String[] forbiddenSubStrings = argument.replaceAll("\"", "").split(",[ ]*");
                return new ForbidSubStr(forbiddenSubStrings);

            case "requiresubstr":
                String[] requiredSubStrings = argument.replaceAll("\"", "").split(",[ ]*");
                return new RequireSubStr(requiredSubStrings);

            case "strnotnullorempty":
                if (argument != null && !argument.isEmpty())
                    throw new IllegalArgumentException("StrNotNullOrEmpty does not expect any argument but has " + argument);
                return new StrNotNullOrEmpty();

            case "requirehashcode":
                String[] hashs = argument.split(",");
                int[] hashcodes = new int[hashs.length];
                for (int i = 0; i < hashs.length; i++) {
                    hashcodes[i] = Integer.parseInt(hashs[i]);
                }
                return new RequireHashCode(hashcodes);

            case "null":
                if (argument != null && !argument.isEmpty())
                    throw new IllegalArgumentException("Null does not expect any argument but has " + argument);
                return null;

            case "isincludedin":
                String[] elements = argument.replaceAll("\"", "").split(",[ ]*");
                return new IsIncludedIn(elements);

            default:
                throw new IllegalArgumentException("[" + method + "] is not an allowed method to define a Cell Processor");
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final CsvPreference csvPref = getPreference(context, flowFile);
        final boolean header = context.getProperty(HEADER).asBoolean();
        final ComponentLog logger = getLogger();
        String schema = context.getProperty(SCHEMA).evaluateAttributeExpressions(flowFile).getValue();
        CellProcessor[] cellProcs = null;
        if (schema != null) {
            cellProcs = this.parseSchema(schema);
        }
        final String validationStrategy = context.getProperty(VALIDATION_STRATEGY).getValue();
        final boolean isWholeFFValidation = !validationStrategy.equals(VALIDATE_LINES_INDIVIDUALLY.getValue());
        final boolean includeAllViolations = context.getProperty(INCLUDE_ALL_VIOLATIONS).asBoolean();

        boolean valid = true;
        int okCount = 0;
        int totalCount = 0;
        FlowFile invalidFF = null;
        FlowFile validFF = null;
        String validationError = null;
        final AtomicReference<Boolean> isFirstLineValid = new AtomicReference<>(true);
        final AtomicReference<Boolean> isFirstLineInvalid = new AtomicReference<>(true);

        if (!isWholeFFValidation) {
            invalidFF = session.create(flowFile);
            validFF = session.create(flowFile);
        }

        InputStream stream;
        if (context.getProperty(CSV_SOURCE_ATTRIBUTE).isSet() && isWholeFFValidation) {
            String csvAttribute = flowFile.getAttribute(context.getProperty(CSV_SOURCE_ATTRIBUTE).evaluateAttributeExpressions().getValue());
            stream = new ByteArrayInputStream(Objects.requireNonNullElse(csvAttribute, "").getBytes(StandardCharsets.UTF_8));
        } else {
            stream = session.read(flowFile);
        }

        stream: try (final NifiCsvListReader listReader = new NifiCsvListReader(new InputStreamReader(stream), csvPref)) {

            // handling of header
            if (header) {

                // read header
                List<String> headers = listReader.read();

                if (schema == null) {
                    if (headers != null && !headers.isEmpty()) {
                        String newSchema = "Optional(StrNotNullOrEmpty()),".repeat(headers.size());
                        schema = newSchema.substring(0, newSchema.length() - 1);
                        cellProcs = this.parseSchema(schema);
                    } else {
                        validationError = "No schema or CSV header could be identified.";
                        valid = false;
                        break stream;
                    }
                }

                if (!isWholeFFValidation) {
                    invalidFF = session.append(invalidFF, out -> out.write(print(listReader.getUntokenizedRow(), csvPref, true)));
                    validFF = session.append(validFF, out -> out.write(print(listReader.getUntokenizedRow(), csvPref, true)));
                    isFirstLineValid.set(false);
                    isFirstLineInvalid.set(false);
                }
            }

            boolean stop = false;

            while (!stop) {
                try {

                    // read next row and check if no more row
                    stop = listReader.read(includeAllViolations && valid, cellProcs) == null;

                    if (!isWholeFFValidation && !stop) {
                        validFF = session.append(validFF, out -> out.write(print(listReader.getUntokenizedRow(), csvPref, isFirstLineValid.get())));
                        okCount++;

                        if (isFirstLineValid.get()) {
                            isFirstLineValid.set(false);
                        }
                    }
                } catch (final SuperCsvException e) {
                    valid = false;
                    if (isWholeFFValidation) {
                        validationError = e.getLocalizedMessage();
                        logger.debug("Failed to validate {} against schema due to {}; routing to 'invalid'", flowFile, e);
                        break;
                    } else {
                        // we append the invalid line to the flow file that will be routed to invalid relationship
                        invalidFF = session.append(invalidFF, out -> out.write(print(listReader.getUntokenizedRow(), csvPref, isFirstLineInvalid.get())));

                        if (isFirstLineInvalid.get()) {
                            isFirstLineInvalid.set(false);
                        }

                        if (validationError == null) {
                            validationError = e.getLocalizedMessage();
                        }
                    }
                } finally {
                    if (!isWholeFFValidation) {
                        totalCount++;
                    }
                }
            }

        } catch (final IOException e) {
            valid = false;
            logger.error("Failed to validate {} against schema due to {}", flowFile, e);
        }

        if (isWholeFFValidation) {
            if (valid) {
                logger.debug("Successfully validated {} against schema; routing to 'valid'", flowFile);
                session.getProvenanceReporter().route(flowFile, REL_VALID);
                session.transfer(flowFile, REL_VALID);
            } else {
                session.getProvenanceReporter().route(flowFile, REL_INVALID);
                session.putAttribute(flowFile, "validation.error.message", validationError);
                session.transfer(flowFile, REL_INVALID);
            }
        } else {
            if (valid) {
                logger.debug("Successfully validated {} against schema; routing to 'valid'", validFF);
                session.getProvenanceReporter().route(validFF, REL_VALID, "All " + totalCount + " line(s) are valid");
                session.putAttribute(validFF, "count.valid.lines", Integer.toString(totalCount));
                session.putAttribute(validFF, "count.total.lines", Integer.toString(totalCount));
                session.transfer(validFF, REL_VALID);
                session.remove(invalidFF);
                session.remove(flowFile);
            } else if (okCount != 0) {
                // because of the finally within the 'while' loop
                totalCount--;

                logger.debug("Successfully validated {}/{} line(s) in {} against schema; routing valid lines to 'valid' and invalid lines to 'invalid'",
                        okCount, totalCount, flowFile);
                session.getProvenanceReporter().route(validFF, REL_VALID, okCount + " valid line(s)");
                session.putAttribute(validFF, "count.total.lines", Integer.toString(totalCount));
                session.putAttribute(validFF, "count.valid.lines", Integer.toString(okCount));
                session.transfer(validFF, REL_VALID);
                session.getProvenanceReporter().route(invalidFF, REL_INVALID, (totalCount - okCount) + " invalid line(s)");
                session.putAttribute(invalidFF, "count.invalid.lines", Integer.toString((totalCount - okCount)));
                session.putAttribute(invalidFF, "count.total.lines", Integer.toString(totalCount));
                session.putAttribute(invalidFF, "validation.error.message", validationError);
                session.transfer(invalidFF, REL_INVALID);
                session.remove(flowFile);
            } else {
                logger.debug("All lines in {} are invalid; routing to 'invalid'", invalidFF);
                session.getProvenanceReporter().route(invalidFF, REL_INVALID, "All " + totalCount + " line(s) are invalid");
                session.putAttribute(invalidFF, "count.invalid.lines", Integer.toString(totalCount));
                session.putAttribute(invalidFF, "count.total.lines", Integer.toString(totalCount));
                session.putAttribute(invalidFF, "validation.error.message", validationError);
                session.transfer(invalidFF, REL_INVALID);
                session.remove(validFF);
                session.remove(flowFile);
            }
        }
    }

    private byte[] print(String row, CsvPreference csvPref, boolean isFirstLine) {
        StringBuffer buffer = new StringBuffer();
        if (!isFirstLine) {
            buffer.append(csvPref.getEndOfLineSymbols());
        }
        return buffer.append(row).toString().getBytes();
    }

    /**
     * This is required to avoid the side effect of Parse* cell processors. If not overriding
     * this method, parsing will return objects and writing objects could result in a different
     * output in comparison to the input.
     */
    private class NifiCsvListReader extends CsvListReader {

        public NifiCsvListReader(Reader reader, CsvPreference preferences) {
            super(reader, preferences);
        }

        public List<Object> read(boolean includeAllViolations, CellProcessor... processors) throws IOException {
            if ( processors == null ) {
                throw new NullPointerException("Processors should not be null");
            }
            if ( readRow() ) {
                executeProcessors(new ArrayList<>(getColumns().size()), processors, includeAllViolations);
                return new ArrayList<>(getColumns());
            }
            return null; // EOF
        }

        protected List<Object> executeProcessors(List<Object> processedColumns, CellProcessor[] processors, boolean includeAllViolations) {
            this.executeCellProcessors(processedColumns, getColumns(), processors, getLineNumber(), getRowNumber(), includeAllViolations);
            return processedColumns;
        }

        private void executeCellProcessors(final List<Object> destination, final List<?> source,
            final CellProcessor[] processors, final int lineNo, final int rowNo, boolean includeAllViolations) {

            // the context used when cell processors report exceptions
            final CsvContext context = new CsvContext(lineNo, rowNo, 1);
            context.setRowSource(new ArrayList<>(source));

            if (source.size() != processors.length) {
                throw new SuperCsvException(String.format(
                    "The number of columns to be processed (%d) must match the number of CellProcessors (%d): check that the number"
                        + " of CellProcessors you have defined matches the expected number of columns being read/written",
                    source.size(), processors.length), context);
            }

            destination.clear();

            List<String> errors = new ArrayList<>();

            for (int i = 0; i < source.size(); i++) {

                try {
                    context.setColumnNumber(i + 1); // update context (columns start at 1)

                    if (processors[i] == null) {
                        destination.add(source.get(i)); // no processing required
                    } else {
                        destination.add(processors[i].execute(source.get(i), context)); // execute the processor chain
                    }

                } catch (SuperCsvException e) {
                    if (includeAllViolations) {
                        if (errors.isEmpty()) {
                            errors.add(String.format("At {line=%d, row=%d}", e.getCsvContext().getLineNumber(), e.getCsvContext().getRowNumber()));
                        }
                        final String coordinates = String.format("{column=%d}", e.getCsvContext().getColumnNumber());
                        final String errorMessage = e.getLocalizedMessage() + " at " + coordinates;
                        errors.add(errorMessage);
                    } else {
                        final String coordinates = String.format("{line=%d, row=%d, column=%d}", e.getCsvContext().getLineNumber(),
                                e.getCsvContext().getRowNumber(), e.getCsvContext().getColumnNumber());
                        final String errorMessage = e.getLocalizedMessage() + " at " + coordinates;
                        throw new SuperCsvException(errorMessage);
                    }
                }
            }

            if (!errors.isEmpty()) {
                throw new SuperCsvException(String.join(", ", errors));
            }
        }
    }
}
