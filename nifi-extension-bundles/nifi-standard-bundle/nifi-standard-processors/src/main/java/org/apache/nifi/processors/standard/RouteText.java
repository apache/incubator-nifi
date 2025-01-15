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

import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.DynamicRelationship;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.AttributeExpression.ResultType;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.util.LineDemarcator;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"attributes", "routing", "text", "regexp", "regex", "Regular Expression", "Expression Language", "csv", "filter", "logs", "delimited", "find", "string", "search", "filter", "detect"})
@CapabilityDescription("Routes textual data based on a set of user-defined rules. Each line in an incoming FlowFile is compared against the values specified by user-defined Properties. "
    + "The mechanism by which the text is compared to these user-defined properties is defined by the 'Matching Strategy'. The data is then routed according to these rules, routing "
    + "each line of the text individually.")
@DynamicProperty(name = "Relationship Name", value = "value to match against", description = "Routes data that matches the value specified in the Dynamic Property Value to the "
    + "Relationship specified in the Dynamic Property Key.", expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
@DynamicRelationship(name = "Name from Dynamic Property", description = "FlowFiles that match the Dynamic Property's value")
@WritesAttributes({
    @WritesAttribute(attribute = "RouteText.Route", description = "The name of the relationship to which the FlowFile was routed."),
    @WritesAttribute(attribute = "RouteText.Group", description = "The value captured by all capturing groups in the 'Grouping Regular Expression' property. "
            + "If this property is not set or contains no capturing groups, this attribute will not be added.")
})
@UseCase(
    description = "Drop blank or empty lines from the FlowFile's content.",
    keywords = {"filter", "drop", "empty", "blank", "remove", "delete", "strip out", "lines", "text"},
    configuration = """
        "Routing Strategy" = "Route to each matching Property Name"
        "Matching Strategy" = "Matches Regular Expression"
        "Empty Line" = "^$"

        Auto-terminate the "Empty Line" relationship.
        Connect the "unmatched" relationship to the next processor in your flow.
        """
)
@UseCase(
    description = "Remove specific lines of text from a file, such as those containing a specific word or having a line length over some threshold.",
    keywords = {"filter", "drop", "empty", "blank", "remove", "delete", "strip out", "lines", "text", "expression language"},
    configuration = """
        "Routing Strategy" = "Route to each matching Property Name"
        "Matching Strategy" = "Satisfies Expression"

        An additional property should be added named "Filter Out." The value should be a NiFi Expression Language Expression that can refer to two variables \
        (in addition to FlowFile attributes): `line`, which is the line of text being evaluated; and `lineNo`, which is the line number in the file (starting with 1). \
        The Expression should return `true` for any line that should be dropped.

        For example, to remove any line that starts with a # symbol, we can set "Filter Out" to `${line:startsWith("#")}`.
        We could also remove the first 2 lines of text by setting "Filter Out" to `${lineNo:le(2)}`. Note that we use the `le` function because we want lines numbers \
        less than or equal to `2`, since the line index is 1-based.

        Auto-terminate the "Filter Out" relationship.
        Connect the "unmatched" relationship to the next processor in your flow.
        """
)
public class RouteText extends AbstractProcessor {

    public static final String ROUTE_ATTRIBUTE_KEY = "RouteText.Route";
    public static final String GROUP_ATTRIBUTE_KEY = "RouteText.Group";

    private static final String routeAllMatchValue = "Route to 'matched' if line matches all conditions";
    private static final String routeAnyMatchValue = "Route to 'matched' if lines matches any condition";
    private static final String routePropertyNameValue = "Route to each matching Property Name";

    private static final String startsWithValue = "Starts With";
    private static final String endsWithValue = "Ends With";
    private static final String containsValue = "Contains";
    private static final String equalsValue = "Equals";
    private static final String matchesRegularExpressionValue = "Matches Regular Expression";
    private static final String containsRegularExpressionValue = "Contains Regular Expression";
    private static final String satisfiesExpression = "Satisfies Expression";

    public static final AllowableValue ROUTE_TO_MATCHING_PROPERTY_NAME = new AllowableValue(routePropertyNameValue, routePropertyNameValue,
        "Lines will be routed to each relationship whose corresponding expression evaluates to 'true'");
    public static final AllowableValue ROUTE_TO_MATCHED_WHEN_ALL_PROPERTIES_MATCH = new AllowableValue(routeAllMatchValue, routeAllMatchValue,
        "Requires that all user-defined expressions evaluate to 'true' for the line to be considered a match");
    public static final AllowableValue ROUTE_TO_MATCHED_WHEN_ANY_PROPERTY_MATCHES = new AllowableValue(routeAnyMatchValue, routeAnyMatchValue,
        "Requires that at least one user-defined expression evaluate to 'true' for the line to be considered a match");

    public static final AllowableValue STARTS_WITH = new AllowableValue(startsWithValue, startsWithValue,
        "Match lines based on whether the line starts with the property value");
    public static final AllowableValue ENDS_WITH = new AllowableValue(endsWithValue, endsWithValue,
        "Match lines based on whether the line ends with the property value");
    public static final AllowableValue CONTAINS = new AllowableValue(containsValue, containsValue,
        "Match lines based on whether the line contains the property value");
    public static final AllowableValue EQUALS = new AllowableValue(equalsValue, equalsValue,
        "Match lines based on whether the line equals the property value");
    public static final AllowableValue MATCHES_REGULAR_EXPRESSION = new AllowableValue(matchesRegularExpressionValue, matchesRegularExpressionValue,
        "Match lines based on whether the line exactly matches the Regular Expression that is provided as the Property value");
    public static final AllowableValue CONTAINS_REGULAR_EXPRESSION = new AllowableValue(containsRegularExpressionValue, containsRegularExpressionValue,
        "Match lines based on whether the line contains some text that matches the Regular Expression that is provided as the Property value");
    public static final AllowableValue SATISFIES_EXPRESSION = new AllowableValue(satisfiesExpression, satisfiesExpression,
        "Match lines based on whether or not the the text satisfies the given Expression Language expression. I.e., the line will match if the property value, evaluated as "
            + "an Expression, returns true. The expression is able to reference FlowFile Attributes, as well as the variables 'line' (which is the text of the line to evaluate) and "
            + "'lineNo' (which is the line number being evaluated. This will be 1 for the first line, 2 for the second and so on).");

    public static final PropertyDescriptor ROUTE_STRATEGY = new PropertyDescriptor.Builder()
        .name("Routing Strategy")
        .description("Specifies how to determine which Relationship(s) to use when evaluating the lines of incoming text against the 'Matching Strategy' and user-defined properties.")
        .required(true)
        .allowableValues(ROUTE_TO_MATCHING_PROPERTY_NAME, ROUTE_TO_MATCHED_WHEN_ALL_PROPERTIES_MATCH, ROUTE_TO_MATCHED_WHEN_ANY_PROPERTY_MATCHES)
        .defaultValue(ROUTE_TO_MATCHING_PROPERTY_NAME.getValue())
        .dynamic(false)
        .build();

    public static final PropertyDescriptor MATCH_STRATEGY = new PropertyDescriptor.Builder()
        .name("Matching Strategy")
        .description("Specifies how to evaluate each line of incoming text against the user-defined properties.")
        .required(true)
        .allowableValues(SATISFIES_EXPRESSION, STARTS_WITH, ENDS_WITH, CONTAINS, EQUALS, MATCHES_REGULAR_EXPRESSION, CONTAINS_REGULAR_EXPRESSION)
        .dynamic(false)
        .build();

    public static final PropertyDescriptor TRIM_WHITESPACE = new PropertyDescriptor.Builder()
        .name("Ignore Leading/Trailing Whitespace")
        .description("Indicates whether or not the whitespace at the beginning and end of the lines should be ignored when evaluating the line.")
        .required(true)
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .defaultValue("true")
        .dynamic(false)
        .build();

    static final PropertyDescriptor IGNORE_CASE = new PropertyDescriptor.Builder()
        .name("Ignore Case")
        .description("If true, capitalization will not be taken into account when comparing values. E.g., matching against 'HELLO' or 'hello' will have the same result. "
            + "This property is ignored if the 'Matching Strategy' is set to 'Satisfies Expression'.")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .allowableValues("true", "false")
        .defaultValue("false")
        .required(true)
        .build();

    static final PropertyDescriptor GROUPING_REGEX = new PropertyDescriptor.Builder()
        .name("Grouping Regular Expression")
        .description("Specifies a Regular Expression to evaluate against each line to determine which Group the line should be placed in. "
            + "The Regular Expression must have at least one Capturing Group that defines the line's Group. If multiple Capturing Groups exist in the Regular Expression, the values from all "
            + "Capturing Groups will be concatenated together. Two lines will not be placed into the same FlowFile unless they both have the same value for the Group "
            + "(or neither line matches the Regular Expression). For example, to group together all lines in a CSV File by the first column, we can set this value to \"(.*?),.*\". "
            + "Two lines that have the same Group but different Relationships will never be placed into the same FlowFile.")
        .addValidator(StandardValidators.createRegexValidator(1, Integer.MAX_VALUE, false))
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .required(false)
        .build();

    public static final PropertyDescriptor CHARACTER_SET = new PropertyDescriptor.Builder()
        .name("Character Set")
        .description("The Character Set in which the incoming text is encoded")
        .required(true)
        .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
        .defaultValue("UTF-8")
        .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            ROUTE_STRATEGY,
            MATCH_STRATEGY,
            CHARACTER_SET,
            TRIM_WHITESPACE,
            IGNORE_CASE,
            GROUPING_REGEX
    );

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
        .name("original")
        .description("The original input file will be routed to this destination when the lines have been successfully routed to 1 or more relationships")
        .build();
    public static final Relationship REL_NO_MATCH = new Relationship.Builder()
        .name("unmatched")
        .description("Data that does not satisfy the required user-defined rules will be routed to this Relationship")
        .build();
    public static final Relationship REL_MATCH = new Relationship.Builder()
        .name("matched")
        .description("Data that satisfies the required user-defined rules will be routed to this Relationship")
        .build();

    private static final Group EMPTY_GROUP = new Group(Collections.emptyList());

    private final AtomicReference<Set<Relationship>> relationships =
            new AtomicReference<>(Set.of(REL_ORIGINAL, REL_NO_MATCH));
    private volatile String configuredRouteStrategy = ROUTE_STRATEGY.getDefaultValue();
    private volatile Set<String> dynamicPropertyNames = new HashSet<>();

    /**
     * Cache of dynamic properties set during {@link #onScheduled(ProcessContext)} for quick access in
     * {@link #onTrigger(ProcessContext, ProcessSession)}
     */
    private volatile Map<Relationship, PropertyValue> propertyMap = new HashMap<>();
    private volatile Pattern groupingRegex = null;

    final static int PATTERNS_CACHE_MAXIMUM_ENTRIES = 1024;

    /**
     * LRU cache for the compiled patterns. The size of the cache is determined by the value of
     * {@link #PATTERNS_CACHE_MAXIMUM_ENTRIES}.
     */
    final ConcurrentMap<String, Pattern> patternsCache = Caffeine.newBuilder()
            .maximumSize(PATTERNS_CACHE_MAXIMUM_ENTRIES)
            .<String, Pattern>build()
            .asMap();

    private Pattern cachedCompiledPattern(final String regex, final boolean ignoreCase) {
        return patternsCache.computeIfAbsent(regex,
                r -> ignoreCase ? Pattern.compile(r, Pattern.CASE_INSENSITIVE) : Pattern.compile(r));
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships.get();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
            .required(false)
            .name(propertyDescriptorName)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dynamic(true)
            .build();
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (descriptor.equals(IGNORE_CASE) && !newValue.equals(oldValue)) {
            patternsCache.clear();
        }

        if (descriptor.equals(ROUTE_STRATEGY)) {
            configuredRouteStrategy = newValue;
        } else {
            final Set<String> newDynamicPropertyNames = new HashSet<>(dynamicPropertyNames);
            if (newValue == null) {
                newDynamicPropertyNames.remove(descriptor.getName());
            } else if (oldValue == null && descriptor.isDynamic()) { // new property
                newDynamicPropertyNames.add(descriptor.getName());
            }

            this.dynamicPropertyNames = Set.copyOf(newDynamicPropertyNames);
        }

        // formulate the new set of Relationships
        final Set<String> allDynamicProps = this.dynamicPropertyNames;
        final Set<Relationship> newRelationships = new HashSet<>();
        final String routeStrategy = configuredRouteStrategy;
        if (ROUTE_TO_MATCHING_PROPERTY_NAME.getValue().equals(routeStrategy)) {
            for (final String propName : allDynamicProps) {
                newRelationships.add(new Relationship.Builder().name(propName).build());
            }
        } else {
            newRelationships.add(REL_MATCH);
        }

        newRelationships.add(REL_ORIGINAL);
        newRelationships.add(REL_NO_MATCH);
        this.relationships.set(newRelationships);
    }

    /**
     * When this processor is scheduled, update the dynamic properties into the map
     * for quick access during each onTrigger call
     *
     * @param context ProcessContext used to retrieve dynamic properties
     */
    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final String regex = context.getProperty(GROUPING_REGEX).getValue();
        if (regex != null) {
            groupingRegex = Pattern.compile(regex);
        }

        final Map<Relationship, PropertyValue> newPropertyMap = new HashMap<>();
        for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
            if (!descriptor.isDynamic()) {
                continue;
            }
            getLogger().debug("Adding new dynamic property: {}", descriptor);
            newPropertyMap.put(new Relationship.Builder().name(descriptor.getName()).build(), context.getProperty(descriptor));
        }

        this.propertyMap = newPropertyMap;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Collection<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));
        boolean dynamicProperty = false;

        final String matchStrategy = validationContext.getProperty(MATCH_STRATEGY).getValue();
        final boolean compileRegex = matchStrategy.equals(matchesRegularExpressionValue) || matchStrategy.equals(containsRegularExpressionValue);
        final boolean requiresExpression = matchStrategy.equalsIgnoreCase(satisfiesExpression);

        Validator validator = null;
        if (compileRegex) {
            validator = StandardValidators.createRegexValidator(0, Integer.MAX_VALUE, true);
        }

        Map<PropertyDescriptor, String> allProperties = validationContext.getProperties();
        for (final PropertyDescriptor descriptor : allProperties.keySet()) {
            if (descriptor.isDynamic()) {
                dynamicProperty = true;

                final String propValue = validationContext.getProperty(descriptor).getValue();

                if (compileRegex) {
                    ValidationResult validationResult = validator.validate(descriptor.getName(), propValue, validationContext);
                    if (validationResult != null) {
                        results.add(validationResult);
                    }
                } else if (requiresExpression) {
                    try {
                        final ResultType resultType = validationContext.newExpressionLanguageCompiler().compile(propValue).getResultType();
                        if (resultType != ResultType.BOOLEAN) {
                            results.add(new ValidationResult.Builder().valid(false).input(propValue).subject(descriptor.getName())
                                .explanation("expression returns type of " + resultType.name() + " but is required to return a Boolean value").build());
                        }
                    } catch (final IllegalArgumentException iae) {
                        results.add(new ValidationResult.Builder().valid(false).input(propValue).subject(descriptor.getName())
                            .explanation("input is not a valid Expression Language expression").build());
                    }
                }
            }
        }

        if (!dynamicProperty) {
            results.add(new ValidationResult.Builder().subject("Dynamic Properties")
                .explanation("In order to route text there must be dynamic properties to match against").valid(false).build());
        }

        return results;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final FlowFile originalFlowFile = session.get();
        if (originalFlowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        final Charset charset = Charset.forName(context.getProperty(CHARACTER_SET).getValue());
        final boolean trim = context.getProperty(TRIM_WHITESPACE).asBoolean();
        final String routeStrategy = context.getProperty(ROUTE_STRATEGY).getValue();
        final String matchStrategy = context.getProperty(MATCH_STRATEGY).getValue();
        final boolean ignoreCase = context.getProperty(IGNORE_CASE).asBoolean();

        final boolean compileRegex = matchStrategy.equals(matchesRegularExpressionValue) || matchStrategy.equals(containsRegularExpressionValue);
        final boolean usePropValue = matchStrategy.equals(satisfiesExpression);

        // Build up a Map of Relationship to object, where the object is the
        // thing that each line is compared against
        final Map<Relationship, Object> propValueMap;
        final Map<Relationship, PropertyValue> propMap = this.propertyMap;
        if (usePropValue) {
            // If we are using an Expression Language we want a Map where the value is the
            // PropertyValue, so we can just use the 'propMap' - no need to copy it.
            propValueMap = (Map) propMap;
        } else {
            propValueMap = new HashMap<>(propMap.size());
            for (final Map.Entry<Relationship, PropertyValue> entry : propMap.entrySet()) {
                final String value = entry.getValue().evaluateAttributeExpressions(originalFlowFile).getValue();

                propValueMap.put(entry.getKey(), compileRegex ? cachedCompiledPattern(value, ignoreCase) : value);
            }
        }

        final Map<Relationship, Map<Group, FlowFile>> flowFileMap = new HashMap<>();
        final Pattern groupPattern = groupingRegex;

        session.read(originalFlowFile, in -> {
            try (final LineDemarcator demarcator = new LineDemarcator(in, charset, Integer.MAX_VALUE, 8192)) {

                final Map<String, String> variables = new HashMap<>(2);

                int lineCount = 0;
                String line;
                while ((line = demarcator.nextLine()) != null) {

                    final String matchLine;
                    if (trim) {
                        matchLine = line.trim();
                    } else {
                        // Always trim off the new-line and carriage return characters before evaluating the line.
                        // The NLKBufferedReader maintains these characters so that when we write the line out we can maintain
                        // these characters. However, we don't actually want to match against these characters.
                        final String lineWithoutEndings;
                        final int indexOfCR = line.indexOf("\r");
                        final int indexOfNL = line.indexOf("\n");
                        if (indexOfCR > 0 && indexOfNL > 0) {
                            lineWithoutEndings = line.substring(0, Math.min(indexOfCR, indexOfNL));
                        } else if (indexOfCR > 0) {
                            lineWithoutEndings = line.substring(0, indexOfCR);
                        } else if (indexOfNL > 0) {
                            lineWithoutEndings = line.substring(0, indexOfNL);
                        } else {
                            lineWithoutEndings = line;
                        }

                        matchLine = lineWithoutEndings;
                    }

                    variables.put("line", line);
                    variables.put("lineNo", String.valueOf(++lineCount));

                    int propertiesThatMatchedLine = 0;
                    for (final Map.Entry<Relationship, Object> entry : propValueMap.entrySet()) {
                        boolean lineMatchesProperty = lineMatches(matchLine, entry.getValue(), matchStrategy, ignoreCase, originalFlowFile, variables);
                        if (lineMatchesProperty) {
                            propertiesThatMatchedLine++;
                        }

                        if (lineMatchesProperty && ROUTE_TO_MATCHING_PROPERTY_NAME.getValue().equals(routeStrategy)) {
                            // route each individual line to each Relationship that matches. This one matches.
                            final Relationship relationship = entry.getKey();

                            final Group group = getGroup(matchLine, groupPattern);
                            appendLine(session, flowFileMap, relationship, originalFlowFile, line, charset, group);
                            continue;
                        }

                        // break as soon as possible to avoid calculating things we don't need to calculate.
                        if (lineMatchesProperty && ROUTE_TO_MATCHED_WHEN_ANY_PROPERTY_MATCHES.getValue().equals(routeStrategy)) {
                            break;
                        }

                        if (!lineMatchesProperty && ROUTE_TO_MATCHED_WHEN_ALL_PROPERTIES_MATCH.getValue().equals(routeStrategy)) {
                            break;
                        }
                    }

                    final Relationship relationship;
                    if (ROUTE_TO_MATCHING_PROPERTY_NAME.getValue().equals(routeStrategy) && propertiesThatMatchedLine > 0) {
                        // Set relationship to null so that we do not append the line to each FlowFile again. #appendLine is called
                        // above within the loop, as the line may need to go to multiple different FlowFiles.
                        relationship = null;
                    } else if (ROUTE_TO_MATCHED_WHEN_ANY_PROPERTY_MATCHES.getValue().equals(routeStrategy) && propertiesThatMatchedLine > 0) {
                        relationship = REL_MATCH;
                    } else if (ROUTE_TO_MATCHED_WHEN_ALL_PROPERTIES_MATCH.getValue().equals(routeStrategy) && propertiesThatMatchedLine == propValueMap.size()) {
                        relationship = REL_MATCH;
                    } else {
                        relationship = REL_NO_MATCH;
                    }

                    // If the target relationship (usually REL_NO_MATCH) is auto-terminated, don't bother creating the flowfile or writing to it.
                    if (relationship != null && !context.isAutoTerminated(relationship)) {
                        final Group group = getGroup(matchLine, groupPattern);
                        appendLine(session, flowFileMap, relationship, originalFlowFile, line, charset, group);
                    }
                }
            }
        });

        for (final Map.Entry<Relationship, Map<Group, FlowFile>> entry : flowFileMap.entrySet()) {
            final Relationship relationship = entry.getKey();
            final Map<Group, FlowFile> groupToFlowFileMap = entry.getValue();

            for (final Map.Entry<Group, FlowFile> flowFileEntry : groupToFlowFileMap.entrySet()) {
                final Group group = flowFileEntry.getKey();
                final FlowFile flowFile = flowFileEntry.getValue();

                logger.info("Created {} from {}; routing to relationship {}", flowFile, originalFlowFile, relationship.getName());
                FlowFile updatedFlowFile = session.putAllAttributes(flowFile, Map.of(
                        ROUTE_ATTRIBUTE_KEY, relationship.getName(),
                        GROUP_ATTRIBUTE_KEY, StringUtils.join(group.getCapturedValues(), ", ")
                ));
                session.getProvenanceReporter().route(updatedFlowFile, entry.getKey());
                session.transfer(updatedFlowFile, entry.getKey());
            }
        }

        // now transfer the original flow file
        FlowFile flowFile = originalFlowFile;
        logger.info("Routing {} to {}", flowFile, REL_ORIGINAL);
        session.getProvenanceReporter().route(originalFlowFile, REL_ORIGINAL);
        flowFile = session.putAttribute(flowFile, ROUTE_ATTRIBUTE_KEY, REL_ORIGINAL.getName());
        session.transfer(flowFile, REL_ORIGINAL);
    }


    private Group getGroup(final String line, final Pattern groupPattern) {
        if (groupPattern == null) {
            return EMPTY_GROUP;
        } else {
            final Matcher matcher = groupPattern.matcher(line);
            if (matcher.matches()) {
                final List<String> capturingGroupValues = new ArrayList<>(matcher.groupCount());
                for (int i = 1; i <= matcher.groupCount(); i++) {
                    capturingGroupValues.add(matcher.group(i));
                }
                return new Group(capturingGroupValues);
            } else {
                return EMPTY_GROUP;
            }
        }
    }

    private void appendLine(final ProcessSession session, final Map<Relationship, Map<Group, FlowFile>> flowFileMap, final Relationship relationship,
        final FlowFile original, final String line, final Charset charset, final Group group) {

        final Map<Group, FlowFile> groupToFlowFileMap = flowFileMap.computeIfAbsent(relationship, k -> new HashMap<>());

        FlowFile flowFile = groupToFlowFileMap.get(group);
        if (flowFile == null) {
            flowFile = session.create(original);
        }

        flowFile = session.append(flowFile, out -> out.write(line.getBytes(charset)));

        groupToFlowFileMap.put(group, flowFile);
    }


    protected static boolean lineMatches(final String line, final Object comparison, final String matchingStrategy, final boolean ignoreCase,
        final FlowFile flowFile, final Map<String, String> variables) {
        switch (matchingStrategy) {
            case startsWithValue:
                if (ignoreCase) {
                    return line.toLowerCase().startsWith(((String) comparison).toLowerCase());
                } else {
                    return line.startsWith((String) comparison);
                }
            case endsWithValue:
                if (ignoreCase) {
                    return line.toLowerCase().endsWith(((String) comparison).toLowerCase());
                } else {
                    return line.endsWith((String) comparison);
                }
            case containsValue:
                if (ignoreCase) {
                    return line.toLowerCase().contains(((String) comparison).toLowerCase());
                } else {
                    return line.contains((String) comparison);
                }
            case equalsValue:
                if (ignoreCase) {
                    return line.equalsIgnoreCase((String) comparison);
                } else {
                    return line.equals(comparison);
                }
            case matchesRegularExpressionValue:
                return ((Pattern) comparison).matcher(line).matches();
            case containsRegularExpressionValue:
                return ((Pattern) comparison).matcher(line).find();
            case satisfiesExpression: {
                final PropertyValue booleanProperty = (PropertyValue) comparison;
                return booleanProperty.evaluateAttributeExpressions(flowFile, variables).asBoolean();
            }
        }

        return false;
    }


    private static class Group {
        private final List<String> capturedValues;

        public Group(final List<String> capturedValues) {
            this.capturedValues = capturedValues;
        }

        public List<String> getCapturedValues() {
            return capturedValues;
        }

        @Override
        public String toString() {
            return "Group" + capturedValues;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((capturedValues == null) ? 0 : capturedValues.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }

            if (obj == null) {
                return false;
            }

            if (getClass() != obj.getClass()) {
                return false;
            }

            Group other = (Group) obj;
            if (capturedValues == null) {
                return other.capturedValues == null;
            } else return capturedValues.equals(other.capturedValues);

        }
    }
}