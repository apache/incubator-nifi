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
package org.apache.nifi.processors.attributes;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DefaultRunDuration;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.search.SearchContext;
import org.apache.nifi.search.SearchResult;
import org.apache.nifi.search.Searchable;
import org.apache.nifi.update.attributes.Action;
import org.apache.nifi.update.attributes.Condition;
import org.apache.nifi.update.attributes.Criteria;
import org.apache.nifi.update.attributes.FlowFilePolicy;
import org.apache.nifi.update.attributes.Rule;
import org.apache.nifi.update.attributes.serde.CriteriaSerDe;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

@SideEffectFree
@SupportsBatching(defaultDuration = DefaultRunDuration.TWENTY_FIVE_MILLIS)
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"attributes", "modification", "update", "delete", "Attribute Expression Language", "state"})
@CapabilityDescription("Updates the Attributes for a FlowFile by using the Attribute Expression Language and/or deletes the attributes based on a regular expression")
@DynamicProperty(name = "A FlowFile attribute to update", value = "The value to set it to", expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        description = "Updates a FlowFile attribute specified by the Dynamic Property's key with the value specified by the Dynamic Property's value")
@WritesAttribute(attribute = "See additional details", description = "This processor may write or remove zero or more attributes as described in additional details")
@Stateful(scopes = {Scope.LOCAL}, description = "Gives the option to store values not only on the FlowFile but as stateful variables to be referenced in a recursive manner.")
@UseCase(
    description = "Add a new FlowFile attribute",
    configuration = """
        Leave "Delete Attributes Expression" and "Stateful Variables Initial Value" unset.
        Set "Store State" to "Do not store state".

        Add a new property. The name of the property will become the name of the newly added attribute.
        The value of the property will become the value of the newly added attribute. The value may use the NiFi Expression Language in order to reference other
        attributes or call Expression Language functions.
        """
)
@UseCase(
    description = "Overwrite a FlowFile attribute with a new value",
    configuration = """
        Leave "Delete Attributes Expression" and "Stateful Variables Initial Value" unset.
        Set "Store State" to "Do not store state".

        Add a new property. The name of the property will become the name of the attribute whose value will be overwritten.
        The value of the property will become the new value of the attribute. The value may use the NiFi Expression Language in order to reference other
        attributes or call Expression Language functions.

        For example, to change the `txId` attribute to the uppercase version of its current value, add a property named `txId` with a value of `${txId:toUpper()}`
        """
)
@UseCase(
    description = "Rename a file",
    configuration = """
        Leave "Delete Attributes Expression" and "Stateful Variables Initial Value" unset.
        Set "Store State" to "Do not store state".

        Add a new property whose name is `filename` and whose value is the desired filename.

        For example, to set the filename to `abc.txt`, add a property named `filename` with a value of `abc.txt`.
        To add the `txId` attribute as a prefix to the filename, add a property named `filename` with a value of `${txId}${filename}`.
        Or, to make the filename more readable, separate the txId from the rest of the filename with a hyphen by using a value of `${txId}-${filename}`.
        """
)
public class UpdateAttribute extends AbstractProcessor implements Searchable {


    public static final String DO_NOT_STORE_STATE = "Do not store state";
    public static final String STORE_STATE_LOCALLY = "Store state locally";

    private final AtomicReference<Criteria> criteriaCache = new AtomicReference<>(null);
    private final ConcurrentMap<String, PropertyValue> propertyValues = new ConcurrentHashMap<>();

    /**
     * This field caches a 'canonical' value for a given attribute value. When this processor is used to update an attribute or add a new
     * attribute, if Expression Language is used, we may well end up with a new String object for each attribute for each FlowFile. As a result,
     * we will store a different String object for the attribute value of every FlowFile, meaning that we have to keep a lot of String objects
     * in heap. By using this 'canonical lookup', we are able to keep only a single String object on the heap.
     *
     * For example, if we have a property named "abc" and the value is "${abc}${xyz}", and we send through 1,000 FlowFiles with attributes abc="abc"
     * and xyz="xyz", then would end up with 1,000 String objects with a value of "abcxyz". By using this canonical representation, we are able to
     * instead hold a single String whose value is "abcxyz" instead of holding 1,000 String objects in heap (1,000 String objects may still be created
     * when calling PropertyValue.evaluateAttributeExpressions, but this way those values are garbage collected).
     */
    private LoadingCache<String, String> canonicalValueLookup;

    // relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .description("All successful FlowFiles are routed to this relationship").name("success").build();
    public static final Relationship REL_FAILED_SET_STATE = new Relationship.Builder()
            .description("A failure to set the state after adding the attributes to the FlowFile will route the FlowFile here.").name("set state fail").build();

    private final static Set<Relationship> STATELESS_RELATIONSHIP_SET = Set.of(
            REL_SUCCESS
    );

    private final static Set<Relationship> STATEFUL_RELATIONSHIP_SET = Set.of(
            REL_SUCCESS,
            REL_FAILED_SET_STATE
    );


    private volatile Set<Relationship> relationships;

    private static final Validator DELETE_PROPERTY_VALIDATOR = new Validator() {
        private final Validator DPV_RE_VALIDATOR = StandardValidators.createRegexValidator(0, Integer.MAX_VALUE, true);
        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
                final AttributeExpression.ResultType resultType = context.newExpressionLanguageCompiler().getResultType(input);
                if (!resultType.equals(AttributeExpression.ResultType.STRING)) {
                    return new ValidationResult.Builder()
                            .subject(subject)
                            .input(input)
                            .valid(false)
                            .explanation("Expected property to to return type " + AttributeExpression.ResultType.STRING +
                                    " but expression returns type " + resultType)
                            .build();
                }
                return new ValidationResult.Builder()
                        .subject(subject)
                        .input(input)
                        .valid(true)
                        .explanation("Property returns type " + AttributeExpression.ResultType.STRING)
                        .build();
            }

            return DPV_RE_VALIDATOR.validate(subject, input, context);
        }
    };

    // static properties
    public static final String DELETE_ATTRIBUTES_EXPRESSION_NAME = "Delete Attributes Expression";
    public static final PropertyDescriptor DELETE_ATTRIBUTES = new PropertyDescriptor.Builder()
            .name(DELETE_ATTRIBUTES_EXPRESSION_NAME)
            .displayName(DELETE_ATTRIBUTES_EXPRESSION_NAME)
            .description("Regular expression for attributes to be deleted from FlowFiles.  Existing attributes that match will be deleted regardless of whether they are updated by this processor.")
            .required(false)
            .addValidator(DELETE_PROPERTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final String STORE_STATE_NAME = "Store State";
    public static final PropertyDescriptor STORE_STATE = new PropertyDescriptor.Builder()
            .name(STORE_STATE_NAME)
            .displayName(STORE_STATE_NAME)
            .description("Select whether or not state will be stored. Selecting 'Stateless' will offer the default functionality of purely updating the attributes on a " +
                    "FlowFile in a stateless manner. Selecting a stateful option will not only store the attributes on the FlowFile but also in the Processors " +
                    "state. See the 'Stateful Usage' topic of the 'Additional Details' section of this processor's documentation for more information")
            .required(true)
            .allowableValues(DO_NOT_STORE_STATE, STORE_STATE_LOCALLY)
            .defaultValue(DO_NOT_STORE_STATE)
            .build();

    public static final String STATEFUL_VARIABLES_INIT_VALUE_NAME = "Stateful Variables Initial Value";
    public static final PropertyDescriptor STATEFUL_VARIABLES_INIT_VALUE = new PropertyDescriptor.Builder()
            .name(STATEFUL_VARIABLES_INIT_VALUE_NAME)
            .displayName(STATEFUL_VARIABLES_INIT_VALUE_NAME)
            .description("If using state to set/reference variables then this value is used to set the initial value of the stateful variable. This will only be used in the @OnScheduled method " +
                    "when state does not contain a value for the variable. This is required if running statefully but can be empty if needed.")
            .required(false)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor CANONICAL_VALUE_LOOKUP_CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("canonical-value-lookup-cache-size")
            .displayName("Cache Value Lookup Cache Size")
            .description("Specifies how many canonical lookup values should be stored in the cache")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("100")
            .required(true)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            DELETE_ATTRIBUTES,
            STORE_STATE,
            STATEFUL_VARIABLES_INIT_VALUE,
            CANONICAL_VALUE_LOOKUP_CACHE_SIZE
    );

    private volatile Map<String, Action> defaultActions;
    private volatile boolean debugEnabled;
    private volatile boolean stateful = false;


    public UpdateAttribute() {
        relationships = STATELESS_RELATIONSHIP_SET;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public boolean isStateful(final ProcessContext context) {
        return !context.getProperty(STORE_STATE).getValue().equals(DO_NOT_STORE_STATE);
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .dynamic(true)
                .build();
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        super.onPropertyModified(descriptor, oldValue, newValue);

        if (descriptor.equals(STORE_STATE)) {
            if (DO_NOT_STORE_STATE.equals(newValue)) {
                stateful = false;
                relationships = STATELESS_RELATIONSHIP_SET;
            } else {
                stateful = true;
                relationships = STATEFUL_RELATIONSHIP_SET;
            }
        }
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        final int cacheSize = context.getProperty(CANONICAL_VALUE_LOOKUP_CACHE_SIZE).asInteger();
        canonicalValueLookup = Caffeine.newBuilder()
                .maximumSize(cacheSize)
                .build(attributeValue -> attributeValue);

        criteriaCache.set(CriteriaSerDe.deserialize(context.getAnnotationData()));

        propertyValues.clear();

        if (stateful) {
            StateManager stateManager = context.getStateManager();
            StateMap state = stateManager.getState(Scope.LOCAL);
            Map<String, String> tempMap = new HashMap<>(state.toMap());
            String initValue = context.getProperty(STATEFUL_VARIABLES_INIT_VALUE).getValue();

            // Initialize the stateful default actions
            for (PropertyDescriptor entry : context.getProperties().keySet()) {
                if (entry.isDynamic()) {
                    if (!tempMap.containsKey(entry.getName())) {
                        tempMap.put(entry.getName(), initValue);
                    }
                }
            }

            // Initialize the stateful actions if the criteria exists
            final Criteria criteria = criteriaCache.get();
            if (criteria != null) {
                for (Rule rule : criteria.getRules()) {
                    for (Action action : rule.getActions()) {
                        if (!tempMap.containsKey(action.getAttribute())) {
                            tempMap.put(action.getAttribute(), initValue);
                        }
                    }
                }
            }

            context.getStateManager().setState(tempMap, Scope.LOCAL);
        }

        defaultActions = getDefaultActions(context.getProperties());
        debugEnabled = getLogger().isDebugEnabled();
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> reasons = new ArrayList<>(super.customValidate(context));

        if (!context.getProperty(STORE_STATE).getValue().equals(DO_NOT_STORE_STATE)) {
            String initValue = context.getProperty(STATEFUL_VARIABLES_INIT_VALUE).getValue();
            if (initValue == null) {
                reasons.add(new ValidationResult.Builder().subject(STATEFUL_VARIABLES_INIT_VALUE.getDisplayName()).valid(false)
                        .explanation("initial state value must be set if the processor is configured to store state.").build());
            }
        }

        Criteria criteria = null;
        try {
            criteria = CriteriaSerDe.deserialize(context.getAnnotationData());
        } catch (IllegalArgumentException iae) {
            reasons.add(new ValidationResult.Builder().valid(false).explanation("Unable to deserialize the update criteria." + iae.getMessage()).build());
        }

        // if there is criteria, validate it
        if (criteria != null) {
            final List<Rule> rules = criteria.getRules();

            if (rules == null) {
                reasons.add(new ValidationResult.Builder().valid(false).explanation("Update criteria has been specified by no rules were found.").build());
            } else {
                // validate the each rule
                for (final Rule rule : rules) {
                    if (rule.getName() == null || rule.getName().trim().isEmpty()) {
                        reasons.add(new ValidationResult.Builder().valid(false).explanation("A rule name was not specified.").build());
                    }

                    // validate each condition
                    final Set<Condition> conditions = rule.getConditions();
                    if (conditions == null) {
                        reasons.add(new ValidationResult.Builder().valid(false).explanation(String.format("No conditions for rule '%s' found.", rule.getName())).build());
                    } else {
                        for (final Condition condition : conditions) {
                            if (condition.getExpression() == null) {
                                reasons.add(new ValidationResult.Builder().valid(false).explanation(String.format("No expression for a condition in rule '%s' was found.", rule.getName())).build());
                            } else {
                                final String expression = condition.getExpression().trim();
                                reasons.add(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.BOOLEAN, false)
                                        .validate(String.format("Condition for rule '%s'.", rule.getName()), expression, context));
                            }
                        }
                    }

                    // validate each action
                    final Set<Action> actions = rule.getActions();
                    if (actions == null) {
                        reasons.add(new ValidationResult.Builder().valid(false).explanation(String.format("No actions for rule '%s' found.", rule.getName())).build());
                    } else {
                        for (final Action action : actions) {
                            if (action.getAttribute() == null) {
                                reasons.add(new ValidationResult.Builder().valid(false).explanation(String.format("An action in rule '%s' is missing the attribute name.", rule.getName())).build());
                            } else if (action.getValue() == null) {
                                reasons.add(new ValidationResult.Builder().valid(false)
                                        .explanation(String.format("No value for attribute '%s' in rule '%s' was found.", action.getAttribute(), rule.getName())).build());
                            } else {
                                reasons.add(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true)
                                        .validate(String.format("Action for rule '%s'.", rule.getName()), action.getValue(), context));
                            }
                        }
                    }
                }
            }
        }

        return reasons;
    }

    @Override
    public Collection<SearchResult> search(final SearchContext context) {
        final String term = context.getSearchTerm();

        final Collection<SearchResult> results = new ArrayList<>();
        if (StringUtils.isBlank(context.getAnnotationData())) {
            return results;
        }

        try {
            // parse the annotation data
            final Criteria criteria = CriteriaSerDe.deserialize(context.getAnnotationData());

            // ensure there are some rules
            if (criteria.getRules() != null) {
                final FlowFilePolicy flowFilePolicy = criteria.getFlowFilePolicy();
                if (flowFilePolicy != null && StringUtils.containsIgnoreCase(flowFilePolicy.name(), term)) {
                    results.add(new SearchResult.Builder().label("FlowFile policy").match(flowFilePolicy.name()).build());
                }

                for (final Rule rule : criteria.getRules()) {
                    if (StringUtils.containsIgnoreCase(rule.getName(), term)) {
                        results.add(new SearchResult.Builder().label("Rule name").match(rule.getName()).build());
                    }

                    // ensure there are some conditions
                    if (rule.getConditions() != null) {
                        for (final Condition condition : rule.getConditions()) {
                            if (StringUtils.containsIgnoreCase(condition.getExpression(), term)) {
                                results.add(new SearchResult.Builder().label(String.format("Condition in rule '%s'", rule.getName())).match(condition.getExpression()).build());
                            }
                        }
                    }

                    // ensure there are some actions
                    if (rule.getActions() != null) {
                        for (final Action action : rule.getActions()) {
                            if (StringUtils.containsIgnoreCase(action.getAttribute(), term)) {
                                results.add(new SearchResult.Builder().label(String.format("Action in rule '%s'", rule.getName())).match(action.getAttribute()).build());
                            }
                            if (StringUtils.containsIgnoreCase(action.getValue(), term)) {
                                results.add(new SearchResult.Builder().label(String.format("Action in rule '%s'", rule.getName())).match(action.getValue()).build());
                            }
                        }
                    }
                }
            }

            return results;
        } catch (Exception e) {
            return results;
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final ComponentLog logger = getLogger();
        final Criteria criteria = criteriaCache.get();

        FlowFile incomingFlowFile = session.get();
        if (incomingFlowFile == null) {
            return;
        }

        // record which rule should be applied to which flow file - when operating
        // in 'use clone' mode, this collection will contain a number of entries
        // that map to single element lists. this is because the original flowfile
        // is cloned for each matching rule. in 'use original' mode, this collection
        // will contain a single entry that maps a list of multiple rules. this is
        // because is the original flowfile is used for all matching rules. in this
        // case the order of the matching rules is preserved in the list
        final Map<FlowFile, List<Rule>> matchedRules = new HashMap<>();

        final Map<String, String> stateInitialAttributes;
        final Map<String, String> stateWorkingAttributes;
        StateMap stateMap = null;

        try {
            if (stateful) {
                stateMap = session.getState(Scope.LOCAL);
                stateInitialAttributes = stateMap.toMap();
                stateWorkingAttributes = new  HashMap<>(stateMap.toMap());
            } else {
                stateInitialAttributes = null;
                stateWorkingAttributes = null;
            }
        } catch (IOException e) {
            logger.error("Failed to get the initial state when processing {}; transferring FlowFile back to its incoming queue", incomingFlowFile, e);
            session.transfer(incomingFlowFile);
            context.yield();
            return;
        }

        Map<String, Action> defaultActions = this.defaultActions;
        List<FlowFile> flowFilesToTransfer = new LinkedList<>();

        // if there is update criteria specified, evaluate it
        if (criteria != null && evaluateCriteria(session, context, criteria, incomingFlowFile, matchedRules, stateInitialAttributes)) {
            // apply the actions for each rule and transfer the flowfile
            for (final Map.Entry<FlowFile, List<Rule>> entry : matchedRules.entrySet()) {
                FlowFile match = entry.getKey();
                final List<Rule> rules = entry.getValue();
                boolean updateWorking = incomingFlowFile.equals(match);

                // execute each matching rule(s)
                match = executeActions(session, context, rules, defaultActions, match, stateInitialAttributes, stateWorkingAttributes);

                if (updateWorking) {
                    incomingFlowFile = match;
                }

                if (debugEnabled) {
                    logger.debug("Updated attributes for {}; transferring to '{}'", match, REL_SUCCESS.getName());
                }

                // add the match to the list to transfer
                flowFilesToTransfer.add(match);
            }
        } else {
            // Either we're running without any rules or the FlowFile didn't match any
            incomingFlowFile = executeActions(session, context, null, defaultActions, incomingFlowFile, stateInitialAttributes, stateWorkingAttributes);

            if (debugEnabled) {
                logger.debug("Updated attributes for {}; transferring to '{}'", incomingFlowFile, REL_SUCCESS.getName());
            }

            // add the flowfile to the list to transfer
            flowFilesToTransfer.add(incomingFlowFile);
        }

        if (stateInitialAttributes != null) {
            try {
                // Able to use "equals()" since we're just checking if the map was modified at all
                if (!stateWorkingAttributes.equals(stateInitialAttributes)) {

                    boolean setState = session.replaceState(stateMap, stateWorkingAttributes, Scope.LOCAL);
                    if (!setState) {
                        logger.warn("Failed to update the state after successfully processing {} due to having an old version of the StateMap. This is normally due to multiple threads running at " +
                                "once; transferring to '{}'", incomingFlowFile, REL_FAILED_SET_STATE.getName());

                        flowFilesToTransfer.remove(incomingFlowFile);
                        if (!flowFilesToTransfer.isEmpty()) {
                            session.remove(flowFilesToTransfer);
                        }

                        session.transfer(incomingFlowFile, REL_FAILED_SET_STATE);
                        return;
                    }
                }
            } catch (IOException e) {
                logger.error("Failed to set the state after successfully processing {} due a failure when setting the state. This is normally due to multiple threads running at " +
                        "once; transferring to '{}'", incomingFlowFile, REL_FAILED_SET_STATE.getName(), e);

                flowFilesToTransfer.remove(incomingFlowFile);
                if (!flowFilesToTransfer.isEmpty()) {
                    session.remove(flowFilesToTransfer);
                }

                session.transfer(incomingFlowFile, REL_FAILED_SET_STATE);
                context.yield();
                return;
            }
        }

        for (FlowFile toTransfer: flowFilesToTransfer) {
            session.getProvenanceReporter().modifyAttributes(toTransfer);
        }
        session.transfer(flowFilesToTransfer, REL_SUCCESS);
    }

    //Evaluates the specified Criteria on the specified flowfile. Clones the
    // specified flow file for each rule that is applied.
    private boolean evaluateCriteria(final ProcessSession session, final ProcessContext context, final Criteria criteria, final FlowFile flowfile, final Map<FlowFile,
            List<Rule>> matchedRules, final Map<String, String> statefulAttributes) {
            final ComponentLog logger = getLogger();
        final List<Rule> rules = criteria.getRules();

        // consider each rule and hold a copy of the flowfile for each matched rule
        for (final Rule rule : rules) {
            // evaluate the rule
            if (evaluateRule(context, rule, flowfile, statefulAttributes)) {
                final FlowFile flowfileToUse;

                // determine if we should use the original flow file or clone
                if (FlowFilePolicy.USE_ORIGINAL.equals(criteria.getFlowFilePolicy()) || matchedRules.isEmpty()) {
                    flowfileToUse = flowfile;
                } else {
                    // clone the original for this rule
                    flowfileToUse = session.clone(flowfile);
                }

                // store the flow file to use when executing this rule
                List<Rule> rulesForFlowFile = matchedRules.computeIfAbsent(flowfileToUse, k -> new ArrayList<>());
                rulesForFlowFile.add(rule);

                // log if appropriate
                if (debugEnabled) {
                    logger.debug("{} all conditions met for rule '{}'. Using flow file - {}", this, rule.getName(), flowfileToUse);
                }
            }
        }

        return !matchedRules.isEmpty();
    }

    //Evaluates the specified rule on the specified flowfile.
    private boolean evaluateRule(final ProcessContext context, final Rule rule, FlowFile flowfile, final Map<String, String> statefulAttributes) {
        // go through each condition
        for (final Condition condition : rule.getConditions()) {

            // fail if any condition is not met
            if (!evaluateCondition(context, condition, flowfile, statefulAttributes)) {
                return false;
            }
        }

        return true;
    }

    private PropertyValue getPropertyValue(final String text, final ProcessContext context) {
        return propertyValues.computeIfAbsent(text, k -> context.newPropertyValue(text));
    }

    // Evaluates the specified condition on the specified flowfile.
    private boolean evaluateCondition(final ProcessContext context, final Condition condition, final FlowFile flowfile, final Map<String, String> statefulAttributes) {
        try {
            // evaluate the expression for the given flow file
            return getPropertyValue(condition.getExpression(), context).evaluateAttributeExpressions(flowfile, null, null, statefulAttributes).asBoolean();
        } catch (final Exception e) {
            getLogger().error("Could not evaluate the condition '{}' while processing Flowfile '{}'", condition.getExpression(), flowfile);
            throw new ProcessException(String.format("Unable to evaluate condition '%s': %s.", condition.getExpression(), e), e);
        }
    }

    // Executes the specified action on the specified flowfile.
    private FlowFile executeActions(final ProcessSession session, final ProcessContext context, final List<Rule> rules, final Map<String, Action> defaultActions, final FlowFile flowfile,
                                    final Map<String, String> stateInitialAttributes, final Map<String, String> stateWorkingAttributes) {
            final ComponentLog logger = getLogger();
        final Map<String, Action> actions = new HashMap<>(defaultActions);
        final String ruleName = (rules == null || rules.isEmpty()) ? "default" : rules.getLast().getName();

        // if a rule matched, get its actions and possible overwrite the default ones
        if (rules != null && !rules.isEmpty()) {
            // combine all rules actions with the default actions... loop through the rules in order, this way
            // subsequent matching rules will take precedence over previously matching rules and default values
            for (final Rule rule : rules) {
                for (final Action action : rule.getActions()) {
                    // store the action and overwrite the previous value (from the defaults or a previously matching rule)
                    actions.put(action.getAttribute(), action);
                }
            }

            // add an action for the matched rule - when matching multiple rules against
            // the original flowfile (use original) this will leave the last matching
            // rule's name as the value of this attribute. this decision was made since
            // this would be the behavior if they user chained multiple UpdateAttributes
            // together with 'use clone' specified
            final Action matchedRuleAction = new Action();
            matchedRuleAction.setAttribute(getClass().getSimpleName() + ".matchedRule");
            matchedRuleAction.setValue(ruleName);
            actions.put(matchedRuleAction.getAttribute(), matchedRuleAction);
        }

        // attribute values that will be applied to the flow file
        final Map<String, String> attributesToUpdate = new HashMap<>(actions.size());
        final Set<String> attributesToDelete = new HashSet<>(actions.size());

        // go through each action
        boolean debugEnabled = this.debugEnabled;
        for (final Action action : actions.values()) {
            String attribute = action.getAttribute();
            if (DELETE_ATTRIBUTES_EXPRESSION_NAME.equals(attribute)) {
                try {
                    final String actionValue = action.getValue();
                    final String regex = (actionValue == null) ? null :
                            getPropertyValue(actionValue, context).evaluateAttributeExpressions(flowfile).getValue();
                    if (regex != null) {
                        Pattern pattern = Pattern.compile(regex);
                        final Set<String> attributeKeys = flowfile.getAttributes().keySet();
                        for (final String key : attributeKeys) {
                            if (pattern.matcher(key).matches()) {

                                // log if appropriate
                                if (debugEnabled) {
                                    logger.debug("{} deleting attribute '{}' for {} per regex '{}'.", this, key, flowfile, regex);
                                }

                                attributesToDelete.add(key);
                            }
                        }
                        // No point in updating if they will be removed
                        attributesToUpdate.keySet().removeAll(attributesToDelete);
                    }
                } catch (final Exception e) {
                    logger.error("Unable to delete attribute '{}' while processing FlowFile '{}' .", attribute, flowfile);
                    throw new ProcessException(String.format("Unable to delete attribute '%s': %s.", attribute, e), e);
                }
            } else {
                boolean notDeleted = !attributesToDelete.contains(attribute);
                boolean setStatefulAttribute = stateInitialAttributes != null && !attribute.equals("UpdateAttribute.matchedRule");

                if (notDeleted || setStatefulAttribute) {
                    try {
                        String newAttributeValue = getPropertyValue(action.getValue(), context).evaluateAttributeExpressions(flowfile, null, null, stateInitialAttributes).getValue();
                        newAttributeValue = canonicalValueLookup.get(newAttributeValue);

                        // log if appropriate
                        if (debugEnabled) {
                            logger.debug("{} setting attribute '{}' = '{}' for {} per rule '{}'.", this, attribute, newAttributeValue, flowfile, ruleName);
                        }

                        if (setStatefulAttribute) {
                            stateWorkingAttributes.put(attribute, newAttributeValue);
                        }

                        // No point in updating if it will be removed
                        if (notDeleted) {
                            attributesToUpdate.put(attribute, newAttributeValue);
                        }
                        // Capture Exception thrown when evaluating the Expression Language
                    } catch (final Exception e) {
                        logger.error("Could not evaluate the FlowFile '{}' against expression '{}' defined by DynamicProperty '{}'", flowfile, action.getValue(), attribute, e);
                        throw new ProcessException(String.format("Unable to evaluate new value for attribute '%s': %s.", attribute, e), e);
                    }
                }
            }
        }

        // If the 'alternate.identifier' attribute is added, then we want to create an ADD_INFO provenance event.
        final String alternateIdentifierAdd = attributesToUpdate.get(CoreAttributes.ALTERNATE_IDENTIFIER.key());
        if (alternateIdentifierAdd != null) {
            try {
                final URI uri = new URI(alternateIdentifierAdd);
                final String namespace = uri.getScheme();
                if (namespace != null) {
                    final String identifier = alternateIdentifierAdd.substring(Math.min(namespace.length() + 1, alternateIdentifierAdd.length() - 1));
                    session.getProvenanceReporter().associate(flowfile, namespace, identifier);
                }
            } catch (final URISyntaxException ignored) {
            }
        }

        // update and delete the FlowFile attributes
        FlowFile returnFlowfile = flowfile;

        if (!attributesToUpdate.isEmpty()) {
            returnFlowfile = session.putAllAttributes(returnFlowfile, attributesToUpdate);
        }

        if (!attributesToDelete.isEmpty()) {
            returnFlowfile = session.removeAllAttributes(returnFlowfile, attributesToDelete);
        }

        return  returnFlowfile;
    }

    // Gets the default actions.
    private Map<String, Action> getDefaultActions(final Map<PropertyDescriptor, String> properties) {
        final Map<String, Action> defaultActions = new HashMap<>();

        for (final Map.Entry<PropertyDescriptor, String> entry : properties.entrySet()) {
            if (entry.getKey() != STORE_STATE && entry.getKey() != STATEFUL_VARIABLES_INIT_VALUE
                    && entry.getKey() != CANONICAL_VALUE_LOOKUP_CACHE_SIZE) {
                final Action action = new Action();
                action.setAttribute(entry.getKey().getName());
                action.setValue(entry.getValue());
                defaultActions.put(action.getAttribute(), action);
            }
        }

        return defaultActions;
    }
}
