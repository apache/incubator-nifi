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
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Collections.singletonMap;

@SideEffectFree
@TriggerSerially
@TriggerWhenEmpty
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"monitor", "flow", "active", "inactive", "activity", "detection"})
@CapabilityDescription("Monitors the flow for activity and sends out an indicator when the flow has not had any data for "
        + "some specified amount of time and again when the flow's activity is restored")
@WritesAttributes({
    @WritesAttribute(attribute = "inactivityStartMillis", description = "The time at which Inactivity began, in the form of milliseconds since Epoch"),
    @WritesAttribute(attribute = "inactivityDurationMillis", description = "The number of milliseconds that the inactivity has spanned")})
@Stateful(
        scopes = { Scope.CLUSTER, Scope.LOCAL },
        description = "MonitorActivity stores the last timestamp at each node as state, "
                + "so that it can examine activity at cluster wide. "
                + "If 'Copy Attribute' is set to true, then flow file attributes are also persisted. "
                + "In local scope, it stores last known activity timestamp if the flow is inactive."
)
public class MonitorActivity extends AbstractProcessor {

    public static final String STATE_KEY_COMMON_FLOW_ACTIVITY_INFO = "CommonFlowActivityInfo.lastSuccessfulTransfer";
    public static final String STATE_KEY_LOCAL_FLOW_ACTIVITY_INFO = "LocalFlowActivityInfo.lastSuccessfulTransfer";

    public static final AllowableValue SCOPE_NODE = new AllowableValue("node");
    public static final AllowableValue SCOPE_CLUSTER = new AllowableValue("cluster");
    public static final AllowableValue REPORT_NODE_ALL = new AllowableValue("all");
    public static final AllowableValue REPORT_NODE_PRIMARY = new AllowableValue("primary");

    public static final PropertyDescriptor THRESHOLD = new PropertyDescriptor.Builder()
            .name("Threshold Duration")
            .description("Determines how much time must elapse before considering the flow to be inactive")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .defaultValue("5 min")
            .build();
    public static final PropertyDescriptor CONTINUALLY_SEND_MESSAGES = new PropertyDescriptor.Builder()
            .name("Continually Send Messages")
            .description("If true, will send inactivity indicator continually every Threshold Duration amount of time until activity is restored; "
                    + "if false, will send an indicator only when the flow first becomes inactive")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();
    public static final PropertyDescriptor ACTIVITY_RESTORED_MESSAGE = new PropertyDescriptor.Builder()
            .name("Activity Restored Message")
            .description("The message that will be the content of FlowFiles that are sent to 'activity.restored' relationship")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("Activity restored at time: ${now():format('yyyy/MM/dd HH:mm:ss')} after being inactive for ${inactivityDurationMillis:toNumber():divide(60000)} minutes")
            .build();
    public static final PropertyDescriptor WAIT_FOR_ACTIVITY = new PropertyDescriptor.Builder()
            .name("Wait for Activity")
            .description("When the processor gets started or restarted, if set to true, only send an inactive indicator if there had been activity beforehand. "
                    + "Otherwise send an inactive indicator even if there had not been activity beforehand.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();
    public static final PropertyDescriptor RESET_STATE_ON_RESTART = new PropertyDescriptor.Builder()
            .name("Reset State on Restart")
            .description("When the processor gets started or restarted, if set to true, the initial state will always be active. "
                    + "Otherwise, the last reported flow state will be preserved.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();
    public static final PropertyDescriptor INACTIVITY_MESSAGE = new PropertyDescriptor.Builder()
            .name("Inactivity Message")
            .description("The message that will be the content of FlowFiles that are sent to the 'inactive' relationship")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("Lacking activity as of time: ${now():format('yyyy/MM/dd HH:mm:ss')}; flow has been inactive for ${inactivityDurationMillis:toNumber():divide(60000)} minutes")
            .build();
    public static final PropertyDescriptor COPY_ATTRIBUTES = new PropertyDescriptor.Builder()
            .name("Copy Attributes")
            .description("If true, will copy all flow file attributes from the flow file that resumed activity to the newly created indicator flow file")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();
    public static final PropertyDescriptor MONITORING_SCOPE = new PropertyDescriptor.Builder()
            .name("Monitoring Scope")
            .description("Specify how to determine activeness of the flow. 'node' means that activeness is examined at individual node separately." +
                    " It can be useful if DFM expects each node should receive flow files in a distributed manner." +
                    " With 'cluster', it defines the flow is active while at least one node receives flow files actively." +
                    " If NiFi is running as standalone mode, this should be set as 'node'," +
                    " if it's 'cluster', NiFi logs a warning message and act as 'node' scope.")
            .required(true)
            .allowableValues(SCOPE_NODE, SCOPE_CLUSTER)
            .defaultValue(SCOPE_NODE.getValue())
            .build();
    public static final PropertyDescriptor REPORTING_NODE = new PropertyDescriptor.Builder()
            .name("Reporting Node")
            .description("Specify which node should send notification flow-files to inactive and activity.restored relationships." +
                    " With 'all', every node in this cluster send notification flow-files." +
                    " 'primary' means flow-files will be sent only from a primary node." +
                    " If NiFi is running as standalone mode, this should be set as 'all'," +
                    " even if it's 'primary', NiFi act as 'all'.")
            .required(true)
            .allowableValues(REPORT_NODE_ALL, REPORT_NODE_PRIMARY)
            .dependsOn(MONITORING_SCOPE, SCOPE_CLUSTER)
            .defaultValue(REPORT_NODE_ALL.getValue())
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            THRESHOLD,
            CONTINUALLY_SEND_MESSAGES,
            INACTIVITY_MESSAGE,
            ACTIVITY_RESTORED_MESSAGE,
            WAIT_FOR_ACTIVITY,
            RESET_STATE_ON_RESTART,
            COPY_ATTRIBUTES,
            MONITORING_SCOPE,
            REPORTING_NODE
    );

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All incoming FlowFiles are routed to success")
            .build();
    public static final Relationship REL_INACTIVE = new Relationship.Builder()
            .name("inactive")
            .description("This relationship is used to transfer an Inactivity indicator when no FlowFiles are routed to 'success' for Threshold "
                    + "Duration amount of time")
            .build();
    public static final Relationship REL_ACTIVITY_RESTORED = new Relationship.Builder()
            .name("activity.restored")
            .description("This relationship is used to transfer an Activity Restored indicator when FlowFiles are routing to 'success' following a "
                    + "period of inactivity")
            .build();

    private final static Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_INACTIVE,
            REL_ACTIVITY_RESTORED
    );

    private final AtomicBoolean connectedWhenLastTriggered = new AtomicBoolean(false);
    private final AtomicLong lastInactiveMessage = new AtomicLong();
    private final AtomicLong inactivityStartMillis = new AtomicLong(nowMillis());
    private final AtomicBoolean wasActive = new AtomicBoolean(true);

    private volatile LocalFlowActivityInfo localFlowActivityInfo;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        // Check configuration.
        isClusterScope(context, true);

        final long thresholdMillis = context.getProperty(THRESHOLD).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);
        final boolean copyAttributes = context.getProperty(COPY_ATTRIBUTES).asBoolean();
        final boolean resetStateOnRestart = context.getProperty(RESET_STATE_ON_RESTART).asBoolean();

        // Attempt to load last state by the time of stopping this processor. A local state only exists if
        // the monitored flow was already inactive, when the processor was shutting down.
        final String storedLastSuccessfulTransfer = resetStateOnRestart ? null : tryLoadLastSuccessfulTransfer(context);

        if (storedLastSuccessfulTransfer != null) {
            // Initialize local flow as being inactive since the stored timestamp.
            localFlowActivityInfo = new LocalFlowActivityInfo(
                    getStartupTime(), thresholdMillis, copyAttributes, Long.parseLong(storedLastSuccessfulTransfer));
            wasActive.set(localFlowActivityInfo.isActive());
            inactivityStartMillis.set(localFlowActivityInfo.getLastActivity());
        } else {
            // Initialize local flow as being active. If there is no traffic, then it will eventually become inactive.
            localFlowActivityInfo = new LocalFlowActivityInfo(
                    getStartupTime(), thresholdMillis, copyAttributes);
            wasActive.set(true);
        }
    }

    @OnStopped
    public void onStopped(final ProcessContext context) {
        if (getNodeTypeProvider().isConfiguredForClustering() && context.isConnectedToCluster()) {
            // Shared state needs to be cleared, in order to avoid getting inactive markers right after starting the
            // flow after a weekend stop. In single-node setup, there is no shared state to be cleared, but the line
            // below would also wipe out the local state. Hence, the check.
            final StateManager stateManager = context.getStateManager();
            try {
                stateManager.clear(Scope.CLUSTER);
            } catch (IOException e) {
                getLogger().error("Failed to clear cluster state", e);
            }
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final ComponentLog logger = getLogger();

        final boolean isClusterScope = isClusterScope(context, false);
        final boolean isConnectedToCluster = context.isConnectedToCluster();
        final boolean wasActive = this.wasActive.get();

        final List<FlowFile> flowFiles = session.get(50);

        if (!flowFiles.isEmpty()) {
            final boolean firstKnownTransfer = !localFlowActivityInfo.hasSuccessfulTransfer();
            final boolean flowStateMustBecomeActive = !wasActive || firstKnownTransfer;

            localFlowActivityInfo.update(flowFiles.getFirst());

            if (isClusterScope && flowStateMustBecomeActive) {
                localFlowActivityInfo.forceSync();
            }

            session.transfer(flowFiles, REL_SUCCESS);
            logger.info("Transferred {} FlowFiles to 'success'", flowFiles.size());
        } else {
            context.yield();
        }

        if (isClusterScope) {
            if (!wasActive || !localFlowActivityInfo.isActive()) {
                localFlowActivityInfo.forceSync();
            }
            synchronizeState(context);
        }

        final long thresholdMillis = context.getProperty(THRESHOLD).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);
        final boolean continuallySendMessages = context.getProperty(CONTINUALLY_SEND_MESSAGES).asBoolean();
        final boolean waitForActivity = context.getProperty(WAIT_FOR_ACTIVITY).asBoolean();

        final boolean isActive = localFlowActivityInfo.isActive() || !flowFiles.isEmpty();
        final long lastActivity = localFlowActivityInfo.getLastActivity();
        final long inactivityStartMillis = this.inactivityStartMillis.get();
        final boolean timeToRepeatInactiveMessage = (lastInactiveMessage.get() + thresholdMillis) <= nowMillis();

        final boolean canReport = !isClusterScope || isConnectedToCluster || !flowFiles.isEmpty();
        final boolean canChangeState = !waitForActivity || localFlowActivityInfo.hasSuccessfulTransfer();

        if (canReport && canChangeState) {
            if (isActive) {
                onTriggerActiveFlow(context, session, wasActive, isClusterScope, inactivityStartMillis);
            } else if (wasActive || continuallySendMessages && timeToRepeatInactiveMessage) {
                onTriggerInactiveFlow(context, session, isClusterScope, lastActivity);
            }
            this.wasActive.set(isActive);
            this.inactivityStartMillis.set(lastActivity);
        } else {
            // We need to block state transition, because we are not connected to the cluster.
            // When we reconnect, and the state persists, then the next onTrigger will do the transition.
            logger.trace("State transition is blocked, because we are not connected to the cluster.");
        }
    }

    protected long nowMillis() {
        return System.currentTimeMillis();
    }

    protected long getStartupTime() {
        return nowMillis();
    }

    protected final long getLastSuccessfulTransfer() {
        return localFlowActivityInfo.getLastSuccessfulTransfer();
    }

    private String tryLoadLastSuccessfulTransfer(ProcessContext context) {
        final StateManager stateManager = context.getStateManager();
        try {
            final StateMap localStateMap = stateManager.getState(Scope.LOCAL);
            return localStateMap.get(STATE_KEY_LOCAL_FLOW_ACTIVITY_INFO);
        } catch (IOException e) {
            throw new ProcessException("Failed to load local state due to " + e, e);
        }
    }

    private void synchronizeState(ProcessContext context) {
        final ComponentLog logger = getLogger();
        final boolean isConnectedToCluster = context.isConnectedToCluster();

        if (isReconnectedToCluster(isConnectedToCluster)) {
            localFlowActivityInfo.forceSync();
            connectedWhenLastTriggered.set(true);
        }
        if (!isConnectedToCluster) {
            connectedWhenLastTriggered.set(false);
        } else if (localFlowActivityInfo.syncNeeded()) {
            final CommonFlowActivityInfo commonFlowActivityInfo = new CommonFlowActivityInfo(context);
            localFlowActivityInfo.update(commonFlowActivityInfo);

            try {
                commonFlowActivityInfo.update(localFlowActivityInfo);
                localFlowActivityInfo.setNextSyncMillis();
            } catch (final SaveSharedFlowStateException ex) {
                logger.debug("Failed to update common state.", ex);
            }
        }
    }

    private void onTriggerInactiveFlow(ProcessContext context, ProcessSession session, boolean isClusterScope, long lastActivity) {
        final ComponentLog logger = getLogger();
        final boolean shouldThisNodeReport = shouldThisNodeReport(isClusterScope, context);

        if (shouldThisNodeReport) {
            sendInactivityMarker(context, session, lastActivity, logger);
        }
        lastInactiveMessage.set(nowMillis());
        setInactivityFlag(context.getStateManager());
    }

    private void onTriggerActiveFlow(ProcessContext context, ProcessSession session, boolean wasActive, boolean isClusterScope,
            long inactivityStartMillis) {
        final ComponentLog logger = getLogger();
        final boolean shouldThisNodeReport = shouldThisNodeReport(isClusterScope, context);

        if (!wasActive) {
            if (shouldThisNodeReport) {
                final Map<String, String> attributes = localFlowActivityInfo.getLastSuccessfulTransferAttributes();
                sendActivationMarker(context, session, attributes, inactivityStartMillis, logger);
            }
            clearInactivityFlag(context.getStateManager());
        }
    }

    private void setInactivityFlag(StateManager stateManager) {
        try {
            stateManager.setState(singletonMap(
                    STATE_KEY_LOCAL_FLOW_ACTIVITY_INFO,
                    String.valueOf(localFlowActivityInfo.getLastActivity())
            ), Scope.LOCAL);
        } catch (IOException e) {
            getLogger().error("Failed to set local state", e);
        }
    }

    private void clearInactivityFlag(StateManager stateManager) {
        try {
            stateManager.clear(Scope.LOCAL);
        } catch (IOException e) {
            throw new ProcessException("Failed to clear local state due to " + e, e);
        }
    }

    private boolean isClusterScope(final ProcessContext context, boolean logInvalidConfig) {
        if (SCOPE_CLUSTER.getValue().equals(context.getProperty(MONITORING_SCOPE).getValue())) {
            if (getNodeTypeProvider().isConfiguredForClustering()) {
                return true;
            }
            if (logInvalidConfig) {
                getLogger().warn("NiFi is running as a Standalone mode, but 'cluster' scope is set. Fallback to 'node' scope. Fix configuration to stop this message.");
            }
        }
        return false;
    }

    private boolean shouldReportOnlyOnPrimary(boolean isClusterScope, final ProcessContext context) {
        if (REPORT_NODE_PRIMARY.getValue().equals(context.getProperty(REPORTING_NODE).getValue())) {
            return isClusterScope;
        }
        return false;
    }

    /**
     * Will return true when the last known state is "not connected" and the current state is "connected". This might
     * happen when during last @OnTrigger the node was not connected, but currently it is (reconnection); or when the
     * processor is triggered first time (initial connection).
     * <br />
     * This second case is due to safety reasons: it is possible that during the first trigger the node is not connected
     * to the cluster thus the default value of the #connected attribute is false and stays as false until it's proven
     * otherwise.
     *
     * @param isConnectedToCluster Current state of the connection.
     *
     * @return The node connected between the last trigger and the current one.
     */
    private boolean isReconnectedToCluster( final boolean isConnectedToCluster) {
        return !connectedWhenLastTriggered.get() && isConnectedToCluster;
    }

    private boolean shouldThisNodeReport(final boolean isClusterScope, final ProcessContext context) {
        final boolean shouldReportOnlyOnPrimary = shouldReportOnlyOnPrimary(isClusterScope, context);
        return !isClusterScope || (!shouldReportOnlyOnPrimary || getNodeTypeProvider().isPrimary());
    }

    private void sendInactivityMarker(ProcessContext context, ProcessSession session, long inactivityStartMillis,
            ComponentLog logger) {
        FlowFile inactiveFlowFile = session.create();
        inactiveFlowFile = session.putAttribute(
                inactiveFlowFile,
                "inactivityStartMillis", String.valueOf(inactivityStartMillis)
        );
        inactiveFlowFile = session.putAttribute(
                inactiveFlowFile,
                "inactivityDurationMillis",
                String.valueOf(nowMillis() - inactivityStartMillis)
        );

        final byte[] outBytes = context.getProperty(INACTIVITY_MESSAGE).evaluateAttributeExpressions(inactiveFlowFile).getValue().getBytes(
                StandardCharsets.UTF_8);
        inactiveFlowFile = session.write(inactiveFlowFile, out -> out.write(outBytes));

        session.getProvenanceReporter().create(inactiveFlowFile);
        session.transfer(inactiveFlowFile, REL_INACTIVE);
        logger.info("Transferred {} to 'inactive'", inactiveFlowFile);
    }

    private void sendActivationMarker(ProcessContext context, ProcessSession session, Map<String, String> attributes,
            long inactivityStartMillis, ComponentLog logger) {
        FlowFile activityRestoredFlowFile = session.create();
        // don't copy the UUID
        attributes.remove(CoreAttributes.UUID.key());
        activityRestoredFlowFile = session.putAllAttributes(activityRestoredFlowFile, attributes);

        activityRestoredFlowFile = session.putAttribute(activityRestoredFlowFile, "inactivityStartMillis", String.valueOf(
                inactivityStartMillis));
        activityRestoredFlowFile = session.putAttribute(activityRestoredFlowFile, "inactivityDurationMillis", String.valueOf(
                nowMillis() - inactivityStartMillis));

        final byte[] outBytes = context.getProperty(ACTIVITY_RESTORED_MESSAGE).evaluateAttributeExpressions(activityRestoredFlowFile).getValue().getBytes(
                StandardCharsets.UTF_8);
        activityRestoredFlowFile = session.write(activityRestoredFlowFile, out -> out.write(outBytes));

        session.getProvenanceReporter().create(activityRestoredFlowFile);
        session.transfer(activityRestoredFlowFile, REL_ACTIVITY_RESTORED);
        logger.info("Transferred {} to 'activity.restored'", activityRestoredFlowFile);
    }

    private class LocalFlowActivityInfo {
        private static final long NO_VALUE = 0;
        private static final int TIMES_SYNC_WITHIN_THRESHOLD = 3;

        private final long startupTimeMillis;
        private final long thresholdMillis;
        private final boolean saveAttributes;
        private final long syncPeriodMillis;

        private long nextSyncMillis = NO_VALUE;
        private long lastSuccessfulTransfer = NO_VALUE;
        private Map<String, String> lastSuccessfulTransferAttributes = new HashMap<>();

        public LocalFlowActivityInfo(long startupTimeMillis, long thresholdMillis, boolean saveAttributes) {
            this.startupTimeMillis = startupTimeMillis;
            this.thresholdMillis = thresholdMillis;
            this.saveAttributes = saveAttributes;
            this.syncPeriodMillis = thresholdMillis / TIMES_SYNC_WITHIN_THRESHOLD;
        }

        public LocalFlowActivityInfo(long startupTimeMillis, long thresholdMillis, boolean saveAttributes, long initialLastSuccessfulTransfer) {
            this(startupTimeMillis, thresholdMillis, saveAttributes);
            lastSuccessfulTransfer = initialLastSuccessfulTransfer;
        }

        public boolean syncNeeded() {
            return nextSyncMillis <= nowMillis();
        }

        public void setNextSyncMillis() {
            nextSyncMillis = nowMillis() + syncPeriodMillis;
        }

        public void forceSync() {
            nextSyncMillis = nowMillis();
        }

        public boolean isActive() {
            if (hasSuccessfulTransfer()) {
                return nowMillis() < (lastSuccessfulTransfer + thresholdMillis);
            } else {
                return nowMillis() < (startupTimeMillis + thresholdMillis);
            }
        }

        public boolean hasSuccessfulTransfer() {
            return lastSuccessfulTransfer != NO_VALUE;
        }

        public long getLastSuccessfulTransfer() {
            return lastSuccessfulTransfer;
        }

        public long getLastActivity() {
            if (hasSuccessfulTransfer()) {
                return lastSuccessfulTransfer;
            } else {
                return startupTimeMillis;
            }
        }

        public Map<String, String> getLastSuccessfulTransferAttributes() {
            return lastSuccessfulTransferAttributes;
        }

        public void update(FlowFile flowFile) {
            final long now = nowMillis();
            if ((now - this.getLastActivity()) > syncPeriodMillis) {
                this.forceSync(); // Immediate synchronization if Flow Files are infrequent, to mitigate false reports
            }
            this.lastSuccessfulTransfer = now;
            if (saveAttributes) {
                lastSuccessfulTransferAttributes = new HashMap<>(flowFile.getAttributes());
                lastSuccessfulTransferAttributes.remove(CoreAttributes.UUID.key());
            }
        }

        public void update(CommonFlowActivityInfo commonFlowActivityInfo) {
            if (!commonFlowActivityInfo.hasSuccessfulTransfer()) {
                return;
            }

            final long lastSuccessfulTransfer = commonFlowActivityInfo.getLastSuccessfulTransfer();

            if (lastSuccessfulTransfer <= getLastSuccessfulTransfer()) {
                return;
            }

            this.lastSuccessfulTransfer = lastSuccessfulTransfer;
            if (saveAttributes) {
                lastSuccessfulTransferAttributes = commonFlowActivityInfo.getLastSuccessfulTransferAttributes();
            }
        }
    }

    private static class CommonFlowActivityInfo {
        private final StateManager stateManager;
        private final StateMap storedState;
        private final Map<String, String> newState = new HashMap<>();

        public CommonFlowActivityInfo(ProcessContext context) {
            this.stateManager = context.getStateManager();
            try {
                storedState = stateManager.getState(Scope.CLUSTER);
            } catch (IOException e) {
                throw new ProcessException("Cannot load common flow activity info.", e);
            }
        }

        public boolean hasSuccessfulTransfer() {
            return storedState.get(STATE_KEY_COMMON_FLOW_ACTIVITY_INFO) != null;
        }

        public long getLastSuccessfulTransfer() {
            return Long.parseLong(storedState.get(STATE_KEY_COMMON_FLOW_ACTIVITY_INFO));
        }

        public Map<String, String> getLastSuccessfulTransferAttributes() {
            final Map<String, String> result = new HashMap<>(storedState.toMap());
            result.remove(STATE_KEY_COMMON_FLOW_ACTIVITY_INFO);
            return result;
        }

        public void update(LocalFlowActivityInfo localFlowActivityInfo) {
            if (!localFlowActivityInfo.hasSuccessfulTransfer()) {
                return;
            }

            final long lastSuccessfulTransfer = localFlowActivityInfo.getLastSuccessfulTransfer();

            if (hasSuccessfulTransfer() && (lastSuccessfulTransfer <= getLastSuccessfulTransfer())) {
                return;
            }

            newState.putAll(localFlowActivityInfo.getLastSuccessfulTransferAttributes());
            newState.put(STATE_KEY_COMMON_FLOW_ACTIVITY_INFO, String.valueOf(lastSuccessfulTransfer));

            final boolean wasSuccessful;
            try {
                wasSuccessful = stateManager.replace(storedState, newState, Scope.CLUSTER);
            } catch (IOException e) {
                throw new SaveSharedFlowStateException("Caught exception while saving state.", e);
            }

            if (!wasSuccessful) {
                throw new SaveSharedFlowStateException("Failed to save state. Probably there was a concurrent update.");
            }
        }
    }

    private static class SaveSharedFlowStateException extends ProcessException {
        public SaveSharedFlowStateException(String message) {
            super(message);
        }

        public SaveSharedFlowStateException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
