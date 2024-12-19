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

import { flowFeatureKey, FlowState, SelectedComponent } from './index';
import { createSelector } from '@ngrx/store';
import { CanvasState, selectCanvasState } from '../index';
import { ComponentType, selectCurrentRoute } from '@nifi/shared';

export const selectFlowState = createSelector(selectCanvasState, (state: CanvasState) => state[flowFeatureKey]);

export const selectFlowLoadingStatus = createSelector(selectFlowState, (state: FlowState) => state.status);

export const selectChangeVersionRequest = createSelector(
    selectFlowState,
    (state: FlowState) => state.changeVersionRequest
);

export const selectPollingProcessor = createSelector(selectFlowState, (state: FlowState) => state.pollingProcessor);

export const selectSaving = createSelector(selectFlowState, (state: FlowState) => state.saving);

export const selectVersionSaving = createSelector(selectFlowState, (state: FlowState) => state.versionSaving);

export const selectCurrentProcessGroupId = createSelector(selectFlowState, (state: FlowState) => state.id);

export const selectCurrentProcessGroupRevision = createSelector(
    selectFlowState,
    (state: FlowState) => state.flow.revision
);

export const selectRefreshRpgDetails = createSelector(selectFlowState, (state: FlowState) => state.refreshRpgDetails);

export const selectCurrentParameterContext = createSelector(
    selectFlowState,
    (state: FlowState) => state.flow.processGroupFlow.parameterContext
);

export const selectCanvasPermissions = createSelector(selectFlowState, (state: FlowState) => state.flow.permissions);

export const selectBreadcrumbs = createSelector(
    selectFlowState,
    (state: FlowState) => state.flow.processGroupFlow.breadcrumb
);

export const selectParentProcessGroupId = createSelector(
    selectFlowState,
    (state: FlowState) => state.flow.processGroupFlow.parentGroupId
);

export const selectProcessGroupIdFromRoute = createSelector(selectCurrentRoute, (route) => {
    if (route) {
        // always select the process group from the route
        return route.params.processGroupId;
    }
    return null;
});

export const selectProcessGroupRoute = createSelector(selectCurrentRoute, (route) => {
    if (route) {
        // only select the process group from the route when not selecting components
        if (route.params.ids == null && route.params.type == null) {
            return route.params.processGroupId;
        }
    }
    return null;
});

export const selectAnySelectedComponentIds = createSelector(selectCurrentRoute, (route) => {
    const ids: string[] = [];
    // handle either bulk or individual component routes
    if (route?.params.ids) {
        ids.push(...route.params.ids.split(','));
    } else if (route?.params.id) {
        ids.push(route.params.id);
    }
    return ids;
});

export const selectBulkSelectedComponentIds = createSelector(selectCurrentRoute, (route) => {
    const ids: string[] = [];
    // only handle bulk component route
    if (route?.params.ids) {
        ids.push(...route.params.ids.split(','));
    }
    return ids;
});

export const selectSingleSelectedComponent = createSelector(selectCurrentRoute, (route) => {
    let selectedComponent: SelectedComponent | null = null;
    if (route?.params.id && route?.params.type) {
        selectedComponent = {
            id: route.params.id,
            componentType: route.params.type
        };
    }
    return selectedComponent;
});

export const selectSingleEditedComponent = createSelector(selectCurrentRoute, (route) => {
    let selectedComponent: SelectedComponent | null = null;
    if (route?.routeConfig?.path == 'edit') {
        if (route.params.id && route.params.type) {
            selectedComponent = {
                id: route.params.id,
                componentType: route.params.type
            };
        }
    }
    return selectedComponent;
});

export const selectEditedCurrentProcessGroup = createSelector(selectCurrentRoute, (route) => {
    if (route?.routeConfig?.path == 'edit') {
        if (route.params.ids == null && route.params.id == null && route.params.type == null) {
            return route.params.processGroupId;
        }
    }

    return null;
});

export const selectViewStatusHistoryComponent = createSelector(selectCurrentRoute, (route) => {
    let selectedComponent: SelectedComponent | null = null;
    if (route?.routeConfig?.path == 'history') {
        if (route.params.id && route.params.type) {
            selectedComponent = {
                id: route.params.id,
                componentType: route.params.type
            };
        }
    }
    return selectedComponent;
});

export const selectViewStatusHistoryCurrentProcessGroup = createSelector(selectCurrentRoute, (route) => {
    if (route?.routeConfig?.path == 'history') {
        if (route.params.processGroupId) {
            return route.params.processGroupId;
        }
    }
    return null;
});

export const selectTransitionRequired = createSelector(selectFlowState, (state: FlowState) => state.transitionRequired);

export const selectDragging = createSelector(selectFlowState, (state: FlowState) => state.dragging);

export const selectSkipTransform = createSelector(selectFlowState, (state: FlowState) => state.skipTransform);

export const selectAllowTransition = createSelector(selectFlowState, (state: FlowState) => state.allowTransition);

export const selectFunnels = createSelector(
    selectFlowState,
    (state: FlowState) => state.flow.processGroupFlow?.flow.funnels
);

export const selectFunnel = (id: string) =>
    createSelector(selectFunnels, (funnels: any[]) => funnels.find((funnel) => id == funnel.id));

export const selectProcessors = createSelector(
    selectFlowState,
    (state: FlowState) => state.flow.processGroupFlow?.flow.processors
);

export const selectProcessor = (id: string) =>
    createSelector(selectProcessors, (processors: any[]) => processors.find((processor) => id == processor.id));

export const selectProcessGroups = createSelector(
    selectFlowState,
    (state: FlowState) => state.flow.processGroupFlow?.flow.processGroups
);

export const selectProcessGroup = (id: string) =>
    createSelector(selectProcessGroups, (processGroups: any[]) =>
        processGroups.find((processGroup) => id == processGroup.id)
    );

export const selectRemoteProcessGroups = createSelector(
    selectFlowState,
    (state: FlowState) => state.flow.processGroupFlow?.flow.remoteProcessGroups
);

export const selectRemoteProcessGroup = (id: string) =>
    createSelector(selectRemoteProcessGroups, (remoteProcessGroups: any[]) =>
        remoteProcessGroups.find((remoteProcessGroup) => id == remoteProcessGroup.id)
    );

export const selectInputPorts = createSelector(
    selectFlowState,
    (state: FlowState) => state.flow.processGroupFlow?.flow.inputPorts
);

export const selectInputPort = (id: string) =>
    createSelector(selectInputPorts, (inputPorts: any[]) => inputPorts.find((inputPort) => id == inputPort.id));

export const selectOutputPorts = createSelector(
    selectFlowState,
    (state: FlowState) => state.flow.processGroupFlow?.flow.outputPorts
);

export const selectOutputPort = (id: string) =>
    createSelector(selectOutputPorts, (outputPorts: any[]) => outputPorts.find((outputPort) => id == outputPort.id));

export const selectPorts = createSelector(
    selectInputPorts,
    selectOutputPorts,
    (inputPorts: any[], outputPorts: any[]) => [...inputPorts, ...outputPorts]
);

export const selectLabels = createSelector(
    selectFlowState,
    (state: FlowState) => state.flow.processGroupFlow?.flow.labels
);

export const selectLabel = (id: string) =>
    createSelector(selectLabels, (labels: any[]) => labels.find((label) => id == label.id));

export const selectConnections = createSelector(
    selectFlowState,
    (state: FlowState) => state.flow.processGroupFlow?.flow.connections
);

export const selectConnection = (id: string) =>
    createSelector(selectConnections, (connections: any[]) => connections.find((connection) => id == connection.id));

export const selectControllerStatus = createSelector(
    selectFlowState,
    (state: FlowState) => state.flowStatus.controllerStatus
);

export const selectLastRefreshed = createSelector(
    selectFlowState,
    (state: FlowState) => state.flow.processGroupFlow.lastRefreshed
);

export const selectControllerBulletins = createSelector(
    selectFlowState,
    (state: FlowState) => state.controllerBulletins.bulletins // TODO - include others?
);

export const selectRegistryClients = createSelector(selectFlowState, (state: FlowState) => state.registryClients);

export const selectNavigationCollapsed = createSelector(
    selectFlowState,
    (state: FlowState) => state.navigationCollapsed
);

export const selectOperationCollapsed = createSelector(selectFlowState, (state: FlowState) => state.operationCollapsed);

export const selectMaxZIndex = (componentType: ComponentType.Connection | ComponentType.Label) => {
    if (componentType === ComponentType.Connection) {
        return createSelector(selectConnections, (connections: any[]) =>
            connections.reduce((maxZIndex, connection) => Math.max(maxZIndex, connection.zIndex), -1)
        );
    } else {
        return createSelector(selectLabels, (labels: any[]) =>
            labels.reduce((maxZIndex, label) => Math.max(maxZIndex, label.zIndex), -1)
        );
    }
};

export const selectFlowAnalysisOpen = createSelector(selectFlowState, (state: FlowState) => state.flowAnalysisOpen);
