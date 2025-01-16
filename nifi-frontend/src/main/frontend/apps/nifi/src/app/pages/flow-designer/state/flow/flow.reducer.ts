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

import { createReducer, on } from '@ngrx/store';
import {
    changeVersionComplete,
    changeVersionSuccess,
    createComponentComplete,
    createComponentSuccess,
    createConnection,
    createFunnel,
    createLabel,
    createPort,
    createProcessGroup,
    createProcessor,
    deleteComponentsSuccess,
    disableComponent,
    disableComponentSuccess,
    disableProcessGroupSuccess,
    enableComponent,
    enableComponentSuccess,
    enableProcessGroupSuccess,
    flowBannerError,
    flowSnackbarError,
    groupComponents,
    groupComponentsSuccess,
    importFromRegistry,
    loadChildProcessGroupSuccess,
    loadConnectionSuccess,
    loadInputPortSuccess,
    loadProcessGroup,
    loadProcessGroupComplete,
    loadProcessGroupSuccess,
    loadProcessorSuccess,
    loadRemoteProcessGroupSuccess,
    navigateWithoutTransform,
    pasteSuccess,
    pollChangeVersionSuccess,
    pollProcessorUntilStoppedSuccess,
    pollRevertChangesSuccess,
    requestRefreshRemoteProcessGroup,
    resetFlowState,
    revertChangesComplete,
    revertChangesSuccess,
    runOnce,
    runOnceSuccess,
    saveToFlowRegistry,
    saveToFlowRegistrySuccess,
    setAllowTransition,
    setDragging,
    setFlowAnalysisOpen,
    setNavigationCollapsed,
    setOperationCollapsed,
    setRegistryClients,
    setSkipTransform,
    setTransitionRequired,
    startComponent,
    startComponentSuccess,
    startPollingProcessorUntilStopped,
    startProcessGroupSuccess,
    startRemoteProcessGroupPolling,
    stopComponent,
    stopComponentSuccess,
    stopPollingProcessor,
    stopProcessGroupSuccess,
    stopRemoteProcessGroupPolling,
    stopVersionControl,
    stopVersionControlSuccess,
    updateComponent,
    updateComponentFailure,
    updateComponentSuccess,
    updateConnection,
    updateConnectionSuccess,
    updateProcessor,
    updateProcessorSuccess,
    uploadProcessGroup
} from './flow.actions';
import { ComponentEntity, FlowState } from './index';
import { ComponentType } from '@nifi/shared';
import { produce } from 'immer';

export const initialState: FlowState = {
    id: 'root',
    changeVersionRequest: null,
    pollingProcessor: null,
    flow: {
        revision: {
            version: 0
        },
        permissions: {
            canRead: false,
            canWrite: false
        },
        processGroupFlow: {
            id: '',
            uri: '',
            parentGroupId: null,
            breadcrumb: {
                id: '',
                permissions: {
                    canRead: false,
                    canWrite: false
                },
                versionedFlowState: '',
                breadcrumb: {
                    id: '',
                    name: ''
                }
            },
            parameterContext: null,
            flow: {
                processGroups: [],
                remoteProcessGroups: [],
                processors: [],
                inputPorts: [],
                outputPorts: [],
                connections: [],
                labels: [],
                funnels: []
            },
            lastRefreshed: ''
        }
    },
    addedCache: [],
    removedCache: [],
    flowStatus: {
        controllerStatus: {
            activeThreadCount: 0,
            terminatedThreadCount: 0,
            queued: '',
            flowFilesQueued: 0,
            bytesQueued: 0,
            runningCount: 0,
            stoppedCount: 0,
            invalidCount: 0,
            disabledCount: 0,
            activeRemotePortCount: 0,
            inactiveRemotePortCount: 0,
            upToDateCount: undefined,
            locallyModifiedCount: undefined,
            staleCount: undefined,
            locallyModifiedAndStaleCount: undefined,
            syncFailureCount: undefined
        }
    },
    refreshRpgDetails: null,
    controllerBulletins: {
        bulletins: [],
        controllerServiceBulletins: [],
        flowRegistryClientBulletins: [],
        parameterProviderBulletins: [],
        reportingTaskBulletins: []
    },
    registryClients: [],
    dragging: false,
    saving: false,
    versionSaving: false,
    transitionRequired: false,
    skipTransform: false,
    allowTransition: false,
    navigationCollapsed: false,
    operationCollapsed: false,
    flowAnalysisOpen: false,
    status: 'pending'
};

export const flowReducer = createReducer(
    initialState,
    on(resetFlowState, () => ({
        ...initialState
    })),
    on(requestRefreshRemoteProcessGroup, (state, { request }) => ({
        ...state,
        refreshRpgDetails: {
            request,
            polling: false
        }
    })),
    on(startRemoteProcessGroupPolling, (state) => {
        return produce(state, (draftState) => {
            if (draftState.refreshRpgDetails) {
                draftState.refreshRpgDetails.polling = true;
            }
        });
    }),
    on(stopRemoteProcessGroupPolling, (state) => ({
        ...state,
        refreshRpgDetails: null
    })),
    on(loadProcessGroup, (state, { request }) => ({
        ...state,
        addedCache: [],
        removedCache: [],
        transitionRequired: request.transitionRequired,
        status: 'loading' as const
    })),
    on(loadProcessGroupSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            draftState.id = response.flow.processGroupFlow.id;
            draftState.flow = {
                ...response.flow,
                processGroupFlow: {
                    ...response.flow.processGroupFlow,
                    flow: {
                        processors: processComponentCollection(
                            response.flow.processGroupFlow.flow.processors,
                            state.flow.processGroupFlow.flow.processors,
                            state.addedCache,
                            state.removedCache,
                            response.connectedStateChanged
                        ),
                        inputPorts: processComponentCollection(
                            response.flow.processGroupFlow.flow.inputPorts,
                            state.flow.processGroupFlow.flow.inputPorts,
                            state.addedCache,
                            state.removedCache,
                            response.connectedStateChanged
                        ),
                        outputPorts: processComponentCollection(
                            response.flow.processGroupFlow.flow.outputPorts,
                            state.flow.processGroupFlow.flow.outputPorts,
                            state.addedCache,
                            state.removedCache,
                            response.connectedStateChanged
                        ),
                        processGroups: processComponentCollection(
                            response.flow.processGroupFlow.flow.processGroups,
                            state.flow.processGroupFlow.flow.processGroups,
                            state.addedCache,
                            state.removedCache,
                            response.connectedStateChanged
                        ),
                        remoteProcessGroups: processComponentCollection(
                            response.flow.processGroupFlow.flow.remoteProcessGroups,
                            state.flow.processGroupFlow.flow.remoteProcessGroups,
                            state.addedCache,
                            state.removedCache,
                            response.connectedStateChanged
                        ),
                        funnels: processComponentCollection(
                            response.flow.processGroupFlow.flow.funnels,
                            state.flow.processGroupFlow.flow.funnels,
                            state.addedCache,
                            state.removedCache,
                            response.connectedStateChanged
                        ),
                        labels: processComponentCollection(
                            response.flow.processGroupFlow.flow.labels,
                            state.flow.processGroupFlow.flow.labels,
                            state.addedCache,
                            state.removedCache,
                            response.connectedStateChanged
                        ),
                        connections: processComponentCollection(
                            response.flow.processGroupFlow.flow.connections,
                            state.flow.processGroupFlow.flow.connections,
                            state.addedCache,
                            state.removedCache,
                            response.connectedStateChanged
                        )
                    }
                }
            };
            draftState.flowStatus = response.flowStatus;
            draftState.controllerBulletins = response.controllerBulletins;
            draftState.registryClients = response.registryClients;
            draftState.addedCache = [];
            draftState.removedCache = [];
            draftState.status = 'success' as const;
        });
    }),
    on(setRegistryClients, (state, { request }) => ({
        ...state,
        registryClients: request
    })),
    on(loadProcessGroupComplete, (state) => ({
        ...state,
        status: 'complete' as const
    })),
    on(loadConnectionSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const proposedConnection = response.connection;
            const componentIndex: number = draftState.flow.processGroupFlow.flow.connections.findIndex(
                (f: any) => proposedConnection.id === f.id
            );
            if (componentIndex > -1) {
                const currentConnection = draftState.flow.processGroupFlow.flow.connections[componentIndex];
                const isNewerOrEqualRevision =
                    proposedConnection.revision.version >= currentConnection.revision.version;

                if (isNewerOrEqualRevision) {
                    draftState.flow.processGroupFlow.flow.connections[componentIndex] = proposedConnection;
                }
            }
        });
    }),
    on(loadProcessorSuccess, pollProcessorUntilStoppedSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const proposedProcessor = response.processor;
            const componentIndex: number = draftState.flow.processGroupFlow.flow.processors.findIndex(
                (f: any) => proposedProcessor.id === f.id
            );
            if (componentIndex > -1) {
                const currentProcessor = draftState.flow.processGroupFlow.flow.processors[componentIndex];
                const isNewerOrEqualRevision = proposedProcessor.revision.version >= currentProcessor.revision.version;

                if (isNewerOrEqualRevision) {
                    draftState.flow.processGroupFlow.flow.processors[componentIndex] = proposedProcessor;
                }
            }
        });
    }),
    on(loadInputPortSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const proposedInputPort = response.inputPort;
            const componentIndex: number = draftState.flow.processGroupFlow.flow.inputPorts.findIndex(
                (f: any) => proposedInputPort.id === f.id
            );
            if (componentIndex > -1) {
                const currentInputPort = draftState.flow.processGroupFlow.flow.inputPorts[componentIndex];
                const isNewerOrEqualRevision = proposedInputPort.revision.version >= currentInputPort.revision.version;

                if (isNewerOrEqualRevision) {
                    draftState.flow.processGroupFlow.flow.inputPorts[componentIndex] = proposedInputPort;
                }
            }
        });
    }),
    on(loadRemoteProcessGroupSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const proposedRemoteProcessGroup = response.remoteProcessGroup;
            const componentIndex: number = draftState.flow.processGroupFlow.flow.remoteProcessGroups.findIndex(
                (f: any) => proposedRemoteProcessGroup.id === f.id
            );
            if (componentIndex > -1) {
                const currentRemoteProcessGroup =
                    draftState.flow.processGroupFlow.flow.remoteProcessGroups[componentIndex];
                const isNewerOrEqualRevision =
                    proposedRemoteProcessGroup.revision.version >= currentRemoteProcessGroup.revision.version;

                if (isNewerOrEqualRevision) {
                    draftState.flow.processGroupFlow.flow.remoteProcessGroups[componentIndex] =
                        proposedRemoteProcessGroup;
                }
            }
        });
    }),
    on(loadChildProcessGroupSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const proposedChildProcessGroup = response;
            const componentIndex: number = draftState.flow.processGroupFlow.flow.processGroups.findIndex(
                (f: any) => proposedChildProcessGroup.id === f.id
            );
            if (componentIndex > -1) {
                const currentChildProcessGroup = draftState.flow.processGroupFlow.flow.processGroups[componentIndex];
                const isNewerOrEqualRevision =
                    proposedChildProcessGroup.revision.version >= currentChildProcessGroup.revision.version;

                if (isNewerOrEqualRevision) {
                    draftState.flow.processGroupFlow.flow.processGroups[componentIndex] = proposedChildProcessGroup;
                }
            }

            draftState.saving = false;
        });
    }),
    on(flowBannerError, flowSnackbarError, (state) => ({
        ...state,
        dragging: false,
        saving: false,
        versionSaving: false
    })),
    on(startPollingProcessorUntilStopped, (state, { request }) => ({
        ...state,
        pollingProcessor: request
    })),
    on(stopPollingProcessor, (state) => ({
        ...state,
        pollingProcessor: null
    })),
    on(
        createProcessor,
        createProcessGroup,
        uploadProcessGroup,
        groupComponents,
        createConnection,
        createPort,
        createFunnel,
        createLabel,
        importFromRegistry,
        (state) => ({
            ...state,
            saving: true
        })
    ),
    on(createComponentSuccess, groupComponentsSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const collection: any[] | null = getComponentCollection(draftState, response.type);

            if (collection) {
                collection.push(response.payload);
            }
        });
    }),
    on(createComponentComplete, (state, { response }) => {
        return produce(state, (draftState) => {
            draftState.addedCache.push(response.payload.id);
            draftState.dragging = false;
            draftState.saving = false;
        });
    }),
    on(
        updateComponent,
        updateProcessor,
        updateConnection,
        enableComponent,
        disableComponent,
        startComponent,
        stopComponent,
        runOnce,
        (state) => ({
            ...state,
            saving: true
        })
    ),
    on(
        enableProcessGroupSuccess,
        disableProcessGroupSuccess,
        startProcessGroupSuccess,
        stopProcessGroupSuccess,
        (state) => ({
            ...state,
            saving: false
        })
    ),
    on(updateComponentSuccess, updateProcessorSuccess, updateConnectionSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const collection: any[] | null = getComponentCollection(draftState, response.type);

            if (collection) {
                const componentIndex: number = collection.findIndex((f: any) => response.id === f.id);
                if (componentIndex > -1) {
                    collection[componentIndex] = response.response;
                }
            }

            draftState.saving = false;
        });
    }),
    on(updateComponentFailure, (state, { response }) => {
        return produce(state, (draftState) => {
            if (response.restoreOnFailure) {
                const collection: any[] | null = getComponentCollection(draftState, response.type);

                if (collection) {
                    const componentIndex: number = collection.findIndex((f: any) => response.id === f.id);
                    if (componentIndex > -1) {
                        const currentComponent: any = collection[componentIndex];
                        collection[componentIndex] = {
                            ...currentComponent,
                            ...response.restoreOnFailure
                        };
                    }
                }
            }

            draftState.saving = false;
        });
    }),
    on(deleteComponentsSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            response.forEach((deleteResponse) => {
                draftState.removedCache.push(deleteResponse.id);

                const collection: any[] | null = getComponentCollection(draftState, deleteResponse.type);

                if (collection) {
                    const componentIndex: number = collection.findIndex((f: any) => deleteResponse.id === f.id);
                    if (componentIndex > -1) {
                        collection.splice(componentIndex, 1);
                    }
                }
            });
        });
    }),
    on(pasteSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const labels: any[] | null = getComponentCollection(draftState, ComponentType.Label);
            if (labels) {
                labels.push(...response.flow.labels);
            }
            const funnels: any[] | null = getComponentCollection(draftState, ComponentType.Funnel);
            if (funnels) {
                funnels.push(...response.flow.funnels);
            }
            const remoteProcessGroups: any[] | null = getComponentCollection(
                draftState,
                ComponentType.RemoteProcessGroup
            );
            if (remoteProcessGroups) {
                remoteProcessGroups.push(...response.flow.remoteProcessGroups);
            }
            const inputPorts: any[] | null = getComponentCollection(draftState, ComponentType.InputPort);
            if (inputPorts) {
                inputPorts.push(...response.flow.inputPorts);
            }
            const outputPorts: any[] | null = getComponentCollection(draftState, ComponentType.OutputPort);
            if (outputPorts) {
                outputPorts.push(...response.flow.outputPorts);
            }
            const processGroups: any[] | null = getComponentCollection(draftState, ComponentType.ProcessGroup);
            if (processGroups) {
                processGroups.push(...response.flow.processGroups);
            }
            const processors: any[] | null = getComponentCollection(draftState, ComponentType.Processor);
            if (processors) {
                processors.push(...response.flow.processors);
            }
            const connections: any[] | null = getComponentCollection(draftState, ComponentType.Connection);
            if (connections) {
                connections.push(...response.flow.connections);
            }
            draftState.flow.revision = response.revision;
        });
    }),
    on(setDragging, (state, { dragging }) => ({
        ...state,
        dragging
    })),
    on(setTransitionRequired, (state, { transitionRequired }) => ({
        ...state,
        transitionRequired
    })),
    on(setSkipTransform, (state, { skipTransform }) => ({
        ...state,
        skipTransform
    })),
    on(setAllowTransition, (state, { allowTransition }) => ({
        ...state,
        allowTransition
    })),
    on(navigateWithoutTransform, (state) => ({
        ...state,
        skipTransform: true
    })),
    on(setNavigationCollapsed, (state, { navigationCollapsed }) => ({
        ...state,
        navigationCollapsed
    })),
    on(setOperationCollapsed, (state, { operationCollapsed }) => ({
        ...state,
        operationCollapsed
    })),
    on(setFlowAnalysisOpen, (state, { flowAnalysisOpen }) => ({
        ...state,
        flowAnalysisOpen
    })),
    on(
        startComponentSuccess,
        stopComponentSuccess,
        enableComponentSuccess,
        disableComponentSuccess,
        (state, { response }) => {
            return produce(state, (draftState) => {
                const collection: any[] | null = getComponentCollection(draftState, response.type);

                if (collection) {
                    const componentIndex: number = collection.findIndex((f: any) => response.component.id === f.id);
                    if (componentIndex > -1) {
                        collection[componentIndex] = response.component;
                    }
                }

                draftState.saving = false;
            });
        }
    ),
    on(runOnceSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const collection: any[] | null = getComponentCollection(draftState, ComponentType.Processor);

            if (collection) {
                const componentIndex: number = collection.findIndex((f: any) => response.component.id === f.id);
                if (componentIndex > -1) {
                    collection[componentIndex] = response.component;
                }
            }

            draftState.saving = false;
        });
    }),
    on(saveToFlowRegistry, stopVersionControl, (state) => ({
        ...state,
        versionSaving: true
    })),
    on(saveToFlowRegistrySuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const collection: any[] | null = getComponentCollection(draftState, ComponentType.ProcessGroup);

            if (collection) {
                const componentIndex: number = collection.findIndex(
                    (f: any) => response.versionControlInformation?.groupId === f.id
                );
                if (componentIndex > -1) {
                    collection[componentIndex].revision = response.processGroupRevision;
                    collection[componentIndex].versionedFlowState = response.versionControlInformation?.state;
                }
            }

            draftState.versionSaving = false;
        });
    }),
    on(stopVersionControlSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const collection: any[] | null = getComponentCollection(draftState, ComponentType.ProcessGroup);

            if (collection) {
                const componentIndex: number = collection.findIndex((f: any) => response.processGroupId === f.id);
                if (componentIndex > -1) {
                    collection[componentIndex].revision = response.processGroupRevision;
                    collection[componentIndex].versionedFlowState = null;
                }
            }

            draftState.versionSaving = false;
        });
    }),
    on(
        changeVersionSuccess,
        pollChangeVersionSuccess,
        revertChangesSuccess,
        pollRevertChangesSuccess,
        (state, { response }) => ({
            ...state,
            changeVersionRequest: response
        })
    ),
    on(changeVersionComplete, revertChangesComplete, (state) => ({
        ...state,
        changeVersionRequest: null
    }))
);

function getComponentCollection(draftState: FlowState, componentType: ComponentType): any[] | null {
    let collection: any[] | null = null;
    switch (componentType) {
        case ComponentType.Processor:
            collection = draftState.flow.processGroupFlow.flow.processors;
            break;
        case ComponentType.ProcessGroup:
            collection = draftState.flow.processGroupFlow.flow.processGroups;
            break;
        case ComponentType.RemoteProcessGroup:
            collection = draftState.flow.processGroupFlow.flow.remoteProcessGroups;
            break;
        case ComponentType.InputPort:
            collection = draftState.flow.processGroupFlow.flow.inputPorts;
            break;
        case ComponentType.OutputPort:
            collection = draftState.flow.processGroupFlow.flow.outputPorts;
            break;
        case ComponentType.Label:
            collection = draftState.flow.processGroupFlow.flow.labels;
            break;
        case ComponentType.Funnel:
            collection = draftState.flow.processGroupFlow.flow.funnels;
            break;
        case ComponentType.Connection:
            collection = draftState.flow.processGroupFlow.flow.connections;
            break;
    }
    return collection;
}

function processComponentCollection(
    proposedComponents: ComponentEntity[],
    currentComponents: ComponentEntity[],
    addedCache: string[],
    removedCache: string[],
    overrideRevisionCheck: boolean
): ComponentEntity[] {
    // components in the proposed collection but not the current collection
    const addedComponents: ComponentEntity[] = proposedComponents.filter((proposedComponent) => {
        return !currentComponents.some((currentComponent) => currentComponent.id === proposedComponent.id);
    });
    // components in the current collection that are no longer in the proposed collection
    const removedComponents: ComponentEntity[] = currentComponents.filter((currentComponent) => {
        return !proposedComponents.some((proposedComponent) => proposedComponent.id === currentComponent.id);
    });
    // components that are in both the proposed collection and the current collection
    const updatedComponents: ComponentEntity[] = currentComponents.filter((currentComponent) => {
        return proposedComponents.some((proposedComponents) => proposedComponents.id === currentComponent.id);
    });

    const components = updatedComponents.map((currentComponent) => {
        const proposedComponent = proposedComponents.find(
            (proposedComponent) => proposedComponent.id === currentComponent.id
        );

        if (proposedComponent) {
            // consider newer when the version is greater or equal. when the revision is equal we want to use the proposed component
            // because it will contain updated stats/metrics. when the revision is greater it indicates the configuration was updated
            const isNewerOrEqualRevision = proposedComponent.revision.version >= currentComponent.revision.version;

            // use the proposed component when the revision is newer or equal or if we are overriding the revision check which
            // happens when a node cluster connection state changes. when this happens we just accept the proposed component
            // because it's revision basis is reset.
            if (isNewerOrEqualRevision || overrideRevisionCheck) {
                return proposedComponent;
            }
        }

        return currentComponent;
    });

    addedComponents.forEach((addedComponent) => {
        // if an added component is in the removed cache it means that the component was removed during the
        // request to load the process group. if it's not in the remove cache we add it to the components
        if (!removedCache.includes(addedComponent.id)) {
            components.push(addedComponent);
        }
    });

    removedComponents.forEach((removedComponent) => {
        // if a removed component is in the added cache it means that the component was added during the
        // request to load the process group. if it's in the added cache we add it to the components
        if (addedCache.includes(removedComponent.id)) {
            components.push(removedComponent);
        }
    });

    return components;
}
