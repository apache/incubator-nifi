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

import { createAction, props } from '@ngrx/store';
import {
    CenterComponentRequest,
    ChangeColorRequest,
    ChangeVersionDialogRequest,
    ComponentEntity,
    ConfirmStopVersionControlRequest,
    CreateComponentRequest,
    CreateComponentResponse,
    CreateConnection,
    CreateConnectionRequest,
    CreatePortRequest,
    CreateProcessGroupDialogRequest,
    CreateProcessGroupRequest,
    CreateProcessorRequest,
    CreateRemoteProcessGroupRequest,
    DeleteComponentRequest,
    DeleteComponentResponse,
    DisableComponentRequest,
    DisableComponentResponse,
    DisableComponentsRequest,
    DisableProcessGroupRequest,
    DisableProcessGroupResponse,
    DownloadFlowRequest,
    EditComponentDialogRequest,
    EditConnectionDialogRequest,
    EditCurrentProcessGroupRequest,
    EnableComponentRequest,
    EnableComponentResponse,
    EnableComponentsRequest,
    EnableProcessGroupRequest,
    EnableProcessGroupResponse,
    EnterProcessGroupRequest,
    FlowUpdateRequestEntity,
    GoToRemoteProcessGroupRequest,
    GroupComponentsDialogRequest,
    GroupComponentsRequest,
    GroupComponentsSuccess,
    ImportFromRegistryDialogRequest,
    ImportFromRegistryRequest,
    LoadChildProcessGroupRequest,
    LoadConnectionSuccess,
    LoadInputPortSuccess,
    LoadProcessGroupRequest,
    LoadProcessGroupResponse,
    LoadProcessorSuccess,
    LoadRemoteProcessGroupSuccess,
    LocalChangesDialogRequest,
    MoveComponentsRequest,
    MoveToFrontRequest,
    NavigateToComponentRequest,
    NavigateToControllerServicesRequest,
    NavigateToManageComponentPoliciesRequest,
    NavigateToParameterContext,
    NavigateToQueueListing,
    OpenChangeVersionDialogRequest,
    OpenComponentDialogRequest,
    OpenGroupComponentsDialogRequest,
    OpenLocalChangesDialogRequest,
    OpenSaveVersionDialogRequest,
    PasteResponseContext,
    RefreshRemoteProcessGroupRequest,
    ReplayLastProvenanceEventRequest,
    RpgManageRemotePortsRequest,
    RunOnceRequest,
    RunOnceResponse,
    SaveToVersionControlRequest,
    SaveVersionDialogRequest,
    SelectComponentsRequest,
    StartComponentRequest,
    StartComponentResponse,
    StartComponentsRequest,
    StartPollingProcessorUntilStoppedRequest,
    StartProcessGroupRequest,
    StartProcessGroupResponse,
    StopComponentRequest,
    StopComponentResponse,
    StopComponentsRequest,
    StopProcessGroupRequest,
    StopProcessGroupResponse,
    StopVersionControlRequest,
    StopVersionControlResponse,
    TerminateThreadsRequest,
    UpdateComponentFailure,
    UpdateComponentRequest,
    UpdateComponentResponse,
    UpdateConnectionRequest,
    UpdateConnectionSuccess,
    UpdatePositionsRequest,
    UpdateProcessorRequest,
    UpdateProcessorResponse,
    UploadProcessGroupRequest,
    VersionControlInformationEntity
} from './index';
import { StatusHistoryRequest } from '../../../../state/status-history';
import { FetchComponentVersionsRequest, RegistryClientEntity } from '../../../../state/shared';
import { ErrorContext } from '../../../../state/error';
import { CopyRequest, CopyResponseContext, CopyResponseEntity } from '../../../../state/copy';

const CANVAS_PREFIX = '[Canvas]';

/*
    Loading Flow
 */

export const resetFlowState = createAction(`${CANVAS_PREFIX} Reset Flow State`);

export const reloadFlow = createAction(`${CANVAS_PREFIX} Reload Flow`);

export const leaveProcessGroup = createAction(`${CANVAS_PREFIX} Leave Process Group`);

export const enterProcessGroup = createAction(
    `${CANVAS_PREFIX} Enter Process Group`,
    props<{ request: EnterProcessGroupRequest }>()
);

export const loadProcessGroup = createAction(
    `${CANVAS_PREFIX} Load Process Group`,
    props<{ request: LoadProcessGroupRequest }>()
);

export const loadProcessGroupSuccess = createAction(
    `${CANVAS_PREFIX} Load Process Group Success`,
    props<{ response: LoadProcessGroupResponse }>()
);

export const loadProcessGroupComplete = createAction(
    `${CANVAS_PREFIX} Load Process Group Complete`,
    props<{ response: LoadProcessGroupResponse }>()
);

export const loadChildProcessGroup = createAction(
    `${CANVAS_PREFIX} Load Child Process Group`,
    props<{ request: LoadChildProcessGroupRequest }>()
);

export const loadChildProcessGroupSuccess = createAction(
    `${CANVAS_PREFIX} Load Child Process Group Success`,
    props<{ response: ComponentEntity }>()
);

export const startProcessGroupPolling = createAction(`${CANVAS_PREFIX} Start Process Group Polling`);

export const stopProcessGroupPolling = createAction(`${CANVAS_PREFIX} Stop Process Group Polling`);

export const setRegistryClients = createAction(
    `${CANVAS_PREFIX} Set Registry Clients`,
    props<{ request: RegistryClientEntity[] }>()
);

export const loadConnectionsForComponent = createAction(
    `${CANVAS_PREFIX} Load Connections For Component`,
    props<{ id: string }>()
);

export const loadConnection = createAction(`${CANVAS_PREFIX} Load Connection`, props<{ id: string }>());

export const loadConnectionSuccess = createAction(
    `${CANVAS_PREFIX} Load Connection Success`,
    props<{ response: LoadConnectionSuccess }>()
);

export const loadComponentsForConnection = createAction(
    `${CANVAS_PREFIX} Load Components For Connection`,
    props<{ connection: any }>()
);

export const loadProcessor = createAction(`${CANVAS_PREFIX} Load Processor`, props<{ id: string }>());

export const loadProcessorSuccess = createAction(
    `${CANVAS_PREFIX} Load Processor Success`,
    props<{ response: LoadProcessorSuccess }>()
);

export const loadInputPort = createAction(`${CANVAS_PREFIX} Load Input Port`, props<{ id: string }>());

export const loadInputPortSuccess = createAction(
    `${CANVAS_PREFIX} Load Input Port Success`,
    props<{ response: LoadInputPortSuccess }>()
);

export const loadRemoteProcessGroup = createAction(
    `${CANVAS_PREFIX} Load Remote Process Group`,
    props<{ id: string }>()
);

export const loadRemoteProcessGroupSuccess = createAction(
    `${CANVAS_PREFIX} Load Remote Process Group Success`,
    props<{ response: LoadRemoteProcessGroupSuccess }>()
);

/*
    Selectable Behavior
 */

export const addSelectedComponents = createAction(
    `${CANVAS_PREFIX} Add Selected Component`,
    props<{ request: SelectComponentsRequest }>()
);

export const selectComponents = createAction(
    `${CANVAS_PREFIX} Select Components`,
    props<{ request: SelectComponentsRequest }>()
);

export const deselectAllComponents = createAction(`${CANVAS_PREFIX} Deselect All Components`);

export const removeSelectedComponents = createAction(
    `${CANVAS_PREFIX} Remove Selected Components`,
    props<{ request: SelectComponentsRequest }>()
);

export const centerSelectedComponents = createAction(
    `${CANVAS_PREFIX} Center Selected Components`,
    props<{ request: CenterComponentRequest }>()
);

/*
    Create Component Actions
 */

export const createComponentRequest = createAction(
    `${CANVAS_PREFIX} Create Component Request`,
    props<{ request: CreateComponentRequest }>()
);

export const createFunnel = createAction(
    `${CANVAS_PREFIX} Create Funnel`,
    props<{ request: CreateComponentRequest }>()
);

export const createLabel = createAction(`${CANVAS_PREFIX} Create Label`, props<{ request: CreateComponentRequest }>());

export const createRemoteProcessGroup = createAction(
    `${CANVAS_PREFIX} Create Remote Process Group`,
    props<{ request: CreateRemoteProcessGroupRequest }>()
);

export const openNewProcessGroupDialog = createAction(
    `${CANVAS_PREFIX} Open New Process Group Dialog`,
    props<{ request: CreateProcessGroupDialogRequest }>()
);

export const openNewRemoteProcessGroupDialog = createAction(
    `${CANVAS_PREFIX} Open New Remote Process Group Dialog`,
    props<{ request: CreateComponentRequest }>()
);

export const goToRemoteProcessGroup = createAction(
    `${CANVAS_PREFIX} Go To Remote Process Group`,
    props<{ request: GoToRemoteProcessGroupRequest }>()
);

export const refreshRemoteProcessGroup = createAction(`${CANVAS_PREFIX} Refresh Remote Process Group`);

export const requestRefreshRemoteProcessGroup = createAction(
    `${CANVAS_PREFIX} Request Refresh Remote Process Group Polling`,
    props<{ request: RefreshRemoteProcessGroupRequest }>()
);

export const startRemoteProcessGroupPolling = createAction(`${CANVAS_PREFIX} Start Remote Process Group Polling`);

export const stopRemoteProcessGroupPolling = createAction(`${CANVAS_PREFIX} Stop Remote Process Group Polling`);

export const createProcessGroup = createAction(
    `${CANVAS_PREFIX} Create Process Group`,
    props<{ request: CreateProcessGroupRequest }>()
);

export const uploadProcessGroup = createAction(
    `${CANVAS_PREFIX} Upload Process Group`,
    props<{ request: UploadProcessGroupRequest }>()
);

export const getParameterContextsAndOpenGroupComponentsDialog = createAction(
    `${CANVAS_PREFIX} Get Parameter Contexts And Open Group Components Dialog`,
    props<{ request: OpenGroupComponentsDialogRequest }>()
);

export const openGroupComponentsDialog = createAction(
    `${CANVAS_PREFIX} Open Group Components Dialog`,
    props<{ request: GroupComponentsDialogRequest }>()
);

export const groupComponents = createAction(
    `${CANVAS_PREFIX} Group Components`,
    props<{ request: GroupComponentsRequest }>()
);

export const groupComponentsSuccess = createAction(
    `${CANVAS_PREFIX} Group Components Success`,
    props<{ response: GroupComponentsSuccess }>()
);

export const openNewProcessorDialog = createAction(
    `${CANVAS_PREFIX} Open New Processor Dialog`,
    props<{ request: CreateComponentRequest }>()
);

export const createProcessor = createAction(
    `${CANVAS_PREFIX} Create Processor`,
    props<{ request: CreateProcessorRequest }>()
);

export const openNewConnectionDialog = createAction(
    `${CANVAS_PREFIX} Open New Connection Dialog`,
    props<{ request: CreateConnectionRequest }>()
);

export const createConnection = createAction(
    `${CANVAS_PREFIX} Create Connection`,
    props<{ request: CreateConnection }>()
);

export const openNewPortDialog = createAction(
    `${CANVAS_PREFIX} Open New Port Dialog`,
    props<{ request: CreateComponentRequest }>()
);

export const createPort = createAction(`${CANVAS_PREFIX} Create Port`, props<{ request: CreatePortRequest }>());

export const openImportFromRegistryDialog = createAction(
    `${CANVAS_PREFIX} Open Import From Registry Dialog`,
    props<{ request: ImportFromRegistryDialogRequest }>()
);

export const importFromRegistry = createAction(
    `${CANVAS_PREFIX} Import From Registry`,
    props<{ request: ImportFromRegistryRequest }>()
);

export const createComponentSuccess = createAction(
    `${CANVAS_PREFIX} Create Component Success`,
    props<{ response: CreateComponentResponse }>()
);

export const createComponentComplete = createAction(
    `${CANVAS_PREFIX} Create Component Complete`,
    props<{ response: CreateComponentResponse }>()
);

export const navigateToViewStatusHistoryForComponent = createAction(
    `${CANVAS_PREFIX} Navigate To Status History For Component`,
    props<{ request: OpenComponentDialogRequest }>()
);

export const navigateToViewStatusHistoryForCurrentProcessGroup = createAction(
    `${CANVAS_PREFIX} Navigate To Status History For Current Process Group`
);

export const viewStatusHistoryForComponent = createAction(
    `${CANVAS_PREFIX} View Status History for Component`,
    props<{ request: StatusHistoryRequest }>()
);
/*
    Update Component Actions
 */

export const navigateToEditComponent = createAction(
    `${CANVAS_PREFIX} Navigate To Edit Component`,
    props<{ request: OpenComponentDialogRequest }>()
);

export const navigateToAdvancedProcessorUi = createAction(
    `${CANVAS_PREFIX} Navigate To Advanced Processor Ui`,
    props<{ id: string }>()
);

export const navigateToManageComponentPolicies = createAction(
    `${CANVAS_PREFIX} Navigate To Manage Component Policies`,
    props<{ request: NavigateToManageComponentPoliciesRequest }>()
);

export const editComponent = createAction(
    `${CANVAS_PREFIX} Edit Component`,
    props<{ request: EditComponentDialogRequest }>()
);

export const navigateToEditCurrentProcessGroup = createAction(
    `${CANVAS_PREFIX} Navigate To Edit Current Process Group`
);

export const navigateToControllerServicesForProcessGroup = createAction(
    `${CANVAS_PREFIX} Navigate To Controller Services For Process Group`,
    props<{ request: NavigateToControllerServicesRequest }>()
);

export const navigateToQueueListing = createAction(
    `${CANVAS_PREFIX} Navigate To Queue Listing`,
    props<{ request: NavigateToQueueListing }>()
);

export const navigateToParameterContext = createAction(
    `${CANVAS_PREFIX} Navigate To Parameter Context`,
    props<{ request: NavigateToParameterContext }>()
);

export const editCurrentProcessGroup = createAction(
    `${CANVAS_PREFIX} Edit Current Process Group`,
    props<{
        request: EditCurrentProcessGroupRequest;
    }>()
);

export const openEditPortDialog = createAction(
    `${CANVAS_PREFIX} Open Edit Port Dialog`,
    props<{ request: EditComponentDialogRequest }>()
);

export const openEditProcessorDialog = createAction(
    `${CANVAS_PREFIX} Open Edit Processor Dialog`,
    props<{ request: EditComponentDialogRequest }>()
);

export const openEditConnectionDialog = createAction(
    `${CANVAS_PREFIX} Open Edit Connection Dialog`,
    props<{ request: EditConnectionDialogRequest }>()
);

export const openEditProcessGroupDialog = createAction(
    `${CANVAS_PREFIX} Open Edit Process Group Dialog`,
    props<{ request: EditComponentDialogRequest }>()
);

export const openEditRemoteProcessGroupDialog = createAction(
    `${CANVAS_PREFIX} Open Edit Remote Process Group Dialog`,
    props<{ request: EditComponentDialogRequest }>()
);

export const openEditLabelDialog = createAction(
    `${CANVAS_PREFIX} Open Edit Label Dialog`,
    props<{ request: EditComponentDialogRequest }>()
);

export const navigateToManageRemotePorts = createAction(
    `${CANVAS_PREFIX} Open Remote Process Group Manage Remote Ports`,
    props<{ request: RpgManageRemotePortsRequest }>()
);

export const updateComponent = createAction(
    `${CANVAS_PREFIX} Update Component`,
    props<{ request: UpdateComponentRequest }>()
);

export const updateComponentSuccess = createAction(
    `${CANVAS_PREFIX} Update Component Success`,
    props<{ response: UpdateComponentResponse }>()
);

export const updateComponentFailure = createAction(
    `${CANVAS_PREFIX} Update Component Failure`,
    props<{ response: UpdateComponentFailure }>()
);

export const updateProcessor = createAction(
    `${CANVAS_PREFIX} Update Processor`,
    props<{ request: UpdateProcessorRequest }>()
);

export const updateProcessorSuccess = createAction(
    `${CANVAS_PREFIX} Update Processor Success`,
    props<{ response: UpdateProcessorResponse }>()
);

export const updateConnection = createAction(
    `${CANVAS_PREFIX} Update Connection`,
    props<{ request: UpdateConnectionRequest }>()
);

export const updateConnectionSuccess = createAction(
    `${CANVAS_PREFIX} Update Connection Success`,
    props<{ response: UpdateConnectionSuccess }>()
);

export const updatePositions = createAction(
    `${CANVAS_PREFIX} Update Positions`,
    props<{ request: UpdatePositionsRequest }>()
);

export const updatePositionComplete = createAction(
    `${CANVAS_PREFIX} Update Position Complete`,
    props<{ response: UpdateComponentResponse }>()
);

export const moveComponents = createAction(
    `${CANVAS_PREFIX} Move Components`,
    props<{ request: MoveComponentsRequest }>()
);

export const copy = createAction(`${CANVAS_PREFIX} Copy`, props<{ request: CopyRequest }>());

export const copySuccess = createAction(`${CANVAS_PREFIX} Copy Success`, props<{ response: CopyResponseContext }>());

export const paste = createAction(`${CANVAS_PREFIX} Paste`, props<{ request: CopyResponseEntity }>());

export const pasteSuccess = createAction(`${CANVAS_PREFIX} Paste Success`, props<{ response: PasteResponseContext }>());

/*
    Delete Component Actions
 */

export const deleteComponents = createAction(
    `${CANVAS_PREFIX} Delete Components`,
    props<{ request: DeleteComponentRequest[] }>()
);

export const deleteComponentsSuccess = createAction(
    `${CANVAS_PREFIX} Delete Components Success`,
    props<{ response: DeleteComponentResponse[] }>()
);

/*
    Transition
 */

export const setDragging = createAction(`${CANVAS_PREFIX} Set Dragging`, props<{ dragging: boolean }>());

export const setTransitionRequired = createAction(
    `${CANVAS_PREFIX} Set Transition Required`,
    props<{ transitionRequired: boolean }>()
);

/**
 * skipTransform is used when handling URL events for loading the current PG and component [bulk] selection. since the
 * URL is the source of truth we need to indicate skipTransform when the URL changes based on the user selection on
 * the canvas. However, we do not want the transform skipped when using link to open or a particular part of the flow.
 * In these cases, we want the transform to be applied so the viewport is restored or the component(s) is centered.
 */
export const setSkipTransform = createAction(
    `${CANVAS_PREFIX} Set Skip Transform`,
    props<{ skipTransform: boolean }>()
);

/**
 * allowTransition is a flag that can be set that indicates if a transition should be used when applying a transform.
 * By default, restoring the viewport or selecting/centering components will not use a transition unless explicitly
 * specified. Zoom based transforms (like fit or 1:1) will always use a transition.
 */
export const setAllowTransition = createAction(
    `${CANVAS_PREFIX} Set Allow Transition`,
    props<{ allowTransition: boolean }>()
);

export const navigateToComponent = createAction(
    `${CANVAS_PREFIX} Navigate To Component`,
    props<{ request: NavigateToComponentRequest }>()
);

export const navigateWithoutTransform = createAction(
    `${CANVAS_PREFIX} Navigate Without Transform`,
    props<{ url: string[] }>()
);

/*
    Palette actions
 */

export const setNavigationCollapsed = createAction(
    `${CANVAS_PREFIX} Set Navigation Collapsed`,
    props<{ navigationCollapsed: boolean }>()
);

export const setOperationCollapsed = createAction(
    `${CANVAS_PREFIX} Set Operation Collapsed`,
    props<{ operationCollapsed: boolean }>()
);

export const setFlowAnalysisOpen = createAction(
    `${CANVAS_PREFIX} Set Flow Analysis Open`,
    props<{ flowAnalysisOpen: boolean }>()
);

/*
    General
 */

export const showOkDialog = createAction(
    `${CANVAS_PREFIX} Show Ok Dialog`,
    props<{ title: string; message: string }>()
);

export const renderConnectionsForComponent = createAction(
    `${CANVAS_PREFIX} Render Connections For Component`,
    props<{ id: string; updatePath: boolean; updateLabel: boolean }>()
);

export const navigateToProvenanceForComponent = createAction(
    `${CANVAS_PREFIX} Navigate To Provenance For Component`,
    props<{ id: string }>()
);

export const replayLastProvenanceEvent = createAction(
    `${CANVAS_PREFIX} Replay Last Provenance Event`,
    props<{ request: ReplayLastProvenanceEventRequest }>()
);

export const enableComponent = createAction(
    `${CANVAS_PREFIX} Enable Component`,
    props<{ request: EnableComponentRequest | EnableProcessGroupRequest }>()
);

export const enableComponents = createAction(
    `${CANVAS_PREFIX} Enable Components`,
    props<{ request: EnableComponentsRequest }>()
);

export const enableComponentSuccess = createAction(
    `${CANVAS_PREFIX} Enable Component Success`,
    props<{ response: EnableComponentResponse }>()
);

export const enableProcessGroupSuccess = createAction(
    `${CANVAS_PREFIX} Enable Process Group Success`,
    props<{ response: EnableProcessGroupResponse }>()
);

export const enableCurrentProcessGroup = createAction(`${CANVAS_PREFIX} Enable Current Process Group`);

export const disableComponent = createAction(
    `${CANVAS_PREFIX} Disable Component`,
    props<{ request: DisableComponentRequest | DisableProcessGroupRequest }>()
);

export const disableComponents = createAction(
    `${CANVAS_PREFIX} Disable Components`,
    props<{ request: DisableComponentsRequest }>()
);

export const disableComponentSuccess = createAction(
    `${CANVAS_PREFIX} Disable Component Success`,
    props<{ response: DisableComponentResponse }>()
);

export const disableProcessGroupSuccess = createAction(
    `${CANVAS_PREFIX} Disable Process Group Success`,
    props<{ response: DisableProcessGroupResponse }>()
);

export const disableCurrentProcessGroup = createAction(`${CANVAS_PREFIX} Disable Current Process Group`);

export const runOnce = createAction(`${CANVAS_PREFIX} Run Once`, props<{ request: RunOnceRequest }>());

export const runOnceSuccess = createAction(`${CANVAS_PREFIX} Run Once Success`, props<{ response: RunOnceResponse }>());

export const startComponent = createAction(
    `${CANVAS_PREFIX} Start Component`,
    props<{ request: StartComponentRequest | StartProcessGroupRequest }>()
);

export const startComponents = createAction(
    `${CANVAS_PREFIX} Start Components`,
    props<{ request: StartComponentsRequest }>()
);

export const startComponentSuccess = createAction(
    `${CANVAS_PREFIX} Start Component Success`,
    props<{ response: StartComponentResponse }>()
);

export const startProcessGroupSuccess = createAction(
    `${CANVAS_PREFIX} Start Process Group Success`,
    props<{ response: StartProcessGroupResponse }>()
);

export const stopComponent = createAction(
    `${CANVAS_PREFIX} Stop Component`,
    props<{ request: StopComponentRequest | StopProcessGroupRequest }>()
);

export const stopComponents = createAction(
    `${CANVAS_PREFIX} Stop Components`,
    props<{ request: StopComponentsRequest }>()
);

export const terminateThreads = createAction(
    `${CANVAS_PREFIX} Terminate Threads`,
    props<{ request: TerminateThreadsRequest }>()
);

export const stopComponentSuccess = createAction(
    `${CANVAS_PREFIX} Stop Component Success`,
    props<{ response: StopComponentResponse }>()
);

export const stopProcessGroupSuccess = createAction(
    `${CANVAS_PREFIX} Stop Process Group Success`,
    props<{ response: StopProcessGroupResponse }>()
);

export const startCurrentProcessGroup = createAction(`${CANVAS_PREFIX} Start Current Process Group`);

export const stopCurrentProcessGroup = createAction(`${CANVAS_PREFIX} Stop Current Process Group`);

export const enableControllerServicesInCurrentProcessGroup = createAction(
    `${CANVAS_PREFIX} Enable Controller Services In Current Process Group`
);

export const disableControllerServicesInCurrentProcessGroup = createAction(
    `${CANVAS_PREFIX} Disable Controller Services In Current Process Group`
);

export const enableControllerServicesInProcessGroup = createAction(
    `${CANVAS_PREFIX} Enable Controller Services In Process Group`,
    props<{ id: string }>()
);

export const disableControllerServicesInProcessGroup = createAction(
    `${CANVAS_PREFIX} Disable Controller Services In Process Group`,
    props<{ id: string }>()
);

export const openChangeVersionDialogRequest = createAction(
    `${CANVAS_PREFIX} Open Change Flow Version Dialog Request`,
    props<{ request: OpenChangeVersionDialogRequest }>()
);

export const openSaveVersionDialogRequest = createAction(
    `${CANVAS_PREFIX} Open Save Flow Version Dialog Request`,
    props<{ request: OpenSaveVersionDialogRequest }>()
);

export const openCommitLocalChangesDialogRequest = createAction(
    `${CANVAS_PREFIX} Open Commit Local Changes Dialog Request`,
    props<{ request: OpenSaveVersionDialogRequest }>()
);

export const openForceCommitLocalChangesDialogRequest = createAction(
    `${CANVAS_PREFIX} Open Force Commit Local Changes Dialog Request`,
    props<{ request: OpenSaveVersionDialogRequest }>()
);

export const openChangeVersionDialog = createAction(
    `${CANVAS_PREFIX} Open Change Flow Version Dialog`,
    props<{ request: ChangeVersionDialogRequest }>()
);

export const openChangeVersionProgressDialog = createAction(
    `${CANVAS_PREFIX} Open Change Flow Version Progress Dialog`,
    props<{ request: VersionControlInformationEntity }>()
);

export const changeVersion = createAction(
    `${CANVAS_PREFIX} Change Flow Version`,
    props<{ request: VersionControlInformationEntity }>()
);

export const changeVersionSuccess = createAction(
    `${CANVAS_PREFIX} Change Flow Version Success`,
    props<{ response: FlowUpdateRequestEntity }>()
);

export const changeVersionComplete = createAction(
    `${CANVAS_PREFIX} Change Flow Version Complete`,
    props<{ response: FlowUpdateRequestEntity }>()
);

export const startPollingChangeVersion = createAction(`${CANVAS_PREFIX} Start Polling Change Version`);

export const pollChangeVersion = createAction(`${CANVAS_PREFIX} Poll Change Version`);

export const pollChangeVersionSuccess = createAction(
    `${CANVAS_PREFIX} Poll Change Version Success`,
    props<{ response: FlowUpdateRequestEntity }>()
);

export const stopPollingChangeVersion = createAction(`${CANVAS_PREFIX} Stop Polling Change Version`);

export const startPollingProcessorUntilStopped = createAction(
    `${CANVAS_PREFIX} Start Polling Processor Until Stopped`,
    props<{ request: StartPollingProcessorUntilStoppedRequest }>()
);

export const pollProcessorUntilStopped = createAction(`${CANVAS_PREFIX} Poll Processor Until Stopped`);

export const pollProcessorUntilStoppedSuccess = createAction(
    `${CANVAS_PREFIX} Poll Processor Until Stopped Success`,
    props<{ response: LoadProcessorSuccess }>()
);

export const stopPollingProcessor = createAction(`${CANVAS_PREFIX} Stop Polling Processor`);

export const openSaveVersionDialog = createAction(
    `${CANVAS_PREFIX} Open Save Flow Version Dialog`,
    props<{ request: SaveVersionDialogRequest }>()
);

export const saveToFlowRegistry = createAction(
    `${CANVAS_PREFIX} Save To Version Control`,
    props<{ request: SaveToVersionControlRequest }>()
);

export const saveToFlowRegistrySuccess = createAction(
    `${CANVAS_PREFIX} Save To Version Control Success`,
    props<{ response: VersionControlInformationEntity }>()
);

export const stopVersionControlRequest = createAction(
    `${CANVAS_PREFIX} Stop Version Control Request`,
    props<{ request: ConfirmStopVersionControlRequest }>()
);

export const stopVersionControl = createAction(
    `${CANVAS_PREFIX} Stop Version Control`,
    props<{ request: StopVersionControlRequest }>()
);

export const stopVersionControlSuccess = createAction(
    `${CANVAS_PREFIX} Stop Version Control Success`,
    props<{ response: StopVersionControlResponse }>()
);

export const flowSnackbarError = createAction(`${CANVAS_PREFIX} Flow Snackbar Error`, props<{ error: string }>());

export const flowBannerError = createAction(
    `${CANVAS_PREFIX} Flow Banner Error`,
    props<{ errorContext: ErrorContext }>()
);

export const openShowLocalChangesDialogRequest = createAction(
    `${CANVAS_PREFIX} Open Show Local Changes Dialog Request`,
    props<{ request: OpenLocalChangesDialogRequest }>()
);

export const openRevertLocalChangesDialogRequest = createAction(
    `${CANVAS_PREFIX} Open Revert Local Changes Dialog Request`,
    props<{ request: OpenLocalChangesDialogRequest }>()
);

export const openLocalChangesDialog = createAction(
    `${CANVAS_PREFIX} Open Local Changes Dialog`,
    props<{ request: LocalChangesDialogRequest }>()
);

export const goToChange = createAction(
    `${CANVAS_PREFIX} Go To Change`,
    props<{
        request: NavigateToComponentRequest;
    }>()
);

export const openRevertChangesProgressDialog = createAction(
    `${CANVAS_PREFIX} Open Revert Changes Progress Dialog`,
    props<{ request: VersionControlInformationEntity }>()
);

export const revertChanges = createAction(
    `${CANVAS_PREFIX} Revert Changes`,
    props<{ request: VersionControlInformationEntity }>()
);

export const revertChangesSuccess = createAction(
    `${CANVAS_PREFIX} Revert Changes Success`,
    props<{ response: FlowUpdateRequestEntity }>()
);

export const revertChangesComplete = createAction(
    `${CANVAS_PREFIX} Revert Changes Complete`,
    props<{ response: FlowUpdateRequestEntity }>()
);

export const startPollingRevertChanges = createAction(`${CANVAS_PREFIX} Start Polling Revert Changes`);

export const pollRevertChanges = createAction(`${CANVAS_PREFIX} Poll Revert Changes`);

export const pollRevertChangesSuccess = createAction(
    `${CANVAS_PREFIX} Poll Revert Changes Success`,
    props<{ response: FlowUpdateRequestEntity }>()
);

export const stopPollingRevertChanges = createAction(`${CANVAS_PREFIX} Stop Polling Revert Changes`);

export const downloadFlow = createAction(
    `${CANVAS_PREFIX} Download Flow Request`,
    props<{ request: DownloadFlowRequest }>()
);

export const moveToFront = createAction(`${CANVAS_PREFIX} Move To Front`, props<{ request: MoveToFrontRequest }>());

export const openChangeProcessorVersionDialog = createAction(
    `${CANVAS_PREFIX} Open Change Processor Version Dialog`,
    props<{ request: FetchComponentVersionsRequest }>()
);

export const openChangeColorDialog = createAction(
    `${CANVAS_PREFIX} Open Change Color Dialog`,
    props<{ request: ChangeColorRequest[] }>()
);
