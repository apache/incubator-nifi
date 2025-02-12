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

import { DestroyRef, inject, Injectable } from '@angular/core';
import { FlowService } from '../../service/flow.service';
import { Actions, createEffect, ofType } from '@ngrx/effects';
import { concatLatestFrom } from '@ngrx/operators';
import * as FlowActions from './flow.actions';
import * as StatusHistoryActions from '../../../../state/status-history/status-history.actions';
import * as ErrorActions from '../../../../state/error/error.actions';
import * as CopyActions from '../../../../state/copy/copy.actions';
import {
    asyncScheduler,
    catchError,
    combineLatest,
    filter,
    from,
    interval,
    map,
    mergeMap,
    NEVER,
    Observable,
    of,
    switchMap,
    take,
    takeUntil,
    tap,
    throttleTime
} from 'rxjs';
import {
    CreateConnectionDialogRequest,
    CreateProcessGroupDialogRequest,
    DeleteComponentResponse,
    DisableComponentRequest,
    EnableComponentRequest,
    GroupComponentsDialogRequest,
    ImportFromRegistryDialogRequest,
    LoadProcessGroupResponse,
    MoveComponentRequest,
    PasteRequest,
    PasteRequestContext,
    PasteRequestEntity,
    SaveVersionDialogRequest,
    SaveVersionRequest,
    SelectedComponent,
    Snippet,
    StartComponentRequest,
    StopComponentRequest,
    StopVersionControlRequest,
    StopVersionControlResponse,
    UpdateComponentFailure,
    UpdateComponentRequest,
    UpdateComponentResponse,
    UpdateConnectionSuccess,
    UpdateProcessorRequest,
    UpdateProcessorResponse,
    VersionControlInformationEntity
} from './index';
import { Action, Store } from '@ngrx/store';
import {
    selectAnySelectedComponentIds,
    selectChangeVersionRequest,
    selectCurrentParameterContext,
    selectCurrentProcessGroupId,
    selectCurrentProcessGroupRevision,
    selectFlowLoadingStatus,
    selectInputPort,
    selectMaxZIndex,
    selectOutputPort,
    selectParentProcessGroupId,
    selectProcessGroup,
    selectProcessor,
    selectPollingProcessor,
    selectRefreshRpgDetails,
    selectRemoteProcessGroup,
    selectSaving,
    selectVersionSaving
} from './flow.selectors';
import { ConnectionManager } from '../../service/manager/connection-manager.service';
import { MatDialog, MatDialogRef } from '@angular/material/dialog';
import { CreatePort } from '../../ui/canvas/items/port/create-port/create-port.component';
import { EditPort } from '../../ui/canvas/items/port/edit-port/edit-port.component';
import {
    BranchEntity,
    BucketEntity,
    OpenChangeComponentVersionDialogRequest,
    RegistryClientEntity,
    VersionedFlowEntity,
    VersionedFlowSnapshotMetadataEntity
} from '../../../../state/shared';
import { Router } from '@angular/router';
import { Client } from '../../../../service/client.service';
import { CanvasUtils } from '../../service/canvas-utils.service';
import { CanvasView } from '../../service/canvas-view.service';
import { selectProcessorTypes } from '../../../../state/extension-types/extension-types.selectors';
import { NiFiState } from '../../../../state';
import { CreateProcessor } from '../../ui/canvas/items/processor/create-processor/create-processor.component';
import { EditProcessor } from '../../ui/canvas/items/processor/edit-processor/edit-processor.component';
import { BirdseyeView } from '../../service/birdseye-view.service';
import { CreateRemoteProcessGroup } from '../../ui/canvas/items/remote-process-group/create-remote-process-group/create-remote-process-group.component';
import { CreateProcessGroup } from '../../ui/canvas/items/process-group/create-process-group/create-process-group.component';
import { CreateConnection } from '../../ui/canvas/items/connection/create-connection/create-connection.component';
import { EditConnectionComponent } from '../../ui/canvas/items/connection/edit-connection/edit-connection.component';
import { OkDialog } from '../../../../ui/common/ok-dialog/ok-dialog.component';
import { GroupComponents } from '../../ui/canvas/items/process-group/group-components/group-components.component';
import { EditProcessGroup } from '../../ui/canvas/items/process-group/edit-process-group/edit-process-group.component';
import { ControllerServiceService } from '../../service/controller-service.service';
import { YesNoDialog } from '@nifi/shared';
import { PropertyTableHelperService } from '../../../../service/property-table-helper.service';
import { ParameterHelperService } from '../../service/parameter-helper.service';
import { RegistryService } from '../../service/registry.service';
import { ImportFromRegistry } from '../../ui/canvas/items/flow/import-from-registry/import-from-registry.component';
import { selectCurrentUser } from '../../../../state/current-user/current-user.selectors';
import { NoRegistryClientsDialog } from '../../ui/common/no-registry-clients-dialog/no-registry-clients-dialog.component';
import { EditRemoteProcessGroup } from '../../ui/canvas/items/remote-process-group/edit-remote-process-group/edit-remote-process-group.component';
import { HttpErrorResponse } from '@angular/common/http';
import { SaveVersionDialog } from '../../ui/canvas/items/flow/save-version-dialog/save-version-dialog.component';
import { ChangeVersionDialog } from '../../ui/canvas/items/flow/change-version-dialog/change-version-dialog';
import { ChangeVersionProgressDialog } from '../../ui/canvas/items/flow/change-version-progress-dialog/change-version-progress-dialog';
import { LocalChangesDialog } from '../../ui/canvas/items/flow/local-changes-dialog/local-changes-dialog';
import { ClusterConnectionService } from '../../../../service/cluster-connection.service';
import { ExtensionTypesService } from '../../../../service/extension-types.service';
import { ChangeComponentVersionDialog } from '../../../../ui/common/change-component-version-dialog/change-component-version-dialog';
import { SnippetService } from '../../service/snippet.service';
import { EditLabel } from '../../ui/canvas/items/label/edit-label/edit-label.component';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { selectConnectedStateChanged } from '../../../../state/cluster-summary/cluster-summary.selectors';
import { resetConnectedStateChanged } from '../../../../state/cluster-summary/cluster-summary.actions';
import { ChangeColorDialog } from '../../ui/canvas/change-color-dialog/change-color-dialog.component';
import {
    resetPropertyVerificationState,
    verifyProperties
} from '../../../../state/property-verification/property-verification.actions';
import {
    selectPropertyVerificationResults,
    selectPropertyVerificationStatus
} from '../../../../state/property-verification/property-verification.selectors';
import { VerifyPropertiesRequestContext } from '../../../../state/property-verification';
import { BackNavigation } from '../../../../state/navigation';
import {
    ComponentType,
    isDefinedAndNotNull,
    LARGE_DIALOG,
    MEDIUM_DIALOG,
    SMALL_DIALOG,
    XL_DIALOG,
    NiFiCommon,
    Storage
} from '@nifi/shared';
import { resetPollingFlowAnalysis } from '../flow-analysis/flow-analysis.actions';
import { selectDocumentVisibilityState } from '../../../../state/document-visibility/document-visibility.selectors';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { DocumentVisibility } from '../../../../state/document-visibility';
import { ErrorContextKey } from '../../../../state/error';
import {
    disableComponent,
    enableComponent,
    setRegistryClients,
    startComponent,
    startPollingProcessorUntilStopped,
    stopComponent
} from './flow.actions';
import { CopyPasteService } from '../../service/copy-paste.service';
import { selectCopiedContent } from '../../../../state/copy/copy.selectors';
import { CopyRequestContext, CopyResponseContext } from '../../../../state/copy';
import * as ParameterActions from '../parameter/parameter.actions';
import { ParameterContextService } from '../../../parameter-contexts/service/parameter-contexts.service';

@Injectable()
export class FlowEffects {
    private createProcessGroupDialogRef: MatDialogRef<CreateProcessGroup, any> | undefined;
    private editProcessGroupDialogRef: MatDialogRef<EditProcessGroup, any> | undefined;
    private destroyRef = inject(DestroyRef);
    private lastReload: number = 0;

    constructor(
        private actions$: Actions,
        private store: Store<NiFiState>,
        private storage: Storage,
        private flowService: FlowService,
        private controllerServiceService: ControllerServiceService,
        private registryService: RegistryService,
        private client: Client,
        private canvasUtils: CanvasUtils,
        private canvasView: CanvasView,
        private birdseyeView: BirdseyeView,
        private connectionManager: ConnectionManager,
        private clusterConnectionService: ClusterConnectionService,
        private snippetService: SnippetService,
        private router: Router,
        private dialog: MatDialog,
        private propertyTableHelperService: PropertyTableHelperService,
        private parameterHelperService: ParameterHelperService,
        private parameterContextService: ParameterContextService,
        private extensionTypesService: ExtensionTypesService,
        private errorHelper: ErrorHelper,
        private copyPasteService: CopyPasteService
    ) {
        this.store
            .select(selectDocumentVisibilityState)
            .pipe(
                takeUntilDestroyed(this.destroyRef),
                filter((documentVisibility) => documentVisibility.documentVisibility === DocumentVisibility.Visible),
                filter(() => this.canvasView.isCanvasInitialized()),
                filter(
                    (documentVisibility) =>
                        documentVisibility.changedTimestamp - this.lastReload > 30 * NiFiCommon.MILLIS_PER_SECOND
                )
            )
            .subscribe(() => {
                this.store.dispatch(FlowActions.reloadFlow());
            });
    }

    reloadFlow$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.reloadFlow),
            throttleTime(1000),
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
            switchMap(([, processGroupId]) => {
                this.lastReload = Date.now();

                return of(
                    FlowActions.loadProcessGroup({
                        request: {
                            id: processGroupId,
                            transitionRequired: true
                        }
                    })
                );
            })
        )
    );

    loadProcessGroup$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.loadProcessGroup),
            map((action) => action.request),
            concatLatestFrom(() => [
                this.store.select(selectFlowLoadingStatus),
                this.store.select(selectConnectedStateChanged)
            ]),
            tap(() => this.store.dispatch(resetConnectedStateChanged())),
            switchMap(([request, status, connectedStateChanged]) =>
                combineLatest([
                    this.flowService.getFlow(request.id),
                    this.flowService.getFlowStatus(),
                    this.flowService.getControllerBulletins(),
                    this.registryService.getRegistryClients()
                ]).pipe(
                    map(([flow, flowStatus, controllerBulletins, registryClientsResponse]) => {
                        this.store.dispatch(resetPollingFlowAnalysis());
                        return FlowActions.loadProcessGroupSuccess({
                            response: {
                                id: request.id,
                                flow: flow,
                                flowStatus: flowStatus,
                                controllerBulletins: controllerBulletins,
                                connectedStateChanged,
                                registryClients: registryClientsResponse.registries
                            }
                        });
                    }),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(this.errorHelper.handleLoadingError(status, errorResponse))
                    )
                )
            )
        )
    );

    loadProcessGroupSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.loadProcessGroupSuccess),
            map((action) => action.response),
            switchMap((response: LoadProcessGroupResponse) => {
                return of(
                    FlowActions.loadProcessGroupComplete({
                        response: response
                    })
                );
            })
        )
    );

    loadProcessGroupComplete$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.loadProcessGroupComplete),
            switchMap(() => {
                this.canvasView.updateCanvasVisibility();
                this.birdseyeView.refresh();

                return of(FlowActions.setTransitionRequired({ transitionRequired: false }));
            })
        )
    );

    startProcessGroupPolling$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.startProcessGroupPolling),
            switchMap(() =>
                interval(30000, asyncScheduler).pipe(
                    takeUntil(this.actions$.pipe(ofType(FlowActions.stopProcessGroupPolling)))
                )
            ),
            concatLatestFrom(() => this.store.select(selectDocumentVisibilityState)),
            filter(([, documentVisibility]) => documentVisibility.documentVisibility === DocumentVisibility.Visible),
            switchMap(() => of(FlowActions.reloadFlow()))
        )
    );

    createComponentRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.createComponentRequest),
            map((action) => action.request),
            switchMap((request) => {
                switch (request.type) {
                    case ComponentType.Processor:
                        return of(FlowActions.openNewProcessorDialog({ request }));
                    case ComponentType.ProcessGroup:
                        return from(this.parameterContextService.getParameterContexts()).pipe(
                            concatLatestFrom(() => this.store.select(selectCurrentParameterContext)),
                            map(([response, parameterContext]) => {
                                const dialogRequest: CreateProcessGroupDialogRequest = {
                                    request,
                                    parameterContexts: response.parameterContexts
                                };

                                if (parameterContext) {
                                    dialogRequest.currentParameterContextId = parameterContext.id;
                                }

                                return FlowActions.openNewProcessGroupDialog({ request: dialogRequest });
                            }),
                            catchError((errorResponse: HttpErrorResponse) =>
                                of(this.snackBarOrFullScreenError(errorResponse))
                            )
                        );
                    case ComponentType.RemoteProcessGroup:
                        return of(FlowActions.openNewRemoteProcessGroupDialog({ request }));
                    case ComponentType.Funnel:
                        return of(FlowActions.createFunnel({ request }));
                    case ComponentType.Label:
                        return of(FlowActions.createLabel({ request }));
                    case ComponentType.InputPort:
                    case ComponentType.OutputPort:
                        return of(FlowActions.openNewPortDialog({ request }));
                    case ComponentType.Flow:
                        return from(this.registryService.getRegistryClients()).pipe(
                            map((response) => {
                                const dialogRequest: ImportFromRegistryDialogRequest = {
                                    request,
                                    registryClients: response.registries
                                };
                                this.store.dispatch(setRegistryClients({ request: response.registries }));
                                return FlowActions.openImportFromRegistryDialog({ request: dialogRequest });
                            }),
                            catchError((errorResponse: HttpErrorResponse) =>
                                of(this.snackBarOrFullScreenError(errorResponse))
                            )
                        );
                    default:
                        return of(FlowActions.flowSnackbarError({ error: 'Unsupported type of Component.' }));
                }
            })
        )
    );

    openNewProcessorDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.openNewProcessorDialog),
                map((action) => action.request),
                concatLatestFrom(() => this.store.select(selectProcessorTypes)),
                tap(([request, processorTypes]) => {
                    this.dialog
                        .open(CreateProcessor, {
                            ...LARGE_DIALOG,
                            data: {
                                request,
                                processorTypes
                            }
                        })
                        .afterClosed()
                        .subscribe(() => {
                            this.store.dispatch(FlowActions.setDragging({ dragging: false }));
                        });
                })
            ),
        { dispatch: false }
    );

    createProcessor$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.createProcessor),
            map((action) => action.request),
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
            switchMap(([request, processGroupId]) =>
                from(this.flowService.createProcessor(processGroupId, request)).pipe(
                    map((response) =>
                        FlowActions.createComponentSuccess({
                            response: {
                                type: request.type,
                                payload: response
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) => of(this.snackBarOrFullScreenError(errorResponse)))
                )
            )
        )
    );

    openNewRemoteProcessGroupDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.openNewRemoteProcessGroupDialog),
                map((action) => action.request),
                tap((request) => {
                    this.dialog
                        .open(CreateRemoteProcessGroup, {
                            ...LARGE_DIALOG,
                            data: request
                        })
                        .afterClosed()
                        .subscribe(() => {
                            this.store.dispatch(FlowActions.setDragging({ dragging: false }));
                        });
                })
            ),
        { dispatch: false }
    );

    createRemoteProcessGroup$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.createRemoteProcessGroup),
            map((action) => action.request),
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
            switchMap(([request, processGroupId]) =>
                from(this.flowService.createRemoteProcessGroup(processGroupId, request)).pipe(
                    map((response) =>
                        FlowActions.createComponentSuccess({
                            response: {
                                type: request.type,
                                payload: response
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(this.bannerOrFullScreenError(errorResponse, ErrorContextKey.REMOTE_PROCESS_GROUP))
                    )
                )
            )
        )
    );

    goToRemoteProcessGroup$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.goToRemoteProcessGroup),
                map((action) => action.request),
                tap((request) => {
                    if (request.uri) {
                        this.flowService.goToRemoteProcessGroup(request);
                    } else {
                        this.store.dispatch(
                            FlowActions.showOkDialog({
                                title: 'Remote Process Group',
                                message: 'No target URI defined.'
                            })
                        );
                    }
                })
            ),
        { dispatch: false }
    );

    startRemoteProcessGroupPolling$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.startRemoteProcessGroupPolling),
            switchMap(() => {
                return interval(3000, asyncScheduler).pipe(
                    takeUntil(this.actions$.pipe(ofType(FlowActions.stopRemoteProcessGroupPolling)))
                );
            }),
            switchMap(() => {
                return of(FlowActions.refreshRemoteProcessGroup());
            })
        )
    );

    requestRefreshRemoteProcessGroup$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.requestRefreshRemoteProcessGroup),
            switchMap(() => {
                return of(FlowActions.refreshRemoteProcessGroup());
            })
        )
    );

    refreshRemoteProcessGroup$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.refreshRemoteProcessGroup),
            concatLatestFrom(() => this.store.select(selectRefreshRpgDetails).pipe(isDefinedAndNotNull())),
            switchMap(([, refreshRpgDetails]) =>
                from(
                    this.flowService.getRemoteProcessGroup(refreshRpgDetails.request.id).pipe(
                        map((response: any) => {
                            const entity = response;

                            if (refreshRpgDetails.request.refreshTimestamp !== entity.component.flowRefreshed) {
                                this.store.dispatch(FlowActions.stopRemoteProcessGroupPolling());

                                // reload the group's connections
                                this.store.dispatch(FlowActions.loadConnectionsForComponent({ id: entity.id }));
                            } else {
                                if (!refreshRpgDetails.polling) {
                                    this.store.dispatch(FlowActions.startRemoteProcessGroupPolling());
                                }

                                entity.component.flowRefreshed = 'Refreshing...';
                            }

                            return FlowActions.loadRemoteProcessGroupSuccess({
                                response: {
                                    id: entity.id,
                                    remoteProcessGroup: entity
                                }
                            });
                        }),
                        catchError((errorResponse: HttpErrorResponse) => {
                            this.store.dispatch(FlowActions.stopRemoteProcessGroupPolling());

                            return of(this.snackBarOrFullScreenError(errorResponse));
                        })
                    )
                )
            )
        )
    );

    openNewProcessGroupDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.openNewProcessGroupDialog),
                map((action) => action.request),
                tap((request) => {
                    this.createProcessGroupDialogRef = this.dialog.open(CreateProcessGroup, {
                        ...MEDIUM_DIALOG,
                        data: request
                    });

                    this.createProcessGroupDialogRef.componentInstance.parameterContexts = request.parameterContexts;

                    this.createProcessGroupDialogRef.afterClosed().subscribe(() => {
                        if (this.createProcessGroupDialogRef !== undefined) {
                            this.createProcessGroupDialogRef = undefined;
                        }

                        this.store.dispatch(FlowActions.setDragging({ dragging: false }));
                    });
                })
            ),
        { dispatch: false }
    );

    createProcessGroup$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.createProcessGroup),
            map((action) => action.request),
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
            switchMap(([request, processGroupId]) =>
                from(this.flowService.createProcessGroup(processGroupId, request)).pipe(
                    map((response) =>
                        FlowActions.createComponentSuccess({
                            response: {
                                type: request.type,
                                payload: response
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(this.bannerOrFullScreenError(errorResponse, ErrorContextKey.PROCESS_GROUP))
                    )
                )
            )
        )
    );

    uploadProcessGroup$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.uploadProcessGroup),
            map((action) => action.request),
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
            switchMap(([request, processGroupId]) =>
                from(this.flowService.uploadProcessGroup(processGroupId, request)).pipe(
                    map((response) =>
                        FlowActions.createComponentSuccess({
                            response: {
                                type: request.type,
                                payload: response
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(this.bannerOrFullScreenError(errorResponse, ErrorContextKey.PROCESS_GROUP))
                    )
                )
            )
        )
    );

    getParameterContextsAndOpenGroupComponentsDialog$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.getParameterContextsAndOpenGroupComponentsDialog),
            map((action) => action.request),
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
            switchMap(([request]) =>
                from(this.parameterContextService.getParameterContexts()).pipe(
                    concatLatestFrom(() => this.store.select(selectCurrentParameterContext)),
                    map(([response, parameterContext]) => {
                        const dialogRequest: GroupComponentsDialogRequest = {
                            request,
                            parameterContexts: response.parameterContexts
                        };

                        if (parameterContext) {
                            dialogRequest.currentParameterContextId = parameterContext.id;
                        }

                        return FlowActions.openGroupComponentsDialog({
                            request: dialogRequest
                        });
                    }),
                    catchError((errorResponse) => of(this.snackBarOrFullScreenError(errorResponse)))
                )
            )
        )
    );

    openGroupComponentsDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.openGroupComponentsDialog),
                map((action) => action.request),
                tap((request) => {
                    this.dialog
                        .open(GroupComponents, {
                            ...MEDIUM_DIALOG,
                            data: request
                        })
                        .afterClosed()
                        .subscribe(() => {
                            this.store.dispatch(FlowActions.setDragging({ dragging: false }));
                        });
                })
            ),
        { dispatch: false }
    );

    groupComponents$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.groupComponents),
            map((action) => action.request),
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
            switchMap(([request, processGroupId]) =>
                from(this.flowService.createProcessGroup(processGroupId, request)).pipe(
                    map((response) =>
                        FlowActions.groupComponentsSuccess({
                            response: {
                                type: request.type,
                                payload: response,
                                components: request.components
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(this.bannerOrFullScreenError(errorResponse, ErrorContextKey.PROCESS_GROUP))
                    )
                )
            )
        )
    );

    groupComponentsSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.groupComponentsSuccess),
            map((action) => action.response),
            tap(() => this.dialog.closeAll()),
            switchMap((response) => [
                FlowActions.createComponentComplete({
                    response: {
                        type: response.type,
                        payload: response.payload
                    }
                }),
                FlowActions.moveComponents({
                    request: {
                        groupId: response.payload.id,
                        components: response.components
                    }
                })
            ])
        )
    );

    openNewConnectionDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.openNewConnectionDialog),
                map((action) => action.request),
                concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
                switchMap(([request, currentProcessGroupId]) =>
                    from(this.flowService.getProcessGroup(currentProcessGroupId)).pipe(
                        map((response) => {
                            return {
                                request,
                                defaults: {
                                    flowfileExpiration: response.component.defaultFlowFileExpiration,
                                    objectThreshold: response.component.defaultBackPressureObjectThreshold,
                                    dataSizeThreshold: response.component.defaultBackPressureDataSizeThreshold
                                }
                            } as CreateConnectionDialogRequest;
                        }),
                        tap({
                            error: (errorResponse: HttpErrorResponse) => {
                                this.canvasUtils.removeTempEdge();
                                this.store.dispatch(this.snackBarOrFullScreenError(errorResponse));
                            }
                        })
                    )
                ),
                tap((request) => {
                    const dialogReference = this.dialog.open(CreateConnection, {
                        ...LARGE_DIALOG,
                        data: request
                    });

                    dialogReference.componentInstance.getChildOutputPorts = (groupId: string): Observable<any> => {
                        return this.flowService.getFlow(groupId).pipe(
                            take(1),
                            map((response) => response.processGroupFlow.flow.outputPorts)
                        );
                    };

                    dialogReference.componentInstance.getChildInputPorts = (groupId: string): Observable<any> => {
                        return this.flowService.getFlow(groupId).pipe(
                            take(1),
                            map((response) => response.processGroupFlow.flow.inputPorts)
                        );
                    };

                    dialogReference.afterClosed().subscribe(() => {
                        this.canvasUtils.removeTempEdge();
                    });
                })
            ),
        { dispatch: false }
    );

    createConnection$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.createConnection),
            map((action) => action.request),
            concatLatestFrom(() => [
                this.store.select(selectCurrentProcessGroupId),
                this.store.select(selectMaxZIndex(ComponentType.Connection))
            ]),
            switchMap(([{ payload }, processGroupId, maxZIndex]) =>
                from(
                    this.flowService.createConnection(processGroupId, {
                        payload: {
                            ...payload,
                            component: {
                                ...payload.component,
                                // pass zIndex as max + 1, so that new connections are always on top
                                zIndex: maxZIndex + 1
                            }
                        }
                    })
                ).pipe(
                    map((response) =>
                        FlowActions.createComponentSuccess({
                            response: {
                                type: ComponentType.Connection,
                                payload: response
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(this.bannerOrFullScreenError(errorResponse, ErrorContextKey.CONNECTION))
                    )
                )
            )
        )
    );

    openNewPortDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.openNewPortDialog),
                map((action) => action.request),
                tap((request) => {
                    this.dialog
                        .open(CreatePort, {
                            ...SMALL_DIALOG,
                            data: request
                        })
                        .afterClosed()
                        .subscribe(() => {
                            this.store.dispatch(FlowActions.setDragging({ dragging: false }));
                        });
                })
            ),
        { dispatch: false }
    );

    createPort$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.createPort),
            map((action) => action.request),
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
            switchMap(([request, processGroupId]) =>
                from(this.flowService.createPort(processGroupId, request)).pipe(
                    map((response) =>
                        FlowActions.createComponentSuccess({
                            response: {
                                type: request.type,
                                payload: response
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(this.bannerOrFullScreenError(errorResponse, ErrorContextKey.PORT))
                    )
                )
            )
        )
    );

    createFunnel$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.createFunnel),
            map((action) => action.request),
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
            switchMap(([request, processGroupId]) =>
                from(this.flowService.createFunnel(processGroupId, request)).pipe(
                    map((response) =>
                        FlowActions.createComponentSuccess({
                            response: {
                                type: request.type,
                                payload: response
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) => of(this.snackBarOrFullScreenError(errorResponse)))
                )
            )
        )
    );

    createLabel$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.createLabel),
            map((action) => action.request),
            concatLatestFrom(() => [
                this.store.select(selectCurrentProcessGroupId),
                // Get the current max index so that we can pass it to the backend.
                this.store.select(selectMaxZIndex(ComponentType.Label))
            ]),
            switchMap(([request, processGroupId, maxZIndex]) =>
                from(this.flowService.createLabel(processGroupId, { ...request, zIndex: maxZIndex + 1 })).pipe(
                    map((response) =>
                        FlowActions.createComponentSuccess({
                            response: {
                                type: request.type,
                                payload: response
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) => of(this.snackBarOrFullScreenError(errorResponse)))
                )
            )
        )
    );

    openImportFromRegistryDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.openImportFromRegistryDialog),
                map((action) => action.request),
                concatLatestFrom(() => this.store.select(selectCurrentUser)),
                tap(([request, currentUser]) => {
                    const someRegistries = request.registryClients.some(
                        (registryClient: RegistryClientEntity) => registryClient.permissions.canRead
                    );

                    if (someRegistries) {
                        const dialogReference = this.dialog.open(ImportFromRegistry, {
                            ...LARGE_DIALOG,
                            data: request
                        });

                        dialogReference.componentInstance.getBranches = (
                            registryId: string
                        ): Observable<BranchEntity[]> => {
                            return this.registryService.getBranches(registryId).pipe(
                                take(1),
                                map((response) => response.branches),
                                tap({
                                    error: (errorResponse: HttpErrorResponse) => {
                                        this.store.dispatch(
                                            FlowActions.flowBannerError({
                                                errorContext: {
                                                    context: ErrorContextKey.REGISTRY_IMPORT,
                                                    errors: [this.errorHelper.getErrorString(errorResponse)]
                                                }
                                            })
                                        );
                                    }
                                })
                            );
                        };

                        dialogReference.componentInstance.getBuckets = (
                            registryId: string,
                            branch?: string | null
                        ): Observable<BucketEntity[]> => {
                            return this.registryService.getBuckets(registryId, branch).pipe(
                                take(1),
                                map((response) => response.buckets),
                                tap({
                                    error: (errorResponse: HttpErrorResponse) => {
                                        this.store.dispatch(
                                            FlowActions.flowBannerError({
                                                errorContext: {
                                                    context: ErrorContextKey.REGISTRY_IMPORT,
                                                    errors: [this.errorHelper.getErrorString(errorResponse)]
                                                }
                                            })
                                        );
                                    }
                                })
                            );
                        };

                        dialogReference.componentInstance.getFlows = (
                            registryId: string,
                            bucketId: string,
                            branch?: string | null
                        ): Observable<VersionedFlowEntity[]> => {
                            return this.registryService.getFlows(registryId, bucketId, branch).pipe(
                                take(1),
                                map((response) => response.versionedFlows),
                                tap({
                                    error: (errorResponse: HttpErrorResponse) => {
                                        this.store.dispatch(
                                            FlowActions.flowBannerError({
                                                errorContext: {
                                                    context: ErrorContextKey.REGISTRY_IMPORT,
                                                    errors: [this.errorHelper.getErrorString(errorResponse)]
                                                }
                                            })
                                        );
                                    }
                                })
                            );
                        };

                        dialogReference.componentInstance.getFlowVersions = (
                            registryId: string,
                            bucketId: string,
                            flowId: string,
                            branch?: string | null
                        ): Observable<VersionedFlowSnapshotMetadataEntity[]> => {
                            return this.registryService.getFlowVersions(registryId, bucketId, flowId, branch).pipe(
                                take(1),
                                map((response) => response.versionedFlowSnapshotMetadataSet),
                                tap({
                                    error: (errorResponse: HttpErrorResponse) => {
                                        this.store.dispatch(
                                            FlowActions.flowBannerError({
                                                errorContext: {
                                                    context: ErrorContextKey.REGISTRY_IMPORT,
                                                    errors: [this.errorHelper.getErrorString(errorResponse)]
                                                }
                                            })
                                        );
                                    }
                                })
                            );
                        };

                        dialogReference.afterClosed().subscribe(() => {
                            this.store.dispatch(FlowActions.setDragging({ dragging: false }));
                        });
                    } else {
                        this.dialog
                            .open(NoRegistryClientsDialog, {
                                ...MEDIUM_DIALOG,
                                data: {
                                    controllerPermissions: currentUser.controllerPermissions
                                }
                            })
                            .afterClosed()
                            .subscribe(() => {
                                this.store.dispatch(FlowActions.setDragging({ dragging: false }));
                            });
                    }
                })
            ),
        { dispatch: false }
    );

    importFromRegistry$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.importFromRegistry),
            map((action) => action.request),
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
            switchMap(([request, processGroupId]) =>
                from(this.registryService.importFromRegistry(processGroupId, request)).pipe(
                    map((response) =>
                        FlowActions.createComponentSuccess({
                            response: {
                                type: ComponentType.ProcessGroup,
                                payload: response
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(this.bannerOrFullScreenError(errorResponse, ErrorContextKey.REGISTRY_IMPORT))
                    )
                )
            )
        )
    );

    createComponentSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.createComponentSuccess),
            map((action) => action.response),
            switchMap((response) => {
                this.dialog.closeAll();
                return of(FlowActions.createComponentComplete({ response }));
            })
        )
    );

    createComponentComplete$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.createComponentComplete),
            map((action) => action.response),
            switchMap((response) => {
                this.canvasView.updateCanvasVisibility();
                this.birdseyeView.refresh();

                const actions: any[] = [
                    FlowActions.selectComponents({
                        request: {
                            components: [
                                {
                                    id: response.payload.id,
                                    componentType: response.type
                                }
                            ]
                        }
                    })
                ];

                // if the component that was just created is a connection, reload the source and destination if necessary
                if (response.type == ComponentType.Connection) {
                    actions.push(FlowActions.loadComponentsForConnection({ connection: response.payload }));
                }

                return actions;
            })
        )
    );

    navigateToEditComponent$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.navigateToEditComponent),
                map((action) => action.request),
                concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
                tap(([request, processGroupId]) => {
                    const url = ['/process-groups', processGroupId, request.type, request.id, 'edit'];
                    if (this.canvasView.isSelectedComponentOnScreen()) {
                        this.store.dispatch(FlowActions.navigateWithoutTransform({ url }));
                    } else {
                        this.router.navigate(url);
                    }
                })
            ),
        { dispatch: false }
    );

    navigateToAdvancedProcessorUi$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.navigateToAdvancedProcessorUi),
                map((action) => action.id),
                concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
                tap(([id, processGroupId]) => {
                    const routeBoundary = ['/process-groups', processGroupId, ComponentType.Processor, id, 'advanced'];
                    this.router.navigate([...routeBoundary], {
                        state: {
                            backNavigation: {
                                route: ['/process-groups', processGroupId, ComponentType.Processor, id],
                                routeBoundary,
                                context: 'Processor'
                            } as BackNavigation
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    navigateToParameterContext$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.navigateToParameterContext),
                map((action) => action.request),
                tap((request) => {
                    this.router.navigate(['/parameter-contexts', request.id, 'edit'], {
                        state: {
                            backNavigation: request.backNavigation
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    navigateToEditCurrentProcessGroup$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.navigateToEditCurrentProcessGroup),
                concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
                tap(([, processGroupId]) => {
                    this.router.navigate(['/process-groups', processGroupId, 'edit']);
                })
            ),
        { dispatch: false }
    );

    navigateToManageComponentPolicies$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.navigateToManageComponentPolicies),
                map((action) => action.request),
                tap((request) => {
                    this.router.navigate(
                        [...request.backNavigation.routeBoundary, 'read', 'component', request.resource, request.id],
                        {
                            state: {
                                backNavigation: request.backNavigation
                            }
                        }
                    );
                })
            ),
        { dispatch: false }
    );

    navigateToQueueListing$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.navigateToQueueListing),
                map((action) => action.request),
                concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
                tap(([request, processGroupId]) => {
                    const routeBoundary: string[] = ['/queue', request.connectionId];
                    this.router.navigate([...routeBoundary], {
                        state: {
                            backNavigation: {
                                route: [
                                    '/process-groups',
                                    processGroupId,
                                    ComponentType.Connection,
                                    request.connectionId
                                ],
                                routeBoundary,
                                context: 'Connection'
                            } as BackNavigation
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    navigateToViewStatusHistoryForComponent$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.navigateToViewStatusHistoryForComponent),
                map((action) => action.request),
                concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
                tap(([request, currentProcessGroupId]) => {
                    const url = ['/process-groups', currentProcessGroupId, request.type, request.id, 'history'];
                    if (this.canvasView.isSelectedComponentOnScreen()) {
                        this.store.dispatch(FlowActions.navigateWithoutTransform({ url }));
                    } else {
                        this.router.navigate(url);
                    }
                })
            ),
        { dispatch: false }
    );

    navigateToViewStatusHistoryForCurrentProcessGroup$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.navigateToViewStatusHistoryForCurrentProcessGroup),
                concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
                tap(([, processGroupId]) => {
                    this.router.navigate(['/process-groups', processGroupId, 'history']);
                })
            ),
        { dispatch: false }
    );

    completeStatusHistoryForComponent$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(StatusHistoryActions.viewStatusHistoryComplete),
                map((action) => action.request),
                filter((request) => request.source === 'canvas'),
                concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
                tap(([request, currentProcessGroupId]) => {
                    if (request.componentId === currentProcessGroupId) {
                        this.router.navigate(['/process-groups', currentProcessGroupId]);
                    } else {
                        this.store.dispatch(
                            FlowActions.selectComponents({
                                request: {
                                    components: [
                                        {
                                            id: request.componentId,
                                            componentType: request.componentType
                                        }
                                    ]
                                }
                            })
                        );
                    }
                })
            ),
        { dispatch: false }
    );

    navigateToControllerServicesForProcessGroup$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.navigateToControllerServicesForProcessGroup),
                map((action) => action.request),
                concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
                tap(([request, processGroupId]) => {
                    let route: string[];
                    if (processGroupId == request.id) {
                        route = ['/process-groups', processGroupId];
                    } else {
                        route = ['/process-groups', processGroupId, ComponentType.ProcessGroup, request.id];
                    }

                    const routeBoundary: string[] = ['/process-groups', request.id, 'controller-services'];
                    this.router.navigate([...routeBoundary], {
                        state: {
                            backNavigation: {
                                route,
                                routeBoundary,
                                context: 'Process Group'
                            } as BackNavigation
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    editComponent$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.editComponent),
            map((action) => action.request),
            switchMap((request) => {
                switch (request.type) {
                    case ComponentType.Processor:
                        return of(FlowActions.openEditProcessorDialog({ request }));
                    case ComponentType.Connection:
                        return of(FlowActions.openEditConnectionDialog({ request }));
                    case ComponentType.ProcessGroup:
                        return from(this.parameterContextService.getParameterContexts()).pipe(
                            map((response) => {
                                const editComponentDialogRequest = {
                                    ...request,
                                    parameterContexts: response.parameterContexts
                                };

                                return FlowActions.openEditProcessGroupDialog({
                                    request: editComponentDialogRequest
                                });
                            }),
                            catchError((errorResponse: HttpErrorResponse) =>
                                of(this.snackBarOrFullScreenError(errorResponse))
                            )
                        );
                    case ComponentType.RemoteProcessGroup:
                        return of(FlowActions.openEditRemoteProcessGroupDialog({ request }));
                    case ComponentType.InputPort:
                    case ComponentType.OutputPort:
                        return of(FlowActions.openEditPortDialog({ request }));
                    case ComponentType.Label:
                        return of(FlowActions.openEditLabelDialog({ request }));
                    default:
                        return of(FlowActions.flowSnackbarError({ error: 'Unsupported type of Component.' }));
                }
            })
        )
    );

    editCurrentProcessGroup$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.editCurrentProcessGroup),
            map((action) => action.request),
            switchMap((request) => {
                return from(this.parameterContextService.getParameterContexts()).pipe(
                    switchMap((parameterContextResponse) =>
                        from(this.flowService.getProcessGroup(request.id)).pipe(
                            map((response) =>
                                FlowActions.openEditProcessGroupDialog({
                                    request: {
                                        type: ComponentType.ProcessGroup,
                                        uri: response.uri,
                                        entity: response,
                                        parameterContexts: parameterContextResponse.parameterContexts
                                    }
                                })
                            ),
                            catchError((errorResponse: HttpErrorResponse) =>
                                of(this.snackBarOrFullScreenError(errorResponse))
                            )
                        )
                    ),
                    catchError((errorResponse: HttpErrorResponse) => of(this.snackBarOrFullScreenError(errorResponse)))
                );
            })
        )
    );

    openEditPortDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.openEditPortDialog),
                map((action) => action.request),
                tap((request) => {
                    this.dialog
                        .open(EditPort, {
                            ...MEDIUM_DIALOG,
                            data: request
                        })
                        .afterClosed()
                        .subscribe(() => {
                            this.store.dispatch(
                                FlowActions.selectComponents({
                                    request: {
                                        components: [
                                            {
                                                id: request.entity.id,
                                                componentType: request.type
                                            }
                                        ]
                                    }
                                })
                            );
                        });
                })
            ),
        { dispatch: false }
    );

    openEditLabelDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.openEditLabelDialog),
                map((action) => action.request),
                tap((request) => {
                    this.dialog
                        .open(EditLabel, {
                            ...MEDIUM_DIALOG,
                            data: request
                        })
                        .afterClosed()
                        .subscribe(() => {
                            this.store.dispatch(
                                FlowActions.selectComponents({
                                    request: {
                                        components: [
                                            {
                                                id: request.entity.id,
                                                componentType: request.type
                                            }
                                        ]
                                    }
                                })
                            );
                        });
                })
            ),
        { dispatch: false }
    );

    openEditProcessorDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.openEditProcessorDialog),
                map((action) => action.request),
                switchMap((request) =>
                    combineLatest([
                        this.flowService.getProcessor(request.entity.id),
                        this.propertyTableHelperService.getComponentHistory(request.entity.id)
                    ]).pipe(
                        map(([entity, history]) => {
                            return {
                                ...request,
                                entity,
                                history: history.componentHistory
                            };
                        }),
                        tap({
                            error: (errorResponse: HttpErrorResponse) => {
                                this.store.dispatch(
                                    FlowActions.selectComponents({
                                        request: {
                                            components: [
                                                {
                                                    id: request.entity.id,
                                                    componentType: request.type
                                                }
                                            ]
                                        }
                                    })
                                );
                                this.store.dispatch(this.snackBarOrFullScreenError(errorResponse));
                            }
                        })
                    )
                ),
                concatLatestFrom(() => [
                    this.store.select(selectCurrentParameterContext),
                    this.store.select(selectCurrentProcessGroupId)
                ]),
                switchMap(([request, parameterContextReference, processGroupId]) => {
                    if (parameterContextReference && parameterContextReference.permissions.canRead) {
                        return from(
                            this.parameterContextService.getParameterContext(parameterContextReference.id)
                        ).pipe(
                            map((parameterContext) => {
                                return [request, parameterContext, processGroupId];
                            }),
                            tap({
                                error: (errorResponse: HttpErrorResponse) => {
                                    this.store.dispatch(
                                        FlowActions.selectComponents({
                                            request: {
                                                components: [
                                                    {
                                                        id: request.entity.id,
                                                        componentType: request.type
                                                    }
                                                ]
                                            }
                                        })
                                    );
                                    this.store.dispatch(this.snackBarOrFullScreenError(errorResponse));
                                }
                            })
                        );
                    }

                    return of([request, null, processGroupId]);
                }),
                tap(([request, parameterContext, processGroupId]) => {
                    const processorId: string = request.entity.id;
                    let runStatusChanged: boolean = false;

                    const editDialogReference = this.dialog.open(EditProcessor, {
                        ...XL_DIALOG,
                        data: request,
                        id: processorId
                    });

                    editDialogReference.componentInstance.saving$ = this.store.select(selectSaving);

                    editDialogReference.componentInstance.createNewProperty =
                        this.propertyTableHelperService.createNewProperty(processorId, this.flowService);

                    editDialogReference.componentInstance.verify
                        .pipe(takeUntil(editDialogReference.afterClosed()))
                        .subscribe((verificationRequest: VerifyPropertiesRequestContext) => {
                            this.store.dispatch(
                                verifyProperties({
                                    request: verificationRequest
                                })
                            );
                        });

                    editDialogReference.componentInstance.propertyVerificationResults$ = this.store.select(
                        selectPropertyVerificationResults
                    );
                    editDialogReference.componentInstance.propertyVerificationStatus$ = this.store.select(
                        selectPropertyVerificationStatus
                    );

                    const goTo = (commands: string[], commandBoundary: string[], destination: string): void => {
                        if (editDialogReference.componentInstance.editProcessorForm.dirty) {
                            const saveChangesDialogReference = this.dialog.open(YesNoDialog, {
                                ...SMALL_DIALOG,
                                data: {
                                    title: 'Processor Configuration',
                                    message: `Save changes before going to this ${destination}?`
                                }
                            });

                            saveChangesDialogReference.componentInstance.yes.pipe(take(1)).subscribe(() => {
                                editDialogReference.componentInstance.submitForm(commands, commandBoundary);
                            });

                            saveChangesDialogReference.componentInstance.no.pipe(take(1)).subscribe(() => {
                                this.router.navigate(commands, {
                                    state: {
                                        backNavigation: {
                                            route: [
                                                '/process-groups',
                                                processGroupId,
                                                ComponentType.Processor,
                                                processorId,
                                                'edit'
                                            ],
                                            routeBoundary: commandBoundary,
                                            context: 'Processor'
                                        } as BackNavigation
                                    }
                                });
                            });
                        } else {
                            this.router.navigate(commands, {
                                state: {
                                    backNavigation: {
                                        route: [
                                            '/process-groups',
                                            processGroupId,
                                            ComponentType.Processor,
                                            processorId,
                                            'edit'
                                        ],
                                        routeBoundary: commandBoundary,
                                        context: 'Processor'
                                    } as BackNavigation
                                }
                            });
                        }
                    };

                    if (parameterContext != null) {
                        editDialogReference.componentInstance.parameterContext = parameterContext;
                        editDialogReference.componentInstance.goToParameter = () => {
                            this.storage.setItem<number>(NiFiCommon.EDIT_PARAMETER_CONTEXT_DIALOG_ID, 1);

                            const commandBoundary: string[] = ['/parameter-contexts'];
                            const commands: string[] = [...commandBoundary, parameterContext.id, 'edit'];
                            goTo(commands, commandBoundary, 'Parameter');
                        };

                        editDialogReference.componentInstance.convertToParameter =
                            this.parameterHelperService.convertToParameter(parameterContext.id);
                    }

                    editDialogReference.componentInstance.goToService = (serviceId: string) => {
                        this.controllerServiceService.getControllerService(serviceId).subscribe({
                            next: (serviceEntity) => {
                                const commandBoundary: string[] = [
                                    '/process-groups',
                                    serviceEntity.component.parentGroupId,
                                    'controller-services'
                                ];
                                const commands: string[] = [...commandBoundary, serviceEntity.id];
                                goTo(commands, commandBoundary, 'Controller Service');
                            },
                            error: (errorResponse: HttpErrorResponse) => {
                                this.store.dispatch(this.snackBarOrFullScreenError(errorResponse));
                            }
                        });
                    };

                    editDialogReference.componentInstance.createNewService =
                        this.propertyTableHelperService.createNewService(
                            processorId,
                            this.controllerServiceService,
                            this.flowService,
                            processGroupId
                        );

                    editDialogReference.componentInstance.editProcessor
                        .pipe(takeUntil(editDialogReference.afterClosed()))
                        .subscribe((updateProcessorRequest: UpdateProcessorRequest) => {
                            this.store.dispatch(
                                FlowActions.updateProcessor({
                                    request: updateProcessorRequest
                                })
                            );
                        });
                    const startPollingIfNecessary = (processorEntity: any): boolean => {
                        if (
                            (processorEntity.status.aggregateSnapshot.runStatus === 'Stopped' &&
                                processorEntity.status.aggregateSnapshot.activeThreadCount > 0) ||
                            processorEntity.status.aggregateSnapshot.runStatus === 'Validating'
                        ) {
                            this.store.dispatch(
                                startPollingProcessorUntilStopped({
                                    request: {
                                        id: processorEntity.id
                                    }
                                })
                            );
                            return true;
                        }

                        return false;
                    };

                    const pollingStarted = startPollingIfNecessary(request.entity);

                    this.store
                        .select(selectProcessor(processorId))
                        .pipe(
                            takeUntil(editDialogReference.afterClosed()),
                            isDefinedAndNotNull(),
                            filter((processorEntity) => {
                                return (
                                    (runStatusChanged || pollingStarted) &&
                                    processorEntity.revision.clientId === this.client.getClientId()
                                );
                            }),
                            concatLatestFrom(() => this.store.select(selectPollingProcessor))
                        )
                        .subscribe(([processorEntity, pollingProcessor]) => {
                            editDialogReference.componentInstance.processorUpdates = processorEntity;

                            // if we're already polling we do not want to start polling again
                            if (!pollingProcessor) {
                                startPollingIfNecessary(processorEntity);
                            }
                        });

                    editDialogReference.componentInstance.stopComponentRequest
                        .pipe(takeUntil(editDialogReference.afterClosed()))
                        .subscribe((stopComponentRequest: StopComponentRequest) => {
                            runStatusChanged = true;
                            this.store.dispatch(
                                stopComponent({
                                    request: {
                                        id: stopComponentRequest.id,
                                        uri: stopComponentRequest.uri,
                                        type: ComponentType.Processor,
                                        revision: stopComponentRequest.revision,
                                        errorStrategy: 'snackbar'
                                    }
                                })
                            );
                        });

                    editDialogReference.componentInstance.disableComponentRequest
                        .pipe(takeUntil(editDialogReference.afterClosed()))
                        .subscribe((disableComponentsRequest: DisableComponentRequest) => {
                            runStatusChanged = true;
                            this.store.dispatch(
                                disableComponent({
                                    request: {
                                        id: disableComponentsRequest.id,
                                        uri: disableComponentsRequest.uri,
                                        type: ComponentType.Processor,
                                        revision: disableComponentsRequest.revision,
                                        errorStrategy: 'snackbar'
                                    }
                                })
                            );
                        });

                    editDialogReference.componentInstance.enableComponentRequest
                        .pipe(takeUntil(editDialogReference.afterClosed()))
                        .subscribe((enableComponentsRequest: EnableComponentRequest) => {
                            runStatusChanged = true;
                            this.store.dispatch(
                                enableComponent({
                                    request: {
                                        id: enableComponentsRequest.id,
                                        uri: enableComponentsRequest.uri,
                                        type: ComponentType.Processor,
                                        revision: enableComponentsRequest.revision,
                                        errorStrategy: 'snackbar'
                                    }
                                })
                            );
                        });

                    editDialogReference.componentInstance.startComponentRequest
                        .pipe(takeUntil(editDialogReference.afterClosed()))
                        .subscribe((startComponentRequest: StartComponentRequest) => {
                            runStatusChanged = true;
                            this.store.dispatch(
                                startComponent({
                                    request: {
                                        id: startComponentRequest.id,
                                        uri: startComponentRequest.uri,
                                        type: ComponentType.Processor,
                                        revision: startComponentRequest.revision,
                                        errorStrategy: 'snackbar'
                                    }
                                })
                            );
                        });

                    editDialogReference.afterClosed().subscribe((response) => {
                        this.store.dispatch(resetPropertyVerificationState());
                        if (response != 'ROUTED') {
                            this.store.dispatch(
                                FlowActions.selectComponents({
                                    request: {
                                        components: [
                                            {
                                                id: processorId,
                                                componentType: request.type
                                            }
                                        ]
                                    }
                                })
                            );
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    startPollingProcessorUntilStopped = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.startPollingProcessorUntilStopped),
            switchMap(() =>
                interval(2000, asyncScheduler).pipe(
                    takeUntil(this.actions$.pipe(ofType(FlowActions.stopPollingProcessor)))
                )
            ),
            switchMap(() => of(FlowActions.pollProcessorUntilStopped()))
        )
    );

    pollProcessorUntilStopped$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.pollProcessorUntilStopped),
            concatLatestFrom(() => [this.store.select(selectPollingProcessor).pipe(isDefinedAndNotNull())]),
            switchMap(([, pollingProcessor]) => {
                return from(
                    this.flowService.getProcessor(pollingProcessor.id).pipe(
                        map((response) =>
                            FlowActions.pollProcessorUntilStoppedSuccess({
                                response: {
                                    id: pollingProcessor.id,
                                    processor: response
                                }
                            })
                        ),
                        catchError((errorResponse: HttpErrorResponse) => {
                            this.store.dispatch(FlowActions.stopPollingProcessor());
                            return of(this.snackBarOrFullScreenError(errorResponse));
                        })
                    )
                );
            })
        )
    );

    pollProcessorUntilStoppedSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.pollProcessorUntilStoppedSuccess),
            map((action) => action.response),
            filter((response) => {
                return (
                    response.processor.status.runStatus === 'Stopped' &&
                    response.processor.status.aggregateSnapshot.activeThreadCount === 0
                );
            }),
            switchMap(() => of(FlowActions.stopPollingProcessor()))
        )
    );

    openEditConnectionDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.openEditConnectionDialog),
                map((action) => action.request),
                tap((request) => {
                    const editDialogReference = this.dialog.open(EditConnectionComponent, {
                        ...LARGE_DIALOG,
                        data: request
                    });

                    editDialogReference.componentInstance.saving$ = this.store.select(selectSaving);

                    editDialogReference.componentInstance.getChildOutputPorts = (groupId: string): Observable<any> => {
                        return this.flowService.getFlow(groupId).pipe(
                            take(1),
                            map((response) => response.processGroupFlow.flow.outputPorts)
                        );
                    };
                    editDialogReference.componentInstance.getChildInputPorts = (groupId: string): Observable<any> => {
                        return this.flowService.getFlow(groupId).pipe(
                            take(1),
                            map((response) => response.processGroupFlow.flow.inputPorts)
                        );
                    };

                    editDialogReference.componentInstance.selectProcessor = (id: string) => {
                        return this.store.select(selectProcessor(id));
                    };
                    editDialogReference.componentInstance.selectInputPort = (id: string) => {
                        return this.store.select(selectInputPort(id));
                    };
                    editDialogReference.componentInstance.selectOutputPort = (id: string) => {
                        return this.store.select(selectOutputPort(id));
                    };
                    editDialogReference.componentInstance.selectProcessGroup = (id: string) => {
                        return this.store.select(selectProcessGroup(id));
                    };
                    editDialogReference.componentInstance.selectRemoteProcessGroup = (id: string) => {
                        return this.store.select(selectRemoteProcessGroup(id));
                    };

                    editDialogReference.afterClosed().subscribe((response) => {
                        if (response == 'CANCELLED') {
                            this.connectionManager.renderConnection(request.entity.id, {
                                updatePath: true,
                                updateLabel: false
                            });
                        }

                        this.store.dispatch(
                            FlowActions.selectComponents({
                                request: {
                                    components: [
                                        {
                                            id: request.entity.id,
                                            componentType: request.type
                                        }
                                    ]
                                }
                            })
                        );
                    });
                })
            ),
        { dispatch: false }
    );

    openEditProcessGroupDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.openEditProcessGroupDialog),
                map((action) => action.request),
                concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
                tap(([request, currentProcessGroupId]) => {
                    this.editProcessGroupDialogRef = this.dialog.open(EditProcessGroup, {
                        ...LARGE_DIALOG,
                        data: request
                    });

                    this.editProcessGroupDialogRef.componentInstance.saving$ = this.store.select(selectSaving);
                    this.editProcessGroupDialogRef.componentInstance.parameterContexts =
                        request.parameterContexts || [];

                    this.editProcessGroupDialogRef.componentInstance.editProcessGroup
                        .pipe(takeUntil(this.editProcessGroupDialogRef.afterClosed()))
                        .subscribe((payload: any) => {
                            this.store.dispatch(
                                FlowActions.updateComponent({
                                    request: {
                                        id: request.entity.id,
                                        uri: request.uri,
                                        type: request.type,
                                        payload,
                                        errorStrategy: 'banner'
                                    }
                                })
                            );
                        });

                    this.editProcessGroupDialogRef.afterClosed().subscribe(() => {
                        if (this.editProcessGroupDialogRef !== undefined) {
                            this.editProcessGroupDialogRef = undefined;
                        }

                        if (request.entity.id === currentProcessGroupId) {
                            this.store.dispatch(
                                FlowActions.loadProcessGroup({
                                    request: {
                                        id: currentProcessGroupId,
                                        transitionRequired: true
                                    }
                                })
                            );

                            // restore the previous route
                            this.router.navigate(['/process-groups', currentProcessGroupId]);
                        } else {
                            this.store.dispatch(
                                FlowActions.selectComponents({
                                    request: {
                                        components: [
                                            {
                                                id: request.entity.id,
                                                componentType: request.type
                                            }
                                        ]
                                    }
                                })
                            );
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    createParameterContextSuccess$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ParameterActions.createParameterContextSuccess),
                tap((action) => {
                    if (this.editProcessGroupDialogRef !== undefined) {
                        // clone the current parameter contexts
                        const parameterContexts = [
                            ...(this.editProcessGroupDialogRef?.componentInstance.parameterContexts || [])
                        ];

                        // add newly created parameter context
                        parameterContexts?.push(action.response.parameterContext);

                        // remove all parameter contexts
                        this.editProcessGroupDialogRef.componentInstance.parameterContexts = [];

                        // set collection of parameter contexts to include the newly created parameter context
                        this.editProcessGroupDialogRef.componentInstance.parameterContexts = parameterContexts || [];

                        // auto select the newly created parameter context
                        this.editProcessGroupDialogRef.componentInstance.editProcessGroupForm
                            .get('parameterContext')
                            ?.setValue(action.response.parameterContext.id);

                        // mark the form as dirty
                        this.editProcessGroupDialogRef.componentInstance.editProcessGroupForm
                            .get('parameterContext')
                            ?.markAsDirty();
                    } else if (this.createProcessGroupDialogRef !== undefined) {
                        // clone the current parameter contexts
                        const parameterContexts = [
                            ...(this.createProcessGroupDialogRef?.componentInstance.parameterContexts || [])
                        ];

                        // add newly created parameter context
                        parameterContexts?.push(action.response.parameterContext);

                        // remove all parameter contexts
                        this.createProcessGroupDialogRef.componentInstance.parameterContexts = [];

                        // set collection of parameter contexts to include the newly created parameter context
                        this.createProcessGroupDialogRef.componentInstance.parameterContexts = parameterContexts || [];

                        // auto select the newly created parameter context
                        this.createProcessGroupDialogRef.componentInstance.createProcessGroupForm
                            .get('newProcessGroupParameterContext')
                            ?.setValue(action.response.parameterContext.id);

                        // mark the form as dirty
                        this.createProcessGroupDialogRef.componentInstance.createProcessGroupForm
                            .get('newProcessGroupParameterContext')
                            ?.markAsDirty();
                    }
                })
            ),
        { dispatch: false }
    );

    navigateToManageRemotePorts$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.navigateToManageRemotePorts),
                map((action) => action.request),
                tap((request) => {
                    this.router.navigate(['/remote-process-group', request.id, 'manage-remote-ports']);
                })
            ),
        { dispatch: false }
    );

    openEditRemoteProcessGroupDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.openEditRemoteProcessGroupDialog),
                map((action) => action.request),
                tap((request) => {
                    const editDialogReference = this.dialog.open(EditRemoteProcessGroup, {
                        ...LARGE_DIALOG,
                        data: request
                    });

                    editDialogReference.componentInstance.saving$ = this.store.select(selectSaving);

                    editDialogReference.componentInstance.editRemoteProcessGroup
                        .pipe(takeUntil(editDialogReference.afterClosed()))
                        .subscribe((payload: any) => {
                            this.store.dispatch(
                                FlowActions.updateComponent({
                                    request: {
                                        id: request.entity.id,
                                        uri: request.uri,
                                        type: request.type,
                                        payload,
                                        errorStrategy: 'banner'
                                    }
                                })
                            );
                        });

                    editDialogReference.afterClosed().subscribe((response) => {
                        if (response != 'ROUTED') {
                            this.store.dispatch(
                                FlowActions.selectComponents({
                                    request: {
                                        components: [
                                            {
                                                id: request.entity.id,
                                                componentType: request.type
                                            }
                                        ]
                                    }
                                })
                            );
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    updateComponent$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.updateComponent),
            map((action) => action.request),
            mergeMap((request) =>
                from(this.flowService.updateComponent(request)).pipe(
                    map((response) => {
                        const updateComponentResponse: UpdateComponentResponse = {
                            requestId: request.requestId,
                            id: request.id,
                            type: request.type,
                            response
                        };
                        return FlowActions.updateComponentSuccess({ response: updateComponentResponse });
                    }),
                    catchError((errorResponse: HttpErrorResponse) => {
                        const updateComponentFailure: UpdateComponentFailure = {
                            id: request.id,
                            type: request.type,
                            errorStrategy: request.errorStrategy,
                            restoreOnFailure: request.restoreOnFailure,
                            errorResponse
                        };
                        return of(FlowActions.updateComponentFailure({ response: updateComponentFailure }));
                    })
                )
            )
        )
    );

    updateComponentSuccess$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.updateComponentSuccess),
                map((action) => action.response),
                tap(() => {
                    this.dialog.closeAll();
                })
            ),
        { dispatch: false }
    );

    updateComponentFailure$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.updateComponentFailure),
            map((action) => action.response),
            switchMap((response) => {
                if (response.errorStrategy === 'banner') {
                    return of(
                        this.bannerOrFullScreenError(
                            response.errorResponse,
                            this.componentTypeToErrorContext(response.type)
                        )
                    );
                } else {
                    return of(this.snackBarOrFullScreenError(response.errorResponse));
                }
            })
        )
    );

    private componentTypeToErrorContext(type: ComponentType): ErrorContextKey {
        switch (type) {
            case ComponentType.Connection:
                return ErrorContextKey.CONNECTION;
            case ComponentType.Label:
                return ErrorContextKey.LABEL;
            case ComponentType.InputPort:
            case ComponentType.OutputPort:
                return ErrorContextKey.PORT;
            case ComponentType.Processor:
                return ErrorContextKey.PROCESSOR;
            case ComponentType.ProcessGroup:
                return ErrorContextKey.PROCESS_GROUP;
            case ComponentType.RemoteProcessGroup:
                return ErrorContextKey.REMOTE_PROCESS_GROUP;
            case ComponentType.Funnel:
                return ErrorContextKey.FUNNEL;
            case ComponentType.ControllerService:
                return ErrorContextKey.CONTROLLER_SERVICES;
            default:
                return ErrorContextKey.FLOW;
        }
    }

    updateProcessor$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.updateProcessor),
            map((action) => action.request),
            mergeMap((request) =>
                from(this.flowService.updateComponent(request)).pipe(
                    map((response) => {
                        const updateProcessorResponse: UpdateProcessorResponse = {
                            requestId: request.requestId,
                            id: request.id,
                            type: request.type,
                            postUpdateNavigation: request.postUpdateNavigation,
                            postUpdateNavigationBoundary: request.postUpdateNavigationBoundary,
                            response
                        };
                        return FlowActions.updateProcessorSuccess({ response: updateProcessorResponse });
                    }),
                    catchError((errorResponse: HttpErrorResponse) => {
                        const updateComponentFailure: UpdateComponentFailure = {
                            id: request.id,
                            type: request.type,
                            errorStrategy: request.errorStrategy,
                            restoreOnFailure: request.restoreOnFailure,
                            errorResponse
                        };
                        return of(FlowActions.updateComponentFailure({ response: updateComponentFailure }));
                    })
                )
            )
        )
    );

    updateProcessorSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.updateProcessorSuccess),
            map((action) => action.response),
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
            tap(([response, processGroupId]) => {
                if (response.postUpdateNavigation) {
                    if (response.postUpdateNavigationBoundary) {
                        this.router.navigate(response.postUpdateNavigation, {
                            state: {
                                backNavigation: {
                                    route: [
                                        '/process-groups',
                                        processGroupId,
                                        ComponentType.Processor,
                                        response.id,
                                        'edit'
                                    ],
                                    routeBoundary: response.postUpdateNavigationBoundary,
                                    context: 'Processor'
                                } as BackNavigation
                            }
                        });
                    } else {
                        this.router.navigate(response.postUpdateNavigation);
                    }
                } else {
                    this.dialog.closeAll();
                }
            }),
            filter(([response]) => response.postUpdateNavigation == null),
            switchMap(([response]) => of(FlowActions.loadConnectionsForComponent({ id: response.id })))
        )
    );

    loadConnectionsForComponent$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.loadConnectionsForComponent),
            map((action) => action.id),
            switchMap((id: string) => {
                const componentConnections: any[] = this.canvasUtils.getComponentConnections(id);
                return componentConnections.map((componentConnection) =>
                    FlowActions.loadConnection({ id: componentConnection.id })
                );
            })
        )
    );

    loadConnection$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.loadConnection),
            map((action) => action.id),
            mergeMap((id) =>
                from(this.flowService.getConnection(id)).pipe(
                    map((response) => {
                        return FlowActions.loadConnectionSuccess({
                            response: {
                                id: id,
                                connection: response
                            }
                        });
                    }),
                    catchError((errorResponse: HttpErrorResponse) => of(this.snackBarOrFullScreenError(errorResponse)))
                )
            )
        )
    );

    loadComponentsForConnection$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.loadComponentsForConnection),
            map((action) => action.connection),
            switchMap((connection: any) => {
                const actions: any[] = [];

                const processTerminal = function (type: ComponentType | null, terminal: any) {
                    switch (type) {
                        case ComponentType.Processor:
                            actions.push(FlowActions.loadProcessor({ id: terminal.id }));
                            break;
                        case ComponentType.InputPort:
                            actions.push(FlowActions.loadInputPort({ id: terminal.id }));
                            break;
                        case ComponentType.RemoteProcessGroup:
                            actions.push(FlowActions.loadRemoteProcessGroup({ id: terminal.groupId }));
                            break;
                    }
                };

                processTerminal(
                    this.canvasUtils.getComponentTypeForSource(connection.component.source.type),
                    connection.component.source
                );
                processTerminal(
                    this.canvasUtils.getComponentTypeForDestination(connection.component.destination.type),
                    connection.component.destination
                );

                return actions;
            })
        )
    );

    loadProcessor$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.loadProcessor),
            map((action) => action.id),
            mergeMap((id) =>
                from(this.flowService.getProcessor(id)).pipe(
                    map((response) => {
                        return FlowActions.loadProcessorSuccess({
                            response: {
                                id: id,
                                processor: response
                            }
                        });
                    }),
                    catchError((errorResponse: HttpErrorResponse) => of(this.snackBarOrFullScreenError(errorResponse)))
                )
            )
        )
    );

    loadInputPort$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.loadInputPort),
            map((action) => action.id),
            mergeMap((id) =>
                from(this.flowService.getInputPort(id)).pipe(
                    map((response) => {
                        return FlowActions.loadInputPortSuccess({
                            response: {
                                id: id,
                                inputPort: response
                            }
                        });
                    }),
                    catchError((errorResponse: HttpErrorResponse) => of(this.snackBarOrFullScreenError(errorResponse)))
                )
            )
        )
    );

    loadRemoteProcessGroup$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.loadRemoteProcessGroup),
            map((action) => action.id),
            mergeMap((id) =>
                from(this.flowService.getRemoteProcessGroup(id)).pipe(
                    map((response) => {
                        return FlowActions.loadRemoteProcessGroupSuccess({
                            response: {
                                id: id,
                                remoteProcessGroup: response
                            }
                        });
                    }),
                    catchError((errorResponse: HttpErrorResponse) => of(this.snackBarOrFullScreenError(errorResponse)))
                )
            )
        )
    );

    updateConnection$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.updateConnection),
            map((action) => action.request),
            mergeMap((request) => {
                return from(this.flowService.updateComponent(request)).pipe(
                    map((response) => {
                        const updateComponentResponse: UpdateConnectionSuccess = {
                            requestId: request.requestId,
                            id: request.id,
                            type: request.type,
                            response: response,
                            previousDestination: request.previousDestination
                        };
                        return FlowActions.updateConnectionSuccess({ response: updateComponentResponse });
                    }),
                    catchError((errorResponse: HttpErrorResponse) => {
                        this.connectionManager.renderConnection(request.id, {
                            updatePath: true,
                            updateLabel: false
                        });

                        const updateComponentFailure: UpdateComponentFailure = {
                            id: request.id,
                            type: request.type,
                            errorStrategy: request.errorStrategy,
                            restoreOnFailure: request.restoreOnFailure,
                            errorResponse
                        };
                        return of(FlowActions.updateComponentFailure({ response: updateComponentFailure }));
                    })
                );
            })
        )
    );

    updateConnectionSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.updateConnectionSuccess),
            tap(() => {
                this.dialog.closeAll();
            }),
            map((action) => action.response),
            switchMap((response) => {
                const actions: Action[] = [FlowActions.loadComponentsForConnection({ connection: response.response })];

                if (response.previousDestination) {
                    const type = this.canvasUtils.getComponentTypeForDestination(response.previousDestination.type);
                    switch (type) {
                        case ComponentType.Processor:
                            actions.push(FlowActions.loadProcessor({ id: response.previousDestination.id }));
                            break;
                        case ComponentType.InputPort:
                            actions.push(FlowActions.loadInputPort({ id: response.previousDestination.id }));
                            break;
                        case ComponentType.RemoteProcessGroup:
                            actions.push(
                                FlowActions.loadRemoteProcessGroup({ id: response.previousDestination.groupId })
                            );
                            break;
                    }
                }

                return actions;
            })
        )
    );

    updatePositions$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.updatePositions),
            map((action) => action.request),
            mergeMap((request) => [
                ...request.componentUpdates.map((componentUpdate) => {
                    return FlowActions.updateComponent({
                        request: {
                            ...componentUpdate,
                            requestId: request.requestId
                        }
                    });
                }),
                ...request.connectionUpdates.map((connectionUpdate) => {
                    return FlowActions.updateComponent({
                        request: {
                            ...connectionUpdate,
                            requestId: request.requestId
                        }
                    });
                })
            ])
        )
    );

    awaitUpdatePositions$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.updatePositions),
            map((action) => action.request),
            mergeMap((request) =>
                this.actions$.pipe(
                    ofType(FlowActions.updateComponentSuccess),
                    filter((updateSuccess) => ComponentType.Connection !== updateSuccess.response.type),
                    filter((updateSuccess) => request.requestId === updateSuccess.response.requestId),
                    map((response) => FlowActions.updatePositionComplete(response))
                )
            )
        )
    );

    updatePositionComplete$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.updatePositionComplete),
            tap(() => {
                this.birdseyeView.refresh();
            }),
            map((action) => action.response),
            switchMap((response) =>
                of(FlowActions.renderConnectionsForComponent({ id: response.id, updatePath: true, updateLabel: false }))
            )
        )
    );

    renderConnectionsForComponent$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.renderConnectionsForComponent),
                tap((action) => {
                    this.connectionManager.renderConnectionsForComponent(action.id, {
                        updatePath: action.updatePath,
                        updateLabel: action.updateLabel
                    });
                })
            ),
        { dispatch: false }
    );

    moveComponents$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.moveComponents),
            map((action) => action.request),
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
            mergeMap(([request, processGroupId]) => {
                const components: MoveComponentRequest[] = request.components;
                const snippet = this.snippetService.marshalSnippet(components, processGroupId);

                return from(this.snippetService.createSnippet(snippet)).pipe(
                    switchMap((response) => this.snippetService.moveSnippet(response.snippet.id, request.groupId)),
                    map(() => {
                        const deleteResponses: DeleteComponentResponse[] = [];

                        // prepare the delete responses with all requested components that are now deleted
                        components.forEach((request) => {
                            deleteResponses.push({
                                id: request.id,
                                type: request.type
                            });
                        });

                        return FlowActions.deleteComponentsSuccess({
                            response: deleteResponses
                        });
                    }),
                    catchError((errorResponse: HttpErrorResponse) => of(this.snackBarOrFullScreenError(errorResponse)))
                );
            })
        )
    );

    copy$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.copy),
            map((action) => action.request),
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
            switchMap(([request, processGroupId]) => {
                const copyRequest: CopyRequestContext = {
                    ...request,
                    processGroupId
                };
                return from(this.copyPasteService.copy(copyRequest)).pipe(
                    switchMap((response) => {
                        const copyBlob = new Blob([JSON.stringify(response, null, 2)], { type: 'text/plain' });
                        const clipboardItem: ClipboardItem = new ClipboardItem({
                            'text/plain': copyBlob
                        });
                        return from(navigator.clipboard.write([clipboardItem])).pipe(
                            switchMap(() => {
                                return of(
                                    FlowActions.copySuccess({
                                        response: {
                                            copyResponse: response,
                                            processGroupId,
                                            pasteCount: 0
                                        } as CopyResponseContext
                                    })
                                );
                            }),
                            catchError(() => {
                                return of(FlowActions.flowSnackbarError({ error: 'Copy failed' }));
                            })
                        );
                    }),
                    catchError((errorResponse: HttpErrorResponse) => of(this.snackBarOrFullScreenError(errorResponse)))
                );
            })
        )
    );

    copySuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.copySuccess),
            map((action) => action.response),
            switchMap((response) => {
                return of(
                    CopyActions.setCopiedContent({
                        content: response
                    })
                );
            })
        )
    );

    paste$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.paste),
            map((action) => action.request),
            concatLatestFrom(() => [
                this.store.select(selectCurrentProcessGroupId),
                this.store.select(selectCurrentProcessGroupRevision),
                this.store.select(selectCopiedContent)
            ]),
            switchMap(([request, processGroupId, revision, copiedContent]) => {
                let pasteRequest: PasteRequest | null = null;

                // Determine if the paste should be positioned based off of previously copied items or centered.
                //   * The current process group is the same as the content that was last copied
                //   * And, the last copied content is the same as the content being pasted
                //   * And, the original content is still in the canvas view
                if (copiedContent && processGroupId === copiedContent.processGroupId) {
                    if (copiedContent.copyResponse.id === request.id) {
                        const isInView = this.copyPasteService.isCopiedContentInView(copiedContent.copyResponse);
                        if (isInView) {
                            pasteRequest = this.copyPasteService.toOffsetPasteRequest(
                                request,
                                copiedContent.pasteCount
                            );
                        }
                    }
                }

                // If no paste request was created before, create one that is centered in the current canvas view
                if (!pasteRequest) {
                    pasteRequest = this.copyPasteService.toCenteredPasteRequest(request);
                }

                const payload: PasteRequestEntity = {
                    copyResponse: pasteRequest.copyResponse,
                    revision
                };
                const pasteRequestContext: PasteRequestContext = {
                    pasteRequest: payload,
                    processGroupId,
                    pasteStrategy: pasteRequest.strategy
                };
                return from(this.copyPasteService.paste(pasteRequestContext)).pipe(
                    map((response) => {
                        return FlowActions.pasteSuccess({
                            response: {
                                ...response,
                                pasteRequest
                            }
                        });
                    }),
                    catchError((errorResponse: HttpErrorResponse) => of(this.snackBarOrFullScreenError(errorResponse)))
                );
            })
        )
    );

    pasteSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.pasteSuccess),
            map((action) => action.response),
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
            switchMap(([response, currentProcessGroupId]) => {
                this.canvasView.updateCanvasVisibility();
                this.birdseyeView.refresh();

                const components: SelectedComponent[] = [];
                components.push(
                    ...response.flow.labels.map((label) => {
                        return {
                            id: label.id,
                            componentType: ComponentType.Label
                        };
                    })
                );
                components.push(
                    ...response.flow.funnels.map((funnel) => {
                        return {
                            id: funnel.id,
                            componentType: ComponentType.Funnel
                        };
                    })
                );
                components.push(
                    ...response.flow.remoteProcessGroups.map((remoteProcessGroups) => {
                        return {
                            id: remoteProcessGroups.id,
                            componentType: ComponentType.RemoteProcessGroup
                        };
                    })
                );
                components.push(
                    ...response.flow.inputPorts.map((inputPorts) => {
                        return {
                            id: inputPorts.id,
                            componentType: ComponentType.InputPort
                        };
                    })
                );
                components.push(
                    ...response.flow.outputPorts.map((outputPorts) => {
                        return {
                            id: outputPorts.id,
                            componentType: ComponentType.OutputPort
                        };
                    })
                );
                components.push(
                    ...response.flow.processGroups.map((processGroup) => {
                        return {
                            id: processGroup.id,
                            componentType: ComponentType.ProcessGroup
                        };
                    })
                );
                components.push(
                    ...response.flow.processors.map((processor) => {
                        return {
                            id: processor.id,
                            componentType: ComponentType.Processor
                        };
                    })
                );
                components.push(
                    ...response.flow.connections.map((connection) => {
                        return {
                            id: connection.id,
                            componentType: ComponentType.Connection
                        };
                    })
                );

                if (response.pasteRequest.fitToScreen && response.pasteRequest.bbox) {
                    this.canvasView.centerBoundingBox(response.pasteRequest.bbox);
                }
                this.store.dispatch(
                    CopyActions.contentPasted({
                        pasted: {
                            copyId: response.pasteRequest.copyResponse.id,
                            processGroupId: currentProcessGroupId,
                            strategy: response.pasteRequest.strategy
                        }
                    })
                );

                return of(
                    FlowActions.selectComponents({
                        request: {
                            components
                        }
                    })
                );
            })
        )
    );

    deleteComponent$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.deleteComponents),
            map((action) => action.request),
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
            mergeMap(([requests, processGroupId]) => {
                if (requests.length === 1) {
                    return from(this.flowService.deleteComponent(requests[0])).pipe(
                        map((response) => {
                            const deleteResponses: DeleteComponentResponse[] = [
                                {
                                    id: requests[0].id,
                                    type: requests[0].type
                                }
                            ];

                            if (requests[0].type !== ComponentType.Connection) {
                                const componentConnections: any[] = this.canvasUtils.getComponentConnections(
                                    requests[0].id
                                );
                                componentConnections.forEach((componentConnection) =>
                                    deleteResponses.push({
                                        id: componentConnection.id,
                                        type: ComponentType.Connection
                                    })
                                );
                            } else {
                                this.store.dispatch(FlowActions.loadComponentsForConnection({ connection: response }));
                            }

                            return FlowActions.deleteComponentsSuccess({
                                response: deleteResponses
                            });
                        }),
                        catchError((errorResponse: HttpErrorResponse) =>
                            of(this.snackBarOrFullScreenError(errorResponse))
                        )
                    );
                } else {
                    const snippet: Snippet = requests.reduce(
                        (snippet, request) => {
                            switch (request.type) {
                                case ComponentType.Processor:
                                    snippet.processors[request.id] = this.client.getRevision(request.entity);
                                    break;
                                case ComponentType.InputPort:
                                    snippet.inputPorts[request.id] = this.client.getRevision(request.entity);
                                    break;
                                case ComponentType.OutputPort:
                                    snippet.outputPorts[request.id] = this.client.getRevision(request.entity);
                                    break;
                                case ComponentType.ProcessGroup:
                                    snippet.processGroups[request.id] = this.client.getRevision(request.entity);
                                    break;
                                case ComponentType.RemoteProcessGroup:
                                    snippet.remoteProcessGroups[request.id] = this.client.getRevision(request.entity);
                                    break;
                                case ComponentType.Funnel:
                                    snippet.funnels[request.id] = this.client.getRevision(request.entity);
                                    break;
                                case ComponentType.Label:
                                    snippet.labels[request.id] = this.client.getRevision(request.entity);
                                    break;
                                case ComponentType.Connection:
                                    snippet.connections[request.id] = this.client.getRevision(request.entity);
                                    break;
                            }
                            return snippet;
                        },
                        {
                            parentGroupId: processGroupId,
                            processors: {},
                            funnels: {},
                            inputPorts: {},
                            outputPorts: {},
                            remoteProcessGroups: {},
                            processGroups: {},
                            connections: {},
                            labels: {}
                        } as Snippet
                    );

                    return from(this.snippetService.createSnippet(snippet)).pipe(
                        switchMap((response) => this.snippetService.deleteSnippet(response.snippet.id)),
                        map(() => {
                            const deleteResponses: DeleteComponentResponse[] = [];

                            // prepare the delete responses with all requested components that are now deleted
                            requests.forEach((request) => {
                                deleteResponses.push({
                                    id: request.id,
                                    type: request.type
                                });

                                // if the component is not a connection, also include any of it's connections
                                if (request.type !== ComponentType.Connection) {
                                    const componentConnections: any[] = this.canvasUtils.getComponentConnections(
                                        request.id
                                    );
                                    componentConnections.forEach((componentConnection) =>
                                        deleteResponses.push({
                                            id: componentConnection.id,
                                            type: ComponentType.Connection
                                        })
                                    );
                                }
                            });

                            return FlowActions.deleteComponentsSuccess({
                                response: deleteResponses
                            });
                        }),
                        catchError((errorResponse: HttpErrorResponse) =>
                            of(this.snackBarOrFullScreenError(errorResponse))
                        )
                    );
                }
            })
        )
    );

    deleteComponentsSuccess$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.deleteComponentsSuccess),
                tap(() => {
                    this.birdseyeView.refresh();
                })
            ),
        { dispatch: false }
    );

    enterProcessGroup$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.enterProcessGroup),
                map((action) => action.request),
                tap((request) => {
                    this.router.navigate(['/process-groups', request.id]);
                })
            ),
        { dispatch: false }
    );

    leaveProcessGroup$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.leaveProcessGroup),
                concatLatestFrom(() => this.store.select(selectParentProcessGroupId)),
                filter(([, parentProcessGroupId]) => parentProcessGroupId != null),
                tap(([, parentProcessGroupId]) => {
                    this.router.navigate(['/process-groups', parentProcessGroupId]);
                })
            ),
        { dispatch: false }
    );

    addSelectedComponents$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.addSelectedComponents),
            map((action) => action.request),
            concatLatestFrom(() => [
                this.store.select(selectCurrentProcessGroupId),
                this.store.select(selectAnySelectedComponentIds)
            ]),
            switchMap(([request, processGroupId, selected]) => {
                let commands: string[] = [];
                if (selected.length === 0) {
                    if (request.components.length === 1) {
                        commands = [
                            '/process-groups',
                            processGroupId,
                            request.components[0].componentType,
                            request.components[0].id
                        ];
                    } else if (request.components.length > 1) {
                        const ids: string[] = request.components.map((selectedComponent) => selectedComponent.id);
                        commands = ['/process-groups', processGroupId, 'bulk', ids.join(',')];
                    }
                } else {
                    const ids: string[] = request.components.map((selectedComponent) => selectedComponent.id);
                    ids.push(...selected);
                    commands = ['/process-groups', processGroupId, 'bulk', ids.join(',')];
                }
                return of(FlowActions.navigateWithoutTransform({ url: commands }));
            })
        )
    );

    removeSelectedComponents$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.removeSelectedComponents),
            map((action) => action.request),
            concatLatestFrom(() => [
                this.store.select(selectCurrentProcessGroupId),
                this.store.select(selectAnySelectedComponentIds)
            ]),
            switchMap(([request, processGroupId, selected]) => {
                let commands: string[];
                if (selected.length === 0) {
                    commands = ['/process-groups', processGroupId];
                } else {
                    const idsToRemove: string[] = request.components.map((selectedComponent) => selectedComponent.id);
                    const ids: string[] = selected.filter((id) => !idsToRemove.includes(id));
                    commands = ['/process-groups', processGroupId, 'bulk', ids.join(',')];
                }
                return of(FlowActions.navigateWithoutTransform({ url: commands }));
            })
        )
    );

    selectComponents$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.selectComponents),
            map((action) => action.request),
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
            switchMap(([request, processGroupId]) => {
                let commands: string[] = [];
                if (request.components.length === 1) {
                    commands = [
                        '/process-groups',
                        processGroupId,
                        request.components[0].componentType,
                        request.components[0].id
                    ];
                } else if (request.components.length > 1) {
                    const ids: string[] = request.components.map((selectedComponent) => selectedComponent.id);
                    commands = ['/process-groups', processGroupId, 'bulk', ids.join(',')];
                }
                return of(FlowActions.navigateWithoutTransform({ url: commands }));
            })
        )
    );

    deselectAllComponent$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.deselectAllComponents),
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
            switchMap(([, processGroupId]) => {
                return of(FlowActions.navigateWithoutTransform({ url: ['/process-groups', processGroupId] }));
            })
        )
    );

    navigateToComponent$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.navigateToComponent),
                map((action) => action.request),
                concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
                tap(([request, currentProcessGroupId]) => {
                    if (request.processGroupId) {
                        this.router.navigate(['/process-groups', request.processGroupId, request.type, request.id]);
                    } else {
                        this.router.navigate(['/process-groups', currentProcessGroupId, request.type, request.id]);
                    }
                })
            ),
        { dispatch: false }
    );

    navigateWithoutTransform$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.navigateWithoutTransform),
                map((action) => action.url),
                tap((url) => {
                    this.router.navigate(url);
                })
            ),
        { dispatch: false }
    );

    centerSelectedComponents$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.centerSelectedComponents),
            map((action) => action.request),
            tap((request) => {
                this.canvasView.centerSelectedComponents(request.allowTransition);
            }),
            switchMap(() => of(FlowActions.setAllowTransition({ allowTransition: false })))
        )
    );

    navigateToProvenanceForComponent$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.navigateToProvenanceForComponent),
                map((action) => action.id),
                concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
                tap(([componentId, processGroupId]) => {
                    this.router.navigate(['/provenance'], {
                        queryParams: { componentId },
                        state: {
                            backNavigation: {
                                route: ['/process-groups', processGroupId, ComponentType.Processor, componentId],
                                routeBoundary: ['/provenance'],
                                context: 'Processor'
                            } as BackNavigation
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    replayLastProvenanceEvent = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.replayLastProvenanceEvent),
                map((action) => action.request),
                tap((request) => {
                    this.flowService.replayLastProvenanceEvent(request).subscribe({
                        next: (response) => {
                            if (response.aggregateSnapshot.failureExplanation) {
                                this.store.dispatch(
                                    FlowActions.showOkDialog({
                                        title: 'Replay Event Failure',
                                        message: response.aggregateSnapshot.failureExplanation
                                    })
                                );
                            } else if (response.aggregateSnapshot.eventAvailable !== true) {
                                this.store.dispatch(
                                    FlowActions.showOkDialog({
                                        title: 'No Event Available',
                                        message: 'There was no recent event available to be replayed.'
                                    })
                                );
                            } else if (response.nodeSnapshots) {
                                let replayedCount = 0;
                                let unavailableCount = 0;

                                response.nodeSnapshots.forEach((nodeResponse: any) => {
                                    if (nodeResponse.snapshot.eventAvailable) {
                                        replayedCount++;
                                    } else {
                                        unavailableCount++;
                                    }
                                });

                                let message: string;
                                if (unavailableCount === 0) {
                                    message = 'All nodes successfully replayed the latest event.';
                                } else {
                                    message = `${replayedCount} nodes successfully replayed the latest event but ${unavailableCount} had no recent event available to be replayed.`;
                                }

                                this.store.dispatch(
                                    FlowActions.showOkDialog({
                                        title: 'Events Replayed',
                                        message
                                    })
                                );
                            } else {
                                this.store.dispatch(
                                    FlowActions.showOkDialog({
                                        title: 'Events Replayed',
                                        message: 'Successfully replayed the latest event.'
                                    })
                                );
                            }

                            this.store.dispatch(
                                FlowActions.loadConnectionsForComponent({
                                    id: request.componentId
                                })
                            );
                        },
                        error: (errorResponse: HttpErrorResponse) => {
                            this.store.dispatch(
                                FlowActions.showOkDialog({
                                    title: 'Failed to Replay Event',
                                    message: this.errorHelper.getErrorString(errorResponse)
                                })
                            );
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    showOkDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.showOkDialog),
                tap((request) => {
                    this.dialog.open(OkDialog, {
                        ...MEDIUM_DIALOG,
                        data: {
                            title: request.title,
                            message: request.message
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    enableCurrentProcessGroup$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.enableCurrentProcessGroup),
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
            switchMap(([, pgId]) => {
                return of(
                    FlowActions.enableComponent({
                        request: {
                            id: pgId,
                            type: ComponentType.ProcessGroup,
                            errorStrategy: 'snackbar'
                        }
                    })
                );
            })
        )
    );

    enableComponents$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.enableComponents),
            map((action) => action.request),
            mergeMap((request) => [
                ...request.components.map((component) => {
                    return FlowActions.enableComponent({
                        request: component
                    });
                })
            ])
        )
    );

    enableComponent$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.enableComponent),
            map((action) => action.request),
            mergeMap((request) => {
                switch (request.type) {
                    case ComponentType.InputPort:
                    case ComponentType.OutputPort:
                    case ComponentType.Processor:
                        if ('uri' in request && 'revision' in request) {
                            return from(this.flowService.enableComponent(request)).pipe(
                                map((response) => {
                                    return FlowActions.enableComponentSuccess({
                                        response: {
                                            type: request.type,
                                            component: response
                                        }
                                    });
                                }),
                                catchError((errorResponse: HttpErrorResponse) => {
                                    if (request.errorStrategy === 'banner') {
                                        return of(
                                            this.bannerOrFullScreenError(
                                                errorResponse,
                                                this.componentTypeToErrorContext(request.type)
                                            )
                                        );
                                    } else {
                                        return of(this.snackBarOrFullScreenError(errorResponse));
                                    }
                                })
                            );
                        }
                        return of(
                            FlowActions.flowSnackbarError({
                                error: `Enabling ${request.type} requires both uri and revision properties`
                            })
                        );
                    case ComponentType.ProcessGroup:
                        return from(this.flowService.enableProcessGroup(request)).pipe(
                            map((enablePgResponse) => {
                                return FlowActions.enableProcessGroupSuccess({
                                    response: {
                                        type: request.type,
                                        component: enablePgResponse
                                    }
                                });
                            }),
                            catchError((errorResponse: HttpErrorResponse) => {
                                if (request.errorStrategy === 'banner') {
                                    return of(
                                        this.bannerOrFullScreenError(
                                            errorResponse,
                                            this.componentTypeToErrorContext(request.type)
                                        )
                                    );
                                } else {
                                    return of(this.snackBarOrFullScreenError(errorResponse));
                                }
                            })
                        );
                    default:
                        return of(
                            FlowActions.flowSnackbarError({ error: `${request.type} does not support enabling` })
                        );
                }
            })
        )
    );

    /**
     * If the component enabled was the current process group, reload the flow
     */
    enableCurrentProcessGroupSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.enableProcessGroupSuccess),
            map((action) => action.response),
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
            filter(([response, currentPg]) => response.component.id === currentPg),
            switchMap(() => of(FlowActions.reloadFlow()))
        )
    );

    /**
     * If a ProcessGroup was enabled, it should be reloaded as the response from the start operation doesn't contain all the displayed info
     */
    enableProcessGroupSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.enableProcessGroupSuccess),
            map((action) => action.response),
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
            filter(([response, currentPg]) => response.component.id !== currentPg),
            switchMap(([response]) =>
                of(
                    FlowActions.loadChildProcessGroup({
                        request: {
                            id: response.component.id
                        }
                    })
                )
            )
        )
    );

    disableCurrentProcessGroup$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.disableCurrentProcessGroup),
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
            switchMap(([, pgId]) => {
                return of(
                    FlowActions.disableComponent({
                        request: {
                            id: pgId,
                            type: ComponentType.ProcessGroup,
                            errorStrategy: 'snackbar'
                        }
                    })
                );
            })
        )
    );

    disableComponents$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.disableComponents),
            map((action) => action.request),
            mergeMap((request) => [
                ...request.components.map((component) => {
                    return FlowActions.disableComponent({
                        request: component
                    });
                })
            ])
        )
    );

    disableComponent$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.disableComponent),
            map((action) => action.request),
            mergeMap((request) => {
                switch (request.type) {
                    case ComponentType.InputPort:
                    case ComponentType.OutputPort:
                    case ComponentType.Processor:
                        if ('uri' in request && 'revision' in request) {
                            return from(this.flowService.disableComponent(request)).pipe(
                                map((response) => {
                                    return FlowActions.disableComponentSuccess({
                                        response: {
                                            type: request.type,
                                            component: response
                                        }
                                    });
                                }),
                                catchError((errorResponse: HttpErrorResponse) => {
                                    if (request.errorStrategy === 'banner') {
                                        return of(
                                            this.bannerOrFullScreenError(
                                                errorResponse,
                                                this.componentTypeToErrorContext(request.type)
                                            )
                                        );
                                    } else {
                                        return of(this.snackBarOrFullScreenError(errorResponse));
                                    }
                                })
                            );
                        }
                        return of(
                            FlowActions.flowSnackbarError({
                                error: `Disabling ${request.type} requires both uri and revision properties`
                            })
                        );
                    case ComponentType.ProcessGroup:
                        return from(this.flowService.disableProcessGroup(request)).pipe(
                            map((enablePgResponse) => {
                                return FlowActions.disableProcessGroupSuccess({
                                    response: {
                                        type: request.type,
                                        component: enablePgResponse
                                    }
                                });
                            }),
                            catchError((errorResponse: HttpErrorResponse) => {
                                if (request.errorStrategy === 'banner') {
                                    return of(
                                        this.bannerOrFullScreenError(
                                            errorResponse,
                                            this.componentTypeToErrorContext(request.type)
                                        )
                                    );
                                } else {
                                    return of(this.snackBarOrFullScreenError(errorResponse));
                                }
                            })
                        );
                    default:
                        return of(
                            FlowActions.flowSnackbarError({ error: `${request.type} does not support disabling` })
                        );
                }
            })
        )
    );

    /**
     * If the component disabled was the current process group, reload the flow
     */
    disableCurrentProcessGroupSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.disableProcessGroupSuccess),
            map((action) => action.response),
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
            filter(([response, currentPg]) => response.component.id === currentPg),
            switchMap(() => of(FlowActions.reloadFlow()))
        )
    );

    /**
     * If a ProcessGroup was disabled, it should be reloaded as the response from the start operation doesn't contain all the displayed info
     */
    disableProcessGroupSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.disableProcessGroupSuccess),
            map((action) => action.response),
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
            filter(([response, currentPg]) => response.component.id !== currentPg),
            switchMap(([response]) =>
                of(
                    FlowActions.loadChildProcessGroup({
                        request: {
                            id: response.component.id
                        }
                    })
                )
            )
        )
    );

    startCurrentProcessGroup$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.startCurrentProcessGroup),
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
            switchMap(([, pgId]) => {
                return of(
                    FlowActions.startComponent({
                        request: {
                            id: pgId,
                            type: ComponentType.ProcessGroup,
                            errorStrategy: 'snackbar'
                        }
                    })
                );
            })
        )
    );

    startComponents$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.startComponents),
            map((action) => action.request),
            mergeMap((request) => [
                ...request.components.map((component) => {
                    return FlowActions.startComponent({
                        request: component
                    });
                })
            ])
        )
    );

    startComponent$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.startComponent),
            map((action) => action.request),
            mergeMap((request) => {
                switch (request.type) {
                    case ComponentType.InputPort:
                    case ComponentType.OutputPort:
                    case ComponentType.Processor:
                    case ComponentType.RemoteProcessGroup:
                        if ('uri' in request && 'revision' in request) {
                            return from(this.flowService.startComponent(request)).pipe(
                                map((response) => {
                                    return FlowActions.startComponentSuccess({
                                        response: {
                                            type: request.type,
                                            component: response
                                        }
                                    });
                                }),
                                catchError((errorResponse: HttpErrorResponse) => {
                                    if (request.errorStrategy === 'banner') {
                                        return of(
                                            this.bannerOrFullScreenError(
                                                errorResponse,
                                                this.componentTypeToErrorContext(request.type)
                                            )
                                        );
                                    } else {
                                        return of(this.snackBarOrFullScreenError(errorResponse));
                                    }
                                })
                            );
                        }
                        return of(
                            FlowActions.flowSnackbarError({
                                error: `Starting ${request.type} requires both uri and revision properties`
                            })
                        );
                    case ComponentType.ProcessGroup:
                        return combineLatest([
                            this.flowService.startProcessGroup(request),
                            this.flowService.startRemoteProcessGroupsInProcessGroup(request)
                        ]).pipe(
                            map(([startPgResponse]) => {
                                return FlowActions.startProcessGroupSuccess({
                                    response: {
                                        type: request.type,
                                        component: startPgResponse
                                    }
                                });
                            }),
                            catchError((errorResponse: HttpErrorResponse) => {
                                if (request.errorStrategy === 'banner') {
                                    return of(
                                        this.bannerOrFullScreenError(
                                            errorResponse,
                                            this.componentTypeToErrorContext(request.type)
                                        )
                                    );
                                } else {
                                    return of(this.snackBarOrFullScreenError(errorResponse));
                                }
                            })
                        );
                    default:
                        return of(
                            FlowActions.flowSnackbarError({ error: `${request.type} does not support starting` })
                        );
                }
            })
        )
    );

    /**
     * If the component started was the current process group, reload the flow
     */
    startCurrentProcessGroupSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.startProcessGroupSuccess),
            map((action) => action.response),
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
            filter(([response, currentPg]) => response.component.id === currentPg),
            switchMap(() => of(FlowActions.reloadFlow()))
        )
    );

    /**
     * If a ProcessGroup was started, it should be reloaded as the response from the start operation doesn't contain all the displayed info
     */
    startProcessGroupSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.startProcessGroupSuccess),
            map((action) => action.response),
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
            filter(([response, currentPg]) => response.component.id !== currentPg),
            switchMap(([response]) =>
                of(
                    FlowActions.loadChildProcessGroup({
                        request: {
                            id: response.component.id
                        }
                    })
                )
            )
        )
    );

    stopCurrentProcessGroup$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.stopCurrentProcessGroup),
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
            switchMap(([, pgId]) => {
                return of(
                    FlowActions.stopComponent({
                        request: {
                            id: pgId,
                            type: ComponentType.ProcessGroup,
                            errorStrategy: 'snackbar'
                        }
                    })
                );
            })
        )
    );

    enableControllerServicesInCurrentProcessGroup$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.enableControllerServicesInCurrentProcessGroup),
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
            switchMap(([, id]) =>
                from(
                    this.flowService.enableAllControllerServices(id).pipe(
                        map(() => FlowActions.reloadFlow()),
                        catchError((errorResponse) => of(this.snackBarOrFullScreenError(errorResponse)))
                    )
                )
            )
        )
    );

    disableControllerServicesInCurrentProcessGroup$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.disableControllerServicesInCurrentProcessGroup),
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
            switchMap(([, id]) =>
                from(
                    this.flowService.disableAllControllerServices(id).pipe(
                        map(() => FlowActions.reloadFlow()),
                        catchError((errorResponse) => of(this.snackBarOrFullScreenError(errorResponse)))
                    )
                )
            )
        )
    );

    enableControllerServicesInProcessGroup$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.enableControllerServicesInProcessGroup),
            map((action) => action.id),
            switchMap((id) =>
                from(
                    this.flowService.enableAllControllerServices(id).pipe(
                        map(() => FlowActions.loadChildProcessGroup({ request: { id } })),
                        catchError((errorResponse) => of(this.snackBarOrFullScreenError(errorResponse)))
                    )
                )
            )
        )
    );

    disableControllerServicesInProcessGroup$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.disableControllerServicesInProcessGroup),
            map((action) => action.id),
            switchMap((id) =>
                from(
                    this.flowService.disableAllControllerServices(id).pipe(
                        map(() => FlowActions.loadChildProcessGroup({ request: { id } })),
                        catchError((errorResponse) => of(this.snackBarOrFullScreenError(errorResponse)))
                    )
                )
            )
        )
    );

    stopComponents$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.stopComponents),
            map((action) => action.request),
            mergeMap((request) => [
                ...request.components.map((component) => {
                    return FlowActions.stopComponent({
                        request: component
                    });
                })
            ])
        )
    );

    stopComponent$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.stopComponent),
            map((action) => action.request),
            mergeMap((request) => {
                switch (request.type) {
                    case ComponentType.InputPort:
                    case ComponentType.OutputPort:
                    case ComponentType.Processor:
                    case ComponentType.RemoteProcessGroup:
                        if ('uri' in request && 'revision' in request) {
                            return from(this.flowService.stopComponent(request)).pipe(
                                map((response) => {
                                    return FlowActions.stopComponentSuccess({
                                        response: {
                                            type: request.type,
                                            component: response
                                        }
                                    });
                                }),
                                catchError((errorResponse: HttpErrorResponse) => {
                                    if (request.errorStrategy === 'banner') {
                                        return of(
                                            this.bannerOrFullScreenError(
                                                errorResponse,
                                                this.componentTypeToErrorContext(request.type)
                                            )
                                        );
                                    } else {
                                        return of(this.snackBarOrFullScreenError(errorResponse));
                                    }
                                })
                            );
                        }
                        return of(
                            FlowActions.flowSnackbarError({
                                error: `Stopping ${request.type} requires both uri and revision properties`
                            })
                        );
                    case ComponentType.ProcessGroup:
                        return combineLatest([
                            this.flowService.stopProcessGroup(request),
                            this.flowService.stopRemoteProcessGroupsInProcessGroup(request)
                        ]).pipe(
                            map(([stopPgResponse]) => {
                                return FlowActions.stopProcessGroupSuccess({
                                    response: {
                                        type: request.type,
                                        component: stopPgResponse
                                    }
                                });
                            }),
                            catchError((errorResponse: HttpErrorResponse) => {
                                if (request.errorStrategy === 'banner') {
                                    return of(
                                        this.bannerOrFullScreenError(
                                            errorResponse,
                                            this.componentTypeToErrorContext(request.type)
                                        )
                                    );
                                } else {
                                    return of(this.snackBarOrFullScreenError(errorResponse));
                                }
                            })
                        );
                    default:
                        return of(
                            FlowActions.flowSnackbarError({ error: `${request.type} does not support stopping` })
                        );
                }
            })
        )
    );

    /**
     * Terminates threads for the processor in the request.
     */
    terminateThreads$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.terminateThreads),
            map((action) => action.request),
            switchMap((request) =>
                from(this.flowService.terminateThreads(request)).pipe(
                    map((response) =>
                        FlowActions.updateComponentSuccess({
                            response: {
                                id: request.id,
                                type: ComponentType.Processor,
                                response
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) => of(this.snackBarOrFullScreenError(errorResponse)))
                )
            )
        )
    );

    /**
     * If the component stopped was the current process group, reload the flow
     */
    stopCurrentProcessGroupSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.stopProcessGroupSuccess),
            map((action) => action.response),
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
            filter(([response, currentPg]) => response.component.id === currentPg),
            switchMap(() => of(FlowActions.reloadFlow()))
        )
    );

    /**
     * If a ProcessGroup was stopped, it should be reloaded as the response from the stop operation doesn't contain all the displayed info
     */
    stopProcessGroupSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.stopProcessGroupSuccess),
            map((action) => action.response),
            concatLatestFrom(() => this.store.select(selectCurrentProcessGroupId)),
            filter(([response, currentPg]) => response.component.id !== currentPg),
            switchMap(([response]) =>
                of(
                    FlowActions.loadChildProcessGroup({
                        request: {
                            id: response.component.id
                        }
                    })
                )
            )
        )
    );

    runOnce$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.runOnce),
            map((action) => action.request),
            switchMap((request) => {
                return from(this.flowService.runOnce(request)).pipe(
                    map((response) =>
                        FlowActions.runOnceSuccess({
                            response: {
                                component: response
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) => of(this.snackBarOrFullScreenError(errorResponse)))
                );
            })
        )
    );

    reloadProcessGroup$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.loadChildProcessGroup),
            map((action) => action.request),
            mergeMap((request) => {
                return from(this.flowService.getProcessGroup(request.id)).pipe(
                    map((response) =>
                        FlowActions.loadChildProcessGroupSuccess({
                            response
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) => of(this.snackBarOrFullScreenError(errorResponse)))
                );
            })
        )
    );

    //////////////////////////////////
    // Start version control effects
    //////////////////////////////////

    openSaveVersionDialogRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.openSaveVersionDialogRequest),
            map((action) => action.request),
            switchMap((request) => {
                return combineLatest([
                    this.registryService.getRegistryClients(),
                    this.flowService.getVersionInformation(request.processGroupId)
                ]).pipe(
                    map(([registryClients, versionInfo]) => {
                        const dialogRequest: SaveVersionDialogRequest = {
                            processGroupId: request.processGroupId,
                            revision: versionInfo.processGroupRevision,
                            registryClients: registryClients.registries
                        };
                        this.store.dispatch(setRegistryClients({ request: registryClients.registries }));
                        return FlowActions.openSaveVersionDialog({ request: dialogRequest });
                    }),
                    catchError((errorResponse: HttpErrorResponse) => of(this.snackBarOrFullScreenError(errorResponse)))
                );
            })
        )
    );

    openSaveVersionDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.openSaveVersionDialog),
                map((action) => action.request),
                tap((request) => {
                    const dialogReference = this.dialog.open(SaveVersionDialog, {
                        ...MEDIUM_DIALOG,
                        data: request
                    });

                    dialogReference.componentInstance.getBranches = (
                        registryId: string
                    ): Observable<BranchEntity[]> => {
                        return this.registryService.getBranches(registryId).pipe(
                            take(1),
                            map((response) => response.branches),
                            tap({
                                error: (errorResponse: HttpErrorResponse) => {
                                    this.store.dispatch(
                                        FlowActions.flowBannerError({
                                            errorContext: {
                                                context: ErrorContextKey.FLOW_VERSION,
                                                errors: [this.errorHelper.getErrorString(errorResponse)]
                                            }
                                        })
                                    );
                                }
                            })
                        );
                    };

                    dialogReference.componentInstance.getBuckets = (
                        registryId: string,
                        branch?: string | null
                    ): Observable<BucketEntity[]> => {
                        return this.registryService.getBuckets(registryId, branch).pipe(
                            take(1),
                            map((response) => response.buckets),
                            tap({
                                error: (errorResponse: HttpErrorResponse) => {
                                    this.store.dispatch(
                                        FlowActions.flowBannerError({
                                            errorContext: {
                                                context: ErrorContextKey.FLOW_VERSION,
                                                errors: [this.errorHelper.getErrorString(errorResponse)]
                                            }
                                        })
                                    );
                                }
                            })
                        );
                    };

                    dialogReference.componentInstance.saving = this.store.selectSignal(selectVersionSaving);

                    dialogReference.componentInstance.save
                        .pipe(takeUntil(dialogReference.afterClosed()))
                        .subscribe((saveRequest: SaveVersionRequest) => {
                            if (saveRequest.existingFlowId) {
                                this.store.dispatch(
                                    FlowActions.saveToFlowRegistry({
                                        request: {
                                            versionedFlow: {
                                                action: request.forceCommit ? 'FORCE_COMMIT' : 'COMMIT',
                                                flowId: saveRequest.existingFlowId,
                                                bucketId: saveRequest.bucket,
                                                registryId: saveRequest.registry,
                                                comments: saveRequest.comments || '',
                                                branch: saveRequest.branch
                                            },
                                            processGroupId: saveRequest.processGroupId,
                                            processGroupRevision: saveRequest.revision
                                        }
                                    })
                                );
                            } else {
                                this.store.dispatch(
                                    FlowActions.saveToFlowRegistry({
                                        request: {
                                            versionedFlow: {
                                                action: 'COMMIT',
                                                bucketId: saveRequest.bucket,
                                                registryId: saveRequest.registry,
                                                flowName: saveRequest.flowName,
                                                description: saveRequest.flowDescription || '',
                                                comments: saveRequest.comments || '',
                                                branch: saveRequest.branch
                                            },
                                            processGroupId: saveRequest.processGroupId,
                                            processGroupRevision: saveRequest.revision
                                        }
                                    })
                                );
                            }
                        });
                })
            ),
        { dispatch: false }
    );

    saveToFlowRegistry$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.saveToFlowRegistry),
            map((action) => action.request),
            switchMap((request) => {
                return from(this.flowService.saveToFlowRegistry(request)).pipe(
                    map((response) => {
                        return FlowActions.saveToFlowRegistrySuccess({ response });
                    }),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(this.bannerOrFullScreenError(errorResponse, ErrorContextKey.FLOW_VERSION))
                    )
                );
            })
        )
    );

    saveToFlowRegistrySuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.saveToFlowRegistrySuccess),
            tap(() => {
                this.dialog.closeAll();
            }),
            switchMap(() => of(FlowActions.reloadFlow()))
        )
    );

    flowBannerError$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.flowBannerError),
            map((action) => action.errorContext),
            switchMap((errorContext) => of(ErrorActions.addBannerError({ errorContext })))
        )
    );

    flowSnackbarError$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.flowSnackbarError),
            map((action) => action.error),
            tap(() => {
                this.dialog.closeAll();
            }),
            switchMap((error) => of(ErrorActions.snackBarError({ error })))
        )
    );

    private bannerOrFullScreenError(errorResponse: HttpErrorResponse, context: ErrorContextKey) {
        if (this.errorHelper.showErrorInContext(errorResponse.status)) {
            return FlowActions.flowBannerError({
                errorContext: {
                    errors: [this.errorHelper.getErrorString(errorResponse)],
                    context
                }
            });
        } else {
            return this.errorHelper.fullScreenError(errorResponse);
        }
    }

    private snackBarOrFullScreenError(errorResponse: HttpErrorResponse) {
        if (this.errorHelper.showErrorInContext(errorResponse.status)) {
            return FlowActions.flowSnackbarError({ error: this.errorHelper.getErrorString(errorResponse) });
        } else {
            return this.errorHelper.fullScreenError(errorResponse);
        }
    }

    /////////////////////////////////
    // Stop version control effects
    /////////////////////////////////

    stopVersionControlRequest$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.stopVersionControlRequest),
                map((action) => action.request),
                switchMap((request) => {
                    return from(this.flowService.getVersionInformation(request.processGroupId)).pipe(
                        map((response) => {
                            const dialogRequest: StopVersionControlRequest = {
                                processGroupId: request.processGroupId,
                                revision: response.processGroupRevision
                            };
                            return dialogRequest;
                        })
                    );
                }),
                tap((request) => {
                    const dialogRef = this.dialog.open(YesNoDialog, {
                        ...SMALL_DIALOG,
                        data: {
                            title: 'Stop Version Control',
                            message: `Are you sure you want to stop version control?`
                        }
                    });

                    dialogRef.componentInstance.yes.pipe(take(1)).subscribe(() => {
                        this.store.dispatch(FlowActions.stopVersionControl({ request }));
                    });

                    dialogRef.componentInstance.no.pipe(take(1)).subscribe(() => {
                        dialogRef.close();
                    });
                }),
                catchError((errorResponse: HttpErrorResponse) => of(this.snackBarOrFullScreenError(errorResponse)))
            ),
        { dispatch: false }
    );

    stopVersionControl$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.stopVersionControl),
            map((action) => action.request),
            switchMap((request) =>
                from(this.flowService.stopVersionControl(request)).pipe(
                    map((response) => {
                        const stopResponse: StopVersionControlResponse = {
                            processGroupRevision: response.processGroupRevision,
                            processGroupId: request.processGroupId
                        };
                        return FlowActions.stopVersionControlSuccess({ response: stopResponse });
                    }),
                    catchError((errorResponse) => of(this.snackBarOrFullScreenError(errorResponse)))
                )
            )
        )
    );

    stopVersionControlSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.stopVersionControlSuccess),
            tap(() => {
                this.store.dispatch(
                    FlowActions.showOkDialog({
                        title: 'Disconnect',
                        message: 'This Process Group is no longer under version control.'
                    })
                );
            }),
            switchMap(() => of(FlowActions.reloadFlow()))
        )
    );

    /////////////////////////////////
    // Commit local changes effects
    /////////////////////////////////

    openCommitLocalChangesDialogRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.openCommitLocalChangesDialogRequest),
            map((action) => action.request),
            switchMap((request) => {
                return from(this.flowService.getVersionInformation(request.processGroupId)).pipe(
                    map((response) => {
                        const dialogRequest: SaveVersionDialogRequest = {
                            processGroupId: request.processGroupId,
                            revision: response.processGroupRevision,
                            versionControlInformation: response.versionControlInformation,
                            forceCommit: request.forceCommit
                        };

                        return FlowActions.openSaveVersionDialog({ request: dialogRequest });
                    }),
                    catchError((errorResponse: HttpErrorResponse) => of(this.snackBarOrFullScreenError(errorResponse)))
                );
            })
        )
    );

    openForceCommitLocalChangesDialogRequest$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.openForceCommitLocalChangesDialogRequest),
                map((action) => action.request),
                tap((request) => {
                    const dialogRef = this.dialog.open(YesNoDialog, {
                        ...SMALL_DIALOG,
                        data: {
                            title: 'Commit',
                            message:
                                'Committing will ignore available upgrades and commit local changes as the next version. Are you sure you want to proceed?'
                        }
                    });

                    dialogRef.componentInstance.yes.pipe(take(1)).subscribe(() => {
                        this.store.dispatch(
                            FlowActions.openCommitLocalChangesDialogRequest({
                                request: {
                                    ...request,
                                    forceCommit: true
                                }
                            })
                        );
                    });

                    dialogRef.componentInstance.no.pipe(take(1)).subscribe(() => {
                        dialogRef.close();
                    });
                })
            ),
        { dispatch: false }
    );

    /////////////////////////////
    // Change version effects
    /////////////////////////////

    openChangeVersionDialogRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.openChangeVersionDialogRequest),
            map((action) => action.request),
            switchMap((request) =>
                from(this.flowService.getVersionInformation(request.processGroupId)).pipe(
                    tap({
                        error: (errorResponse: HttpErrorResponse) =>
                            this.store.dispatch(this.snackBarOrFullScreenError(errorResponse))
                    })
                )
            ),
            switchMap((versionControlInfo: VersionControlInformationEntity) => {
                const vci = versionControlInfo.versionControlInformation;
                if (vci) {
                    return from(
                        this.registryService.getFlowVersions(vci.registryId, vci.bucketId, vci.flowId, vci.branch)
                    ).pipe(
                        map((versions) =>
                            FlowActions.openChangeVersionDialog({
                                request: {
                                    processGroupId: vci.groupId,
                                    revision: versionControlInfo.processGroupRevision,
                                    versionControlInformation: vci,
                                    versions:
                                        versions.versionedFlowSnapshotMetadataSet as unknown as VersionedFlowSnapshotMetadataEntity[]
                                }
                            })
                        ),
                        catchError((errorResponse: HttpErrorResponse) =>
                            of(this.snackBarOrFullScreenError(errorResponse))
                        )
                    );
                } else {
                    // should never happen
                    return NEVER;
                }
            })
        )
    );

    openChangeVersionDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.openChangeVersionDialog),
                map((action) => action.request),
                tap((request) => {
                    const dialogRef = this.dialog.open(ChangeVersionDialog, {
                        ...LARGE_DIALOG,
                        data: request,
                        autoFocus: false
                    });

                    dialogRef.componentInstance.changeVersion.pipe(take(1)).subscribe((selectedVersion) => {
                        const entity: VersionControlInformationEntity = {
                            versionControlInformation: {
                                ...request.versionControlInformation,
                                version: selectedVersion.version
                            },
                            processGroupRevision: request.revision,
                            disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged()
                        };
                        dialogRef.close();

                        this.store.dispatch(FlowActions.openChangeVersionProgressDialog({ request: entity }));
                    });
                })
            ),
        { dispatch: false }
    );

    openChangeVersionProgressDialog$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.openChangeVersionProgressDialog),
            map((action) => action.request),
            tap(() => {
                const dialogRef = this.dialog.open(ChangeVersionProgressDialog, {
                    ...SMALL_DIALOG,
                    minWidth: 365,
                    disableClose: true,
                    autoFocus: false
                });
                dialogRef.componentInstance.flowUpdateRequest$ = this.store.select(selectChangeVersionRequest);
                dialogRef.componentInstance.changeVersionComplete.pipe(take(1)).subscribe((entity) => {
                    this.store.dispatch(FlowActions.changeVersionComplete({ response: entity }));
                });
            }),
            switchMap((request) => of(FlowActions.changeVersion({ request })))
        )
    );

    changeVersion$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.changeVersion),
            map((action) => action.request),
            switchMap((request) => {
                return from(this.flowService.initiateChangeVersionUpdate(request)).pipe(
                    map((flowUpdate) => FlowActions.changeVersionSuccess({ response: flowUpdate })),
                    catchError((errorResponse: HttpErrorResponse) => of(this.snackBarOrFullScreenError(errorResponse)))
                );
            })
        )
    );

    changeVersionSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.changeVersionSuccess),
            map((action) => action.response),
            filter((response) => !response.request.complete),
            switchMap(() => {
                return of(FlowActions.startPollingChangeVersion());
            })
        )
    );

    startPollingChangeVersion = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.startPollingChangeVersion),
            switchMap(() =>
                interval(2000, asyncScheduler).pipe(
                    takeUntil(this.actions$.pipe(ofType(FlowActions.stopPollingChangeVersion)))
                )
            ),
            switchMap(() => of(FlowActions.pollChangeVersion()))
        )
    );

    pollChangeVersion$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.pollChangeVersion),
            concatLatestFrom(() => [this.store.select(selectChangeVersionRequest).pipe(isDefinedAndNotNull())]),
            switchMap(([, changeVersionRequest]) => {
                return from(
                    this.flowService.getChangeVersionUpdateRequest(changeVersionRequest.request.requestId).pipe(
                        map((response) => FlowActions.pollChangeVersionSuccess({ response })),
                        catchError((errorResponse: HttpErrorResponse) => {
                            this.store.dispatch(FlowActions.stopPollingChangeVersion());
                            return of(this.snackBarOrFullScreenError(errorResponse));
                        })
                    )
                );
            })
        )
    );

    pollChangeVersionSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.pollChangeVersionSuccess),
            map((action) => action.response),
            filter((response) => response.request.complete),
            switchMap(() => of(FlowActions.stopPollingChangeVersion()))
        )
    );

    changeVersionComplete$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.changeVersionComplete),
            map((action) => action.response),
            switchMap((response) =>
                from(this.flowService.deleteChangeVersionUpdateRequest(response.request.requestId)).pipe(
                    map(() => FlowActions.reloadFlow()),
                    catchError((errorResponse: HttpErrorResponse) => of(this.snackBarOrFullScreenError(errorResponse)))
                )
            )
        )
    );

    ///////////////////////////////
    // Show local changes effects
    ///////////////////////////////

    openLocalChangesDialogRequest = (mode: 'SHOW' | 'REVERT') =>
        createEffect(() =>
            this.actions$.pipe(
                ofType(
                    mode === 'SHOW'
                        ? FlowActions.openShowLocalChangesDialogRequest
                        : FlowActions.openRevertLocalChangesDialogRequest
                ),
                map((action) => action.request),
                switchMap((request) =>
                    combineLatest([
                        this.flowService.getVersionInformation(request.processGroupId),
                        this.flowService.getLocalModifications(request.processGroupId)
                    ]).pipe(
                        map(([versionControlInfo, localModifications]) =>
                            FlowActions.openLocalChangesDialog({
                                request: {
                                    localModifications,
                                    versionControlInformation: versionControlInfo,
                                    mode
                                }
                            })
                        ),
                        catchError((errorResponse: HttpErrorResponse) =>
                            of(this.snackBarOrFullScreenError(errorResponse))
                        )
                    )
                )
            )
        );

    openShowLocalChangesDialogRequest$ = this.openLocalChangesDialogRequest('SHOW');

    openLocalChangesDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.openLocalChangesDialog),
                map((action) => action.request),
                tap((request) => {
                    const dialogRef = this.dialog.open(LocalChangesDialog, {
                        ...XL_DIALOG,
                        data: request
                    });
                    if (request.mode === 'REVERT') {
                        dialogRef.componentInstance.revert.pipe(take(1)).subscribe((request) => {
                            dialogRef.close();
                            this.store.dispatch(
                                FlowActions.openRevertChangesProgressDialog({
                                    request: request.versionControlInformation
                                })
                            );
                        });
                    }
                    dialogRef.componentInstance.goToChange
                        .pipe(take(1))
                        .subscribe((request) => this.store.dispatch(FlowActions.goToChange({ request })));
                })
            ),
        { dispatch: false }
    );

    goToChange$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.goToChange),
            map((action) => action.request),
            tap(() => this.dialog.closeAll()),
            switchMap((request) => of(FlowActions.navigateToComponent({ request })))
        )
    );

    /////////////////////////////////
    // Revert version effects
    /////////////////////////////////

    openRevertLocalChangesDialogRequest$ = this.openLocalChangesDialogRequest('REVERT');

    openRevertChangesProgressDialog$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.openRevertChangesProgressDialog),
            map((action) => action.request),
            tap(() => {
                const dialogRef = this.dialog.open(ChangeVersionProgressDialog, {
                    ...SMALL_DIALOG,
                    minWidth: 365,
                    disableClose: true,
                    autoFocus: false
                });
                dialogRef.componentInstance.flowUpdateRequest$ = this.store.select(selectChangeVersionRequest);
                dialogRef.componentInstance.changeVersionComplete.pipe(take(1)).subscribe((entity) => {
                    this.store.dispatch(FlowActions.revertChangesComplete({ response: entity }));
                });
            }),
            switchMap((request) => of(FlowActions.revertChanges({ request })))
        )
    );

    revertChanges$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.revertChanges),
            map((action) => action.request),
            switchMap((request) => {
                return from(this.flowService.initiateRevertFlowVersion(request)).pipe(
                    map((flowUpdate) => FlowActions.revertChangesSuccess({ response: flowUpdate })),
                    catchError((errorResponse: HttpErrorResponse) => of(this.snackBarOrFullScreenError(errorResponse)))
                );
            })
        )
    );

    revertChangesSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.revertChangesSuccess),
            map((action) => action.response),
            filter((response) => !response.request.complete),
            switchMap(() => {
                return of(FlowActions.startPollingRevertChanges());
            })
        )
    );

    startPollingRevertChanges = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.startPollingRevertChanges),
            switchMap(() =>
                interval(2000, asyncScheduler).pipe(
                    takeUntil(this.actions$.pipe(ofType(FlowActions.stopPollingRevertChanges)))
                )
            ),
            switchMap(() => of(FlowActions.pollRevertChanges()))
        )
    );

    pollRevertChanges$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.pollRevertChanges),
            concatLatestFrom(() => [this.store.select(selectChangeVersionRequest).pipe(isDefinedAndNotNull())]),
            switchMap(([, changeVersionRequest]) => {
                return from(
                    this.flowService.getRevertChangesUpdateRequest(changeVersionRequest.request.requestId).pipe(
                        map((response) => FlowActions.pollRevertChangesSuccess({ response })),
                        catchError((errorResponse: HttpErrorResponse) => {
                            this.store.dispatch(FlowActions.stopPollingRevertChanges());
                            return of(this.snackBarOrFullScreenError(errorResponse));
                        })
                    )
                );
            })
        )
    );

    pollRevertChangesSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.pollRevertChangesSuccess),
            map((action) => action.response),
            filter((response) => response.request.complete),
            switchMap(() => of(FlowActions.stopPollingRevertChanges()))
        )
    );

    revertChangesComplete$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.revertChangesComplete),
            map((action) => action.response),
            switchMap((response) =>
                from(this.flowService.deleteRevertChangesUpdateRequest(response.request.requestId)).pipe(
                    map(() => FlowActions.reloadFlow()),
                    catchError((errorResponse: HttpErrorResponse) => of(this.snackBarOrFullScreenError(errorResponse)))
                )
            )
        )
    );

    downloadFlow$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.downloadFlow),
                map((action) => action.request),
                tap((request) => {
                    this.flowService.downloadFlow(request);
                })
            ),
        { dispatch: false }
    );

    moveToFront$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.moveToFront),
            map((action) => action.request),
            concatLatestFrom((request) => this.store.select(selectMaxZIndex(request.componentType))),
            // Bump up the index if it's less or same as the max.
            // In addition of zIndex order there's also a DOM order of components, which might have the same zIndex.
            filter(([request, maxZIndex]) => request.zIndex <= maxZIndex),
            switchMap(([request, maxZIndex]) => {
                const updateRequest: UpdateComponentRequest = {
                    id: request.id,
                    type: request.componentType,
                    uri: request.uri,
                    payload: {
                        revision: request.revision,
                        disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
                        component: {
                            id: request.id,
                            zIndex: maxZIndex + 1
                        }
                    },
                    errorStrategy: 'snackbar'
                };

                return from(this.flowService.updateComponent(updateRequest)).pipe(
                    map((response) => {
                        const updateResponse: UpdateComponentResponse = {
                            id: updateRequest.id,
                            type: updateRequest.type,
                            response
                        };
                        return FlowActions.updateComponentSuccess({ response: updateResponse });
                    }),
                    catchError((errorResponse: HttpErrorResponse) => {
                        const updateComponentFailure: UpdateComponentFailure = {
                            id: updateRequest.id,
                            type: updateRequest.type,
                            errorStrategy: updateRequest.errorStrategy,
                            restoreOnFailure: updateRequest.restoreOnFailure,
                            errorResponse
                        };
                        return of(FlowActions.updateComponentFailure({ response: updateComponentFailure }));
                    })
                );
            })
        )
    );

    openChangeProcessorVersionDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.openChangeProcessorVersionDialog),
                map((action) => action.request),
                switchMap((request) =>
                    from(this.extensionTypesService.getProcessorVersionsForType(request.type, request.bundle)).pipe(
                        map(
                            (response) =>
                                ({
                                    fetchRequest: request,
                                    componentVersions: response.processorTypes
                                }) as OpenChangeComponentVersionDialogRequest
                        ),
                        tap({
                            error: (errorResponse: HttpErrorResponse) => {
                                this.store.dispatch(this.snackBarOrFullScreenError(errorResponse));
                            }
                        })
                    )
                ),
                tap((request) => {
                    const dialogRequest = this.dialog.open(ChangeComponentVersionDialog, {
                        ...LARGE_DIALOG,
                        data: request,
                        autoFocus: false
                    });

                    dialogRequest.componentInstance.changeVersion.pipe(take(1)).subscribe((newVersion) => {
                        this.store.dispatch(
                            FlowActions.updateProcessor({
                                request: {
                                    id: request.fetchRequest.id,
                                    uri: request.fetchRequest.uri,
                                    type: ComponentType.Processor,
                                    payload: {
                                        component: {
                                            bundle: newVersion.bundle,
                                            id: request.fetchRequest.id
                                        },
                                        revision: request.fetchRequest.revision
                                    },
                                    errorStrategy: 'snackbar'
                                }
                            })
                        );
                        dialogRequest.close();
                    });
                })
            ),
        { dispatch: false }
    );

    changeColor$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.openChangeColorDialog),
                map((action) => action.request),
                tap((request) => {
                    const dialogRef = this.dialog.open(ChangeColorDialog, {
                        ...SMALL_DIALOG,
                        data: request
                    });

                    dialogRef.componentInstance.changeColor.pipe(take(1)).subscribe((requests) => {
                        requests.forEach((request) => {
                            let style: any = {};
                            if (request.style) {
                                style = { ...request.style };
                            }
                            if (request.type === ComponentType.Processor) {
                                if (request.color) {
                                    style['background-color'] = request.color;
                                } else {
                                    // for processors, removing the background-color from the style map effectively unsets it
                                    delete style['background-color'];
                                }
                                this.store.dispatch(
                                    FlowActions.updateProcessor({
                                        request: {
                                            id: request.id,
                                            type: ComponentType.Processor,
                                            errorStrategy: 'snackbar',
                                            uri: request.uri,
                                            payload: {
                                                revision: request.revision,
                                                component: {
                                                    id: request.id,
                                                    style
                                                }
                                            }
                                        }
                                    })
                                );
                            } else if (request.type === ComponentType.Label) {
                                if (request.color) {
                                    style['background-color'] = request.color;
                                } else {
                                    // for labels, removing the setting the background-color style effectively unsets it
                                    style['background-color'] = null;
                                }
                                this.store.dispatch(
                                    FlowActions.updateComponent({
                                        request: {
                                            id: request.id,
                                            type: ComponentType.Label,
                                            errorStrategy: 'snackbar',
                                            uri: request.uri,
                                            payload: {
                                                revision: request.revision,
                                                component: {
                                                    id: request.id,
                                                    style
                                                }
                                            }
                                        }
                                    })
                                );
                            }
                        });
                        dialogRef.close();
                    });
                })
            ),
        { dispatch: false }
    );
}
