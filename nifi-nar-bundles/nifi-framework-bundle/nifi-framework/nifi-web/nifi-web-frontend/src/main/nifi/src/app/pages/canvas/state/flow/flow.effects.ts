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

import { Injectable } from '@angular/core';
import { FlowService } from '../../service/flow.service';
import { Actions, createEffect, ofType } from '@ngrx/effects';
import * as FlowActions from './flow.actions';
import { navigateWithoutTransform, updateProcessor } from './flow.actions';
import {
    asyncScheduler,
    catchError,
    combineLatest,
    filter,
    from,
    interval,
    map,
    mergeMap,
    Observable,
    of,
    switchMap,
    take,
    takeUntil,
    tap,
    withLatestFrom
} from 'rxjs';
import {
    DeleteComponentResponse,
    LoadProcessGroupRequest,
    LoadProcessGroupResponse,
    Snippet,
    UpdateComponentFailure,
    UpdateComponentResponse
} from './index';
import { Store } from '@ngrx/store';
import {
    selectAnySelectedComponentIds,
    selectCurrentProcessGroupId,
    selectParentProcessGroupId,
    selectSaving
} from './flow.selectors';
import { ConnectionManager } from '../../service/manager/connection-manager.service';
import { MatDialog } from '@angular/material/dialog';
import { CreatePort } from '../../ui/port/create-port/create-port.component';
import { EditPort } from '../../ui/port/edit-port/edit-port.component';
import { ComponentType, NewPropertyDialogRequest, NewPropertyDialogResponse, Property } from '../../../../state/shared';
import { Router } from '@angular/router';
import { selectUrl } from '../../../../state/router/router.selectors';
import { Client } from '../../../../service/client.service';
import { CanvasUtils } from '../../service/canvas-utils.service';
import { CanvasView } from '../../service/canvas-view.service';
import { selectProcessorTypes } from '../../../../state/extension-types/extension-types.selectors';
import { NiFiState } from '../../../../state';
import { CreateProcessor } from '../../ui/processor/create-processor/create-processor.component';
import { EditProcessor } from '../../ui/processor/edit-processor/edit-processor.component';
import { NewPropertyDialog } from '../../../../ui/common/new-property-dialog/new-property-dialog.component';

@Injectable()
export class FlowEffects {
    constructor(
        private actions$: Actions,
        private store: Store<NiFiState>,
        private flowService: FlowService,
        private client: Client,
        private canvasUtils: CanvasUtils,
        private canvasView: CanvasView,
        private connectionManager: ConnectionManager,
        private router: Router,
        private dialog: MatDialog
    ) {}

    reloadFlow$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.reloadFlow),
            withLatestFrom(this.store.select(selectCurrentProcessGroupId)),
            switchMap(([action, processGroupId]) => {
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
            switchMap((request: LoadProcessGroupRequest) =>
                combineLatest([
                    this.flowService.getFlow(request.id),
                    this.flowService.getFlowStatus(),
                    this.flowService.getClusterSummary(),
                    this.flowService.getControllerBulletins()
                ]).pipe(
                    map(([flow, flowStatus, clusterSummary, controllerBulletins]) => {
                        return FlowActions.loadProcessGroupSuccess({
                            response: {
                                id: request.id,
                                flow: flow,
                                flowStatus: flowStatus,
                                clusterSummary: clusterSummary.clusterSummary,
                                controllerBulletins: controllerBulletins
                            }
                        });
                    }),
                    catchError((error) => of(FlowActions.flowApiError({ error: error.error })))
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
            switchMap((request) => of(FlowActions.reloadFlow()))
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
                    case ComponentType.Funnel:
                        return of(FlowActions.createFunnel({ request }));
                    case ComponentType.Label:
                        return of(FlowActions.createLabel({ request }));
                    case ComponentType.InputPort:
                    case ComponentType.OutputPort:
                        return of(FlowActions.openNewPortDialog({ request }));
                    default:
                        return of(FlowActions.flowApiError({ error: 'Unsupported type of Component.' }));
                }
            })
        )
    );

    openNewProcessorDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.openNewProcessorDialog),
                map((action) => action.request),
                withLatestFrom(this.store.select(selectProcessorTypes)),
                tap(([request, processorTypes]) => {
                    this.dialog
                        .open(CreateProcessor, {
                            data: {
                                request,
                                processorTypes
                            },
                            panelClass: 'medium-dialog'
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
            withLatestFrom(this.store.select(selectCurrentProcessGroupId)),
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
                    catchError((error) => of(FlowActions.flowApiError({ error: error.error })))
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
                            data: request,
                            panelClass: 'small-dialog'
                        })
                        .afterClosed()
                        .subscribe(() => {
                            this.store.dispatch(FlowActions.setDragging({ dragging: false }));
                            this.store.dispatch(FlowActions.clearFlowApiError());
                        });
                })
            ),
        { dispatch: false }
    );

    createPort$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.createPort),
            map((action) => action.request),
            withLatestFrom(this.store.select(selectCurrentProcessGroupId)),
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
                    catchError((error) => of(FlowActions.flowApiError({ error: error.error })))
                )
            )
        )
    );

    createFunnel$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.createFunnel),
            map((action) => action.request),
            withLatestFrom(this.store.select(selectCurrentProcessGroupId)),
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
                    catchError((error) => of(FlowActions.flowApiError({ error: error.error })))
                )
            )
        )
    );

    createLabel$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.createLabel),
            map((action) => action.request),
            withLatestFrom(this.store.select(selectCurrentProcessGroupId)),
            switchMap(([request, processGroupId]) =>
                from(this.flowService.createLabel(processGroupId, request)).pipe(
                    map((response) =>
                        FlowActions.createComponentSuccess({
                            response: {
                                type: request.type,
                                payload: response
                            }
                        })
                    ),
                    catchError((error) => of(FlowActions.flowApiError({ error: error.error })))
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

                return of(
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
                );
            })
        )
    );

    navigateToEditComponent$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.navigateToEditComponent),
                map((action) => action.request),
                withLatestFrom(this.store.select(selectCurrentProcessGroupId)),
                tap(([request, processGroupId]) => {
                    this.router.navigate(['/process-groups', processGroupId, request.type, request.id, 'edit']);
                })
            ),
        { dispatch: false }
    );

    editComponentRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.editComponent),
            map((action) => action.request),
            switchMap((request) => {
                switch (request.type) {
                    case ComponentType.Processor:
                        return of(FlowActions.openEditProcessorDialog({ request }));
                    case ComponentType.InputPort:
                    case ComponentType.OutputPort:
                        return of(FlowActions.openEditPortDialog({ request }));
                    default:
                        return of(FlowActions.flowApiError({ error: 'Unsupported type of Component.' }));
                }
            })
        )
    );

    openEditPortDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.openEditPortDialog),
                map((action) => action.request),
                withLatestFrom(this.store.select(selectUrl)),
                tap(([request, currentUrl]) => {
                    this.dialog
                        .open(EditPort, {
                            data: request,
                            panelClass: 'medium-dialog'
                        })
                        .afterClosed()
                        .subscribe(() => {
                            // determine the parent url (TODO: not sure how best to access
                            // the current activated route for use in navigate below). Could
                            // possible subscribe to router events but router-state does not
                            // seem to surface the activated route
                            const url: string[] = currentUrl.split('/');
                            url.pop();

                            this.store.dispatch(FlowActions.clearFlowApiError());
                            this.router.navigate(url);
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
                withLatestFrom(this.store.select(selectUrl)),
                tap(([request, currentUrl]) => {
                    const editDialogReference = this.dialog.open(EditProcessor, {
                        data: request,
                        panelClass: 'large-dialog'
                    });

                    editDialogReference.componentInstance.saving$ = this.store.select(selectSaving);

                    editDialogReference.componentInstance.createNewProperty = (
                        existingProperties: string[],
                        allowsSensitive: boolean
                    ): Observable<Property> => {
                        const dialogRequest: NewPropertyDialogRequest = { existingProperties, allowsSensitive };
                        const newPropertyDialogReference = this.dialog.open(NewPropertyDialog, {
                            data: dialogRequest,
                            panelClass: 'small-dialog'
                        });

                        return newPropertyDialogReference.componentInstance.newProperty.pipe(
                            take(1),
                            switchMap((dialogResponse: NewPropertyDialogResponse) => {
                                return this.flowService
                                    .getPropertyDescriptor(
                                        request.entity.id,
                                        dialogResponse.name,
                                        dialogResponse.sensitive
                                    )
                                    .pipe(
                                        take(1),
                                        map((response) => {
                                            newPropertyDialogReference.close();

                                            return {
                                                property: dialogResponse.name,
                                                value: null,
                                                descriptor: response.propertyDescriptor
                                            };
                                        })
                                    );
                            })
                        );
                    };

                    editDialogReference.componentInstance.getServiceLink = (serviceId: string) => {
                        return this.flowService.getControllerService(serviceId).pipe(
                            take(1),
                            map((serviceEntity) => {
                                // TODO - finalize once route is defined
                                return [
                                    '/process-groups',
                                    serviceEntity.component.parentGroupId,
                                    'controller-services',
                                    serviceEntity.id
                                ];
                            })
                        );
                    };

                    // TODO - inline service creation...

                    editDialogReference.componentInstance.editProcessor.pipe(take(1)).subscribe((payload: any) => {
                        this.store.dispatch(
                            updateProcessor({
                                request: {
                                    id: request.entity.id,
                                    uri: request.uri,
                                    type: request.type,
                                    payload
                                }
                            })
                        );
                    });

                    editDialogReference.afterClosed().subscribe(() => {
                        this.store.dispatch(FlowActions.clearFlowApiError());
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
                            response: response
                        };
                        return FlowActions.updateComponentSuccess({ response: updateComponentResponse });
                    }),
                    catchError((error) => {
                        const updateComponentFailure: UpdateComponentFailure = {
                            id: request.id,
                            type: request.type,
                            restoreOnFailure: request.restoreOnFailure,
                            error: error.error
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
            switchMap((response) => of(FlowActions.flowApiError({ error: response.error })))
        )
    );

    updateProcessor$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.updateProcessor),
            map((action) => action.request),
            mergeMap((request) =>
                from(this.flowService.updateComponent(request)).pipe(
                    map((response) => {
                        const updateComponentResponse: UpdateComponentResponse = {
                            requestId: request.requestId,
                            id: request.id,
                            type: request.type,
                            response: response
                        };
                        return FlowActions.updateProcessorSuccess({ response: updateComponentResponse });
                    }),
                    catchError((error) => {
                        const updateComponentFailure: UpdateComponentFailure = {
                            id: request.id,
                            type: request.type,
                            restoreOnFailure: request.restoreOnFailure,
                            error: error.error
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
            tap(() => {
                this.dialog.closeAll();
            }),
            map((action) => action.response),
            switchMap((response) => of(FlowActions.loadConnectionsForComponent({ id: response.id })))
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
                    catchError((error) => of(FlowActions.flowApiError({ error })))
                )
            )
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

    updatePositionComplete$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.updatePositionComplete),
                map((action) => action.response),
                tap((response) => {
                    this.connectionManager.renderConnectionForComponent(response.id, {
                        updatePath: true,
                        updateLabel: true
                    });
                })
            ),
        { dispatch: false }
    );

    deleteComponent$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.deleteComponents),
            map((action) => action.request),
            withLatestFrom(this.store.select(selectCurrentProcessGroupId)),
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
                            }

                            return FlowActions.deleteComponentsSuccess({
                                response: deleteResponses
                            });
                        }),
                        catchError((error) => of(FlowActions.flowApiError({ error: error.error })))
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

                    return from(this.flowService.createSnippet(snippet)).pipe(
                        switchMap((response) => this.flowService.deleteSnippet(response.snippet.id)),
                        map((response) => {
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
                        catchError((error) => of(FlowActions.flowApiError({ error: error.error })))
                    );
                }
            })
        )
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
                withLatestFrom(this.store.select(selectParentProcessGroupId)),
                filter(([action, parentProcessGroupId]) => parentProcessGroupId != null),
                tap(([action, parentProcessGroupId]) => {
                    this.router.navigate(['/process-groups', parentProcessGroupId]);
                })
            ),
        { dispatch: false }
    );

    addSelectedComponents$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.addSelectedComponents),
            map((action) => action.request),
            withLatestFrom(
                this.store.select(selectCurrentProcessGroupId),
                this.store.select(selectAnySelectedComponentIds)
            ),
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
                return of(navigateWithoutTransform({ url: commands }));
            })
        )
    );

    removeSelectedComponents$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.removeSelectedComponents),
            map((action) => action.request),
            withLatestFrom(
                this.store.select(selectCurrentProcessGroupId),
                this.store.select(selectAnySelectedComponentIds)
            ),
            switchMap(([request, processGroupId, selected]) => {
                let commands: string[];
                if (selected.length === 0) {
                    commands = ['/process-groups', processGroupId];
                } else {
                    const idsToRemove: string[] = request.components.map((selectedComponent) => selectedComponent.id);
                    const ids: string[] = selected.filter((id) => !idsToRemove.includes(id));
                    commands = ['/process-groups', processGroupId, 'bulk', ids.join(',')];
                }
                return of(navigateWithoutTransform({ url: commands }));
            })
        )
    );

    selectComponents$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.selectComponents),
            map((action) => action.request),
            withLatestFrom(this.store.select(selectCurrentProcessGroupId)),
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
                return of(navigateWithoutTransform({ url: commands }));
            })
        )
    );

    deselectAllComponent$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.deselectAllComponents),
            withLatestFrom(this.store.select(selectCurrentProcessGroupId)),
            switchMap(([action, processGroupId]) => {
                return of(navigateWithoutTransform({ url: ['/process-groups', processGroupId] }));
            })
        )
    );

    navigateToComponent$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.navigateToComponent),
                map((action) => action.request),
                withLatestFrom(this.store.select(selectCurrentProcessGroupId)),
                tap(([request, processGroupId]) => {
                    this.router.navigate(['/process-groups', processGroupId, request.type, request.id]);
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

    centerSelectedComponent$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.centerSelectedComponent),
                tap(() => {
                    this.canvasView.centerSelectedComponent();
                })
            ),
        { dispatch: false }
    );
}
