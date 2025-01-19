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

import { Component, HostListener, OnDestroy, OnInit } from '@angular/core';
import { CanvasState } from '../../state';
import { Position } from '../../state/shared';
import { Store } from '@ngrx/store';
import {
    centerSelectedComponents,
    deselectAllComponents,
    editComponent,
    editCurrentProcessGroup,
    loadProcessGroup,
    paste,
    resetFlowState,
    selectComponents,
    setSkipTransform,
    startProcessGroupPolling,
    stopProcessGroupPolling
} from '../../state/flow/flow.actions';
import * as d3 from 'd3';
import { CanvasView } from '../../service/canvas-view.service';
import { INITIAL_SCALE, INITIAL_TRANSLATE } from '../../state/transform/transform.reducer';
import { selectTransform } from '../../state/transform/transform.selectors';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { SelectedComponent } from '../../state/flow';
import {
    selectAllowTransition,
    selectBulkSelectedComponentIds,
    selectConnection,
    selectCurrentProcessGroupId,
    selectEditedCurrentProcessGroup,
    selectFlowAnalysisOpen,
    selectFlowLoadingStatus,
    selectFunnel,
    selectInputPort,
    selectLabel,
    selectOutputPort,
    selectProcessGroup,
    selectProcessGroupIdFromRoute,
    selectProcessGroupRoute,
    selectProcessor,
    selectRemoteProcessGroup,
    selectSingleEditedComponent,
    selectSingleSelectedComponent,
    selectSkipTransform,
    selectViewStatusHistoryComponent,
    selectViewStatusHistoryCurrentProcessGroup
} from '../../state/flow/flow.selectors';
import { distinctUntilChanged, filter, map, NEVER, switchMap, take } from 'rxjs';
import { restoreViewport } from '../../state/transform/transform.actions';
import { initialState } from '../../state/flow/flow.reducer';
import { CanvasContextMenu } from '../../service/canvas-context-menu.service';
import { getStatusHistoryAndOpenDialog } from '../../../../state/status-history/status-history.actions';
import { concatLatestFrom } from '@ngrx/operators';
import { ComponentType, isDefinedAndNotNull, NiFiCommon, selectUrl, Storage } from '@nifi/shared';
import { CanvasUtils } from '../../service/canvas-utils.service';
import { CanvasActionsService } from '../../service/canvas-actions.service';
import { MatDialog } from '@angular/material/dialog';
import { CopyResponseEntity } from '../../../../state/copy';
import { snackBarError } from '../../../../state/error/error.actions';

@Component({
    selector: 'fd-canvas',
    templateUrl: './canvas.component.html',
    styleUrls: ['./canvas.component.scss']
})
export class Canvas implements OnInit, OnDestroy {
    private svg: any;
    private canvas: any;

    private scale: number = INITIAL_SCALE;
    private canvasClicked = false;

    flowAnalysisOpen = this.store.selectSignal(selectFlowAnalysisOpen);

    constructor(
        private store: Store<CanvasState>,
        private canvasView: CanvasView,
        private storage: Storage,
        private canvasUtils: CanvasUtils,
        public canvasContextMenu: CanvasContextMenu,
        private canvasActionsService: CanvasActionsService,
        private dialog: MatDialog,
        public nifiCommon: NiFiCommon
    ) {
        this.store
            .select(selectTransform)
            .pipe(takeUntilDestroyed())
            .subscribe((transform) => {
                this.scale = transform.scale;
            });

        this.store
            .select(selectUrl)
            .pipe(takeUntilDestroyed())
            .subscribe((route) => {
                if (!route.endsWith('/edit') && !route.endsWith('/history')) {
                    this.storage.setItem('current-canvas-route', route);
                }
            });

        // load the process group from the route
        this.store
            .select(selectProcessGroupIdFromRoute)
            .pipe(
                filter((processGroupId) => processGroupId != null),
                takeUntilDestroyed()
            )
            .subscribe((processGroupId) => {
                this.store.dispatch(
                    loadProcessGroup({
                        request: {
                            id: processGroupId,
                            transitionRequired: false
                        }
                    })
                );
            });

        // handle process group loading and viewport restoration
        this.store
            .select(selectFlowLoadingStatus)
            .pipe(
                filter((status) => status === 'complete'),
                switchMap(() => this.store.select(selectCurrentProcessGroupId)),
                distinctUntilChanged(),
                switchMap(() => this.store.select(selectProcessGroupRoute)),
                filter((processGroupRoute) => processGroupRoute != null),
                concatLatestFrom(() => this.store.select(selectSkipTransform)),
                takeUntilDestroyed()
            )
            .subscribe(([, skipTransform]) => {
                if (skipTransform) {
                    this.store.dispatch(setSkipTransform({ skipTransform: false }));
                } else {
                    this.store.dispatch(restoreViewport());
                }
            });

        // handle single component selection
        this.store
            .select(selectFlowLoadingStatus)
            .pipe(
                filter((status) => status === 'complete'),
                switchMap(() => this.store.select(selectCurrentProcessGroupId)),
                distinctUntilChanged(),
                switchMap(() => this.store.select(selectSingleSelectedComponent)),
                filter((selectedComponent) => selectedComponent != null),
                concatLatestFrom(() => [
                    this.store.select(selectSkipTransform),
                    this.store.select(selectAllowTransition)
                ]),
                takeUntilDestroyed()
            )
            .subscribe(([, skipTransform, allowTransition]) => {
                if (skipTransform) {
                    this.store.dispatch(setSkipTransform({ skipTransform: false }));
                } else {
                    this.store.dispatch(centerSelectedComponents({ request: { allowTransition } }));
                }
            });

        // handle bulk component selection
        this.store
            .select(selectFlowLoadingStatus)
            .pipe(
                filter((status) => status === 'complete'),
                switchMap(() => this.store.select(selectCurrentProcessGroupId)),
                distinctUntilChanged(),
                switchMap(() => this.store.select(selectBulkSelectedComponentIds)),
                filter((ids) => ids.length > 0),
                concatLatestFrom(() => [
                    this.store.select(selectSkipTransform),
                    this.store.select(selectAllowTransition)
                ]),
                takeUntilDestroyed()
            )
            .subscribe(([, skipTransform, allowTransition]) => {
                if (skipTransform) {
                    this.store.dispatch(setSkipTransform({ skipTransform: false }));
                } else {
                    this.store.dispatch(centerSelectedComponents({ request: { allowTransition } }));
                }
            });

        // handle component edit
        this.store
            .select(selectCurrentProcessGroupId)
            .pipe(
                filter((processGroupId) => processGroupId != initialState.id),
                switchMap(() => this.store.select(selectSingleEditedComponent)),
                isDefinedAndNotNull(),
                switchMap((selectedComponent) => {
                    let component$;
                    switch (selectedComponent.componentType) {
                        case ComponentType.Processor:
                            component$ = this.store.select(selectProcessor(selectedComponent.id));
                            break;
                        case ComponentType.InputPort:
                            component$ = this.store.select(selectInputPort(selectedComponent.id));
                            break;
                        case ComponentType.OutputPort:
                            component$ = this.store.select(selectOutputPort(selectedComponent.id));
                            break;
                        case ComponentType.ProcessGroup:
                            component$ = this.store.select(selectProcessGroup(selectedComponent.id));
                            break;
                        case ComponentType.RemoteProcessGroup:
                            component$ = this.store.select(selectRemoteProcessGroup(selectedComponent.id));
                            break;
                        case ComponentType.Connection:
                            component$ = this.store.select(selectConnection(selectedComponent.id));
                            break;
                        case ComponentType.Funnel:
                            component$ = this.store.select(selectFunnel(selectedComponent.id));
                            break;
                        case ComponentType.Label:
                            component$ = this.store.select(selectLabel(selectedComponent.id));
                            break;
                        default:
                            component$ = NEVER;
                            break;
                    }

                    // combine the original selection with the component
                    return component$.pipe(
                        filter((component) => component != null),
                        take(1),
                        map((component) => [selectedComponent, component])
                    );
                }),
                takeUntilDestroyed()
            )
            .subscribe(([selectedComponent, component]) => {
                // initiate the edit request
                this.store.dispatch(
                    editComponent({
                        request: {
                            type: selectedComponent.componentType,
                            uri: component.uri,
                            entity: component
                        }
                    })
                );
            });

        // edit the current process group from the route
        this.store
            .select(selectEditedCurrentProcessGroup)
            .pipe(
                filter((processGroupId) => processGroupId != null),
                takeUntilDestroyed()
            )
            .subscribe((processGroupId) => {
                this.store.dispatch(
                    editCurrentProcessGroup({
                        request: {
                            id: processGroupId
                        }
                    })
                );
            });

        // Handle status history for a selected component on the canvas
        this.store
            .select(selectCurrentProcessGroupId)
            .pipe(
                filter((processGroupId) => processGroupId != initialState.id),
                switchMap(() => this.store.select(selectViewStatusHistoryComponent)),
                filter((selectedComponent) => selectedComponent != null),
                takeUntilDestroyed()
            )
            .subscribe((component) => {
                if (component) {
                    this.store.dispatch(
                        getStatusHistoryAndOpenDialog({
                            request: {
                                source: 'canvas',
                                componentType: component.componentType,
                                componentId: component.id
                            }
                        })
                    );
                }
            });

        // Handle status history for the current process group
        this.store
            .select(selectViewStatusHistoryCurrentProcessGroup)
            .pipe(isDefinedAndNotNull(), takeUntilDestroyed())
            .subscribe((currentProcessGroupId) => {
                this.store.dispatch(
                    getStatusHistoryAndOpenDialog({
                        request: {
                            source: 'canvas',
                            componentType: ComponentType.ProcessGroup,
                            componentId: currentProcessGroupId
                        }
                    })
                );
            });
    }

    ngOnInit(): void {
        // initialize the canvas svg
        this.createSvg();
        this.canvasView.init(this.svg, this.canvas);

        this.store.dispatch(startProcessGroupPolling());
    }

    private createSvg(): void {
        this.svg = d3
            .select('#canvas-container')
            .append('svg')
            .attr('class', 'canvas-svg')
            .on('contextmenu', (event) => {
                // reset the canvas click flag
                this.canvasClicked = false;

                // if this context menu click was on the canvas (and not a nested
                // element) we need to clear the selection
                if (event.target === this.svg.node()) {
                    this.store.dispatch(deselectAllComponents());
                }
            });

        this.createDefs();

        // initialize the canvas element
        this.initCanvas();
    }

    private createDefs(): void {
        // create the definitions element
        const defs = this.svg.append('defs');

        // create arrow definitions for the various line types
        defs.selectAll('marker')
            .data(['normal', 'ghost', 'unauthorized', 'full'])
            .enter()
            .append('marker')
            .attr('id', (d: string) => {
                return d;
            })
            .attr('viewBox', '0 0 6 6')
            .attr('refX', 5)
            .attr('refY', 3)
            .attr('markerWidth', 6)
            .attr('markerHeight', 6)
            .attr('orient', 'auto')
            .attr('class', (d: string) => {
                if (d === 'ghost') {
                    return 'ghost neutral-color';
                } else if (d === 'unauthorized') {
                    return 'unauthorized error-color';
                } else if (d === 'full') {
                    return 'full error-color';
                } else {
                    return 'neutral-contrast';
                }
            })
            .append('path')
            .attr('d', 'M2,3 L0,6 L6,3 L0,0 z');

        // filter for drop shadow
        const componentDropShadowFilter = defs
            .append('filter')
            .attr('id', 'component-drop-shadow')
            .attr('height', '140%')
            .attr('y', '-20%');

        // blur
        componentDropShadowFilter
            .append('feGaussianBlur')
            .attr('in', 'SourceAlpha')
            .attr('stdDeviation', 3)
            .attr('result', 'blur');

        // offset
        componentDropShadowFilter
            .append('feOffset')
            .attr('in', 'blur')
            .attr('dx', 0)
            .attr('dy', 1)
            .attr('result', 'offsetBlur');

        // color/opacity
        componentDropShadowFilter.append('feFlood').attr('flood-opacity', 0.4).attr('result', 'offsetColor');

        // combine
        componentDropShadowFilter
            .append('feComposite')
            .attr('in', 'offsetColor')
            .attr('in2', 'offsetBlur')
            .attr('operator', 'in')
            .attr('result', 'offsetColorBlur');

        // stack the effect under the source graph
        const componentDropShadowFeMerge = componentDropShadowFilter.append('feMerge');
        componentDropShadowFeMerge.append('feMergeNode').attr('in', 'offsetColorBlur');
        componentDropShadowFeMerge.append('feMergeNode').attr('in', 'SourceGraphic');

        // filter for drop shadow
        const connectionFullDropShadowFilter = defs
            .append('filter')
            .attr('id', 'connection-full-drop-shadow')
            .attr('height', '140%')
            .attr('y', '-20%');

        // blur
        connectionFullDropShadowFilter
            .append('feGaussianBlur')
            .attr('in', 'SourceAlpha')
            .attr('stdDeviation', 3)
            .attr('result', 'blur');

        // offset
        connectionFullDropShadowFilter
            .append('feOffset')
            .attr('in', 'blur')
            .attr('dx', 0)
            .attr('dy', 1)
            .attr('result', 'offsetBlur');

        // color/opacity
        connectionFullDropShadowFilter.append('feFlood').attr('flood-opacity', 1).attr('result', 'offsetColor');

        // combine
        connectionFullDropShadowFilter
            .append('feComposite')
            .attr('in', 'offsetColor')
            .attr('in2', 'offsetBlur')
            .attr('operator', 'in')
            .attr('result', 'offsetColorBlur');

        // stack the effect under the source graph
        const connectionFullFeMerge = connectionFullDropShadowFilter.append('feMerge');
        connectionFullFeMerge.append('feMergeNode').attr('in', 'offsetColorBlur');
        connectionFullFeMerge.append('feMergeNode').attr('in', 'SourceGraphic');
    }

    private initCanvas(): void {
        const t = [INITIAL_TRANSLATE.x, INITIAL_TRANSLATE.y];
        this.canvas = this.svg
            .append('g')
            .attr('transform', 'translate(' + t + ') scale(' + INITIAL_SCALE + ')')
            .attr('pointer-events', 'all')
            .attr('id', 'canvas');

        // handle canvas events
        this.svg
            .on('mousedown.selection', (event: MouseEvent) => {
                this.canvasClicked = true;

                if (event.button !== 0) {
                    // prevent further propagation (to parents and others handlers
                    // on the same element to prevent zoom behavior)
                    event.stopImmediatePropagation();
                    return;
                }

                // show selection box if shift is held down
                if (event.shiftKey) {
                    const position: any = d3.pointer(event, this.canvas.node());
                    this.canvas
                        .append('rect')
                        .attr('rx', 6)
                        .attr('ry', 6)
                        .attr('x', position[0])
                        .attr('y', position[1])
                        .attr('class', 'component-selection')
                        .attr('width', 0)
                        .attr('height', 0)
                        .attr('stroke-width', () => {
                            return 1 / this.scale;
                        })
                        .attr('stroke-dasharray', () => {
                            return 4 / this.scale;
                        })
                        .datum(position);

                    // prevent further propagation (to parents and others handlers
                    // on the same element to prevent zoom behavior)
                    event.stopImmediatePropagation();

                    // prevents the browser from changing to a text selection cursor
                    event.preventDefault();
                }
            })
            .on('mousemove.selection', (event: MouseEvent) => {
                // update selection box if shift is held down
                if (event.shiftKey) {
                    // get the selection box
                    const selectionBox: any = d3.select('rect.component-selection');
                    if (!selectionBox.empty()) {
                        // get the original position
                        const originalPosition: any = selectionBox.datum();
                        const position: any = d3.pointer(event, this.canvas.node());

                        const d: any = {};
                        if (originalPosition[0] < position[0]) {
                            d.x = originalPosition[0];
                            d.width = position[0] - originalPosition[0];
                        } else {
                            d.x = position[0];
                            d.width = originalPosition[0] - position[0];
                        }

                        if (originalPosition[1] < position[1]) {
                            d.y = originalPosition[1];
                            d.height = position[1] - originalPosition[1];
                        } else {
                            d.y = position[1];
                            d.height = originalPosition[1] - position[1];
                        }

                        // update the selection box
                        selectionBox.attr('width', d.width).attr('height', d.height).attr('x', d.x).attr('y', d.y);

                        // prevent further propagation (to parents)
                        event.stopPropagation();
                    }
                }
            })
            .on('mouseup.selection', () => {
                // ensure this originated from clicking the canvas, not a component.
                // when clicking on a component, the event propagation is stopped so
                // it never reaches the canvas. we cannot do this however on up events
                // since the drag events break down
                if (!this.canvasClicked) {
                    return;
                }

                // reset the canvas click flag
                this.canvasClicked = false;

                // get the selection box
                const selectionBox: any = d3.select('rect.component-selection');
                if (!selectionBox.empty()) {
                    const selection: SelectedComponent[] = [];

                    const selectionBoundingBox: any = {
                        x: parseInt(selectionBox.attr('x'), 10),
                        y: parseInt(selectionBox.attr('y'), 10),
                        width: parseInt(selectionBox.attr('width'), 10),
                        height: parseInt(selectionBox.attr('height'), 10)
                    };

                    // see if a component should be selected or not
                    d3.selectAll('g.component').each((d: any, i, nodes) => {
                        const item = nodes[i];
                        // consider it selected if its already selected or enclosed in the bounding box
                        if (
                            d3.select(item).classed('selected') ||
                            (d.position.x >= selectionBoundingBox.x &&
                                d.position.x + d.dimensions.width <=
                                    selectionBoundingBox.x + selectionBoundingBox.width &&
                                d.position.y >= selectionBoundingBox.y &&
                                d.position.y + d.dimensions.height <=
                                    selectionBoundingBox.y + selectionBoundingBox.height)
                        ) {
                            selection.push({
                                id: d.id,
                                componentType: d.type
                            });
                        }
                    });

                    // see if a connection should be selected or not
                    d3.selectAll('g.connection').each((d: any, i, nodes) => {
                        // consider all points
                        const points: Position[] = [d.start].concat(d.bends, [d.end]);

                        // determine the bounding box
                        const x: any = d3.extent(points, (pt: Position) => {
                            return pt.x;
                        });
                        const y: any = d3.extent(points, (pt: Position) => {
                            return pt.y;
                        });

                        const item = nodes[i];
                        // consider it selected if its already selected or enclosed in the bounding box
                        if (
                            d3.select(item).classed('selected') ||
                            (x[0] >= selectionBoundingBox.x &&
                                x[1] <= selectionBoundingBox.x + selectionBoundingBox.width &&
                                y[0] >= selectionBoundingBox.y &&
                                y[1] <= selectionBoundingBox.y + selectionBoundingBox.height)
                        ) {
                            selection.push({
                                id: d.id,
                                componentType: d.type
                            });
                        }
                    });

                    // dispatch the selected components
                    this.store.dispatch(
                        selectComponents({
                            request: {
                                components: selection
                            }
                        })
                    );

                    // remove the selection box
                    selectionBox.remove();
                }
            });
    }

    ngOnDestroy(): void {
        this.store.dispatch(resetFlowState());
        this.store.dispatch(stopProcessGroupPolling());

        this.canvasView.destroy();
    }

    private processKeyboardEvents(event: KeyboardEvent | ClipboardEvent): boolean {
        const source = event.target as any;
        let searchFieldIsEventSource = false;
        if (source) {
            searchFieldIsEventSource = source.classList.contains('search-input') || false;
        }

        return this.dialog.openDialogs.length === 0 && !searchFieldIsEventSource;
    }

    private executeAction(actionId: string, event: KeyboardEvent, bypassCondition?: boolean): boolean {
        if (this.processKeyboardEvents(event)) {
            const selection = this.canvasUtils.getSelection();
            const canvasAction = this.canvasActionsService.getAction(actionId);
            if (canvasAction) {
                if (bypassCondition || canvasAction.condition(selection)) {
                    canvasAction.action(selection);
                    return true;
                }
            }
        }
        return false;
    }

    @HostListener('window:keydown.delete', ['$event'])
    handleKeyDownDelete(event: KeyboardEvent) {
        this.executeAction('delete', event);
    }

    @HostListener('window:keydown.backspace', ['$event'])
    handleKeyDownBackspace(event: KeyboardEvent) {
        this.executeAction('delete', event);
    }

    @HostListener('window:keydown.control.r', ['$event'])
    handleKeyDownCtrlR(event: KeyboardEvent) {
        if (this.executeAction('refresh', event, true)) {
            event.preventDefault();
        }
    }

    @HostListener('window:keydown.meta.r', ['$event'])
    handleKeyDownMetaR(event: KeyboardEvent) {
        if (this.executeAction('refresh', event, true)) {
            event.preventDefault();
        }
    }

    @HostListener('window:keydown.escape', ['$event'])
    handleKeyDownEsc(event: KeyboardEvent) {
        this.executeAction('leaveGroup', event);
    }

    @HostListener('window:keydown.control.c', ['$event'])
    handleKeyDownCtrlC(event: KeyboardEvent) {
        if (this.executeAction('copy', event)) {
            event.preventDefault();
        }
    }

    @HostListener('window:keydown.meta.c', ['$event'])
    handleKeyDownMetaC(event: KeyboardEvent) {
        if (this.executeAction('copy', event)) {
            event.preventDefault();
        }
    }

    @HostListener('window:paste', ['$event'])
    handlePasteEvent(event: ClipboardEvent) {
        if (!this.processKeyboardEvents(event) || !this.canvasUtils.isPastable()) {
            // don't attempt to paste flow content
            return;
        }

        const textToPaste = event.clipboardData?.getData('text/plain');
        if (textToPaste) {
            const copyResponse: CopyResponseEntity | null = this.toCopyResponseEntity(textToPaste);
            if (copyResponse) {
                this.store.dispatch(
                    paste({
                        request: copyResponse
                    })
                );
                event.preventDefault();
            } else {
                this.store.dispatch(snackBarError({ error: 'Cannot paste: incompatible format' }));
            }
        }
    }

    @HostListener('window:keydown.control.a', ['$event'])
    handleKeyDownCtrlA(event: KeyboardEvent) {
        if (this.executeAction('selectAll', event)) {
            event.preventDefault();
        }
    }

    @HostListener('window:keydown.meta.a', ['$event'])
    handleKeyDownMetaA(event: KeyboardEvent) {
        if (this.executeAction('selectAll', event)) {
            event.preventDefault();
        }
    }

    toCopyResponseEntity(json: string): CopyResponseEntity | null {
        try {
            const copyResponse: CopyResponseEntity = JSON.parse(json);
            const supportedKeys: string[] = [
                'processGroups',
                'remoteProcessGroups',
                'processors',
                'inputPorts',
                'outputPorts',
                'connections',
                'labels',
                'funnels'
            ];

            // ensure at least one of the copyable component types has something to paste
            const hasCopiedContent = Object.entries(copyResponse).some((entry) => {
                return supportedKeys.includes(entry[0]) && Array.isArray(entry[1]) && entry[1].length > 0;
            });

            if (hasCopiedContent) {
                return copyResponse;
            }

            // check to see if this is a FlowDefinition
            const maybeFlowDefinition: any = JSON.parse(json);
            const isFlowDefinition = this.nifiCommon.isDefinedAndNotNull(maybeFlowDefinition.flowContents);
            if (isFlowDefinition) {
                // make sure the flow has some components
                const flowHasCopiedContent = Object.entries(maybeFlowDefinition.flowContents).some((entry) => {
                    return supportedKeys.includes(entry[0]) && Array.isArray(entry[1]) && entry[1].length > 0;
                });
                if (flowHasCopiedContent) {
                    // construct a new CopyResponseEntity from the FlowDefinition
                    const copiedFlow: CopyResponseEntity = {
                        id: maybeFlowDefinition.flowContents.identifier,
                        processGroups: [
                            {
                                ...maybeFlowDefinition.flowContents
                            }
                        ]
                    };

                    // include parameter contexts, providers, and external cs if defined
                    if (this.nifiCommon.isDefinedAndNotNull(maybeFlowDefinition.parameterContexts)) {
                        copiedFlow.parameterContexts = { ...maybeFlowDefinition.parameterContexts };
                    }
                    if (this.nifiCommon.isDefinedAndNotNull(maybeFlowDefinition.parameterProviders)) {
                        copiedFlow.parameterProviders = { ...maybeFlowDefinition.parameterProviders };
                    }
                    if (this.nifiCommon.isDefinedAndNotNull(maybeFlowDefinition.externalControllerServices)) {
                        copiedFlow.externalControllerServiceReferences = {
                            ...maybeFlowDefinition.externalControllerServices
                        };
                    }
                    return copiedFlow;
                }
            }

            // attempting to paste something other than CopyResponseEntity or a flow definition
            return null;
        } catch (e) {
            // attempting to paste something other than CopyResponseEntity or a flow definition
            return null;
        }
    }
}
