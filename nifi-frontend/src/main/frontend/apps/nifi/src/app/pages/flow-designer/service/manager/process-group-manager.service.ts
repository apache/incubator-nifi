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

import { Injectable, OnDestroy } from '@angular/core';
import { CanvasState } from '../../state';
import { Store } from '@ngrx/store';
import { PositionBehavior } from '../behavior/position-behavior.service';
import { SelectableBehavior } from '../behavior/selectable-behavior.service';
import { EditableBehavior } from '../behavior/editable-behavior.service';
import * as d3 from 'd3';
import {
    selectFlowLoadingStatus,
    selectProcessGroups,
    selectAnySelectedComponentIds,
    selectTransitionRequired,
    selectRegistryClients
} from '../../state/flow/flow.selectors';
import { CanvasUtils } from '../canvas-utils.service';
import { enterProcessGroup } from '../../state/flow/flow.actions';
import { VersionControlTip } from '../../ui/common/tooltips/version-control-tip/version-control-tip.component';
import { Dimension } from '../../state/shared';
import { filter, Subject, switchMap, takeUntil } from 'rxjs';
import { ComponentType, NiFiCommon, TextTip } from '@nifi/shared';

@Injectable({
    providedIn: 'root'
})
export class ProcessGroupManager implements OnDestroy {
    private destroyed$: Subject<boolean> = new Subject();
    private registryClients = this.store.selectSignal(selectRegistryClients);

    private dimensions: Dimension = {
        width: 384,
        height: 176
    };

    // attempt of space between component count and icon for process group contents
    private static readonly CONTENTS_SPACER: number = 10;
    private static readonly CONTENTS_VALUE_SPACER: number = 5;
    private static readonly PREVIEW_NAME_LENGTH: number = 30;

    private processGroups: [] = [];
    private processGroupContainer: any = null;
    private transitionRequired = false;

    constructor(
        private store: Store<CanvasState>,
        private canvasUtils: CanvasUtils,
        private nifiCommon: NiFiCommon,
        private positionBehavior: PositionBehavior,
        private selectableBehavior: SelectableBehavior,
        private editableBehavior: EditableBehavior
    ) {}

    private select() {
        return this.processGroupContainer.selectAll('g.process-group').data(this.processGroups, function (d: any) {
            return d.id;
        });
    }

    private renderProcessGroups(entered: any) {
        if (entered.empty()) {
            return entered;
        }
        const self: ProcessGroupManager = this;

        const processGroup = entered
            .append('g')
            .attr('id', function (d: any) {
                return 'id-' + d.id;
            })
            .attr('class', 'process-group component');

        // ----
        // body
        // ----

        // process group border
        processGroup
            .append('rect')
            .attr('class', 'border')
            .attr('width', function (d: any) {
                return d.dimensions.width;
            })
            .attr('height', function (d: any) {
                return d.dimensions.height;
            })
            .attr('fill', 'transparent')
            .attr('stroke', 'transparent');

        // process group body
        processGroup
            .append('rect')
            .attr('class', 'body')
            .attr('width', function (d: any) {
                return d.dimensions.width;
            })
            .attr('height', function (d: any) {
                return d.dimensions.height;
            })
            .attr('filter', 'url(#component-drop-shadow)')
            .attr('stroke-width', 0);

        // process group name background
        processGroup
            .append('rect')
            .attr('class', 'process-group-banner')
            .attr('width', function (d: any) {
                return d.dimensions.width;
            })
            .attr('height', 32);

        // process group name
        processGroup
            .append('text')
            .attr('x', 10)
            .attr('y', 20)
            .attr('width', 300)
            .attr('height', 16)
            .attr('class', 'process-group-name secondary-contrast');

        // process group name
        processGroup.append('text').attr('x', 10).attr('y', 21).attr('class', 'version-control');

        // always support selecting and navigation
        processGroup.on('dblclick', function (event: MouseEvent, d: any) {
            // enter this group on double click
            self.store.dispatch(
                enterProcessGroup({
                    request: {
                        id: d.id
                    }
                })
            );
        });

        this.selectableBehavior.activate(processGroup);

        // only support dragging, connection, and drag and drop if appropriate
        processGroup
            .filter(function (d: any) {
                return d.permissions.canWrite && d.permissions.canRead;
            })
            .on('mouseover.drop', function (this: any) {
                // Using mouseover/out to workaround chrome issue #122746
                // get the target and ensure it's not already been marked for drop
                const target: any = d3.select(this);
                if (!target.classed('drop')) {
                    const targetData: any = target.datum();

                    // see if there is a selection being dragged
                    const drag = d3.select('rect.drag-selection');
                    if (!drag.empty()) {
                        // filter the current selection by this group
                        const selection = self.canvasUtils.getSelection().filter(function (d: any) {
                            return targetData.id === d.id;
                        });

                        // ensure this group isn't in the selection
                        if (selection.empty()) {
                            // mark that we are hovering over a drop area if appropriate
                            target.classed('drop', function () {
                                // get the current selection and ensure its disconnected
                                return self.canvasUtils.isDisconnected(self.canvasUtils.getSelection());
                            });
                        }
                    }
                }
            })
            .on('mouseout.drop', function (this: any) {
                // mark that we are no longer hovering over a drop area unconditionally
                d3.select(this).classed('drop', false);
            });

        return processGroup;
    }

    private updateProcessGroups(updated: any): void {
        if (updated.empty()) {
            return;
        }

        const self: ProcessGroupManager = this;

        // funnel border authorization
        updated.select('rect.border').classed('unauthorized', function (d: any) {
            return d.permissions.canRead === false;
        });

        // funnel body authorization
        updated.select('rect.body').classed('unauthorized', function (d: any) {
            return d.permissions.canRead === false;
        });

        updated.each(function (this: any, processGroupData: any) {
            const processGroup: any = d3.select(this);
            let details: any = processGroup.select('g.process-group-details');

            // update the component behavior as appropriate
            self.editableBehavior.editable(processGroup);

            // if this processor is visible, render everything
            if (processGroup.classed('visible')) {
                if (details.empty()) {
                    details = processGroup.append('g').attr('class', 'process-group-details');

                    // -------------------
                    // contents background
                    // -------------------

                    details
                        .append('rect')
                        .attr('class', 'process-group-details-banner banner')
                        .attr('x', 0)
                        .attr('y', 32)
                        .attr('width', function () {
                            return processGroupData.dimensions.width;
                        })
                        .attr('height', 24);

                    details
                        .append('rect')
                        .attr('class', 'process-group-details-banner banner')
                        .attr('x', 0)
                        .attr('y', function () {
                            return processGroupData.dimensions.height - 24;
                        })
                        .attr('width', function () {
                            return processGroupData.dimensions.width;
                        })
                        .attr('height', 24);

                    // --------
                    // contents
                    // --------

                    // transmitting icon
                    details
                        .append('text')
                        .attr('x', 10)
                        .attr('y', 49)
                        .attr('class', 'process-group-transmitting process-group-contents-icon secondary-color')
                        .attr('font-family', 'FontAwesome')
                        .text('\uf140')
                        .append('title')
                        .text('Transmitting Remote Process Groups');

                    // transmitting count
                    details
                        .append('text')
                        .attr('y', 49)
                        .attr('class', 'process-group-transmitting-count process-group-contents-count');

                    // not transmitting icon
                    details
                        .append('text')
                        .attr('y', 49)
                        .attr('class', 'process-group-not-transmitting process-group-contents-icon secondary-color')
                        .attr('font-family', 'flowfont')
                        .text('\ue80a')
                        .append('title')
                        .text('Not Transmitting Remote Process Groups');

                    // not transmitting count
                    details
                        .append('text')
                        .attr('y', 49)
                        .attr('class', 'process-group-not-transmitting-count process-group-contents-count');

                    // running icon
                    details
                        .append('text')
                        .attr('y', 49)
                        .attr('class', 'process-group-running process-group-contents-icon secondary-color')
                        .attr('font-family', 'FontAwesome')
                        .text('\uf04b')
                        .append('title')
                        .text('Running Components');

                    // running count
                    details
                        .append('text')
                        .attr('y', 49)
                        .attr('class', 'process-group-running-count process-group-contents-count');

                    // stopped icon
                    details
                        .append('text')
                        .attr('y', 49)
                        .attr('class', 'process-group-stopped process-group-contents-icon secondary-color')
                        .attr('font-family', 'FontAwesome')
                        .text('\uf04d')
                        .append('title')
                        .text('Stopped Components');

                    // stopped count
                    details
                        .append('text')
                        .attr('y', 49)
                        .attr('class', 'process-group-stopped-count process-group-contents-count');

                    // invalid icon
                    details
                        .append('text')
                        .attr('y', 49)
                        .attr('class', 'process-group-invalid process-group-contents-icon secondary-color')
                        .attr('font-family', 'FontAwesome')
                        .text('\uf071')
                        .append('title')
                        .text('Invalid Components');

                    // invalid count
                    details
                        .append('text')
                        .attr('y', 49)
                        .attr('class', 'process-group-invalid-count process-group-contents-count');

                    // disabled icon
                    details
                        .append('text')
                        .attr('y', 49)
                        .attr('class', 'process-group-disabled process-group-contents-icon secondary-color')
                        .attr('font-family', 'flowfont')
                        .text('\ue802')
                        .append('title')
                        .text('Disabled Components');

                    // disabled count
                    details
                        .append('text')
                        .attr('y', 49)
                        .attr('class', 'process-group-disabled-count process-group-contents-count');

                    // up to date icon
                    details
                        .append('text')
                        .attr('x', 10)
                        .attr('y', function () {
                            return processGroupData.dimensions.height - 7;
                        })
                        .attr('class', 'process-group-up-to-date process-group-contents-icon secondary-color')
                        .attr('font-family', 'FontAwesome')
                        .text('\uf00c')
                        .append('title')
                        .text('Up to date Versioned Process Groups');

                    // up to date count
                    details
                        .append('text')
                        .attr('y', function () {
                            return processGroupData.dimensions.height - 7;
                        })
                        .attr('class', 'process-group-up-to-date-count process-group-contents-count');

                    // locally modified icon
                    details
                        .append('text')
                        .attr('y', function () {
                            return processGroupData.dimensions.height - 7;
                        })
                        .attr('class', 'process-group-locally-modified process-group-contents-icon secondary-color')
                        .attr('font-family', 'FontAwesome')
                        .text('\uf069')
                        .append('title')
                        .text('Locally modified Versioned Process Groups');

                    // locally modified count
                    details
                        .append('text')
                        .attr('y', function () {
                            return processGroupData.dimensions.height - 7;
                        })
                        .attr('class', 'process-group-locally-modified-count process-group-contents-count');

                    // stale icon
                    details
                        .append('text')
                        .attr('y', function () {
                            return processGroupData.dimensions.height - 7;
                        })
                        .attr('class', 'process-group-stale process-group-contents-icon secondary-color')
                        .attr('font-family', 'FontAwesome')
                        .text('\uf0aa')
                        .append('title')
                        .text('Stale Versioned Process Groups');

                    // stale count
                    details
                        .append('text')
                        .attr('y', function () {
                            return processGroupData.dimensions.height - 7;
                        })
                        .attr('class', 'process-group-stale-count process-group-contents-count');

                    // locally modified and stale icon
                    details
                        .append('text')
                        .attr('y', function () {
                            return processGroupData.dimensions.height - 7;
                        })
                        .attr(
                            'class',
                            'process-group-locally-modified-and-stale process-group-contents-icon secondary-color'
                        )
                        .attr('font-family', 'FontAwesome')
                        .text('\uf06a')
                        .append('title')
                        .text('Locally modified and stale Versioned Process Groups');

                    // locally modified and stale count
                    details
                        .append('text')
                        .attr('y', function () {
                            return processGroupData.dimensions.height - 7;
                        })
                        .attr('class', 'process-group-locally-modified-and-stale-count process-group-contents-count');

                    // sync failure icon
                    details
                        .append('text')
                        .attr('y', function () {
                            return processGroupData.dimensions.height - 7;
                        })
                        .attr('class', 'process-group-sync-failure process-group-contents-icon secondary-color')
                        .attr('font-family', 'FontAwesome')
                        .text('\uf128')
                        .append('title')
                        .text('Sync failure Versioned Process Groups');

                    // sync failure count
                    details
                        .append('text')
                        .attr('y', function () {
                            return processGroupData.dimensions.height - 7;
                        })
                        .attr('class', 'process-group-sync-failure-count process-group-contents-count');

                    // ----------------
                    // stats background
                    // ----------------

                    // queued
                    details
                        .append('rect')
                        .attr('class', 'process-group-queued-stats odd')
                        .attr('width', function () {
                            return processGroupData.dimensions.width;
                        })
                        .attr('height', 19)
                        .attr('x', 0)
                        .attr('y', 66);

                    // border
                    details
                        .append('rect')
                        .attr('class', 'process-group-stats-border')
                        .attr('width', function () {
                            return processGroupData.dimensions.width;
                        })
                        .attr('height', 1)
                        .attr('x', 0)
                        .attr('y', 84);

                    // in
                    details
                        .append('rect')
                        .attr('class', 'process-group-stats-in-out even')
                        .attr('width', function () {
                            return processGroupData.dimensions.width;
                        })
                        .attr('height', 19)
                        .attr('x', 0)
                        .attr('y', 85);

                    // border
                    details
                        .append('rect')
                        .attr('class', 'process-group-stats-border')
                        .attr('width', function () {
                            return processGroupData.dimensions.width;
                        })
                        .attr('height', 1)
                        .attr('x', 0)
                        .attr('y', 103);

                    // read/write
                    details
                        .append('rect')
                        .attr('class', 'process-group-read-write-stats odd')
                        .attr('width', function () {
                            return processGroupData.dimensions.width;
                        })
                        .attr('height', 19)
                        .attr('x', 0)
                        .attr('y', 104);

                    // border
                    details
                        .append('rect')
                        .attr('class', 'process-group-stats-border')
                        .attr('width', function () {
                            return processGroupData.dimensions.width;
                        })
                        .attr('height', 1)
                        .attr('x', 0)
                        .attr('y', 122);

                    // out
                    details
                        .append('rect')
                        .attr('class', 'process-group-stats-in-out even')
                        .attr('width', function () {
                            return processGroupData.dimensions.width;
                        })
                        .attr('height', 19)
                        .attr('x', 0)
                        .attr('y', 123);

                    // -----
                    // stats
                    // -----

                    // stats label container
                    const processGroupStatsLabel = details.append('g').attr('transform', 'translate(6, 75)');

                    // queued label
                    processGroupStatsLabel
                        .append('text')
                        .attr('width', 73)
                        .attr('height', 10)
                        .attr('x', 4)
                        .attr('y', 5)
                        .attr('class', 'stats-label')
                        .text('Queued');

                    // in label
                    processGroupStatsLabel
                        .append('text')
                        .attr('width', 73)
                        .attr('height', 10)
                        .attr('x', 4)
                        .attr('y', 24)
                        .attr('class', 'stats-label')
                        .text('In');

                    // read/write label
                    processGroupStatsLabel
                        .append('text')
                        .attr('width', 73)
                        .attr('height', 10)
                        .attr('x', 4)
                        .attr('y', 42)
                        .attr('class', 'stats-label')
                        .text('Read/Write');

                    // out label
                    processGroupStatsLabel
                        .append('text')
                        .attr('width', 73)
                        .attr('height', 10)
                        .attr('x', 4)
                        .attr('y', 60)
                        .attr('class', 'stats-label')
                        .text('Out');

                    // stats value container
                    const processGroupStatsValue = details.append('g').attr('transform', 'translate(95, 75)');

                    // queued value
                    const queuedText = processGroupStatsValue
                        .append('text')
                        .attr('width', 180)
                        .attr('height', 10)
                        .attr('x', 4)
                        .attr('y', 5)
                        .attr('class', 'process-group-queued stats-value');

                    // queued count
                    queuedText.append('tspan').attr('class', 'count');

                    // queued size
                    queuedText.append('tspan').attr('class', 'size');

                    // in value
                    const inText = processGroupStatsValue
                        .append('text')
                        .attr('width', 180)
                        .attr('height', 10)
                        .attr('x', 4)
                        .attr('y', 24)
                        .attr('class', 'process-group-in stats-value');

                    // in count
                    inText.append('tspan').attr('class', 'count');

                    // in size
                    inText.append('tspan').attr('class', 'size');

                    // in
                    inText.append('tspan').attr('class', 'ports');

                    // in (remote)
                    inText.append('tspan').attr('class', 'public-ports');

                    // read/write value
                    processGroupStatsValue
                        .append('text')
                        .attr('width', 180)
                        .attr('height', 10)
                        .attr('x', 4)
                        .attr('y', 42)
                        .attr('class', 'process-group-read-write stats-value');

                    // out value
                    const outText = processGroupStatsValue
                        .append('text')
                        .attr('width', 180)
                        .attr('height', 10)
                        .attr('x', 4)
                        .attr('y', 60)
                        .attr('class', 'process-group-out stats-value');

                    // out ports
                    outText.append('tspan').attr('class', 'ports');

                    // out ports (remote)
                    outText.append('tspan').attr('class', 'public-ports');

                    // out count
                    outText.append('tspan').attr('class', 'count');

                    // out size
                    outText.append('tspan').attr('class', 'size');

                    // stats value container
                    const processGroupStatsInfo = details.append('g').attr('transform', 'translate(335, 75)');

                    // in info
                    processGroupStatsInfo
                        .append('text')
                        .attr('width', 25)
                        .attr('height', 10)
                        .attr('x', 4)
                        .attr('y', 24)
                        .attr('class', 'stats-info')
                        .text('5 min');

                    // read/write info
                    processGroupStatsInfo
                        .append('text')
                        .attr('width', 25)
                        .attr('height', 10)
                        .attr('x', 4)
                        .attr('y', 42)
                        .attr('class', 'stats-info')
                        .text('5 min');

                    // out info
                    processGroupStatsInfo
                        .append('text')
                        .attr('width', 25)
                        .attr('height', 10)
                        .attr('x', 4)
                        .attr('y', 60)
                        .attr('class', 'stats-info')
                        .text('5 min');

                    // --------
                    // comments
                    // --------

                    details
                        .append('text')
                        .attr('class', 'component-comments')
                        .attr(
                            'transform',
                            'translate(' +
                                (processGroupData.dimensions.width - 11) +
                                ', ' +
                                (processGroupData.dimensions.height - 3) +
                                ')'
                        )
                        .text('\uf075');

                    // -------------------
                    // active thread count
                    // -------------------

                    // active thread count
                    details.append('text').attr('class', 'active-thread-count-icon').attr('y', 20).text('\ue83f');

                    // active thread icon
                    details.append('text').attr('class', 'active-thread-count').attr('y', 20);

                    // ---------
                    // bulletins
                    // ---------

                    // bulletin background
                    details
                        .append('rect')
                        .attr('class', 'bulletin-background')
                        .attr('x', function () {
                            return processGroupData.dimensions.width - 24;
                        })
                        .attr('y', 32)
                        .attr('width', 24)
                        .attr('height', 24);

                    // bulletin icon
                    details
                        .append('text')
                        .attr('class', 'bulletin-icon')
                        .attr('x', function () {
                            return processGroupData.dimensions.width - 17;
                        })
                        .attr('y', 49)
                        .text('\uf24a');
                }

                // update transmitting
                const transmitting = details
                    .select('text.process-group-transmitting')
                    .classed('success-color-variant', function (d: any) {
                        return d.permissions.canRead && d.activeRemotePortCount > 0;
                    })
                    .classed('zero', function (d: any) {
                        return d.permissions.canRead && d.activeRemotePortCount === 0;
                    });
                const transmittingCount = details
                    .select('text.process-group-transmitting-count')
                    .attr('x', function () {
                        const transmittingCountX = parseInt(transmitting.attr('x'), 10);
                        return (
                            transmittingCountX +
                            Math.round(transmitting.node().getComputedTextLength()) +
                            ProcessGroupManager.CONTENTS_VALUE_SPACER
                        );
                    })
                    .text(function (d: any) {
                        return d.activeRemotePortCount;
                    });
                transmittingCount.append('title').text('Transmitting Remote Process Groups');

                // update not transmitting
                const notTransmitting = details
                    .select('text.process-group-not-transmitting')
                    .classed('not-transmitting neutral-color', function (d: any) {
                        return d.permissions.canRead && d.inactiveRemotePortCount > 0;
                    })
                    .classed('zero', function (d: any) {
                        return d.permissions.canRead && d.inactiveRemotePortCount === 0;
                    })
                    .attr('x', function () {
                        const transmittingX = parseInt(transmittingCount.attr('x'), 10);
                        return (
                            transmittingX +
                            Math.round(transmittingCount.node().getComputedTextLength()) +
                            ProcessGroupManager.CONTENTS_SPACER
                        );
                    });
                const notTransmittingCount = details
                    .select('text.process-group-not-transmitting-count')
                    .attr('x', function () {
                        const notTransmittingCountX = parseInt(notTransmitting.attr('x'), 10);
                        return (
                            notTransmittingCountX +
                            Math.round(notTransmitting.node().getComputedTextLength()) +
                            ProcessGroupManager.CONTENTS_VALUE_SPACER
                        );
                    })
                    .text(function (d: any) {
                        return d.inactiveRemotePortCount;
                    });
                notTransmittingCount.append('title').text('Not transmitting Remote Process Groups');

                // update running
                const running = details
                    .select('text.process-group-running')
                    .classed('success-color-default', function (d: any) {
                        return d.permissions.canRead && d.component.runningCount > 0;
                    })
                    .classed('zero', function (d: any) {
                        return d.permissions.canRead && d.component.runningCount === 0;
                    })
                    .attr('x', function () {
                        const notTransmittingX = parseInt(notTransmittingCount.attr('x'), 10);
                        return (
                            notTransmittingX +
                            Math.round(notTransmittingCount.node().getComputedTextLength()) +
                            ProcessGroupManager.CONTENTS_SPACER
                        );
                    });
                const runningCount = details
                    .select('text.process-group-running-count')
                    .attr('x', function () {
                        const runningCountX = parseInt(running.attr('x'), 10);
                        return (
                            runningCountX +
                            Math.round(running.node().getComputedTextLength()) +
                            ProcessGroupManager.CONTENTS_VALUE_SPACER
                        );
                    })
                    .text(function (d: any) {
                        return d.runningCount;
                    });
                runningCount.append('title').text('Running Components');

                // update stopped
                const stopped = details
                    .select('text.process-group-stopped')
                    .classed('error-color-variant', function (d: any) {
                        return d.permissions.canRead && d.component.stoppedCount > 0;
                    })
                    .classed('zero', function (d: any) {
                        return d.permissions.canRead && d.component.stoppedCount === 0;
                    })
                    .attr('x', function () {
                        const runningX = parseInt(runningCount.attr('x'), 10);
                        return (
                            runningX +
                            Math.round(runningCount.node().getComputedTextLength()) +
                            ProcessGroupManager.CONTENTS_SPACER
                        );
                    });
                const stoppedCount = details
                    .select('text.process-group-stopped-count')
                    .attr('x', function () {
                        const stoppedCountX = parseInt(stopped.attr('x'), 10);
                        return (
                            stoppedCountX +
                            Math.round(stopped.node().getComputedTextLength()) +
                            ProcessGroupManager.CONTENTS_VALUE_SPACER
                        );
                    })
                    .text(function (d: any) {
                        return d.stoppedCount;
                    });
                stoppedCount.append('title').text('Stopped Components');

                // update invalid
                const invalid = details
                    .select('text.process-group-invalid')
                    .classed('invalid caution-color', function (d: any) {
                        return d.permissions.canRead && d.component.invalidCount > 0;
                    })
                    .classed('zero', function (d: any) {
                        return d.permissions.canRead && d.component.invalidCount === 0;
                    })
                    .attr('x', function () {
                        const stoppedX = parseInt(stoppedCount.attr('x'), 10);
                        return (
                            stoppedX +
                            Math.round(stoppedCount.node().getComputedTextLength()) +
                            ProcessGroupManager.CONTENTS_SPACER
                        );
                    });
                const invalidCount = details
                    .select('text.process-group-invalid-count')
                    .attr('x', function () {
                        const invalidCountX = parseInt(invalid.attr('x'), 10);
                        return (
                            invalidCountX +
                            Math.round(invalid.node().getComputedTextLength()) +
                            ProcessGroupManager.CONTENTS_VALUE_SPACER
                        );
                    })
                    .text(function (d: any) {
                        return d.invalidCount;
                    });
                invalidCount.append('title').text('Invalid Components');

                // update disabled
                const disabled = details
                    .select('text.process-group-disabled')
                    .classed('disabled neutral-color', function (d: any) {
                        return d.permissions.canRead && d.component.disabledCount > 0;
                    })
                    .classed('zero', function (d: any) {
                        return d.permissions.canRead && d.component.disabledCount === 0;
                    })
                    .attr('x', function () {
                        const invalidX = parseInt(invalidCount.attr('x'), 10);
                        return (
                            invalidX +
                            Math.round(invalidCount.node().getComputedTextLength()) +
                            ProcessGroupManager.CONTENTS_SPACER
                        );
                    });
                const disabledCount = details
                    .select('text.process-group-disabled-count')
                    .attr('x', function () {
                        const disabledCountX = parseInt(disabled.attr('x'), 10);
                        return (
                            disabledCountX +
                            Math.round(disabled.node().getComputedTextLength()) +
                            ProcessGroupManager.CONTENTS_VALUE_SPACER
                        );
                    })
                    .text(function (d: any) {
                        return d.disabledCount;
                    });
                disabledCount.append('title').text('Disabled Components');

                // up to date current
                const upToDate = details
                    .select('text.process-group-up-to-date')
                    .classed('success-color-variant', function (d: any) {
                        return d.permissions.canRead && d.component.upToDateCount > 0;
                    })
                    .classed('zero', function (d: any) {
                        return d.permissions.canRead && d.component.upToDateCount === 0;
                    });
                const upToDateCount = details
                    .select('text.process-group-up-to-date-count')
                    .attr('x', function () {
                        const updateToDateCountX = parseInt(upToDate.attr('x'), 10);
                        return (
                            updateToDateCountX +
                            Math.round(upToDate.node().getComputedTextLength()) +
                            ProcessGroupManager.CONTENTS_VALUE_SPACER
                        );
                    })
                    .text(function (d: any) {
                        return d.upToDateCount;
                    });
                upToDateCount.append('title').text('Up to date Versioned Process Groups');

                // update locally modified
                const locallyModified = details
                    .select('text.process-group-locally-modified')
                    .classed('neutral-color', function (d: any) {
                        return d.permissions.canRead && d.component.locallyModifiedCount > 0;
                    })
                    .classed('zero', function (d: any) {
                        return d.permissions.canRead && d.component.locallyModifiedCount === 0;
                    })
                    .attr('x', function () {
                        const upToDateX = parseInt(upToDateCount.attr('x'), 10);
                        return (
                            upToDateX +
                            Math.round(upToDateCount.node().getComputedTextLength()) +
                            ProcessGroupManager.CONTENTS_SPACER
                        );
                    });
                const locallyModifiedCount = details
                    .select('text.process-group-locally-modified-count')
                    .attr('x', function () {
                        const locallyModifiedCountX = parseInt(locallyModified.attr('x'), 10);
                        return (
                            locallyModifiedCountX +
                            Math.round(locallyModified.node().getComputedTextLength()) +
                            ProcessGroupManager.CONTENTS_VALUE_SPACER
                        );
                    })
                    .text(function (d: any) {
                        return d.locallyModifiedCount;
                    });
                locallyModifiedCount.append('title').text('Locally modified Versioned Process Groups');

                // update stale
                const stale = details
                    .select('text.process-group-stale')
                    .classed('error-color-variant', function (d: any) {
                        return d.permissions.canRead && d.component.staleCount > 0;
                    })
                    .classed('zero', function (d: any) {
                        return d.permissions.canRead && d.component.staleCount === 0;
                    })
                    .attr('x', function () {
                        const locallyModifiedX = parseInt(locallyModifiedCount.attr('x'), 10);
                        return (
                            locallyModifiedX +
                            Math.round(locallyModifiedCount.node().getComputedTextLength()) +
                            ProcessGroupManager.CONTENTS_SPACER
                        );
                    });
                const staleCount = details
                    .select('text.process-group-stale-count')
                    .attr('x', function () {
                        const staleCountX = parseInt(stale.attr('x'), 10);
                        return (
                            staleCountX +
                            Math.round(stale.node().getComputedTextLength()) +
                            ProcessGroupManager.CONTENTS_VALUE_SPACER
                        );
                    })
                    .text(function (d: any) {
                        return d.staleCount;
                    });
                staleCount.append('title').text('Stale Versioned Process Groups');

                // update locally modified and stale
                const locallyModifiedAndStale = details
                    .select('text.process-group-locally-modified-and-stale')
                    .classed('error-color-variant', function (d: any) {
                        return d.permissions.canRead && d.component.locallyModifiedAndStaleCount > 0;
                    })
                    .classed('zero', function (d: any) {
                        return d.permissions.canRead && d.component.locallyModifiedAndStaleCount === 0;
                    })
                    .attr('x', function () {
                        const staleX = parseInt(staleCount.attr('x'), 10);
                        return (
                            staleX +
                            Math.round(staleCount.node().getComputedTextLength()) +
                            ProcessGroupManager.CONTENTS_SPACER
                        );
                    });
                const locallyModifiedAndStaleCount = details
                    .select('text.process-group-locally-modified-and-stale-count')
                    .attr('x', function () {
                        const locallyModifiedAndStaleCountX = parseInt(locallyModifiedAndStale.attr('x'), 10);
                        return (
                            locallyModifiedAndStaleCountX +
                            Math.round(locallyModifiedAndStale.node().getComputedTextLength()) +
                            ProcessGroupManager.CONTENTS_VALUE_SPACER
                        );
                    })
                    .text(function (d: any) {
                        return d.locallyModifiedAndStaleCount;
                    });
                locallyModifiedAndStaleCount
                    .append('title')
                    .text('Locally modified and stale Versioned Process Groups');

                // update sync failure
                const syncFailure = details
                    .select('text.process-group-sync-failure')
                    .classed('neutral-color', function (d: any) {
                        return d.permissions.canRead && d.component.syncFailureCount > 0;
                    })
                    .classed('zero', function (d: any) {
                        return d.permissions.canRead && d.component.syncFailureCount === 0;
                    })
                    .attr('x', function () {
                        const syncFailureX = parseInt(locallyModifiedAndStaleCount.attr('x'), 10);
                        return (
                            syncFailureX +
                            Math.round(locallyModifiedAndStaleCount.node().getComputedTextLength()) +
                            ProcessGroupManager.CONTENTS_SPACER -
                            2
                        );
                    });
                const syncFailureCount = details
                    .select('text.process-group-sync-failure-count')
                    .attr('x', function () {
                        const syncFailureCountX = parseInt(syncFailure.attr('x'), 10);
                        return (
                            syncFailureCountX +
                            Math.round(syncFailure.node().getComputedTextLength()) +
                            ProcessGroupManager.CONTENTS_VALUE_SPACER
                        );
                    })
                    .text(function (d: any) {
                        return d.syncFailureCount;
                    });
                syncFailureCount.append('title').text('Sync failure Versioned Process Groups');

                // update version control information
                const versionControl = processGroup
                    .select('text.version-control')
                    .style('visibility', self.isUnderVersionControl(processGroupData) ? 'visible' : 'hidden')
                    .attr('class', function () {
                        if (self.isUnderVersionControl(processGroupData)) {
                            const vciState = processGroupData.versionedFlowState;
                            if (vciState === 'SYNC_FAILURE') {
                                return `version-control neutral-color`;
                            } else if (vciState === 'LOCALLY_MODIFIED_AND_STALE') {
                                return `version-control error-color-variant`;
                            } else if (vciState === 'STALE') {
                                return `version-control error-color-variant`;
                            } else if (vciState === 'LOCALLY_MODIFIED') {
                                return `version-control neutral-color`;
                            } else {
                                // up to date
                                return `version-control success-color-default`;
                            }
                        } else {
                            return 'version-control neutral-contrast';
                        }
                    })
                    .text(function () {
                        if (self.isUnderVersionControl(processGroupData)) {
                            const vciState = processGroupData.versionedFlowState;
                            if (vciState === 'SYNC_FAILURE') {
                                return '\uf128';
                            } else if (vciState === 'LOCALLY_MODIFIED_AND_STALE') {
                                return '\uf06a';
                            } else if (vciState === 'STALE') {
                                return '\uf0aa';
                            } else if (vciState === 'LOCALLY_MODIFIED') {
                                return '\uf069';
                            } else {
                                return '\uf00c';
                            }
                        } else {
                            return '';
                        }
                    });

                if (processGroupData.permissions.canRead) {
                    // version control tooltip
                    versionControl.each(function (this: any) {
                        if (self.isUnderVersionControl(processGroupData)) {
                            self.canvasUtils.canvasTooltip(VersionControlTip, d3.select(this), {
                                versionControlInformation: processGroupData.component.versionControlInformation,
                                registryClients: self.registryClients()
                            });
                        } else {
                            self.canvasUtils.resetCanvasTooltip(d3.select(this));
                        }
                    });

                    // update the process group comments
                    processGroup
                        .select('text.component-comments')
                        .style(
                            'visibility',
                            self.nifiCommon.isBlank(processGroupData.component.comments) ? 'hidden' : 'visible'
                        )
                        .each(function (this: any) {
                            if (!self.nifiCommon.isBlank(processGroupData.component.comments)) {
                                self.canvasUtils.canvasTooltip(
                                    TextTip,
                                    d3.select(this),
                                    processGroupData.component.comments
                                );
                            } else {
                                self.canvasUtils.resetCanvasTooltip(d3.select(this));
                            }
                        });

                    // update the process group name
                    processGroup
                        .select('text.process-group-name')
                        .attr('x', function () {
                            if (self.isUnderVersionControl(processGroupData)) {
                                const versionControlX = parseInt(versionControl.attr('x'), 10);
                                return (
                                    versionControlX +
                                    Math.round(versionControl.node().getComputedTextLength()) +
                                    ProcessGroupManager.CONTENTS_VALUE_SPACER
                                );
                            } else {
                                return 10;
                            }
                        })
                        .attr('width', function (this: any) {
                            if (self.isUnderVersionControl(processGroupData)) {
                                const versionControlX = parseInt(versionControl.attr('x'), 10);
                                const processGroupNameX = parseInt(d3.select(this).attr('x'), 10);
                                return 300 - (processGroupNameX - versionControlX);
                            } else {
                                return 300;
                            }
                        })
                        .each(function (this: any, d: any) {
                            const processGroupName = d3.select(this);

                            // reset the process group name to handle any previous state
                            processGroupName.text(null).selectAll('title').remove();

                            // apply ellipsis to the process group name as necessary
                            self.canvasUtils.ellipsis(processGroupName, d.component.name, 'group-name');
                        })
                        .append('title')
                        .text(function (d: any) {
                            return d.component.name;
                        });
                } else {
                    // clear the process group comments
                    processGroup.select('text.component-comments').style('visibility', 'hidden');

                    // clear the process group name
                    processGroup.select('text.process-group-name').attr('x', 10).attr('width', 316).text(null);
                }

                // populate the stats
                self.updateProcessGroupStatus(processGroup);
            } else {
                if (processGroupData.permissions.canRead) {
                    // update the process group name
                    processGroup.select('text.process-group-name').text(function (d: any) {
                        const name = d.component.name;
                        if (name.length > ProcessGroupManager.PREVIEW_NAME_LENGTH) {
                            return (
                                name.substring(0, ProcessGroupManager.PREVIEW_NAME_LENGTH) + String.fromCharCode(8230)
                            );
                        } else {
                            return name;
                        }
                    });
                } else {
                    // clear the process group name
                    processGroup.select('text.process-group-name').text(null);
                }

                // remove the details if necessary
                if (!details.empty()) {
                    details.remove();
                }
            }
        });
    }

    private updateProcessGroupStatus(updated: any) {
        if (updated.empty()) {
            return;
        }
        const self: ProcessGroupManager = this;

        // queued count value
        updated.select('text.process-group-queued tspan.count').text(function (d: any) {
            return self.nifiCommon.substringBeforeFirst(d.status.aggregateSnapshot.queued, ' ');
        });

        // queued size value
        updated.select('text.process-group-queued tspan.size').text(function (d: any) {
            return ' ' + self.nifiCommon.substringAfterFirst(d.status.aggregateSnapshot.queued, ' ');
        });

        // in count value
        updated.select('text.process-group-in tspan.count').text(function (d: any) {
            return self.nifiCommon.substringBeforeFirst(d.status.aggregateSnapshot.input, ' ');
        });

        // in size value
        updated.select('text.process-group-in tspan.size').text(function (d: any) {
            return ' ' + self.nifiCommon.substringAfterFirst(d.status.aggregateSnapshot.input, ' ');
        });

        // in ports value
        updated.select('text.process-group-in tspan.ports').text(function (d: any) {
            return ' ' + String.fromCharCode(8594) + ' ' + d.inputPortCount;
        });

        // in ports value (remote)
        updated.select('text.process-group-in tspan.public-ports').text(function (d: any) {
            return d.publicInputPortCount > 0 ? ' (' + d.publicInputPortCount + ' remote)' : '';
        });

        // read/write value
        updated.select('text.process-group-read-write').text(function (d: any) {
            return d.status.aggregateSnapshot.read + ' / ' + d.status.aggregateSnapshot.written;
        });

        // out ports value
        updated.select('text.process-group-out tspan.ports').text(function (d: any) {
            return d.outputPortCount;
        });

        // out ports value (remote)
        updated.select('text.process-group-out tspan.public-ports').text(function (d: any) {
            return d.publicOutputPortCount > 0 ? ' (' + d.publicOutputPortCount + ' remote) ' : '';
        });

        // out count value
        updated.select('text.process-group-out tspan.count').text(function (d: any) {
            return (
                ' ' +
                String.fromCharCode(8594) +
                ' ' +
                self.nifiCommon.substringBeforeFirst(d.status.aggregateSnapshot.output, ' ')
            );
        });

        // out size value
        updated.select('text.process-group-out tspan.size').text(function (d: any) {
            return ' ' + self.nifiCommon.substringAfterFirst(d.status.aggregateSnapshot.output, ' ');
        });

        updated.each(function (this: any, d: any) {
            const processGroup = d3.select(this);

            // -------------------
            // active thread count
            // -------------------

            self.canvasUtils.activeThreadCount(processGroup, d);

            // ---------
            // bulletins
            // ---------

            self.canvasUtils.bulletins(processGroup, d.bulletins);
        });
    }

    private removeProcessGroups(removed: any) {
        if (removed.empty()) {
            return;
        }

        removed.remove();
    }

    private isUnderVersionControl(d: any): boolean {
        return !!d.versionedFlowState;
    }

    public init(): void {
        this.processGroupContainer = d3
            .select('#canvas')
            .append('g')
            .attr('pointer-events', 'all')
            .attr('class', 'process-groups');

        this.store
            .select(selectProcessGroups)
            .pipe(
                filter(() => this.processGroupContainer !== null),
                takeUntil(this.destroyed$)
            )
            .subscribe((processGroups) => {
                this.set(processGroups);
            });

        this.store
            .select(selectFlowLoadingStatus)
            .pipe(
                filter((status) => status === 'success'),
                filter(() => this.processGroupContainer !== null),
                switchMap(() => this.store.select(selectAnySelectedComponentIds)),
                takeUntil(this.destroyed$)
            )
            .subscribe((selected) => {
                this.processGroupContainer.selectAll('g.process-group').classed('selected', function (d: any) {
                    return selected.includes(d.id);
                });
            });

        this.store
            .select(selectTransitionRequired)
            .pipe(takeUntil(this.destroyed$))
            .subscribe((transitionRequired) => {
                this.transitionRequired = transitionRequired;
            });
    }

    public destroy(): void {
        this.processGroupContainer = null;
        this.destroyed$.next(true);
    }

    ngOnDestroy(): void {
        this.destroyed$.complete();
    }

    private set(processGroups: any): void {
        // update the process groups
        this.processGroups = processGroups.map((processGroup: any) => {
            return {
                ...processGroup,
                type: ComponentType.ProcessGroup,
                dimensions: this.dimensions
            };
        });

        // select
        const selection = this.select();

        // enter
        const entered = this.renderProcessGroups(selection.enter());

        // update
        const updated = selection.merge(entered);
        this.updateProcessGroups(updated);

        // position
        this.positionBehavior.position(updated, this.transitionRequired);

        // exit
        this.removeProcessGroups(selection.exit());
    }

    public selectAll(): any {
        return this.processGroupContainer.selectAll('g.process-group');
    }

    public render(): void {
        this.updateProcessGroups(this.selectAll());
    }

    public pan(): void {
        this.updateProcessGroups(
            this.processGroupContainer.selectAll('g.process-group.entering, g.process-group.leaving')
        );
    }
}
