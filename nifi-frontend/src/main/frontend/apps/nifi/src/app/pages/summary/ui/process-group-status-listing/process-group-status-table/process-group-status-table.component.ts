/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Component, Input } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MatSortModule, Sort } from '@angular/material/sort';
import { MatTableModule } from '@angular/material/table';
import {
    SummaryTableFilter,
    SummaryTableFilterColumn
} from '../../common/summary-table-filter/summary-table-filter.component';
import { NiFiCommon } from '@nifi/shared';
import { RouterLink } from '@angular/router';
import { MatPaginatorModule } from '@angular/material/paginator';
import { ProcessGroupStatusSnapshot, ProcessGroupStatusSnapshotEntity } from '../../../state';
import { ComponentStatusTable } from '../../common/component-status-table/component-status-table.component';
import { MatButtonModule } from '@angular/material/button';
import { MatMenu, MatMenuItem, MatMenuTrigger } from '@angular/material/menu';

export type SupportedColumns =
    | 'name'
    | 'versionedFlowState'
    | 'transferred'
    | 'in'
    | 'readWrite'
    | 'out'
    | 'sent'
    | 'received'
    | 'activeThreads'
    | 'tasks';

@Component({
    selector: 'process-group-status-table',
    imports: [
        CommonModule,
        MatSortModule,
        MatTableModule,
        SummaryTableFilter,
        RouterLink,
        MatPaginatorModule,
        MatButtonModule,
        MatMenu,
        MatMenuItem,
        MatMenuTrigger
    ],
    templateUrl: './process-group-status-table.component.html',
    styleUrls: ['./process-group-status-table.component.scss']
})
export class ProcessGroupStatusTable extends ComponentStatusTable<ProcessGroupStatusSnapshotEntity> {
    filterableColumns: SummaryTableFilterColumn[] = [{ key: 'name', label: 'name' }];

    displayedColumns: string[] = [
        'name',
        'versionedFlowState',
        'transferred',
        'in',
        'readWrite',
        'out',
        'sent',
        'received',
        'activeThreads',
        'tasks',
        'actions'
    ];

    constructor(private nifiCommon: NiFiCommon) {
        super();
    }

    @Input() rootProcessGroup!: ProcessGroupStatusSnapshot;

    override filterPredicate(data: ProcessGroupStatusSnapshotEntity, filter: string): boolean {
        const { filterTerm, filterColumn, filterVersionedFlowState } = JSON.parse(filter);
        const matchOnVersionedFlowState: boolean = filterVersionedFlowState !== 'All';

        if (matchOnVersionedFlowState) {
            if (data.processGroupStatusSnapshot.versionedFlowState !== filterVersionedFlowState) {
                return false;
            }
        }

        if (filterTerm === '') {
            return true;
        }

        const field: string = data.processGroupStatusSnapshot[
            filterColumn as keyof ProcessGroupStatusSnapshot
        ] as string;
        return this.nifiCommon.stringContains(field, filterTerm, true);
    }

    formatName(pg: ProcessGroupStatusSnapshotEntity): string {
        return pg.processGroupStatusSnapshot.name;
    }

    private versionedFlowStateMap: { [key: string]: { classes: string; label: string } } = {
        STALE: {
            classes: 'stale fa fa-arrow-circle-up error-color-variant',
            label: 'Stale'
        },
        LOCALLY_MODIFIED: {
            classes: 'locally-modified fa fa-asterisk neutral-color',
            label: 'Locally modified'
        },
        UP_TO_DATE: {
            classes: 'up-to-date fa fa-check success-color-default',
            label: 'Up to date'
        },
        LOCALLY_MODIFIED_AND_STALE: {
            classes: 'locally-modified-and-stale fa fa-exclamation-circle error-color-variant',
            label: 'Locally modified and stale'
        },
        SYNC_FAILURE: {
            classes: 'sync-failure fa fa-question neutral-color',
            label: 'Sync failure'
        }
    };

    formatVersionedFlowState(pg: ProcessGroupStatusSnapshotEntity): string {
        if (!pg.processGroupStatusSnapshot.versionedFlowState) {
            return '';
        }
        return this.versionedFlowStateMap[pg.processGroupStatusSnapshot.versionedFlowState].label;
    }

    getVersionedFlowStateIcon(pg: ProcessGroupStatusSnapshotEntity): string {
        if (!pg.processGroupStatusSnapshot.versionedFlowState) {
            return '';
        }
        return this.versionedFlowStateMap[pg.processGroupStatusSnapshot.versionedFlowState].classes;
    }

    formatTransferred(pg: ProcessGroupStatusSnapshotEntity): string {
        return pg.processGroupStatusSnapshot.transferred;
    }

    formatIn(pg: ProcessGroupStatusSnapshotEntity): string {
        return pg.processGroupStatusSnapshot.input;
    }

    formatReadWrite(pg: ProcessGroupStatusSnapshotEntity): string {
        return `${pg.processGroupStatusSnapshot.read} | ${pg.processGroupStatusSnapshot.written}`;
    }

    formatOut(pg: ProcessGroupStatusSnapshotEntity): string {
        return pg.processGroupStatusSnapshot.output;
    }

    formatSent(pg: ProcessGroupStatusSnapshotEntity): string {
        return pg.processGroupStatusSnapshot.sent;
    }

    formatReceived(pg: ProcessGroupStatusSnapshotEntity): string {
        return pg.processGroupStatusSnapshot.received;
    }

    formatActiveThreads(pg: ProcessGroupStatusSnapshotEntity): string {
        const percentage: number = this.calculatePercent(
            pg.processGroupStatusSnapshot.activeThreadCount,
            this.rootProcessGroup.activeThreadCount
        );

        return `${pg.processGroupStatusSnapshot.activeThreadCount} (${percentage}%)`;
    }

    formatTasks(pg: ProcessGroupStatusSnapshotEntity): string {
        const percentage: number = this.calculatePercent(
            pg.processGroupStatusSnapshot.processingNanos,
            this.rootProcessGroup.processingNanos
        );

        return `${this.nifiCommon.formatDuration(pg.processGroupStatusSnapshot.processingNanos)} (${percentage}%)`;
    }

    private calculatePercent(used: number, total: number): number {
        if (total !== undefined && total > 0) {
            return Math.round((used / total) * 100);
        }
        return 0;
    }

    override supportsMultiValuedSort(sort: Sort): boolean {
        switch (sort.active) {
            case 'transferred':
            case 'in':
            case 'out':
            case 'readWrite':
            case 'received':
            case 'sent':
            case 'activeThreads':
            case 'tasks':
                return true;
            default:
                return false;
        }
    }

    override sortEntities(data: ProcessGroupStatusSnapshotEntity[], sort: Sort): ProcessGroupStatusSnapshotEntity[] {
        if (!data) {
            return [];
        }

        let aggregateDuration = 0;
        let aggregateActiveThreads = 0;
        if (this.rootProcessGroup) {
            aggregateDuration = this.rootProcessGroup.processingNanos;
            aggregateActiveThreads = this.rootProcessGroup.activeThreadCount;
        }

        return data.slice().sort((a, b) => {
            const isAsc = sort.direction === 'asc';
            let retVal = 0;
            switch (sort.active) {
                case 'name':
                    retVal = this.nifiCommon.compareString(this.formatName(a), this.formatName(b));
                    break;
                case 'versionedFlowState':
                    retVal = this.nifiCommon.compareString(
                        this.formatVersionedFlowState(a),
                        this.formatVersionedFlowState(b)
                    );
                    break;
                case 'transferred':
                    if (this.multiSort.sortValueIndex === 0) {
                        retVal = this.nifiCommon.compareNumber(
                            a.processGroupStatusSnapshot.flowFilesTransferred,
                            b.processGroupStatusSnapshot.flowFilesTransferred
                        );
                    } else {
                        retVal = this.nifiCommon.compareNumber(
                            a.processGroupStatusSnapshot.bytesTransferred,
                            b.processGroupStatusSnapshot.bytesTransferred
                        );
                    }
                    break;
                case 'in':
                    if (this.multiSort.sortValueIndex === 0) {
                        retVal = this.nifiCommon.compareNumber(
                            a.processGroupStatusSnapshot.flowFilesIn,
                            b.processGroupStatusSnapshot.flowFilesIn
                        );
                    } else {
                        retVal = this.nifiCommon.compareNumber(
                            a.processGroupStatusSnapshot.bytesIn,
                            b.processGroupStatusSnapshot.bytesIn
                        );
                    }
                    break;
                case 'out':
                    if (this.multiSort.sortValueIndex === 0) {
                        retVal = this.nifiCommon.compareNumber(
                            a.processGroupStatusSnapshot.flowFilesOut,
                            b.processGroupStatusSnapshot.flowFilesOut
                        );
                    } else {
                        retVal = this.nifiCommon.compareNumber(
                            a.processGroupStatusSnapshot.bytesOut,
                            b.processGroupStatusSnapshot.bytesOut
                        );
                    }
                    break;
                case 'readWrite':
                    if (this.multiSort.sortValueIndex === 0) {
                        retVal = this.nifiCommon.compareNumber(
                            a.processGroupStatusSnapshot.bytesRead,
                            b.processGroupStatusSnapshot.bytesRead
                        );
                    } else {
                        retVal = this.nifiCommon.compareNumber(
                            a.processGroupStatusSnapshot.bytesWritten,
                            b.processGroupStatusSnapshot.bytesWritten
                        );
                    }
                    break;
                case 'sent':
                    if (this.multiSort.sortValueIndex === 0) {
                        retVal = this.nifiCommon.compareNumber(
                            a.processGroupStatusSnapshot.flowFilesSent,
                            b.processGroupStatusSnapshot.flowFilesSent
                        );
                    } else {
                        retVal = this.nifiCommon.compareNumber(
                            a.processGroupStatusSnapshot.bytesSent,
                            b.processGroupStatusSnapshot.bytesSent
                        );
                    }
                    break;
                case 'received':
                    if (this.multiSort.sortValueIndex === 0) {
                        retVal = this.nifiCommon.compareNumber(
                            a.processGroupStatusSnapshot.flowFilesReceived,
                            b.processGroupStatusSnapshot.flowFilesReceived
                        );
                    } else {
                        retVal = this.nifiCommon.compareNumber(
                            a.processGroupStatusSnapshot.bytesReceived,
                            b.processGroupStatusSnapshot.bytesReceived
                        );
                    }
                    break;
                case 'activeThreads':
                    if (this.multiSort.sortValueIndex === 0) {
                        retVal = this.nifiCommon.compareNumber(
                            a.processGroupStatusSnapshot.activeThreadCount,
                            b.processGroupStatusSnapshot.activeThreadCount
                        );
                    } else {
                        retVal = this.nifiCommon.compareNumber(
                            a.processGroupStatusSnapshot.activeThreadCount / aggregateActiveThreads,
                            b.processGroupStatusSnapshot.activeThreadCount / aggregateActiveThreads
                        );
                    }
                    break;
                case 'tasks':
                    if (this.multiSort.sortValueIndex === 0) {
                        retVal = this.nifiCommon.compareNumber(
                            a.processGroupStatusSnapshot.processingNanos,
                            b.processGroupStatusSnapshot.processingNanos
                        );
                    } else {
                        retVal = this.nifiCommon.compareNumber(
                            a.processGroupStatusSnapshot.processingNanos / aggregateDuration,
                            b.processGroupStatusSnapshot.processingNanos / aggregateDuration
                        );
                    }
                    break;
                default:
                    retVal = 0;
            }
            return retVal * (isAsc ? 1 : -1);
        });
    }

    canRead(pg: ProcessGroupStatusSnapshotEntity) {
        return pg.canRead;
    }

    getProcessGroupLink(pg: ProcessGroupStatusSnapshotEntity): string[] {
        return ['/process-groups', pg.id];
    }
}
