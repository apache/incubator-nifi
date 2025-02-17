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

import { Component } from '@angular/core';
import { MatTableModule } from '@angular/material/table';
import { MatSortModule, Sort } from '@angular/material/sort';
import {
    SummaryTableFilter,
    SummaryTableFilterColumn
} from '../../common/summary-table-filter/summary-table-filter.component';
import { RouterLink } from '@angular/router';
import { NgClass } from '@angular/common';
import { ComponentType, NiFiCommon } from '@nifi/shared';
import { MatPaginatorModule } from '@angular/material/paginator';
import { ProcessorStatusSnapshot, ProcessorStatusSnapshotEntity } from '../../../state';
import { ComponentStatusTable } from '../../common/component-status-table/component-status-table.component';
import { MatButtonModule } from '@angular/material/button';
import { MatMenu, MatMenuItem, MatMenuTrigger } from '@angular/material/menu';

export type SupportedColumns = 'name' | 'type' | 'processGroup' | 'runStatus' | 'in' | 'out' | 'readWrite' | 'tasks';

@Component({
    selector: 'processor-status-table',
    templateUrl: './processor-status-table.component.html',
    styleUrls: ['./processor-status-table.component.scss'],
    imports: [
        RouterLink,
        SummaryTableFilter,
        MatTableModule,
        MatSortModule,
        NgClass,
        MatPaginatorModule,
        MatButtonModule,
        MatMenu,
        MatMenuItem,
        MatMenuTrigger
    ]
})
export class ProcessorStatusTable extends ComponentStatusTable<ProcessorStatusSnapshotEntity> {
    filterableColumns: SummaryTableFilterColumn[] = [
        { key: 'name', label: 'name' },
        { key: 'type', label: 'type' }
    ];

    displayedColumns: string[] = [
        'name',
        'type',
        'processGroup',
        'runStatus',
        'in',
        'out',
        'readWrite',
        'tasks',
        'actions'
    ];

    constructor(private nifiCommon: NiFiCommon) {
        super();
    }

    formatName(processor: ProcessorStatusSnapshotEntity): string {
        return processor.processorStatusSnapshot.name;
    }

    formatType(processor: ProcessorStatusSnapshotEntity): string {
        return processor.processorStatusSnapshot.type;
    }

    formatProcessGroup(processor: ProcessorStatusSnapshotEntity): string {
        return processor.processorStatusSnapshot.parentProcessGroupName;
    }

    formatRunStatus(processor: ProcessorStatusSnapshotEntity): string {
        return processor.processorStatusSnapshot.runStatus;
    }

    formatIn(processor: ProcessorStatusSnapshotEntity): string {
        return processor.processorStatusSnapshot.input;
    }

    formatOut(processor: ProcessorStatusSnapshotEntity): string {
        return processor.processorStatusSnapshot.output;
    }

    formatReadWrite(processor: ProcessorStatusSnapshotEntity): string {
        return `${processor.processorStatusSnapshot.read} | ${processor.processorStatusSnapshot.written}`;
    }

    formatTasks(processor: ProcessorStatusSnapshotEntity): string {
        return `${processor.processorStatusSnapshot.tasks} | ${processor.processorStatusSnapshot.tasksDuration}`;
    }

    canRead(processor: ProcessorStatusSnapshotEntity): boolean {
        return processor.canRead;
    }

    getProcessorLink(processor: ProcessorStatusSnapshotEntity): string[] {
        return ['/process-groups', processor.processorStatusSnapshot.groupId, ComponentType.Processor, processor.id];
    }

    getRunStatusIcon(processor: ProcessorStatusSnapshotEntity): string {
        switch (processor.processorStatusSnapshot.runStatus.toLowerCase()) {
            case 'running':
                return 'running fa fa-play success-color-default';
            case 'stopped':
                return 'stopped fa fa-stop error-color-variant';
            case 'enabled':
                return 'enabled fa fa-flash success-color-variant';
            case 'disabled':
                return 'disabled icon icon-enable-false neutral-color';
            case 'validating':
                return 'validating fa fa-spin fa-circle-notch neutral-color';
            case 'invalid':
                return 'invalid fa fa-warning caution-color';
            default:
                return '';
        }
    }

    override sortEntities(data: ProcessorStatusSnapshotEntity[], sort: Sort): ProcessorStatusSnapshotEntity[] {
        if (!data) {
            return [];
        }
        return data.slice().sort((a, b) => {
            const isAsc = sort.direction === 'asc';
            switch (sort.active) {
                case 'name':
                    return this.compare(this.formatName(a), this.formatName(b), isAsc);
                case 'type':
                    return this.compare(this.formatType(a), this.formatType(b), isAsc);
                case 'processGroup':
                    return this.compare(this.formatProcessGroup(a), this.formatProcessGroup(b), isAsc);
                case 'runStatus':
                    return this.compare(this.formatRunStatus(a), this.formatRunStatus(b), isAsc);
                case 'in':
                    if (this.multiSort.sortValueIndex === 0) {
                        return this.compare(
                            a.processorStatusSnapshot.flowFilesIn,
                            b.processorStatusSnapshot.flowFilesIn,
                            isAsc
                        );
                    } else {
                        return this.compare(
                            a.processorStatusSnapshot.bytesIn,
                            b.processorStatusSnapshot.bytesIn,
                            isAsc
                        );
                    }
                case 'out':
                    if (this.multiSort.sortValueIndex === 0) {
                        return this.compare(
                            a.processorStatusSnapshot.flowFilesOut,
                            b.processorStatusSnapshot.flowFilesOut,
                            isAsc
                        );
                    } else {
                        return this.compare(
                            a.processorStatusSnapshot.bytesOut,
                            b.processorStatusSnapshot.bytesOut,
                            isAsc
                        );
                    }
                case 'readWrite':
                    if (this.multiSort.sortValueIndex === 0) {
                        return this.compare(
                            a.processorStatusSnapshot.bytesRead,
                            b.processorStatusSnapshot.bytesRead,
                            isAsc
                        );
                    } else {
                        return this.compare(
                            a.processorStatusSnapshot.bytesWritten,
                            b.processorStatusSnapshot.bytesWritten,
                            isAsc
                        );
                    }
                case 'tasks':
                    if (this.multiSort.sortValueIndex === 0) {
                        return this.compare(
                            a.processorStatusSnapshot.taskCount,
                            b.processorStatusSnapshot.taskCount,
                            isAsc
                        );
                    } else {
                        return this.compare(
                            a.processorStatusSnapshot.tasksDurationNanos,
                            b.processorStatusSnapshot.tasksDurationNanos,
                            isAsc
                        );
                    }
                default:
                    return 0;
            }
        });
    }

    override supportsMultiValuedSort(sort: Sort): boolean {
        switch (sort.active) {
            case 'in':
            case 'out':
            case 'readWrite':
            case 'tasks':
                return true;
            default:
                return false;
        }
    }

    override filterPredicate(data: ProcessorStatusSnapshotEntity, filter: string): boolean {
        const { filterTerm, filterColumn, filterStatus, primaryOnly } = JSON.parse(filter);
        const matchOnStatus: boolean = filterStatus !== 'All';

        if (primaryOnly) {
            if (data.processorStatusSnapshot.executionNode !== 'PRIMARY') {
                return false;
            }
        }
        if (matchOnStatus) {
            if (data.processorStatusSnapshot.runStatus !== filterStatus) {
                return false;
            }
        }
        if (filterTerm === '') {
            return true;
        }

        const field: string = data.processorStatusSnapshot[filterColumn as keyof ProcessorStatusSnapshot] as string;
        return this.nifiCommon.stringContains(field, filterTerm, true);
    }
}
