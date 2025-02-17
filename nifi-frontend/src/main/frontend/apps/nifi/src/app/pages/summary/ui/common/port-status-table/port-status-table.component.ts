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
import { SummaryTableFilter, SummaryTableFilterColumn } from '../summary-table-filter/summary-table-filter.component';
import { RouterLink } from '@angular/router';
import { ComponentType, NiFiCommon } from '@nifi/shared';
import { MatPaginatorModule } from '@angular/material/paginator';
import { PortStatusSnapshot, PortStatusSnapshotEntity } from '../../../state';
import { ComponentStatusTable } from '../component-status-table/component-status-table.component';
import { MatButtonModule } from '@angular/material/button';
import { MatMenu, MatMenuItem, MatMenuTrigger } from '@angular/material/menu';

export type SupportedColumns = 'name' | 'runStatus' | 'in' | 'out';

@Component({
    selector: 'port-status-table',
    imports: [
        CommonModule,
        SummaryTableFilter,
        MatSortModule,
        MatTableModule,
        RouterLink,
        MatPaginatorModule,
        MatButtonModule,
        MatMenu,
        MatMenuItem,
        MatMenuTrigger
    ],
    templateUrl: './port-status-table.component.html',
    styleUrls: ['./port-status-table.component.scss']
})
export class PortStatusTable extends ComponentStatusTable<PortStatusSnapshotEntity> {
    private _portType!: 'input' | 'output';

    filterableColumns: SummaryTableFilterColumn[] = [{ key: 'name', label: 'name' }];

    displayedColumns: string[] = [];

    constructor(private nifiCommon: NiFiCommon) {
        super();
    }

    @Input() set portType(type: 'input' | 'output') {
        if (type === 'input') {
            this.displayedColumns = ['name', 'runStatus', 'out', 'actions'];
        } else {
            this.displayedColumns = ['name', 'runStatus', 'in', 'actions'];
        }
        this._portType = type;
    }

    get portType() {
        return this._portType;
    }

    override filterPredicate(data: PortStatusSnapshotEntity, filter: string): boolean {
        const { filterTerm, filterColumn, filterStatus } = JSON.parse(filter);
        const matchOnStatus: boolean = filterStatus !== 'All';

        if (matchOnStatus) {
            if (data.portStatusSnapshot.runStatus !== filterStatus) {
                return false;
            }
        }
        if (filterTerm === '') {
            return true;
        }

        const field: string = data.portStatusSnapshot[filterColumn as keyof PortStatusSnapshot] as string;
        return this.nifiCommon.stringContains(field, filterTerm, true);
    }

    formatName(port: PortStatusSnapshotEntity): string {
        return port.portStatusSnapshot.name;
    }

    formatRunStatus(port: PortStatusSnapshotEntity): string {
        return port.portStatusSnapshot.runStatus;
    }

    formatIn(port: PortStatusSnapshotEntity): string {
        return port.portStatusSnapshot.input;
    }

    formatOut(port: PortStatusSnapshotEntity): string {
        return port.portStatusSnapshot.output;
    }

    getRunStatusIcon(port: PortStatusSnapshotEntity): string {
        switch (port.portStatusSnapshot.runStatus.toLowerCase()) {
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

    getPortLink(port: PortStatusSnapshotEntity): string[] {
        const componentType: ComponentType =
            this._portType === 'input' ? ComponentType.InputPort : ComponentType.OutputPort;
        return ['/process-groups', port.portStatusSnapshot.groupId, componentType, port.id];
    }

    canRead(port: PortStatusSnapshotEntity) {
        return port.canRead;
    }

    override supportsMultiValuedSort(sort: Sort): boolean {
        switch (sort.active) {
            case 'in':
            case 'out':
                return true;
            default:
                return false;
        }
    }

    override sortEntities(data: PortStatusSnapshotEntity[], sort: Sort): PortStatusSnapshotEntity[] {
        if (!data) {
            return [];
        }
        return data.slice().sort((a, b) => {
            const isAsc: boolean = sort.direction === 'asc';
            let retVal = 0;
            switch (sort.active) {
                case 'name':
                    retVal = this.nifiCommon.compareString(a.portStatusSnapshot.name, b.portStatusSnapshot.name);
                    break;
                case 'runStatus':
                    retVal = this.nifiCommon.compareString(
                        a.portStatusSnapshot.runStatus,
                        b.portStatusSnapshot.runStatus
                    );
                    break;
                case 'in':
                    if (this.multiSort.sortValueIndex === 0) {
                        retVal = this.nifiCommon.compareNumber(
                            a.portStatusSnapshot.flowFilesIn,
                            b.portStatusSnapshot.flowFilesIn
                        );
                    } else {
                        retVal = this.nifiCommon.compareNumber(
                            a.portStatusSnapshot.bytesIn,
                            b.portStatusSnapshot.bytesIn
                        );
                    }
                    break;
                case 'out':
                    if (this.multiSort.sortValueIndex === 0) {
                        retVal = this.nifiCommon.compareNumber(
                            a.portStatusSnapshot.flowFilesOut,
                            b.portStatusSnapshot.flowFilesOut
                        );
                    } else {
                        retVal = this.nifiCommon.compareNumber(
                            a.portStatusSnapshot.bytesOut,
                            b.portStatusSnapshot.bytesOut
                        );
                    }
                    break;
                default:
                    retVal = 0;
            }
            return retVal * (isAsc ? 1 : -1);
        });
    }
}
