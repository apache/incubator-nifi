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
import { CommonModule } from '@angular/common';
import { MatSortModule, Sort } from '@angular/material/sort';
import { ComponentType, NiFiCommon } from '@nifi/shared';
import {
    SummaryTableFilter,
    SummaryTableFilterColumn
} from '../../common/summary-table-filter/summary-table-filter.component';
import { MatTableModule } from '@angular/material/table';
import { RouterLink } from '@angular/router';
import { MatPaginatorModule } from '@angular/material/paginator';
import { ConnectionStatusSnapshot, ConnectionStatusSnapshotEntity } from '../../../state';
import { ComponentStatusTable } from '../../common/component-status-table/component-status-table.component';
import { MatButtonModule } from '@angular/material/button';
import { MatMenu, MatMenuItem, MatMenuTrigger } from '@angular/material/menu';

export type SupportedColumns = 'name' | 'queue' | 'in' | 'out' | 'threshold' | 'sourceName' | 'destinationName';

@Component({
    selector: 'connection-status-table',
    imports: [
        CommonModule,
        SummaryTableFilter,
        MatSortModule,
        RouterLink,
        MatTableModule,
        MatPaginatorModule,
        MatButtonModule,
        MatMenu,
        MatMenuItem,
        MatMenuTrigger
    ],
    templateUrl: './connection-status-table.component.html',
    styleUrls: ['./connection-status-table.component.scss']
})
export class ConnectionStatusTable extends ComponentStatusTable<ConnectionStatusSnapshotEntity> {
    filterableColumns: SummaryTableFilterColumn[] = [
        { key: 'sourceName', label: 'source' },
        { key: 'name', label: 'name' },
        { key: 'destinationName', label: 'destination' }
    ];

    displayedColumns: string[] = [
        'name',
        'queue',
        'threshold',
        'in',
        'sourceName',
        'out',
        'destinationName',
        'actions'
    ];

    constructor(private nifiCommon: NiFiCommon) {
        super();
    }

    override filterPredicate(data: ConnectionStatusSnapshotEntity, filter: string): boolean {
        const { filterTerm, filterColumn } = JSON.parse(filter);

        if (filterTerm === '') {
            return true;
        }

        const field: string = data.connectionStatusSnapshot[filterColumn as keyof ConnectionStatusSnapshot] as string;
        return this.nifiCommon.stringContains(field, filterTerm, true);
    }

    getConnectionLink(connection: ConnectionStatusSnapshotEntity): string[] {
        return [
            '/process-groups',
            connection.connectionStatusSnapshot.groupId,
            ComponentType.Connection,
            connection.id
        ];
    }

    canRead(connection: ConnectionStatusSnapshotEntity): boolean {
        return connection.canRead;
    }

    formatName(connection: ConnectionStatusSnapshotEntity): string {
        return connection.connectionStatusSnapshot.name;
    }

    formatSource(connection: ConnectionStatusSnapshotEntity): string {
        return connection.connectionStatusSnapshot.sourceName;
    }

    formatDestination(connection: ConnectionStatusSnapshotEntity): string {
        return connection.connectionStatusSnapshot.destinationName;
    }

    formatIn(connection: ConnectionStatusSnapshotEntity): string {
        return connection.connectionStatusSnapshot.input;
    }

    formatOut(connection: ConnectionStatusSnapshotEntity): string {
        return connection.connectionStatusSnapshot.output;
    }

    formatQueue(connection: ConnectionStatusSnapshotEntity): string {
        return connection.connectionStatusSnapshot.queued;
    }

    formatThreshold(connection: ConnectionStatusSnapshotEntity): string {
        return `${connection.connectionStatusSnapshot.percentUseCount}% | ${connection.connectionStatusSnapshot.percentUseBytes}%`;
    }

    override supportsMultiValuedSort(sort: Sort): boolean {
        switch (sort.active) {
            case 'in':
            case 'out':
            case 'threshold':
            case 'queue':
                return true;
            default:
                return false;
        }
    }

    override sortEntities(data: ConnectionStatusSnapshotEntity[], sort: Sort): ConnectionStatusSnapshotEntity[] {
        if (!data) {
            return [];
        }
        return data.slice().sort((a, b) => {
            const isAsc: boolean = sort.direction === 'asc';
            let retVal = 0;
            switch (sort.active) {
                case 'name':
                    retVal = this.nifiCommon.compareString(
                        a.connectionStatusSnapshot.name,
                        b.connectionStatusSnapshot.name
                    );
                    break;
                case 'sourceName':
                    retVal = this.nifiCommon.compareString(
                        a.connectionStatusSnapshot.sourceName,
                        b.connectionStatusSnapshot.sourceName
                    );
                    break;
                case 'destinationName':
                    retVal = this.nifiCommon.compareString(
                        a.connectionStatusSnapshot.destinationName,
                        b.connectionStatusSnapshot.destinationName
                    );
                    break;
                case 'in':
                    if (this.multiSort.sortValueIndex === 0) {
                        retVal = this.nifiCommon.compareNumber(
                            a.connectionStatusSnapshot.flowFilesIn,
                            b.connectionStatusSnapshot.flowFilesIn
                        );
                    } else {
                        retVal = this.nifiCommon.compareNumber(
                            a.connectionStatusSnapshot.bytesIn,
                            b.connectionStatusSnapshot.bytesIn
                        );
                    }
                    break;
                case 'out':
                    if (this.multiSort.sortValueIndex === 0) {
                        retVal = this.nifiCommon.compareNumber(
                            a.connectionStatusSnapshot.flowFilesOut,
                            b.connectionStatusSnapshot.flowFilesOut
                        );
                    } else {
                        retVal = this.nifiCommon.compareNumber(
                            a.connectionStatusSnapshot.bytesOut,
                            b.connectionStatusSnapshot.bytesOut
                        );
                    }
                    break;
                case 'queue':
                    if (this.multiSort.sortValueIndex === 0) {
                        retVal = this.nifiCommon.compareNumber(
                            a.connectionStatusSnapshot.flowFilesQueued,
                            b.connectionStatusSnapshot.flowFilesQueued
                        );
                    } else {
                        retVal = this.nifiCommon.compareNumber(
                            a.connectionStatusSnapshot.bytesQueued,
                            b.connectionStatusSnapshot.bytesQueued
                        );
                    }
                    break;
                case 'threshold':
                    if (this.multiSort.sortValueIndex === 0) {
                        retVal = this.nifiCommon.compareNumber(
                            a.connectionStatusSnapshot.percentUseCount,
                            b.connectionStatusSnapshot.percentUseCount
                        );
                    } else {
                        retVal = this.nifiCommon.compareNumber(
                            a.connectionStatusSnapshot.percentUseBytes,
                            b.connectionStatusSnapshot.percentUseBytes
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
