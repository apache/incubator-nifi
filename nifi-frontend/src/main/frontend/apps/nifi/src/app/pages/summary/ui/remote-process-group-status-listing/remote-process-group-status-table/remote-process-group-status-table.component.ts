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
import {
    SummaryTableFilter,
    SummaryTableFilterColumn
} from '../../common/summary-table-filter/summary-table-filter.component';
import { MatTableModule } from '@angular/material/table';
import { ComponentType, NiFiCommon } from '@nifi/shared';
import { RouterLink } from '@angular/router';
import { MatPaginatorModule } from '@angular/material/paginator';
import { RemoteProcessGroupStatusSnapshot, RemoteProcessGroupStatusSnapshotEntity } from '../../../state';
import { ComponentStatusTable } from '../../common/component-status-table/component-status-table.component';
import { MatButtonModule } from '@angular/material/button';
import { MatMenu, MatMenuItem, MatMenuTrigger } from '@angular/material/menu';

export type SupportedColumns = 'name' | 'uri' | 'transmitting' | 'sent' | 'received';

@Component({
    selector: 'remote-process-group-status-table',
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
    templateUrl: './remote-process-group-status-table.component.html',
    styleUrls: ['./remote-process-group-status-table.component.scss']
})
export class RemoteProcessGroupStatusTable extends ComponentStatusTable<RemoteProcessGroupStatusSnapshotEntity> {
    filterableColumns: SummaryTableFilterColumn[] = [
        { key: 'name', label: 'name' },
        { key: 'targetUri', label: 'uri' }
    ];
    displayedColumns: string[] = ['name', 'uri', 'transmitting', 'sent', 'received', 'actions'];

    constructor(private nifiCommon: NiFiCommon) {
        super();
    }

    override filterPredicate(data: RemoteProcessGroupStatusSnapshotEntity, filter: string): boolean {
        const { filterTerm, filterColumn } = JSON.parse(filter);

        if (filterTerm === '') {
            return true;
        }

        const field: string = data.remoteProcessGroupStatusSnapshot[
            filterColumn as keyof RemoteProcessGroupStatusSnapshot
        ] as string;
        return this.nifiCommon.stringContains(field, filterTerm, true);
    }

    getRemoteProcessGroupLink(rpg: RemoteProcessGroupStatusSnapshotEntity): string[] {
        return [
            '/process-groups',
            rpg.remoteProcessGroupStatusSnapshot.groupId,
            ComponentType.RemoteProcessGroup,
            rpg.id
        ];
    }

    canRead(rpg: RemoteProcessGroupStatusSnapshotEntity): boolean {
        return rpg.canRead;
    }

    formatName(rpg: RemoteProcessGroupStatusSnapshotEntity): string {
        return rpg.remoteProcessGroupStatusSnapshot.name;
    }

    formatTransmitting(rpg: RemoteProcessGroupStatusSnapshotEntity): string {
        if (rpg.remoteProcessGroupStatusSnapshot.transmissionStatus === 'Transmitting') {
            return rpg.remoteProcessGroupStatusSnapshot.transmissionStatus;
        } else {
            return 'Not Transmitting';
        }
    }

    formatUri(rpg: RemoteProcessGroupStatusSnapshotEntity): string {
        return rpg.remoteProcessGroupStatusSnapshot.targetUri;
    }

    formatSent(rpg: RemoteProcessGroupStatusSnapshotEntity): string {
        return rpg.remoteProcessGroupStatusSnapshot.sent;
    }

    formatReceived(rpg: RemoteProcessGroupStatusSnapshotEntity): string {
        return rpg.remoteProcessGroupStatusSnapshot.received;
    }

    getTransmissionStatusIcon(rpg: RemoteProcessGroupStatusSnapshotEntity): string {
        if (rpg.remoteProcessGroupStatusSnapshot.transmissionStatus === 'Transmitting') {
            return 'transmitting success-color-variant fa fa-bullseye';
        } else {
            return 'not-transmitting icon icon-transmit-false neutral-color';
        }
    }

    override supportsMultiValuedSort(sort: Sort): boolean {
        switch (sort.active) {
            case 'sent':
            case 'received':
                return true;
            default:
                return false;
        }
    }

    override sortEntities(
        data: RemoteProcessGroupStatusSnapshotEntity[],
        sort: Sort
    ): RemoteProcessGroupStatusSnapshotEntity[] {
        if (!data) {
            return [];
        }

        return data.slice().sort((a, b) => {
            const isAsc: boolean = sort.direction === 'asc';
            let retVal = 0;
            switch (sort.active) {
                case 'name':
                    retVal = this.nifiCommon.compareString(
                        a.remoteProcessGroupStatusSnapshot.name,
                        b.remoteProcessGroupStatusSnapshot.name
                    );
                    break;
                case 'transmitting':
                    retVal = this.nifiCommon.compareString(
                        a.remoteProcessGroupStatusSnapshot.transmissionStatus,
                        b.remoteProcessGroupStatusSnapshot.transmissionStatus
                    );
                    break;
                case 'uri':
                    retVal = this.nifiCommon.compareString(
                        a.remoteProcessGroupStatusSnapshot.targetUri,
                        b.remoteProcessGroupStatusSnapshot.targetUri
                    );
                    break;
                case 'sent':
                    if (this.multiSort.sortValueIndex === 0) {
                        retVal = this.nifiCommon.compareNumber(
                            a.remoteProcessGroupStatusSnapshot.flowFilesSent,
                            b.remoteProcessGroupStatusSnapshot.flowFilesSent
                        );
                    } else {
                        retVal = this.nifiCommon.compareNumber(
                            a.remoteProcessGroupStatusSnapshot.bytesSent,
                            b.remoteProcessGroupStatusSnapshot.bytesSent
                        );
                    }
                    break;
                case 'received':
                    if (this.multiSort.sortValueIndex === 0) {
                        retVal = this.nifiCommon.compareNumber(
                            a.remoteProcessGroupStatusSnapshot.flowFilesReceived,
                            b.remoteProcessGroupStatusSnapshot.flowFilesReceived
                        );
                    } else {
                        retVal = this.nifiCommon.compareNumber(
                            a.remoteProcessGroupStatusSnapshot.bytesReceived,
                            b.remoteProcessGroupStatusSnapshot.bytesReceived
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
