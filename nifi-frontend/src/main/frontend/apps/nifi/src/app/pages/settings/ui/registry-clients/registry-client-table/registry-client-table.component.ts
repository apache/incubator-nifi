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

import { Component, EventEmitter, Input, Output } from '@angular/core';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { MatSortModule, Sort } from '@angular/material/sort';
import { ReportingTaskEntity } from '../../../state/reporting-tasks';
import { TextTip, NiFiCommon, NifiTooltipDirective } from '@nifi/shared';
import { BulletinsTip } from '../../../../../ui/common/tooltips/bulletins-tip/bulletins-tip.component';
import { ValidationErrorsTip } from '../../../../../ui/common/tooltips/validation-errors-tip/validation-errors-tip.component';
import { BulletinsTipInput, RegistryClientEntity, ValidationErrorsTipInput } from '../../../../../state/shared';
import { MatIconButton } from '@angular/material/button';
import { MatMenu, MatMenuItem, MatMenuTrigger } from '@angular/material/menu';

@Component({
    selector: 'registry-client-table',
    templateUrl: './registry-client-table.component.html',
    styleUrls: ['./registry-client-table.component.scss'],
    imports: [MatTableModule, MatSortModule, NifiTooltipDirective, MatIconButton, MatMenuTrigger, MatMenu, MatMenuItem]
})
export class RegistryClientTable {
    @Input() set registryClients(registryClientEntities: RegistryClientEntity[]) {
        if (registryClientEntities) {
            this.dataSource.data = this.sortEvents(registryClientEntities, this.sort);
        }
    }

    @Input() selectedRegistryClientId!: string;

    @Output() selectRegistryClient: EventEmitter<RegistryClientEntity> = new EventEmitter<RegistryClientEntity>();
    @Output() configureRegistryClient: EventEmitter<RegistryClientEntity> = new EventEmitter<RegistryClientEntity>();
    @Output() deleteRegistryClient: EventEmitter<RegistryClientEntity> = new EventEmitter<RegistryClientEntity>();

    protected readonly TextTip = TextTip;
    protected readonly BulletinsTip = BulletinsTip;
    protected readonly ValidationErrorsTip = ValidationErrorsTip;

    displayedColumns: string[] = ['moreDetails', 'name', 'description', 'type', 'bundle', 'actions'];
    dataSource: MatTableDataSource<RegistryClientEntity> = new MatTableDataSource<RegistryClientEntity>();

    sort: Sort = {
        active: 'name',
        direction: 'asc'
    };

    constructor(private nifiCommon: NiFiCommon) {}

    canRead(entity: RegistryClientEntity): boolean {
        return entity.permissions.canRead;
    }

    canWrite(entity: RegistryClientEntity): boolean {
        return entity.permissions.canWrite;
    }

    hasErrors(entity: RegistryClientEntity): boolean {
        return this.canRead(entity) && !this.nifiCommon.isEmpty(entity.component.validationErrors);
    }

    getValidationErrorsTipData(entity: RegistryClientEntity): ValidationErrorsTipInput {
        return {
            isValidating: false,
            validationErrors: entity.component.validationErrors
        };
    }

    hasBulletins(entity: RegistryClientEntity): boolean {
        return this.canRead(entity) && !this.nifiCommon.isEmpty(entity.bulletins);
    }

    getBulletinsTipData(entity: RegistryClientEntity): BulletinsTipInput {
        return {
            // @ts-ignore
            bulletins: entity.bulletins
        };
    }

    formatType(entity: RegistryClientEntity): string {
        return this.nifiCommon.formatType(entity.component);
    }

    formatBundle(entity: RegistryClientEntity): string {
        return this.nifiCommon.formatBundle(entity.component.bundle);
    }

    updateSort(sort: Sort): void {
        this.sort = sort;
        this.dataSource.data = this.sortEvents(this.dataSource.data, sort);
    }

    sortEvents(entities: RegistryClientEntity[], sort: Sort): RegistryClientEntity[] {
        const data: RegistryClientEntity[] = entities.slice();
        return data.sort((a, b) => {
            const isAsc = sort.direction === 'asc';

            let retVal = 0;
            switch (sort.active) {
                case 'name':
                    retVal = this.nifiCommon.compareString(a.component.name, b.component.name);
                    break;
                case 'description':
                    retVal = this.nifiCommon.compareString(a.component.description, b.component.description);
                    break;
                case 'type':
                    retVal = this.nifiCommon.compareString(this.formatType(a), this.formatType(b));
                    break;
                case 'bundle':
                    retVal = this.nifiCommon.compareString(this.formatBundle(a), this.formatBundle(b));
                    break;
            }

            return retVal * (isAsc ? 1 : -1);
        });
    }

    canConfigure(entity: RegistryClientEntity): boolean {
        return this.canRead(entity) && this.canWrite(entity);
    }

    configureClicked(entity: RegistryClientEntity): void {
        this.configureRegistryClient.next(entity);
    }

    canDelete(entity: RegistryClientEntity): boolean {
        return this.canRead(entity) && this.canWrite(entity);
    }

    deleteClicked(entity: RegistryClientEntity): void {
        this.deleteRegistryClient.next(entity);
    }

    select(entity: ReportingTaskEntity): void {
        this.selectRegistryClient.next(entity);
    }

    isSelected(entity: ReportingTaskEntity): boolean {
        if (this.selectedRegistryClientId) {
            return entity.id == this.selectedRegistryClientId;
        }
        return false;
    }
}
