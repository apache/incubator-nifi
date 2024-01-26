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

import { Component, EventEmitter, Input, Output } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MatSortModule, Sort, SortDirection } from '@angular/material/sort';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { ParameterProviderEntity } from '../../../state/parameter-providers';
import { NiFiCommon } from '../../../../../service/nifi-common.service';
import { CurrentUser } from '../../../../../state/current-user';
import { FlowConfiguration } from '../../../../../state/flow-configuration';
import { MatPaginatorModule } from '@angular/material/paginator';
import { SummaryTableFilterModule } from '../../../../summary/ui/common/summary-table-filter/summary-table-filter.module';
import { PortStatusSnapshotEntity } from '../../../../summary/state/summary-listing';
import { ValidationErrorsTip } from '../../../../../ui/common/tooltips/validation-errors-tip/validation-errors-tip.component';
import { NifiTooltipDirective } from '../../../../../ui/common/tooltips/nifi-tooltip.directive';
import { ControllerServiceEntity, ValidationErrorsTipInput } from '../../../../../state/shared';
import { RouterLink } from '@angular/router';

export type SupportedColumns = 'name' | 'type' | 'bundle';

@Component({
    selector: 'parameter-providers-table',
    standalone: true,
    imports: [
        CommonModule,
        MatPaginatorModule,
        MatSortModule,
        MatTableModule,
        SummaryTableFilterModule,
        NifiTooltipDirective,
        RouterLink
    ],
    templateUrl: './parameter-providers-table.component.html',
    styleUrls: ['./parameter-providers-table.component.scss', '../../../../../../assets/styles/listing-table.scss']
})
export class ParameterProvidersTable {
    @Input() initialSortColumn: SupportedColumns = 'name';
    @Input() initialSortDirection: SortDirection = 'asc';

    displayedColumns: string[] = ['moreDetails', 'name', 'type', 'bundle', 'actions'];
    dataSource: MatTableDataSource<ParameterProviderEntity> = new MatTableDataSource<ParameterProviderEntity>();
    activeSort: Sort = {
        active: this.initialSortColumn,
        direction: this.initialSortDirection
    };

    constructor(private nifiCommon: NiFiCommon) {}

    @Input() selectedParameterProviderId!: string;
    @Input() currentUser!: CurrentUser;
    @Input() flowConfiguration!: FlowConfiguration;

    @Input() set parameterProviders(parameterProviders: ParameterProviderEntity[]) {
        if (parameterProviders) {
            this.dataSource.data = this.sortEntities(parameterProviders, this.activeSort);
        }
    }

    @Output() selectParameterProvider: EventEmitter<ParameterProviderEntity> =
        new EventEmitter<ParameterProviderEntity>();
    @Output() configureParameterProvider: EventEmitter<ParameterProviderEntity> =
        new EventEmitter<ParameterProviderEntity>();
    @Output() deleteParameterProvider: EventEmitter<ParameterProviderEntity> =
        new EventEmitter<ParameterProviderEntity>();
    @Output() fetchParameterProvider: EventEmitter<ParameterProviderEntity> =
        new EventEmitter<ParameterProviderEntity>();
    @Output() manageAccessPolicies: EventEmitter<ParameterProviderEntity> = new EventEmitter<ParameterProviderEntity>();

    protected readonly ValidationErrorsTip = ValidationErrorsTip;

    canRead(entity: ParameterProviderEntity): boolean {
        return entity.permissions.canRead;
    }

    canWrite(entity: ParameterProviderEntity): boolean {
        return entity.permissions.canWrite;
    }

    canManageAccessPolicies(): boolean {
        return this.flowConfiguration.supportsManagedAuthorizer && this.currentUser.tenantsPermissions.canRead;
    }

    isSelected(parameterProvider: ParameterProviderEntity): boolean {
        if (this.selectedParameterProviderId) {
            return parameterProvider.id === this.selectedParameterProviderId;
        }
        return false;
    }

    formatName(entity: ParameterProviderEntity): string {
        return this.canRead(entity) ? entity.component.name : entity.id;
    }

    formatType(entity: ParameterProviderEntity): string {
        return this.canRead(entity) ? this.nifiCommon.formatType(entity.component) : '';
    }

    formatBundle(entity: ParameterProviderEntity): string {
        return this.canRead(entity) ? this.nifiCommon.formatBundle(entity.component.bundle) : '';
    }

    hasErrors(entity: ParameterProviderEntity): boolean {
        return this.canRead(entity) && !this.nifiCommon.isEmpty(entity.component.validationErrors);
    }

    getValidationErrorsTipData(entity: ParameterProviderEntity): ValidationErrorsTipInput | null {
        return {
            isValidating: entity.component.validationStatus === 'VALIDATING',
            validationErrors: entity.component?.validationErrors || []
        };
    }

    sortData(sort: Sort) {
        this.activeSort = sort;
        this.dataSource.data = this.sortEntities(this.dataSource.data, sort);
    }

    private sortEntities(data: ParameterProviderEntity[], sort: Sort): ParameterProviderEntity[] {
        if (!data) {
            return [];
        }
        return data.slice().sort((a, b) => {
            const isAsc: boolean = sort.direction === 'asc';
            let retVal = 0;
            switch (sort.active) {
                case 'name':
                    retVal = this.nifiCommon.compareString(this.formatName(a), this.formatName(b));
                    break;
                case 'type':
                    retVal = this.nifiCommon.compareString(this.formatType(a), this.formatType(b));
                    break;
                case 'bundle':
                    retVal = this.nifiCommon.compareString(this.formatBundle(a), this.formatBundle(b));
                    break;
                default:
                    return 0;
            }

            return retVal * (isAsc ? 1 : -1);
        });
    }

    configureClicked(entity: ParameterProviderEntity, event: MouseEvent) {
        event.stopPropagation();
        this.configureParameterProvider.next(entity);
    }

    fetchClicked(entity: ParameterProviderEntity, event: MouseEvent) {
        event.stopPropagation();
        this.fetchParameterProvider.next(entity);
    }

    deleteClicked(entity: ParameterProviderEntity, event: MouseEvent) {
        event.stopPropagation();
        this.deleteParameterProvider.next(entity);
    }

    getPolicyLink(entity: ParameterProviderEntity): string[] {
        return ['/access-policies', 'read', 'component', 'parameter-providers', entity.id];
    }
}
