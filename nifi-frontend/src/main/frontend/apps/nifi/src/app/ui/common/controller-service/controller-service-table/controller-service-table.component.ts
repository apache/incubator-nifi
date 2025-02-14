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
import { MatButtonModule } from '@angular/material/button';
import { MatDialogModule } from '@angular/material/dialog';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { MatSortModule, Sort } from '@angular/material/sort';
import { NgClass } from '@angular/common';
import { BulletinsTipInput, ControllerServiceEntity, ValidationErrorsTipInput } from '../../../../state/shared';
import { NifiTooltipDirective, TextTip, NiFiCommon } from '@nifi/shared';
import { BulletinsTip } from '../../tooltips/bulletins-tip/bulletins-tip.component';
import { ValidationErrorsTip } from '../../tooltips/validation-errors-tip/validation-errors-tip.component';
import { FlowConfiguration } from '../../../../state/flow-configuration';
import { CurrentUser } from '../../../../state/current-user';
import { MatMenu, MatMenuItem, MatMenuTrigger } from '@angular/material/menu';

@Component({
    selector: 'controller-service-table',
    templateUrl: './controller-service-table.component.html',
    imports: [
        MatButtonModule,
        MatDialogModule,
        MatTableModule,
        MatSortModule,
        NgClass,
        NifiTooltipDirective,
        MatMenu,
        MatMenuItem,
        MatMenuTrigger
    ],
    styleUrls: ['./controller-service-table.component.scss']
})
export class ControllerServiceTable {
    @Input() initialSortColumn: 'name' | 'type' | 'bundle' | 'state' | 'scope' = 'name';
    @Input() initialSortDirection: 'asc' | 'desc' = 'asc';
    activeSort: Sort = {
        active: this.initialSortColumn,
        direction: this.initialSortDirection
    };

    @Input() set controllerServices(controllerServiceEntities: ControllerServiceEntity[]) {
        this.dataSource.data = this.sortEntities(controllerServiceEntities, this.activeSort);
    }

    @Input() selectedServiceId!: string;
    @Input() formatScope!: (entity: ControllerServiceEntity) => string;
    @Input() showMoveOption!: boolean;
    @Input() definedByCurrentGroup!: (entity: ControllerServiceEntity) => boolean;
    @Input() flowConfiguration!: FlowConfiguration;
    @Input() currentUser!: CurrentUser;
    @Input() canModifyParent!: (entity: ControllerServiceEntity) => boolean;

    @Output() selectControllerService: EventEmitter<ControllerServiceEntity> =
        new EventEmitter<ControllerServiceEntity>();
    @Output() viewControllerServiceDocumentation: EventEmitter<ControllerServiceEntity> =
        new EventEmitter<ControllerServiceEntity>();
    @Output() deleteControllerService: EventEmitter<ControllerServiceEntity> =
        new EventEmitter<ControllerServiceEntity>();
    @Output() configureControllerService: EventEmitter<ControllerServiceEntity> =
        new EventEmitter<ControllerServiceEntity>();
    @Output() manageAccessPolicies: EventEmitter<ControllerServiceEntity> = new EventEmitter<ControllerServiceEntity>();
    @Output() openAdvancedUi: EventEmitter<ControllerServiceEntity> = new EventEmitter<ControllerServiceEntity>();
    @Output() moveControllerService: EventEmitter<ControllerServiceEntity> =
        new EventEmitter<ControllerServiceEntity>();
    @Output() enableControllerService: EventEmitter<ControllerServiceEntity> =
        new EventEmitter<ControllerServiceEntity>();
    @Output() disableControllerService: EventEmitter<ControllerServiceEntity> =
        new EventEmitter<ControllerServiceEntity>();
    @Output() viewStateControllerService: EventEmitter<ControllerServiceEntity> =
        new EventEmitter<ControllerServiceEntity>();
    @Output() changeControllerServiceVersion: EventEmitter<ControllerServiceEntity> =
        new EventEmitter<ControllerServiceEntity>();
    @Output() goToControllerService: EventEmitter<ControllerServiceEntity> =
        new EventEmitter<ControllerServiceEntity>();

    protected readonly TextTip = TextTip;
    protected readonly BulletinsTip = BulletinsTip;
    protected readonly ValidationErrorsTip = ValidationErrorsTip;

    displayedColumns: string[] = ['moreDetails', 'name', 'type', 'bundle', 'state', 'scope', 'actions'];
    dataSource: MatTableDataSource<ControllerServiceEntity> = new MatTableDataSource<ControllerServiceEntity>();

    constructor(private nifiCommon: NiFiCommon) {}

    canRead(entity: ControllerServiceEntity): boolean {
        return entity.permissions.canRead;
    }

    canWrite(entity: ControllerServiceEntity): boolean {
        return entity.permissions.canWrite;
    }

    canOperate(entity: ControllerServiceEntity): boolean {
        if (this.canWrite(entity)) {
            return true;
        }
        return !!entity.operatePermissions?.canWrite;
    }

    hasComments(entity: ControllerServiceEntity): boolean {
        return !this.nifiCommon.isBlank(entity.component.comments);
    }

    viewDocumentationClicked(entity: ControllerServiceEntity): void {
        this.viewControllerServiceDocumentation.next(entity);
    }

    hasErrors(entity: ControllerServiceEntity): boolean {
        return !this.nifiCommon.isEmpty(entity.component.validationErrors);
    }

    getValidationErrorsTipData(entity: ControllerServiceEntity): ValidationErrorsTipInput {
        return {
            isValidating: entity.status.validationStatus === 'VALIDATING',
            validationErrors: entity.component.validationErrors
        };
    }

    hasBulletins(entity: ControllerServiceEntity): boolean {
        return !this.nifiCommon.isEmpty(entity.bulletins);
    }

    getBulletinsTipData(entity: ControllerServiceEntity): BulletinsTipInput {
        return {
            bulletins: entity.bulletins
        };
    }

    getStateIcon(entity: ControllerServiceEntity): string {
        if (entity.status.validationStatus === 'VALIDATING') {
            return 'validating neutral-color fa fa-spin fa-circle-o-notch';
        } else if (entity.status.validationStatus === 'INVALID') {
            return 'invalid fa fa-warning caution-color';
        } else {
            if (entity.status.runStatus === 'DISABLED') {
                return 'disabled icon icon-enable-false neutral-color';
            } else if (entity.status.runStatus === 'DISABLING') {
                return 'disabled icon icon-enable-false neutral-color';
            } else if (entity.status.runStatus === 'ENABLED') {
                return 'enabled fa fa-flash success-color-variant';
            } else if (entity.status.runStatus === 'ENABLING') {
                return 'enabled fa fa-flash success-color-variant';
            }
        }
        return '';
    }

    formatState(entity: ControllerServiceEntity): string {
        if (entity.status.validationStatus === 'VALIDATING') {
            return 'Validating';
        } else if (entity.status.validationStatus === 'INVALID') {
            return 'Invalid';
        } else {
            if (entity.status.runStatus === 'DISABLED') {
                return 'Disabled';
            } else if (entity.status.runStatus === 'DISABLING') {
                return 'Disabling';
            } else if (entity.status.runStatus === 'ENABLED') {
                return 'Enabled';
            } else if (entity.status.runStatus === 'ENABLING') {
                return 'Enabling';
            }
        }
        return '';
    }

    formatName(entity: ControllerServiceEntity): string {
        return this.canRead(entity) ? entity.component.name : entity.id;
    }

    formatType(entity: ControllerServiceEntity): string {
        return this.canRead(entity) ? this.nifiCommon.formatType(entity.component) : '';
    }

    formatBundle(entity: ControllerServiceEntity): string {
        return this.canRead(entity) ? this.nifiCommon.formatBundle(entity.component.bundle) : '';
    }

    goToControllerServiceClicked(entity: ControllerServiceEntity): void {
        this.goToControllerService.next(entity);
    }

    isDisabled(entity: ControllerServiceEntity): boolean {
        return entity.status.runStatus === 'DISABLED';
    }

    isEnabledOrEnabling(entity: ControllerServiceEntity): boolean {
        return entity.status.runStatus === 'ENABLED' || entity.status.runStatus === 'ENABLING';
    }

    canConfigure(entity: ControllerServiceEntity): boolean {
        return this.canRead(entity) && this.canWrite(entity) && this.isDisabled(entity);
    }

    configureClicked(entity: ControllerServiceEntity): void {
        this.configureControllerService.next(entity);
    }

    hasAdvancedUi(entity: ControllerServiceEntity): boolean {
        return this.canRead(entity) && !!entity.component.customUiUrl;
    }

    advancedClicked(entity: ControllerServiceEntity): void {
        this.openAdvancedUi.next(entity);
    }

    canEnable(entity: ControllerServiceEntity): boolean {
        const userAuthorized: boolean = this.canRead(entity) && this.canOperate(entity);
        return userAuthorized && this.isDisabled(entity) && entity.status.validationStatus === 'VALID';
    }

    enabledClicked(entity: ControllerServiceEntity): void {
        this.enableControllerService.next(entity);
    }

    canMove(entity: ControllerServiceEntity): boolean {
        return this.showMoveOption && this.canRead(entity) && this.canWrite(entity);
    }

    moveClicked(entity: ControllerServiceEntity): void {
        this.moveControllerService.next(entity);
    }

    canDisable(entity: ControllerServiceEntity): boolean {
        const userAuthorized: boolean = this.canRead(entity) && this.canOperate(entity);
        return userAuthorized && this.isEnabledOrEnabling(entity);
    }

    disableClicked(entity: ControllerServiceEntity): void {
        this.disableControllerService.next(entity);
    }

    canChangeVersion(entity: ControllerServiceEntity): boolean {
        return (
            this.isDisabled(entity) &&
            this.canRead(entity) &&
            this.canWrite(entity) &&
            entity.component.multipleVersionsAvailable
        );
    }

    canDelete(entity: ControllerServiceEntity): boolean {
        return this.isDisabled(entity) && this.canRead(entity) && this.canWrite(entity) && this.canModifyParent(entity);
    }

    deleteClicked(entity: ControllerServiceEntity, event: MouseEvent): void {
        event.stopPropagation();
        this.deleteControllerService.next(entity);
    }

    manageAccessPoliciesClicked(entity: ControllerServiceEntity): void {
        this.manageAccessPolicies.next(entity);
    }

    changeVersionClicked(entity: ControllerServiceEntity) {
        this.changeControllerServiceVersion.next(entity);
    }

    canViewState(entity: ControllerServiceEntity): boolean {
        return this.canRead(entity) && this.canWrite(entity) && entity.component.persistsState === true;
    }

    viewStateClicked(entity: ControllerServiceEntity): void {
        this.viewStateControllerService.next(entity);
    }

    canManageAccessPolicies(): boolean {
        return this.flowConfiguration.supportsManagedAuthorizer && this.currentUser.tenantsPermissions.canRead;
    }

    select(entity: ControllerServiceEntity): void {
        this.selectControllerService.next(entity);
    }

    isSelected(entity: ControllerServiceEntity): boolean {
        if (this.selectedServiceId) {
            return entity.id == this.selectedServiceId;
        }
        return false;
    }

    sortData(sort: Sort) {
        this.activeSort = sort;
        this.dataSource.data = this.sortEntities(this.dataSource.data, sort);
    }

    private sortEntities(data: ControllerServiceEntity[], sort: Sort): ControllerServiceEntity[] {
        if (!data) {
            return [];
        }
        return data.slice().sort((a, b) => {
            const isAsc = sort.direction === 'asc';
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
                case 'state':
                    retVal = this.nifiCommon.compareString(this.formatState(a), this.formatState(b));
                    break;
                case 'scope':
                    retVal = this.nifiCommon.compareString(this.formatScope(a), this.formatScope(b));
                    break;
                default:
                    return 0;
            }
            return retVal * (isAsc ? 1 : -1);
        });
    }
}
