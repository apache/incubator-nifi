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
import { MatTableDataSource } from '@angular/material/table';
import { Sort } from '@angular/material/sort';
import { ReportingTaskEntity } from '../../../state/reporting-tasks';
import { TextTip } from '../../../../../ui/common/tooltips/text-tip/text-tip.component';
import { BulletinsTip } from '../../../../../ui/common/tooltips/bulletins-tip/bulletins-tip.component';
import { ValidationErrorsTip } from '../../../../../ui/common/tooltips/validation-errors-tip/validation-errors-tip.component';
import { NiFiCommon } from '../../../../../service/nifi-common.service';
import { BulletinsTipInput, TextTipInput, ValidationErrorsTipInput } from '../../../../../state/shared';
import { FlowConfiguration } from '../../../../../state/flow-configuration';
import { CurrentUser } from '../../../../../state/current-user';

@Component({
    selector: 'reporting-task-table',
    templateUrl: './reporting-task-table.component.html',
    styleUrls: ['./reporting-task-table.component.scss']
})
export class ReportingTaskTable {
    @Input() initialSortColumn: 'name' | 'type' | 'bundle' | 'state' = 'name';
    @Input() initialSortDirection: 'asc' | 'desc' = 'asc';
    activeSort: Sort = {
        active: this.initialSortColumn,
        direction: this.initialSortDirection
    };

    @Input() set reportingTasks(reportingTaskEntities: ReportingTaskEntity[]) {
        this.dataSource.data = this.sortEntities(reportingTaskEntities, this.activeSort);
    }

    @Input() selectedReportingTaskId!: string;
    @Input() flowConfiguration!: FlowConfiguration;
    @Input() currentUser!: CurrentUser;

    @Output() selectReportingTask: EventEmitter<ReportingTaskEntity> = new EventEmitter<ReportingTaskEntity>();
    @Output() viewReportingTaskDocumentation: EventEmitter<ReportingTaskEntity> =
        new EventEmitter<ReportingTaskEntity>();
    @Output() deleteReportingTask: EventEmitter<ReportingTaskEntity> = new EventEmitter<ReportingTaskEntity>();
    @Output() startReportingTask: EventEmitter<ReportingTaskEntity> = new EventEmitter<ReportingTaskEntity>();
    @Output() configureReportingTask: EventEmitter<ReportingTaskEntity> = new EventEmitter<ReportingTaskEntity>();
    @Output() openAdvancedUi: EventEmitter<ReportingTaskEntity> = new EventEmitter<ReportingTaskEntity>();
    @Output() viewStateReportingTask: EventEmitter<ReportingTaskEntity> = new EventEmitter<ReportingTaskEntity>();
    @Output() stopReportingTask: EventEmitter<ReportingTaskEntity> = new EventEmitter<ReportingTaskEntity>();

    protected readonly TextTip = TextTip;
    protected readonly BulletinsTip = BulletinsTip;
    protected readonly ValidationErrorsTip = ValidationErrorsTip;

    displayedColumns: string[] = ['moreDetails', 'name', 'type', 'bundle', 'state', 'actions'];
    dataSource: MatTableDataSource<ReportingTaskEntity> = new MatTableDataSource<ReportingTaskEntity>();

    constructor(private nifiCommon: NiFiCommon) {}

    canRead(entity: ReportingTaskEntity): boolean {
        return entity.permissions.canRead;
    }

    canWrite(entity: ReportingTaskEntity): boolean {
        return entity.permissions.canWrite;
    }

    canOperate(entity: ReportingTaskEntity): boolean {
        if (this.canWrite(entity)) {
            return true;
        }
        return !!entity.operatePermissions?.canWrite;
    }

    viewDocumentationClicked(entity: ReportingTaskEntity, event: MouseEvent): void {
        event.stopPropagation();
        this.viewReportingTaskDocumentation.next(entity);
    }

    hasComments(entity: ReportingTaskEntity): boolean {
        return !this.nifiCommon.isBlank(entity.component.comments);
    }

    getCommentsTipData(entity: ReportingTaskEntity): TextTipInput {
        return {
            text: entity.component.comments
        };
    }

    hasErrors(entity: ReportingTaskEntity): boolean {
        return !this.nifiCommon.isEmpty(entity.component.validationErrors);
    }

    getValidationErrorsTipData(entity: ReportingTaskEntity): ValidationErrorsTipInput {
        return {
            isValidating: entity.status.validationStatus === 'VALIDATING',
            validationErrors: entity.component.validationErrors
        };
    }

    hasBulletins(entity: ReportingTaskEntity): boolean {
        return !this.nifiCommon.isEmpty(entity.bulletins);
    }

    getBulletinsTipData(entity: ReportingTaskEntity): BulletinsTipInput {
        return {
            bulletins: entity.bulletins
        };
    }

    getStateIcon(entity: ReportingTaskEntity): string {
        if (entity.status.validationStatus === 'VALIDATING') {
            return 'validating nifi-surface-default fa fa-spin fa-circle-o-notch';
        } else if (entity.status.validationStatus === 'INVALID') {
            return 'invalid fa fa-warning';
        } else {
            if (entity.status.runStatus === 'STOPPED') {
                return 'fa fa-stop nifi-warn-lighter stopped';
            } else if (entity.status.runStatus === 'RUNNING') {
                return 'fa fa-play nifi-success-lighter running';
            } else {
                return 'icon icon-enable-false mat-primary disabled';
            }
        }
    }

    formatState(entity: ReportingTaskEntity): string {
        if (entity.status.validationStatus === 'VALIDATING') {
            return 'Validating';
        } else if (entity.status.validationStatus === 'INVALID') {
            return 'Invalid';
        } else {
            if (entity.status.runStatus === 'STOPPED') {
                return 'Stopped';
            } else if (entity.status.runStatus === 'RUNNING') {
                return 'Running';
            } else {
                return 'Disabled';
            }
        }
    }

    hasActiveThreads(entity: ReportingTaskEntity): boolean {
        return entity.status?.activeThreadCount > 0;
    }

    formatName(entity: ReportingTaskEntity): string {
        return this.canRead(entity) ? entity.component.name : entity.id;
    }

    formatType(entity: ReportingTaskEntity): string {
        return this.canRead(entity) ? this.nifiCommon.formatType(entity.component) : '';
    }

    formatBundle(entity: ReportingTaskEntity): string {
        return this.canRead(entity) ? this.nifiCommon.formatBundle(entity.component.bundle) : '';
    }

    isDisabled(entity: ReportingTaskEntity): boolean {
        return entity.status.runStatus === 'DISABLED';
    }

    isValid(entity: ReportingTaskEntity): boolean {
        return entity.status.validationStatus === 'VALID';
    }

    isRunning(entity: ReportingTaskEntity): boolean {
        return entity.status.runStatus === 'RUNNING';
    }

    isStopped(entity: ReportingTaskEntity): boolean {
        return entity.status.runStatus === 'STOPPED';
    }

    isStoppedOrDisabled(entity: ReportingTaskEntity): boolean {
        return this.isStopped(entity) || entity.status.runStatus === 'DISABLED';
    }

    canStop(entity: ReportingTaskEntity): boolean {
        return this.canOperate(entity) && this.isRunning(entity);
    }

    stopClicked(entity: ReportingTaskEntity): void {
        this.stopReportingTask.next(entity);
    }

    canEdit(entity: ReportingTaskEntity): boolean {
        return this.canRead(entity) && this.canWrite(entity) && this.isStoppedOrDisabled(entity);
    }

    hasAdvancedUi(entity: ReportingTaskEntity): boolean {
        return this.canRead(entity) && !!entity.component.customUiUrl;
    }

    advancedClicked(entity: ReportingTaskEntity, event: MouseEvent): void {
        event.stopPropagation();
        this.openAdvancedUi.next(entity);
    }

    canStart(entity: ReportingTaskEntity): boolean {
        return this.canOperate(entity) && this.isStopped(entity) && this.isValid(entity);
    }

    startClicked(entity: ReportingTaskEntity): void {
        this.startReportingTask.next(entity);
    }

    canChangeVersion(entity: ReportingTaskEntity): boolean {
        return (
            (this.isDisabled(entity) || this.isStopped(entity)) &&
            this.canRead(entity) &&
            this.canWrite(entity) &&
            entity.component.multipleVersionsAvailable === true
        );
    }

    canDelete(entity: ReportingTaskEntity): boolean {
        const canWriteParent: boolean =
            this.currentUser.controllerPermissions.canRead && this.currentUser.controllerPermissions.canWrite;
        return (
            (this.isDisabled(entity) || this.isStopped(entity)) &&
            this.canRead(entity) &&
            this.canWrite(entity) &&
            canWriteParent
        );
    }

    deleteClicked(entity: ReportingTaskEntity): void {
        this.deleteReportingTask.next(entity);
    }

    configureClicked(entity: ReportingTaskEntity, event: MouseEvent): void {
        event.stopPropagation();
        this.configureReportingTask.next(entity);
    }

    canViewState(entity: ReportingTaskEntity): boolean {
        return this.canRead(entity) && this.canWrite(entity) && entity.component.persistsState === true;
    }

    viewStateClicked(entity: ReportingTaskEntity): void {
        this.viewStateReportingTask.next(entity);
    }

    canManageAccessPolicies(): boolean {
        return this.flowConfiguration.supportsManagedAuthorizer && this.currentUser.tenantsPermissions.canRead;
    }

    getPolicyLink(entity: ReportingTaskEntity): string[] {
        return ['/access-policies', 'read', 'component', 'reporting-tasks', entity.id];
    }

    select(entity: ReportingTaskEntity): void {
        this.selectReportingTask.next(entity);
    }

    isSelected(entity: ReportingTaskEntity): boolean {
        if (this.selectedReportingTaskId) {
            return entity.id == this.selectedReportingTaskId;
        }
        return false;
    }

    sortData(sort: Sort) {
        this.activeSort = sort;
        this.dataSource.data = this.sortEntities(this.dataSource.data, sort);
    }

    private sortEntities(data: ReportingTaskEntity[], sort: Sort): ReportingTaskEntity[] {
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
                default:
                    return 0;
            }
            return retVal * (isAsc ? 1 : -1);
        });
    }
}
