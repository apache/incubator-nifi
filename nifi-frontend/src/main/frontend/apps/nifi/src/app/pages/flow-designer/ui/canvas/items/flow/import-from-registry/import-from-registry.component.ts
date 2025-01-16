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

import { Component, Inject, Input, OnInit } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { ImportFromRegistryDialogRequest } from '../../../../../state/flow';
import { Store } from '@ngrx/store';
import { CanvasState } from '../../../../../state';
import {
    BranchEntity,
    BucketEntity,
    RegistryClientEntity,
    VersionedFlow,
    VersionedFlowEntity,
    VersionedFlowSnapshotMetadata,
    VersionedFlowSnapshotMetadataEntity
} from '../../../../../../../state/shared';
import { selectSaving } from '../../../../../state/flow/flow.selectors';
import { AsyncPipe, JsonPipe, NgForOf, NgIf, NgTemplateOutlet } from '@angular/common';
import { ErrorBanner } from '../../../../../../../ui/common/error-banner/error-banner.component';
import { MatButtonModule } from '@angular/material/button';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatOptionModule } from '@angular/material/core';
import { MatSelectModule } from '@angular/material/select';
import { NifiSpinnerDirective } from '../../../../../../../ui/common/spinner/nifi-spinner.directive';
import { FormBuilder, FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { MatIconModule } from '@angular/material/icon';
import { Observable, of, take } from 'rxjs';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { MatSortModule, Sort } from '@angular/material/sort';
import {
    isDefinedAndNotNull,
    SelectOption,
    NiFiCommon,
    TextTip,
    NifiTooltipDirective,
    CloseOnEscapeDialog
} from '@nifi/shared';
import { selectTimeOffset } from '../../../../../../../state/flow-configuration/flow-configuration.selectors';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { Client } from '../../../../../../../service/client.service';
import { importFromRegistry } from '../../../../../state/flow/flow.actions';
import { ClusterConnectionService } from '../../../../../../../service/cluster-connection.service';
import { ErrorContextKey } from '../../../../../../../state/error';
import { ContextErrorBanner } from '../../../../../../../ui/common/context-error-banner/context-error-banner.component';

@Component({
    selector: 'import-from-registry',
    standalone: true,
    imports: [
        AsyncPipe,
        ErrorBanner,
        MatButtonModule,
        MatDialogModule,
        MatFormFieldModule,
        MatInputModule,
        NgIf,
        NifiSpinnerDirective,
        ReactiveFormsModule,
        MatOptionModule,
        MatSelectModule,
        NgForOf,
        NifiTooltipDirective,
        MatIconModule,
        NgTemplateOutlet,
        JsonPipe,
        MatCheckboxModule,
        MatSortModule,
        MatTableModule,
        ContextErrorBanner
    ],
    templateUrl: './import-from-registry.component.html',
    styleUrls: ['./import-from-registry.component.scss']
})
export class ImportFromRegistry extends CloseOnEscapeDialog implements OnInit {
    @Input() getBranches: (registryId: string) => Observable<BranchEntity[]> = () => of([]);
    @Input() getBuckets!: (registryId: string, branch?: string | null) => Observable<BucketEntity[]>;
    @Input() getFlows!: (
        registryId: string,
        bucketId: string,
        branch?: string | null
    ) => Observable<VersionedFlowEntity[]>;
    @Input() getFlowVersions!: (
        registryId: string,
        bucketId: string,
        flowId: string,
        branch?: string | null
    ) => Observable<VersionedFlowSnapshotMetadataEntity[]>;

    saving$ = this.store.select(selectSaving);
    timeOffset = 0;

    protected readonly TextTip = TextTip;

    importFromRegistryForm: FormGroup;
    registryClientOptions: SelectOption[] = [];
    branchOptions: SelectOption[] = [];
    bucketOptions: SelectOption[] = [];
    flowOptions: SelectOption[] = [];

    flowLookup: Map<string, VersionedFlow> = new Map<string, VersionedFlow>();
    selectedFlowDescription: string | undefined;
    supportsBranching = false;
    private clientBranchingSupportMap: Map<string, boolean> = new Map<string, boolean>();

    sort: Sort = {
        active: 'created',
        direction: 'desc'
    };
    displayedColumns: string[] = ['version', 'created', 'comments'];
    dataSource: MatTableDataSource<VersionedFlowSnapshotMetadata> =
        new MatTableDataSource<VersionedFlowSnapshotMetadata>();
    selectedFlowVersion: string | null = null;

    constructor(
        @Inject(MAT_DIALOG_DATA) private dialogRequest: ImportFromRegistryDialogRequest,
        private formBuilder: FormBuilder,
        private store: Store<CanvasState>,
        private nifiCommon: NiFiCommon,
        private client: Client,
        private clusterConnectionService: ClusterConnectionService
    ) {
        super();
        this.store
            .select(selectTimeOffset)
            .pipe(isDefinedAndNotNull(), takeUntilDestroyed())
            .subscribe((timeOffset: number) => {
                this.timeOffset = timeOffset;
            });

        const sortedRegistries = dialogRequest.registryClients
            .slice()
            .filter((registry) => registry.permissions.canRead)
            .sort((a, b) => {
                return this.nifiCommon.compareString(a.component.name, b.component.name);
            });

        sortedRegistries.forEach((registryClient: RegistryClientEntity) => {
            if (registryClient.permissions.canRead) {
                this.registryClientOptions.push({
                    text: registryClient.component.name,
                    value: registryClient.id,
                    description: registryClient.component.description
                });
            }
            this.clientBranchingSupportMap.set(registryClient.id, registryClient.component.supportsBranching);
        });

        const initialRegistry = this.registryClientOptions.length > 0 ? this.registryClientOptions[0].value : null;
        this.importFromRegistryForm = this.formBuilder.group({
            registry: new FormControl(initialRegistry, Validators.required),
            branch: new FormControl(null),
            bucket: new FormControl(null, Validators.required),
            flow: new FormControl(null, Validators.required),
            keepParameterContexts: new FormControl(true, Validators.required)
        });
    }

    ngOnInit(): void {
        const selectedRegistryId = this.importFromRegistryForm.get('registry')?.value;

        if (selectedRegistryId) {
            this.supportsBranching = this.clientBranchingSupportMap.get(selectedRegistryId) || false;
            if (this.supportsBranching) {
                this.loadBranches(selectedRegistryId);
            } else {
                this.loadBuckets(selectedRegistryId);
            }
        }
    }

    registryChanged(registryId: string): void {
        this.supportsBranching = this.clientBranchingSupportMap.get(registryId) || false;
        if (this.supportsBranching) {
            this.clearBranches();
            this.loadBranches(registryId);
        } else {
            this.clearBuckets();
            this.loadBuckets(registryId);
        }
    }

    private clearBranches(): void {
        this.branchOptions = [];
        this.importFromRegistryForm.get('branch')?.setValue(null);
        this.clearBuckets();
    }

    branchChanged(branch: string): void {
        this.clearBuckets();
        const registryId = this.importFromRegistryForm.get('registry')?.value;
        this.loadBuckets(registryId, branch);
    }

    private clearBuckets(): void {
        this.bucketOptions = [];
        this.importFromRegistryForm.get('bucket')?.setValue(null);
        this.clearFlows();
    }

    bucketChanged(bucketId: string): void {
        this.clearFlows();
        const registryId = this.importFromRegistryForm.get('registry')?.value;
        const branch = this.importFromRegistryForm.get('branch')?.value;
        this.loadFlows(registryId, bucketId, branch);
    }

    private clearFlows() {
        this.importFromRegistryForm.get('flow')?.setValue(null);
        this.flowOptions = [];
        this.dataSource.data = [];
        this.selectedFlowVersion = null;
    }

    flowChanged(flowId: string): void {
        const registryId = this.importFromRegistryForm.get('registry')?.value;
        const bucketId = this.importFromRegistryForm.get('bucket')?.value;
        const branch = this.importFromRegistryForm.get('branch')?.value;
        this.loadVersions(registryId, bucketId, flowId, branch);
    }

    loadBranches(registryId: string): void {
        if (registryId) {
            this.branchOptions = [];

            this.getBranches(registryId)
                .pipe(take(1))
                .subscribe((branches: BranchEntity[]) => {
                    if (branches.length > 0) {
                        branches.forEach((entity: BranchEntity) => {
                            this.branchOptions.push({
                                text: entity.branch.name,
                                value: entity.branch.name
                            });
                        });

                        const branchId = this.branchOptions[0].value;
                        if (branchId) {
                            this.importFromRegistryForm.get('branch')?.setValue(branchId);
                            this.loadBuckets(registryId, branchId);
                        }
                    }
                });
        }
    }

    loadBuckets(registryId: string, branch?: string | null): void {
        this.bucketOptions = [];

        this.getBuckets(registryId, branch)
            .pipe(take(1))
            .subscribe((buckets: BucketEntity[]) => {
                if (buckets.length > 0) {
                    buckets.forEach((entity: BucketEntity) => {
                        if (entity.permissions.canRead) {
                            this.bucketOptions.push({
                                text: entity.bucket.name,
                                value: entity.id,
                                description: entity.bucket.description
                            });
                        }
                    });

                    const bucketId = this.bucketOptions[0].value;
                    if (bucketId) {
                        this.importFromRegistryForm.get('bucket')?.setValue(bucketId);
                        this.loadFlows(registryId, bucketId, branch);
                    }
                }
            });
    }

    loadFlows(registryId: string, bucketId: string, branch?: string | null): void {
        this.flowOptions = [];
        this.flowLookup.clear();

        this.getFlows(registryId, bucketId, branch)
            .pipe(take(1))
            .subscribe((versionedFlows: VersionedFlowEntity[]) => {
                if (versionedFlows.length > 0) {
                    versionedFlows.forEach((entity: VersionedFlowEntity) => {
                        this.flowLookup.set(entity.versionedFlow.flowId!, entity.versionedFlow);

                        this.flowOptions.push({
                            text: entity.versionedFlow.flowName,
                            value: entity.versionedFlow.flowId!,
                            description: entity.versionedFlow.description
                        });
                    });

                    const flowId = this.flowOptions[0].value;
                    if (flowId) {
                        this.importFromRegistryForm.get('flow')?.setValue(flowId);
                        this.loadVersions(registryId, bucketId, flowId, branch);
                    }
                }
            });
    }

    loadVersions(registryId: string, bucketId: string, flowId: string, branch?: string | null): void {
        this.dataSource.data = [];
        this.selectedFlowVersion = null;
        this.selectedFlowDescription = this.flowLookup.get(flowId)?.description;

        this.getFlowVersions(registryId, bucketId, flowId, branch)
            .pipe(take(1))
            .subscribe((metadataEntities: VersionedFlowSnapshotMetadataEntity[]) => {
                if (metadataEntities.length > 0) {
                    const flowVersions = metadataEntities.map(
                        (entity: VersionedFlowSnapshotMetadataEntity) => entity.versionedFlowSnapshotMetadata
                    );

                    const sortedFlowVersions = this.sortVersions(flowVersions, this.sort);
                    this.selectedFlowVersion = sortedFlowVersions[0].version;

                    this.dataSource.data = sortedFlowVersions;
                }
            });
    }

    formatTimestamp(flowVersion: VersionedFlowSnapshotMetadata) {
        // get the current user time to properly convert the server time
        const now: Date = new Date();

        // convert the user offset to millis
        const userTimeOffset: number = now.getTimezoneOffset() * 60 * 1000;

        // create the proper date by adjusting by the offsets
        const date: Date = new Date(flowVersion.timestamp + userTimeOffset + this.timeOffset);
        return this.nifiCommon.formatDateTime(date);
    }

    sortData(sort: Sort) {
        this.sort = sort;
        this.dataSource.data = this.sortVersions(this.dataSource.data, sort);
    }

    sortVersions(data: VersionedFlowSnapshotMetadata[], sort: Sort): VersionedFlowSnapshotMetadata[] {
        if (!data) {
            return [];
        }
        return data.slice().sort((a, b) => {
            const isAsc = sort.direction === 'asc';
            let retVal = 0;
            switch (sort.active) {
                case 'version':
                    retVal = this.compareVersion(a.version, b.version);
                    break;
                case 'created':
                    retVal = this.nifiCommon.compareNumber(a.timestamp, b.timestamp);
                    break;
                case 'comments':
                    retVal = this.nifiCommon.compareString(a.comments, b.comments);
                    break;
            }
            return retVal * (isAsc ? 1 : -1);
        });
    }

    private compareVersion(a: string, b: string): number {
        if (this.nifiCommon.isNumber(a) && this.nifiCommon.isNumber(b)) {
            return this.nifiCommon.compareNumber(parseInt(a, 10), parseInt(b, 10));
        } else {
            return this.nifiCommon.compareString(a, b);
        }
    }

    select(flowVersion: VersionedFlowSnapshotMetadata): void {
        this.selectedFlowVersion = flowVersion.version;
    }

    isSelected(flowVersion: VersionedFlowSnapshotMetadata): boolean {
        if (this.selectedFlowVersion) {
            return flowVersion.version == this.selectedFlowVersion;
        }
        return false;
    }

    importFromRegistry(): void {
        if (this.selectedFlowVersion != null) {
            const payload: any = {
                revision: this.client.getRevision({
                    revision: {
                        version: 0
                    }
                }),
                disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
                component: {
                    position: {
                        x: this.dialogRequest.request.position.x,
                        y: this.dialogRequest.request.position.y
                    },
                    versionControlInformation: {
                        registryId: this.importFromRegistryForm.get('registry')?.value,
                        bucketId: this.importFromRegistryForm.get('bucket')?.value,
                        flowId: this.importFromRegistryForm.get('flow')?.value,
                        version: this.selectedFlowVersion
                    }
                }
            };

            if (this.supportsBranching) {
                payload.component.versionControlInformation.branch = this.importFromRegistryForm.get('branch')?.value;
            }

            this.store.dispatch(
                importFromRegistry({
                    request: {
                        payload,
                        keepExistingParameterContext: this.importFromRegistryForm.get('keepParameterContexts')?.value
                    }
                })
            );
        }
    }

    override isDirty(): boolean {
        return this.importFromRegistryForm.dirty;
    }

    protected readonly ErrorContextKey = ErrorContextKey;
}
