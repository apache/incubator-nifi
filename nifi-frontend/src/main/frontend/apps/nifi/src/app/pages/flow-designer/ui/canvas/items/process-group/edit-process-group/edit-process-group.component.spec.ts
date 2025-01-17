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

import { ComponentFixture, TestBed } from '@angular/core/testing';

import { EditProcessGroup } from './edit-process-group.component';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { ClusterConnectionService } from '../../../../../../../service/cluster-connection.service';
import { provideMockStore } from '@ngrx/store/testing';
import { initialState } from '../../../../../state/flow/flow.reducer';

const noPermissionsParameterContextId = '95d509b9-018b-1000-daff-b7957ea7935e';
const selectedParameterContextId = '95d509b9-018b-1000-daff-b7957ea7934f';
const parameterContexts = [
    {
        revision: {
            version: 0
        },
        id: selectedParameterContextId,
        uri: '',
        permissions: {
            canRead: true,
            canWrite: true
        },
        component: {
            name: 'params 2',
            description: '',
            parameters: [],
            boundProcessGroups: [],
            inheritedParameterContexts: [],
            id: '95d509b9-018b-1000-daff-b7957ea7934f'
        }
    },
    {
        revision: {
            version: 0
        },
        id: noPermissionsParameterContextId,
        uri: '',
        permissions: {
            canRead: false,
            canWrite: false
        }
    }
];

describe('EditProcessGroup', () => {
    let component: EditProcessGroup;
    let fixture: ComponentFixture<EditProcessGroup>;

    describe('user has permission to current parameter context', () => {
        const data: any = {
            type: 'ProcessGroup',
            uri: 'https://localhost:4200/nifi-api/process-groups/162380af-018c-1000-a7eb-f5d06f77168b',
            entity: {
                revision: {
                    clientId: 'de5d3be3-05be-4ba5-bc42-729e7a4b00c4',
                    version: 14
                },
                id: '162380af-018c-1000-a7eb-f5d06f77168b',
                uri: 'https://localhost:4200/nifi-api/process-groups/162380af-018c-1000-a7eb-f5d06f77168b',
                position: {
                    x: 446,
                    y: 151
                },
                permissions: {
                    canRead: true,
                    canWrite: true
                },
                bulletins: [],
                component: {
                    id: '162380af-018c-1000-a7eb-f5d06f77168b',
                    parentGroupId: '1621f9d1-018c-1000-cb13-7eab94ffe23c',
                    position: {
                        x: 446,
                        y: 151
                    },
                    name: 'pg2',
                    comments: '',
                    flowfileConcurrency: 'UNBOUNDED',
                    flowfileOutboundPolicy: 'BATCH_OUTPUT',
                    defaultFlowFileExpiration: '0 sec',
                    defaultBackPressureObjectThreshold: 10000,
                    defaultBackPressureDataSizeThreshold: '1 GB',
                    parameterContext: {
                        id: selectedParameterContextId
                    },
                    executionEngine: 'INHERITED',
                    maxConcurrentTasks: 1,
                    statelessFlowTimeout: '1 min',
                    runningCount: 0,
                    stoppedCount: 0,
                    invalidCount: 0,
                    disabledCount: 0,
                    activeRemotePortCount: 0,
                    inactiveRemotePortCount: 0,
                    upToDateCount: 0,
                    locallyModifiedCount: 0,
                    staleCount: 0,
                    locallyModifiedAndStaleCount: 0,
                    syncFailureCount: 0,
                    localInputPortCount: 0,
                    localOutputPortCount: 0,
                    publicInputPortCount: 0,
                    publicOutputPortCount: 0,
                    statelessGroupScheduledState: 'STOPPED',
                    inputPortCount: 0,
                    outputPortCount: 0
                },
                runningCount: 0,
                stoppedCount: 0,
                invalidCount: 0,
                disabledCount: 0,
                activeRemotePortCount: 0,
                inactiveRemotePortCount: 0,
                upToDateCount: 0,
                locallyModifiedCount: 0,
                staleCount: 0,
                locallyModifiedAndStaleCount: 0,
                syncFailureCount: 0,
                localInputPortCount: 0,
                localOutputPortCount: 0,
                publicInputPortCount: 0,
                publicOutputPortCount: 0,
                inputPortCount: 0,
                outputPortCount: 0
            }
        };

        beforeEach(() => {
            TestBed.configureTestingModule({
                imports: [EditProcessGroup, NoopAnimationsModule],
                providers: [
                    { provide: MAT_DIALOG_DATA, useValue: data },
                    provideMockStore({ initialState }),
                    {
                        provide: ClusterConnectionService,
                        useValue: {
                            isDisconnectionAcknowledged: jest.fn()
                        }
                    },
                    { provide: MatDialogRef, useValue: null }
                ]
            });
            fixture = TestBed.createComponent(EditProcessGroup);
            component = fixture.componentInstance;
            component.parameterContexts = parameterContexts;

            fixture.detectChanges();
        });

        it('should create', () => {
            expect(component).toBeTruthy();
        });

        it('verify parameter context value initialized', () => {
            expect(component.editProcessGroupForm.get('parameterContext')?.value).toEqual(selectedParameterContextId);
        });

        it('verify no parameter context value selected', () => {
            component.request.entity.component.parameterContext.parameterContextId = null;
            fixture.detectChanges();
            expect(component.editProcessGroupForm.get('parameterContext')?.value).toEqual(selectedParameterContextId);
        });

        it('verify parameter context value', () => {
            expect(component.parameterContextsOptions.length).toEqual(2);
            expect(component.editProcessGroupForm.get('parameterContext')?.value).toEqual(selectedParameterContextId);
        });

        it('verify selected parameter context with permissions', () => {
            expect(component.parameterContextsOptions.length).toEqual(2);
            component.parameterContextsOptions.forEach((parameterContextsOption) => {
                if (
                    parameterContextsOption.value === noPermissionsParameterContextId &&
                    parameterContextsOption.text === noPermissionsParameterContextId
                ) {
                    expect(parameterContextsOption.disabled).toBeTruthy();
                } else if (parameterContextsOption.value === selectedParameterContextId) {
                    expect(parameterContextsOption.disabled).toBeFalsy();
                }
            });
            expect(component.editProcessGroupForm.get('parameterContext')?.value).toEqual(selectedParameterContextId);
        });
    });

    describe('user does NOT have permission to current parameter context', () => {
        const data: any = {
            type: 'ProcessGroup',
            uri: 'https://localhost:4200/nifi-api/process-groups/162380af-018c-1000-a7eb-f5d06f77168b',
            entity: {
                revision: {
                    clientId: 'de5d3be3-05be-4ba5-bc42-729e7a4b00c4',
                    version: 14
                },
                id: '162380af-018c-1000-a7eb-f5d06f77168b',
                uri: 'https://localhost:4200/nifi-api/process-groups/162380af-018c-1000-a7eb-f5d06f77168b',
                position: {
                    x: 446,
                    y: 151
                },
                permissions: {
                    canRead: true,
                    canWrite: true
                },
                bulletins: [],
                component: {
                    id: '162380af-018c-1000-a7eb-f5d06f77168b',
                    parentGroupId: '1621f9d1-018c-1000-cb13-7eab94ffe23c',
                    position: {
                        x: 446,
                        y: 151
                    },
                    name: 'pg2',
                    comments: '',
                    flowfileConcurrency: 'UNBOUNDED',
                    flowfileOutboundPolicy: 'BATCH_OUTPUT',
                    defaultFlowFileExpiration: '0 sec',
                    defaultBackPressureObjectThreshold: 10000,
                    defaultBackPressureDataSizeThreshold: '1 GB',
                    parameterContext: {
                        id: noPermissionsParameterContextId
                    },
                    executionEngine: 'INHERITED',
                    maxConcurrentTasks: 1,
                    statelessFlowTimeout: '1 min',
                    runningCount: 0,
                    stoppedCount: 0,
                    invalidCount: 0,
                    disabledCount: 0,
                    activeRemotePortCount: 0,
                    inactiveRemotePortCount: 0,
                    upToDateCount: 0,
                    locallyModifiedCount: 0,
                    staleCount: 0,
                    locallyModifiedAndStaleCount: 0,
                    syncFailureCount: 0,
                    localInputPortCount: 0,
                    localOutputPortCount: 0,
                    publicInputPortCount: 0,
                    publicOutputPortCount: 0,
                    statelessGroupScheduledState: 'STOPPED',
                    inputPortCount: 0,
                    outputPortCount: 0
                },
                runningCount: 0,
                stoppedCount: 0,
                invalidCount: 0,
                disabledCount: 0,
                activeRemotePortCount: 0,
                inactiveRemotePortCount: 0,
                upToDateCount: 0,
                locallyModifiedCount: 0,
                staleCount: 0,
                locallyModifiedAndStaleCount: 0,
                syncFailureCount: 0,
                localInputPortCount: 0,
                localOutputPortCount: 0,
                publicInputPortCount: 0,
                publicOutputPortCount: 0,
                inputPortCount: 0,
                outputPortCount: 0
            }
        };

        beforeEach(() => {
            TestBed.configureTestingModule({
                imports: [EditProcessGroup, NoopAnimationsModule],
                providers: [
                    { provide: MAT_DIALOG_DATA, useValue: data },
                    provideMockStore({ initialState }),
                    {
                        provide: ClusterConnectionService,
                        useValue: {
                            isDisconnectionAcknowledged: jest.fn()
                        }
                    },
                    { provide: MatDialogRef, useValue: null }
                ]
            });
            fixture = TestBed.createComponent(EditProcessGroup);
            component = fixture.componentInstance;
            component.parameterContexts = parameterContexts;

            fixture.detectChanges();
        });

        it('verify selected parameter context with no permissions', () => {
            expect(component.parameterContextsOptions.length).toEqual(2);
            component.parameterContextsOptions.forEach((parameterContextsOption) => {
                if (
                    parameterContextsOption.value === noPermissionsParameterContextId &&
                    parameterContextsOption.text === noPermissionsParameterContextId
                ) {
                    expect(parameterContextsOption.disabled).toBeFalsy();
                } else if (parameterContextsOption.value === selectedParameterContextId) {
                    expect(parameterContextsOption.disabled).toBeFalsy();
                }
            });
            expect(component.editProcessGroupForm.get('parameterContext')?.value).toEqual(
                noPermissionsParameterContextId
            );
        });
    });

    describe('when no current parameter context is set', () => {
        const data: any = {
            type: 'ProcessGroup',
            uri: 'https://localhost:4200/nifi-api/process-groups/162380af-018c-1000-a7eb-f5d06f77168b',
            entity: {
                revision: {
                    clientId: 'de5d3be3-05be-4ba5-bc42-729e7a4b00c4',
                    version: 14
                },
                id: '162380af-018c-1000-a7eb-f5d06f77168b',
                uri: 'https://localhost:4200/nifi-api/process-groups/162380af-018c-1000-a7eb-f5d06f77168b',
                position: {
                    x: 446,
                    y: 151
                },
                permissions: {
                    canRead: true,
                    canWrite: true
                },
                bulletins: [],
                component: {
                    id: '162380af-018c-1000-a7eb-f5d06f77168b',
                    parentGroupId: '1621f9d1-018c-1000-cb13-7eab94ffe23c',
                    position: {
                        x: 446,
                        y: 151
                    },
                    name: 'pg2',
                    comments: '',
                    flowfileConcurrency: 'UNBOUNDED',
                    flowfileOutboundPolicy: 'BATCH_OUTPUT',
                    defaultFlowFileExpiration: '0 sec',
                    defaultBackPressureObjectThreshold: 10000,
                    defaultBackPressureDataSizeThreshold: '1 GB',
                    parameterContext: {
                        id: undefined
                    },
                    executionEngine: 'INHERITED',
                    maxConcurrentTasks: 1,
                    statelessFlowTimeout: '1 min',
                    runningCount: 0,
                    stoppedCount: 0,
                    invalidCount: 0,
                    disabledCount: 0,
                    activeRemotePortCount: 0,
                    inactiveRemotePortCount: 0,
                    upToDateCount: 0,
                    locallyModifiedCount: 0,
                    staleCount: 0,
                    locallyModifiedAndStaleCount: 0,
                    syncFailureCount: 0,
                    localInputPortCount: 0,
                    localOutputPortCount: 0,
                    publicInputPortCount: 0,
                    publicOutputPortCount: 0,
                    statelessGroupScheduledState: 'STOPPED',
                    inputPortCount: 0,
                    outputPortCount: 0
                },
                runningCount: 0,
                stoppedCount: 0,
                invalidCount: 0,
                disabledCount: 0,
                activeRemotePortCount: 0,
                inactiveRemotePortCount: 0,
                upToDateCount: 0,
                locallyModifiedCount: 0,
                staleCount: 0,
                locallyModifiedAndStaleCount: 0,
                syncFailureCount: 0,
                localInputPortCount: 0,
                localOutputPortCount: 0,
                publicInputPortCount: 0,
                publicOutputPortCount: 0,
                inputPortCount: 0,
                outputPortCount: 0
            }
        };

        beforeEach(() => {
            TestBed.configureTestingModule({
                imports: [EditProcessGroup, NoopAnimationsModule],
                providers: [
                    { provide: MAT_DIALOG_DATA, useValue: data },
                    provideMockStore({ initialState }),
                    {
                        provide: ClusterConnectionService,
                        useValue: {
                            isDisconnectionAcknowledged: jest.fn()
                        }
                    },
                    { provide: MatDialogRef, useValue: null }
                ]
            });
            fixture = TestBed.createComponent(EditProcessGroup);
            component = fixture.componentInstance;
            component.parameterContexts = parameterContexts;

            fixture.detectChanges();
        });

        it('verify no selected parameter context', () => {
            expect(component.parameterContextsOptions.length).toEqual(2);
            component.parameterContextsOptions.forEach((parameterContextsOption) => {
                if (parameterContextsOption.value === noPermissionsParameterContextId) {
                    expect(parameterContextsOption.disabled).toBeTruthy();
                } else if (parameterContextsOption.value === selectedParameterContextId) {
                    expect(parameterContextsOption.disabled).toBeFalsy();
                }
            });
            expect(component.editProcessGroupForm.get('parameterContext')?.value).toEqual(undefined);
        });
    });
});
