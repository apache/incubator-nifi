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

import { CreateProcessGroup } from './create-process-group.component';
import { CreateProcessGroupDialogRequest } from '../../../../../state/flow';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { ComponentType } from '@nifi/shared';
import { provideMockStore } from '@ngrx/store/testing';
import { initialState } from '../../../../../state/flow/flow.reducer';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';

const noPermissionsParameterContextId = '95d509b9-018b-1000-daff-b7957ea7935e';
const parameterContextId = '95d509b9-018b-1000-daff-b7957ea7934f';
const parameterContexts = [
    {
        revision: {
            version: 0
        },
        id: parameterContextId,
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
            id: parameterContextId
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

describe('CreateProcessGroup', () => {
    describe('user has permission to current parameter context', () => {
        let component: CreateProcessGroup;
        let fixture: ComponentFixture<CreateProcessGroup>;

        const data: CreateProcessGroupDialogRequest = {
            request: {
                revision: {
                    clientId: 'a6482293-7fe8-43b4-8ab4-ee95b3b27721',
                    version: 0
                },
                type: ComponentType.ProcessGroup,
                position: {
                    x: -4,
                    y: -698.5
                }
            },
            currentParameterContextId: parameterContextId,
            parameterContexts
        };

        beforeEach(() => {
            TestBed.configureTestingModule({
                imports: [CreateProcessGroup, NoopAnimationsModule],
                providers: [
                    { provide: MAT_DIALOG_DATA, useValue: data },
                    provideMockStore({ initialState }),
                    { provide: MatDialogRef, useValue: null }
                ]
            });
            fixture = TestBed.createComponent(CreateProcessGroup);
            component = fixture.componentInstance;
            component.parameterContexts = [...parameterContexts];
            fixture.detectChanges();
        });

        it('should create', () => {
            expect(component).toBeTruthy();
        });

        it('verify current parameter context selected', () => {
            fixture.detectChanges();
            expect(component.createProcessGroupForm.get('newProcessGroupParameterContext')?.value).toEqual(
                parameterContextId
            );
        });

        it('verify setting new parameter contexts does not append', () => {
            component.parameterContexts = parameterContexts;
            fixture.detectChanges();
            expect(component.parameterContextsOptions.length).toEqual(2);
        });
    });

    describe('user does NOT have permission to current parameter context', () => {
        let component: CreateProcessGroup;
        let fixture: ComponentFixture<CreateProcessGroup>;

        const data: CreateProcessGroupDialogRequest = {
            request: {
                revision: {
                    clientId: 'a6482293-7fe8-43b4-8ab4-ee95b3b27721',
                    version: 0
                },
                type: ComponentType.ProcessGroup,
                position: {
                    x: -4,
                    y: -698.5
                }
            },
            currentParameterContextId: noPermissionsParameterContextId,
            parameterContexts
        };

        beforeEach(() => {
            TestBed.configureTestingModule({
                imports: [CreateProcessGroup, NoopAnimationsModule],
                providers: [
                    { provide: MAT_DIALOG_DATA, useValue: data },
                    provideMockStore({ initialState }),
                    { provide: MatDialogRef, useValue: null }
                ]
            });
            fixture = TestBed.createComponent(CreateProcessGroup);
            component = fixture.componentInstance;
            component.parameterContexts = [...parameterContexts];
            fixture.detectChanges();
        });

        it('verify no parameter context selected', () => {
            expect(component.createProcessGroupForm.get('newProcessGroupParameterContext')?.value).toEqual(null);
        });
    });
});
