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

import { EditParameterDialog } from './edit-parameter-dialog.component';
import { EditParameterRequest } from '../../../state/shared';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';

describe('EditParameterDialog', () => {
    let component: EditParameterDialog;
    let fixture: ComponentFixture<EditParameterDialog>;

    const data: EditParameterRequest = {
        isNewParameterContext: false,
        parameter: {
            name: 'one',
            description: 'Description for one.',
            sensitive: false,
            value: 'value',
            provided: false,
            referencingComponents: [],
            parameterContext: {
                id: '95d4f3d2-018b-1000-b7c7-b830c49a8026',
                permissions: {
                    canRead: true,
                    canWrite: true
                },
                component: {
                    id: '95d4f3d2-018b-1000-b7c7-b830c49a8026',
                    name: 'params 1'
                }
            },
            inherited: false
        }
    };

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [EditParameterDialog, NoopAnimationsModule],
            providers: [
                { provide: MAT_DIALOG_DATA, useValue: data },
                { provide: MatDialogRef, useValue: null }
            ]
        });
        fixture = TestBed.createComponent(EditParameterDialog);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
