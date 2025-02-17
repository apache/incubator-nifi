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

import { FlowAnalysisRules } from './flow-analysis-rules.component';
import { provideMockStore } from '@ngrx/store/testing';
import { initialState } from '../../state/flow-analysis-rules/flow-analysis-rules.reducer';

describe('FlowAnalysisRules', () => {
    let component: FlowAnalysisRules;
    let fixture: ComponentFixture<FlowAnalysisRules>;

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [FlowAnalysisRules],
            providers: [
                provideMockStore({
                    initialState
                })
            ]
        });
        fixture = TestBed.createComponent(FlowAnalysisRules);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
