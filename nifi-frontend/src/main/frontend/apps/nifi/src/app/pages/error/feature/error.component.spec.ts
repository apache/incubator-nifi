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

import { Error } from './error.component';
import { provideMockStore } from '@ngrx/store/testing';
import { initialState } from '../../../state/current-user/current-user.reducer';
import { errorFeatureKey } from '../../../state/error';
import { MockComponent } from 'ng-mocks';
import { PageContent } from '../../../ui/common/page-content/page-content.component';

describe('Error', () => {
    let component: Error;
    let fixture: ComponentFixture<Error>;

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [Error, MockComponent(PageContent)],
            providers: [
                provideMockStore({
                    initialState: {
                        [errorFeatureKey]: initialState
                    }
                })
            ]
        });
        fixture = TestBed.createComponent(Error);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
