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

import { ControllerServices } from './controller-services.component';
import { provideMockStore } from '@ngrx/store/testing';
import { initialState } from '../../state/controller-services/controller-services.reducer';
import { RouterTestingModule } from '@angular/router/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { MockComponent } from 'ng-mocks';
import { Navigation } from '../../../../ui/common/navigation/navigation.component';
import { canvasFeatureKey } from '../../state';
import { controllerServicesFeatureKey } from '../../state/controller-services';
import { NgxSkeletonLoaderComponent } from 'ngx-skeleton-loader';

describe('ControllerServices', () => {
    let component: ControllerServices;
    let fixture: ComponentFixture<ControllerServices>;

    beforeEach(() => {
        TestBed.configureTestingModule({
            declarations: [ControllerServices],
            imports: [
                RouterTestingModule,
                MockComponent(Navigation),
                HttpClientTestingModule,
                MockComponent(NgxSkeletonLoaderComponent)
            ],
            providers: [
                provideMockStore({
                    initialState: {
                        [canvasFeatureKey]: {
                            [controllerServicesFeatureKey]: initialState
                        }
                    }
                })
            ]
        });
        fixture = TestBed.createComponent(ControllerServices);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
