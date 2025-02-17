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

import { Component, OnDestroy, OnInit } from '@angular/core';
import { GeneralState } from '../../state/general';
import { Store } from '@ngrx/store';
import { loadControllerConfig, resetGeneralState } from '../../state/general/general.actions';
import { selectGeneral } from '../../state/general/general.selectors';
import { AsyncPipe } from '@angular/common';
import { NgxSkeletonLoaderComponent } from 'ngx-skeleton-loader';
import { GeneralForm } from './general-form/general-form.component';

@Component({
    selector: 'general',
    templateUrl: './general.component.html',
    imports: [AsyncPipe, NgxSkeletonLoaderComponent, GeneralForm],
    styleUrls: ['./general.component.scss']
})
export class General implements OnInit, OnDestroy {
    general$ = this.store.select(selectGeneral);

    constructor(private store: Store<GeneralState>) {}

    ngOnInit(): void {
        this.store.dispatch(loadControllerConfig());
    }

    ngOnDestroy(): void {
        this.store.dispatch(resetGeneralState());
    }
}
