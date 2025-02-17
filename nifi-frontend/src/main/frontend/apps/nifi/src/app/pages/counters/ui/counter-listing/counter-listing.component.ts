/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Component, OnInit } from '@angular/core';
import { CounterEntity, CounterListingState } from '../../state/counter-listing';
import { Store } from '@ngrx/store';
import { loadCounters, promptCounterReset } from '../../state/counter-listing/counter-listing.actions';
import { selectCounterListingState } from '../../state/counter-listing/counter-listing.selectors';
import { initialState } from '../../state/counter-listing/counter-listing.reducer';
import { selectCurrentUser } from '../../../../state/current-user/current-user.selectors';
import { AsyncPipe } from '@angular/common';
import { NgxSkeletonLoaderComponent } from 'ngx-skeleton-loader';
import { MatIconButton } from '@angular/material/button';
import { CounterTable } from './counter-table/counter-table.component';

@Component({
    selector: 'counter-listing',
    templateUrl: './counter-listing.component.html',
    imports: [CounterTable, AsyncPipe, NgxSkeletonLoaderComponent, MatIconButton],
    styleUrls: ['./counter-listing.component.scss']
})
export class CounterListing implements OnInit {
    counterListingState$ = this.store.select(selectCounterListingState);
    currentUser$ = this.store.select(selectCurrentUser);

    constructor(private store: Store<CounterListingState>) {}

    ngOnInit(): void {
        this.store.dispatch(loadCounters());
    }

    isInitialLoading(state: CounterListingState): boolean {
        return state.loadedTimestamp == initialState.loadedTimestamp;
    }

    refreshCounterListing() {
        this.store.dispatch(loadCounters());
    }

    resetCounter(entity: CounterEntity): void {
        this.store.dispatch(
            promptCounterReset({
                request: {
                    counter: entity
                }
            })
        );
    }
}
