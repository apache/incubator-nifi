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

import { Component } from '@angular/core';
import { Store } from '@ngrx/store';
import { ParameterContextEntity, ParameterContextListingState } from '../../state/parameter-context-listing';
import {
    selectContext,
    selectParameterContextIdFromRoute,
    selectParameterContextListingState,
    selectSingleEditedParameterContext
} from '../../state/parameter-context-listing/parameter-context-listing.selectors';
import {
    loadParameterContexts,
    navigateToEditParameterContext,
    openNewParameterContextDialog,
    openParameterContextServiceDialog,
    promptParameterContextDeletion,
    selectParameterContext
} from '../../state/parameter-context-listing/parameter-context-listing.actions';
import { initialState } from '../../state/parameter-context-listing/parameter-context-listing.reducer';
import { filter, switchMap, take } from 'rxjs';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

@Component({
    selector: 'parameter-context-listing',
    templateUrl: './parameter-context-listing.component.html',
    styleUrls: ['./parameter-context-listing.component.scss']
})
export class ParameterContextListing {
    parameterContextListingState$ = this.store.select(selectParameterContextListingState);
    selectedParameterContextId$ = this.store.select(selectParameterContextIdFromRoute);

    constructor(private store: Store<ParameterContextListingState>) {
        this.store
            .select(selectSingleEditedParameterContext)
            .pipe(
                filter((id: string) => id != null),
                switchMap((id: string) =>
                    this.store.select(selectContext(id)).pipe(
                        filter((entity) => entity != null),
                        take(1)
                    )
                ),
                takeUntilDestroyed()
            )
            .subscribe((entity) => {
                if (entity) {
                    this.store.dispatch(
                        openParameterContextServiceDialog({
                            request: {
                                parameterContext: entity
                            }
                        })
                    );
                }
            });
    }

    ngOnInit(): void {
        this.store.dispatch(loadParameterContexts());
    }

    isInitialLoading(state: ParameterContextListingState): boolean {
        // using the current timestamp to detect the initial load event
        return state.loadedTimestamp == initialState.loadedTimestamp;
    }

    openNewParameterContextDialog(): void {
        this.store.dispatch(openNewParameterContextDialog());
    }

    refreshParameterContextListing(): void {
        this.store.dispatch(loadParameterContexts());
    }

    selectParameterContext(entity: ParameterContextEntity): void {
        this.store.dispatch(
            selectParameterContext({
                request: {
                    id: entity.id
                }
            })
        );
    }

    editParameterContext(entity: ParameterContextEntity): void {
        this.store.dispatch(
            navigateToEditParameterContext({
                id: entity.id
            })
        );
    }

    deleteParameterContext(entity: ParameterContextEntity): void {
        this.store.dispatch(
            promptParameterContextDeletion({
                request: {
                    parameterContext: entity
                }
            })
        );
    }
}
