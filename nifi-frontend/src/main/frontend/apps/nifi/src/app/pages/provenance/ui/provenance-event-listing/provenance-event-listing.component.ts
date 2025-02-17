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
import { Store } from '@ngrx/store';
import {
    GoToProvenanceEventSourceRequest,
    Provenance,
    ProvenanceEventListingState,
    ProvenanceEventRequest,
    ProvenanceRequest,
    ProvenanceResults
} from '../../state/provenance-event-listing';
import {
    selectCompletedProvenance,
    selectLoadedTimestamp,
    selectProvenanceRequest,
    selectSearchableFieldsFromRoute,
    selectStatus
} from '../../state/provenance-event-listing/provenance-event-listing.selectors';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { filter, map, take, tap } from 'rxjs';
import {
    clearProvenanceRequest,
    goToProvenanceEventSource,
    loadClusterNodesAndOpenSearchDialog,
    openProvenanceEventDialog,
    resetProvenanceState,
    resubmitProvenanceQuery,
    saveProvenanceRequest
} from '../../state/provenance-event-listing/provenance-event-listing.actions';
import { ProvenanceSearchDialog } from './provenance-search-dialog/provenance-search-dialog.component';
import { resetLineage, submitLineageQuery } from '../../state/lineage/lineage.actions';
import { LineageRequest } from '../../state/lineage';
import { selectCompletedLineage } from '../../state/lineage/lineage.selectors';
import { clearBannerErrors } from '../../../../state/error/error.actions';
import { selectClusterSummary } from '../../../../state/cluster-summary/cluster-summary.selectors';
import { loadClusterSummary } from '../../../../state/cluster-summary/cluster-summary.actions';
import { ErrorContextKey } from '../../../../state/error';
import { AsyncPipe } from '@angular/common';
import { ProvenanceEventTable } from './provenance-event-table/provenance-event-table.component';
import { NgxSkeletonLoaderComponent } from 'ngx-skeleton-loader';

@Component({
    selector: 'provenance-event-listing',
    templateUrl: './provenance-event-listing.component.html',
    imports: [AsyncPipe, ProvenanceEventTable, NgxSkeletonLoaderComponent],
    styleUrls: ['./provenance-event-listing.component.scss']
})
export class ProvenanceEventListing implements OnInit, OnDestroy {
    status$ = this.store.select(selectStatus);
    loadedTimestamp$ = this.store.select(selectLoadedTimestamp);
    provenance$ = this.store.select(selectCompletedProvenance);
    lineage$ = this.store.select(selectCompletedLineage);
    clusterSummary$ = this.store.select(selectClusterSummary);

    request!: ProvenanceRequest;
    stateReset = false;

    constructor(private store: Store<ProvenanceEventListingState>) {
        this.store
            .select(selectSearchableFieldsFromRoute)
            .pipe(
                filter((queryParams) => queryParams != null),
                // only consider the first searchable fields from route, subsequent changes to the route will be present in the saved request
                take(1),
                map((queryParams) => {
                    const request: ProvenanceRequest = {
                        incrementalResults: false,
                        maxResults: 1000,
                        summarize: true
                    };

                    if (queryParams) {
                        if (queryParams.componentId || queryParams.flowFileUuid) {
                            request.searchTerms = {};

                            if (queryParams.componentId) {
                                request.searchTerms['ProcessorID'] = {
                                    value: queryParams.componentId,
                                    inverse: false
                                };
                            }
                            if (queryParams.flowFileUuid) {
                                request.searchTerms['FlowFileUUID'] = {
                                    value: queryParams.flowFileUuid,
                                    inverse: false
                                };
                            }
                        }
                    }

                    return request;
                })
            )
            .subscribe((request) => {
                this.store.dispatch(
                    saveProvenanceRequest({
                        request
                    })
                );
            });

        // any changes to the saved request will trigger a provenance query submission
        this.store
            .select(selectProvenanceRequest)
            .pipe(
                map((request) => {
                    if (request) {
                        return request;
                    }

                    const initialRequest: ProvenanceRequest = {
                        incrementalResults: false,
                        maxResults: 1000,
                        summarize: true
                    };

                    return initialRequest;
                }),
                filter(() => !this.stateReset),
                tap((request) => (this.request = request)),
                takeUntilDestroyed()
            )
            .subscribe((request) => {
                this.store.dispatch(
                    resubmitProvenanceQuery({
                        request
                    })
                );
            });
    }

    ngOnInit(): void {
        this.store.dispatch(loadClusterSummary());
    }

    getResultsMessage(provenance: Provenance): string {
        const request: ProvenanceRequest = provenance.request;
        const results: ProvenanceResults = provenance.results;

        if (this.hasRequest(request)) {
            if (results.totalCount >= ProvenanceSearchDialog.MAX_RESULTS) {
                return `Showing ${ProvenanceSearchDialog.MAX_RESULTS} of ${results.totalCount} events that match the specified query, please refine the search.`;
            } else {
                return 'Showing the events that match the specified query.';
            }
        } else {
            if (results.totalCount >= ProvenanceSearchDialog.MAX_RESULTS) {
                return `Showing the most recent ${ProvenanceSearchDialog.MAX_RESULTS} of ${results.totalCount} events, please refine the search.`;
            } else {
                return 'Showing the most recent events.';
            }
        }
    }

    hasRequest(request: ProvenanceRequest): boolean {
        const hasSearchTerms: boolean = !!request.searchTerms && Object.keys(request.searchTerms).length > 0;
        return !!request.startDate || !!request.endDate || hasSearchTerms;
    }

    clearRequest(): void {
        this.store.dispatch(clearProvenanceRequest());
    }

    openSearchCriteria(): void {
        this.store.dispatch(loadClusterNodesAndOpenSearchDialog());
    }

    openEventDialog(request: ProvenanceEventRequest): void {
        this.store.dispatch(
            openProvenanceEventDialog({
                request
            })
        );
    }

    goToEventSource(request: GoToProvenanceEventSourceRequest): void {
        this.store.dispatch(
            goToProvenanceEventSource({
                request
            })
        );
    }

    resubmitProvenanceQuery(): void {
        this.store.dispatch(
            resubmitProvenanceQuery({
                request: this.request
            })
        );
    }

    queryLineage(request: LineageRequest): void {
        this.store.dispatch(
            submitLineageQuery({
                request
            })
        );
    }

    clearBannerErrors(): void {
        this.store.dispatch(clearBannerErrors({ context: ErrorContextKey.PROVENANCE }));
    }

    resetLineage(): void {
        this.store.dispatch(resetLineage());
    }

    ngOnDestroy(): void {
        this.stateReset = true;
        this.store.dispatch(resetProvenanceState());
        this.store.dispatch(resetLineage());
    }
}
