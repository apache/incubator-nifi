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

import { AfterViewInit, Component, DestroyRef, inject, OnDestroy, ViewChild } from '@angular/core';
import { Store } from '@ngrx/store';
import {
    selectProcessorIdFromRoute,
    selectProcessorStatus,
    selectProcessorStatusSnapshots,
    selectSelectedClusterNode,
    selectSummaryListingLoadedTimestamp,
    selectSummaryListingStatus,
    selectViewStatusHistory
} from '../../state/summary-listing/summary-listing.selectors';
import { SummaryListingState } from '../../state/summary-listing';
import { selectCurrentUser } from '../../../../state/current-user/current-user.selectors';
import { initialState } from '../../state/summary-listing/summary-listing.reducer';
import { getStatusHistoryAndOpenDialog } from '../../../../state/status-history/status-history.actions';
import { ComponentType, isDefinedAndNotNull } from '@nifi/shared';
import { combineLatest, delay, filter, map, Subject, switchMap, take } from 'rxjs';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import * as SummaryListingActions from '../../state/summary-listing/summary-listing.actions';
import * as ClusterStatusActions from '../../state/component-cluster-status/component-cluster-status.actions';
import { MatPaginator } from '@angular/material/paginator';
import { ProcessorStatusTable } from './processor-status-table/processor-status-table.component';
import {
    selectClusterSearchResults,
    selectClusterSummary
} from '../../../../state/cluster-summary/cluster-summary.selectors';
import { loadClusterSummary } from '../../../../state/cluster-summary/cluster-summary.actions';
import { ProcessorStatusSnapshotEntity } from '../../state';
import { NodeSearchResult } from '../../../../state/cluster-summary';
import { AsyncPipe } from '@angular/common';
import { NgxSkeletonLoaderComponent } from 'ngx-skeleton-loader';

@Component({
    selector: 'processor-status-listing',
    templateUrl: './processor-status-listing.component.html',
    imports: [AsyncPipe, NgxSkeletonLoaderComponent, ProcessorStatusTable],
    styleUrls: ['./processor-status-listing.component.scss']
})
export class ProcessorStatusListing implements AfterViewInit, OnDestroy {
    private destroyRef = inject(DestroyRef);

    processorStatusSnapshots$ = this.store.select(selectProcessorStatusSnapshots);
    loadedTimestamp$ = this.store.select(selectSummaryListingLoadedTimestamp);
    summaryListingStatus$ = this.store.select(selectSummaryListingStatus);
    selectedProcessorId$ = this.store.select(selectProcessorIdFromRoute);
    connectedToCluster$ = this.store.select(selectClusterSummary).pipe(
        isDefinedAndNotNull(),
        map((cluster) => cluster.connectedToCluster)
    );
    clusterNodes$ = this.store.select(selectClusterSearchResults).pipe(
        isDefinedAndNotNull(),
        map((results) => results.nodeResults)
    );
    selectedClusterNode$ = this.store.select(selectSelectedClusterNode);

    currentUser$ = this.store.select(selectCurrentUser);

    @ViewChild(MatPaginator) paginator!: MatPaginator;
    @ViewChild(ProcessorStatusTable) table!: ProcessorStatusTable;
    private subject: Subject<void> = new Subject<void>();

    constructor(private store: Store<SummaryListingState>) {
        this.store
            .select(selectViewStatusHistory)
            .pipe(
                filter((id: string) => !!id),
                switchMap((id: string) =>
                    this.store.select(selectProcessorStatus(id)).pipe(
                        filter((processor) => !!processor),
                        take(1)
                    )
                ),
                takeUntilDestroyed()
            )
            .subscribe((processor) => {
                if (processor) {
                    this.store.dispatch(
                        getStatusHistoryAndOpenDialog({
                            request: {
                                source: 'summary',
                                componentType: ComponentType.Processor,
                                componentId: processor.id
                            }
                        })
                    );
                }
            });
    }

    ngAfterViewInit(): void {
        combineLatest([this.processorStatusSnapshots$, this.loadedTimestamp$])
            .pipe(
                filter(([processors, ts]) => !!processors && !this.isInitialLoading(ts)),
                delay(0),
                takeUntilDestroyed(this.destroyRef)
            )
            .subscribe(() => {
                this.subject.next();
            });

        this.subject.pipe(takeUntilDestroyed(this.destroyRef)).subscribe(() => {
            if (this.table) {
                this.table.paginator = this.paginator;
            }
        });
    }

    ngOnDestroy(): void {
        this.subject.complete();
    }

    isInitialLoading(loadedTimestamp: string): boolean {
        return loadedTimestamp == initialState.loadedTimestamp;
    }

    refreshSummaryListing() {
        this.store.dispatch(SummaryListingActions.loadSummaryListing({ recursive: true }));
        this.store.dispatch(loadClusterSummary());
    }

    viewStatusHistory(processor: ProcessorStatusSnapshotEntity): void {
        this.store.dispatch(
            SummaryListingActions.navigateToViewProcessorStatusHistory({
                id: processor.id
            })
        );
    }

    selectProcessor(processor: ProcessorStatusSnapshotEntity): void {
        this.store.dispatch(
            SummaryListingActions.selectProcessorStatus({
                request: {
                    id: processor.id
                }
            })
        );
    }

    clearSelection() {
        this.store.dispatch(SummaryListingActions.clearProcessorStatusSelection());
    }

    viewClusteredDetails(processor: ProcessorStatusSnapshotEntity): void {
        this.store.dispatch(
            ClusterStatusActions.loadComponentClusterStatusAndOpenDialog({
                request: {
                    id: processor.id,
                    componentType: ComponentType.Processor
                }
            })
        );
    }

    clusterNodeSelected(clusterNode: NodeSearchResult) {
        this.store.dispatch(SummaryListingActions.selectClusterNode({ clusterNode }));
    }
}
