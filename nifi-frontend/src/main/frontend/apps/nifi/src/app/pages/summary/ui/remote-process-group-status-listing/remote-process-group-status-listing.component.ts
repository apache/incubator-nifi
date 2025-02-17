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

import { Component } from '@angular/core';
import {
    selectRemoteProcessGroupIdFromRoute,
    selectRemoteProcessGroupStatus,
    selectRemoteProcessGroupStatusSnapshots,
    selectSelectedClusterNode,
    selectSummaryListingLoadedTimestamp,
    selectSummaryListingStatus,
    selectViewStatusHistory
} from '../../state/summary-listing/summary-listing.selectors';
import { selectCurrentUser } from '../../../../state/current-user/current-user.selectors';
import { Store } from '@ngrx/store';
import { SummaryListingState } from '../../state/summary-listing';
import { filter, map, switchMap, take } from 'rxjs';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { getStatusHistoryAndOpenDialog } from '../../../../state/status-history/status-history.actions';
import { ComponentType, isDefinedAndNotNull } from '@nifi/shared';
import { initialState } from '../../state/summary-listing/summary-listing.reducer';
import * as SummaryListingActions from '../../state/summary-listing/summary-listing.actions';
import { loadClusterSummary } from '../../../../state/cluster-summary/cluster-summary.actions';
import { RemoteProcessGroupStatusSnapshotEntity } from '../../state';
import {
    selectClusterSearchResults,
    selectClusterSummary
} from '../../../../state/cluster-summary/cluster-summary.selectors';
import * as ClusterStatusActions from '../../state/component-cluster-status/component-cluster-status.actions';
import { NodeSearchResult } from '../../../../state/cluster-summary';
import { AsyncPipe } from '@angular/common';
import { NgxSkeletonLoaderComponent } from 'ngx-skeleton-loader';
import { RemoteProcessGroupStatusTable } from './remote-process-group-status-table/remote-process-group-status-table.component';

@Component({
    selector: 'remote-process-group-status-listing',
    templateUrl: './remote-process-group-status-listing.component.html',
    imports: [AsyncPipe, NgxSkeletonLoaderComponent, RemoteProcessGroupStatusTable],
    styleUrls: ['./remote-process-group-status-listing.component.scss']
})
export class RemoteProcessGroupStatusListing {
    loadedTimestamp$ = this.store.select(selectSummaryListingLoadedTimestamp);
    summaryListingStatus$ = this.store.select(selectSummaryListingStatus);
    currentUser$ = this.store.select(selectCurrentUser);
    rpgStatusSnapshots$ = this.store.select(selectRemoteProcessGroupStatusSnapshots);
    selectedRpgId$ = this.store.select(selectRemoteProcessGroupIdFromRoute);
    connectedToCluster$ = this.store.select(selectClusterSummary).pipe(
        isDefinedAndNotNull(),
        map((cluster) => cluster.connectedToCluster)
    );
    clusterNodes$ = this.store.select(selectClusterSearchResults).pipe(
        isDefinedAndNotNull(),
        map((results) => results.nodeResults)
    );
    selectedClusterNode$ = this.store.select(selectSelectedClusterNode);

    constructor(private store: Store<SummaryListingState>) {
        this.store
            .select(selectViewStatusHistory)
            .pipe(
                filter((id: string) => !!id),
                switchMap((id: string) =>
                    this.store.select(selectRemoteProcessGroupStatus(id)).pipe(
                        filter((connection) => !!connection),
                        take(1)
                    )
                ),
                takeUntilDestroyed()
            )
            .subscribe((rpg) => {
                if (rpg) {
                    this.store.dispatch(
                        getStatusHistoryAndOpenDialog({
                            request: {
                                source: 'summary',
                                componentType: ComponentType.RemoteProcessGroup,
                                componentId: rpg.id
                            }
                        })
                    );
                }
            });
    }

    isInitialLoading(loadedTimestamp: string): boolean {
        return loadedTimestamp == initialState.loadedTimestamp;
    }

    refreshSummaryListing() {
        this.store.dispatch(SummaryListingActions.loadSummaryListing({ recursive: true }));
        this.store.dispatch(loadClusterSummary());
    }

    selectRemoteProcessGroup(rpg: RemoteProcessGroupStatusSnapshotEntity): void {
        this.store.dispatch(
            SummaryListingActions.selectRemoteProcessGroupStatus({
                request: {
                    id: rpg.id
                }
            })
        );
    }

    clearSelection() {
        this.store.dispatch(SummaryListingActions.clearRemoteProcessGroupStatusSelection());
    }

    viewStatusHistory(rpg: RemoteProcessGroupStatusSnapshotEntity): void {
        this.store.dispatch(
            SummaryListingActions.navigateToViewRemoteProcessGroupStatusHistory({
                id: rpg.id
            })
        );
    }

    viewClusteredDetails(processor: RemoteProcessGroupStatusSnapshotEntity): void {
        this.store.dispatch(
            ClusterStatusActions.loadComponentClusterStatusAndOpenDialog({
                request: {
                    id: processor.id,
                    componentType: ComponentType.RemoteProcessGroup
                }
            })
        );
    }

    clusterNodeSelected(clusterNode: NodeSearchResult) {
        this.store.dispatch(SummaryListingActions.selectClusterNode({ clusterNode }));
    }
}
