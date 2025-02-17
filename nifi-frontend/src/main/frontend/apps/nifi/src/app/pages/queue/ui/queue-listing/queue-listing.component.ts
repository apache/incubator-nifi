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
import { distinctUntilChanged, filter } from 'rxjs';
import {
    selectCompletedListingRequest,
    selectConnectionIdFromRoute,
    selectLoadedTimestamp,
    selectSelectedConnection,
    selectStatus
} from '../../state/queue-listing/queue-listing.selectors';
import { FlowFileSummary } from '../../state/queue-listing';
import {
    downloadFlowFileContent,
    loadConnectionLabel,
    resetQueueListingState,
    resubmitQueueListingRequest,
    submitQueueListingRequest,
    viewFlowFile,
    viewFlowFileContent
} from '../../state/queue-listing/queue-listing.actions';
import { selectCurrentUser } from '../../../../state/current-user/current-user.selectors';
import { NiFiState } from '../../../../state';
import { selectAbout } from '../../../../state/about/about.selectors';
import { About } from '../../../../state/about';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { selectClusterSummary } from '../../../../state/cluster-summary/cluster-summary.selectors';
import { loadClusterSummary } from '../../../../state/cluster-summary/cluster-summary.actions';
import { AsyncPipe } from '@angular/common';
import { FlowFileTable } from './flowfile-table/flowfile-table.component';
import { NgxSkeletonLoaderComponent } from 'ngx-skeleton-loader';
import { MatIconButton } from '@angular/material/button';

@Component({
    selector: 'queue-listing',
    templateUrl: './queue-listing.component.html',
    imports: [AsyncPipe, FlowFileTable, NgxSkeletonLoaderComponent, MatIconButton],
    styleUrls: ['./queue-listing.component.scss']
})
export class QueueListing implements OnInit, OnDestroy {
    status$ = this.store.select(selectStatus);
    selectedConnection$ = this.store.select(selectSelectedConnection);
    loadedTimestamp$ = this.store.select(selectLoadedTimestamp);
    listingRequest$ = this.store.select(selectCompletedListingRequest);
    currentUser$ = this.store.select(selectCurrentUser);
    about$ = this.store.select(selectAbout);
    clusterSummary$ = this.store.select(selectClusterSummary);

    constructor(private store: Store<NiFiState>) {
        this.store
            .select(selectConnectionIdFromRoute)
            .pipe(
                filter((connectionId) => connectionId != null),
                distinctUntilChanged(),
                takeUntilDestroyed()
            )
            .subscribe((connectionId) => {
                this.store.dispatch(
                    loadConnectionLabel({
                        request: {
                            connectionId
                        }
                    })
                );
                this.store.dispatch(
                    submitQueueListingRequest({
                        request: {
                            connectionId
                        }
                    })
                );
            });
    }

    ngOnInit(): void {
        this.store.dispatch(loadClusterSummary());
    }

    refreshClicked(): void {
        this.store.dispatch(resubmitQueueListingRequest());
    }

    contentViewerAvailable(about: About): boolean {
        return about.contentViewerUrl != null;
    }

    viewFlowFile(flowfileSummary: FlowFileSummary): void {
        this.store.dispatch(viewFlowFile({ request: { flowfileSummary } }));
    }

    downloadContent(flowfileSummary: FlowFileSummary): void {
        this.store.dispatch(
            downloadFlowFileContent({
                request: {
                    uri: flowfileSummary.uri,
                    clusterNodeId: flowfileSummary.clusterNodeId
                }
            })
        );
    }

    viewContent(flowfileSummary: FlowFileSummary): void {
        this.store.dispatch(
            viewFlowFileContent({
                request: {
                    uri: flowfileSummary.uri,
                    mimeType: flowfileSummary.mimeType,
                    clusterNodeId: flowfileSummary.clusterNodeId
                }
            })
        );
    }

    ngOnDestroy(): void {
        this.store.dispatch(resetQueueListingState());
    }
}
