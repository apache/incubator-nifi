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
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { Store } from '@ngrx/store';
import { filter, switchMap, take } from 'rxjs';
import {
    selectFlowAnalysisRuleIdFromRoute,
    selectFlowAnalysisRulesState,
    selectRule,
    selectSingleEditedFlowAnalysisRule
} from '../../state/flow-analysis-rules/flow-analysis-rules.selectors';
import {
    disableFlowAnalysisRule,
    enableFlowAnalysisRule,
    loadFlowAnalysisRules,
    navigateToEditFlowAnalysisRule,
    openChangeFlowAnalysisRuleVersionDialog,
    openConfigureFlowAnalysisRuleDialog,
    openNewFlowAnalysisRuleDialog,
    promptFlowAnalysisRuleDeletion,
    resetFlowAnalysisRulesState,
    selectFlowAnalysisRule
} from '../../state/flow-analysis-rules/flow-analysis-rules.actions';
import { initialState } from '../../state/flow-analysis-rules/flow-analysis-rules.reducer';
import { selectCurrentUser } from '../../../../state/current-user/current-user.selectors';
import { NiFiState } from '../../../../state';
import { FlowAnalysisRuleEntity, FlowAnalysisRulesState } from '../../state/flow-analysis-rules';
import { getComponentStateAndOpenDialog } from '../../../../state/component-state/component-state.actions';
import { navigateToComponentDocumentation } from '../../../../state/documentation/documentation.actions';
import { ComponentType } from '@nifi/shared';
import { AsyncPipe } from '@angular/common';
import { NgxSkeletonLoaderComponent } from 'ngx-skeleton-loader';
import { MatIconButton } from '@angular/material/button';
import { FlowAnalysisRuleTable } from './flow-analysis-rule-table/flow-analysis-rule-table.component';

@Component({
    selector: 'flow-analysis-rules',
    templateUrl: './flow-analysis-rules.component.html',
    imports: [AsyncPipe, NgxSkeletonLoaderComponent, MatIconButton, FlowAnalysisRuleTable],
    styleUrls: ['./flow-analysis-rules.component.scss']
})
export class FlowAnalysisRules implements OnInit, OnDestroy {
    flowAnalysisRuleState$ = this.store.select(selectFlowAnalysisRulesState);
    selectedFlowAnalysisRuleId$ = this.store.select(selectFlowAnalysisRuleIdFromRoute);
    currentUser$ = this.store.select(selectCurrentUser);

    constructor(private store: Store<NiFiState>) {
        this.store
            .select(selectSingleEditedFlowAnalysisRule)
            .pipe(
                filter((id: string) => id != null),
                switchMap((id: string) =>
                    this.store.select(selectRule(id)).pipe(
                        filter((entity) => entity != null),
                        take(1)
                    )
                ),
                takeUntilDestroyed()
            )
            .subscribe((entity) => {
                if (entity) {
                    this.store.dispatch(
                        openConfigureFlowAnalysisRuleDialog({
                            request: {
                                id: entity.id,
                                flowAnalysisRule: entity
                            }
                        })
                    );
                }
            });
    }

    ngOnInit(): void {
        this.store.dispatch(loadFlowAnalysisRules());
    }

    isInitialLoading(state: FlowAnalysisRulesState): boolean {
        // using the current timestamp to detect the initial load event
        return state.loadedTimestamp == initialState.loadedTimestamp;
    }

    openNewFlowAnalysisRuleDialog(): void {
        this.store.dispatch(openNewFlowAnalysisRuleDialog());
    }

    refreshFlowAnalysisRuleListing(): void {
        this.store.dispatch(loadFlowAnalysisRules());
    }

    selectFlowAnalysisRule(entity: FlowAnalysisRuleEntity): void {
        this.store.dispatch(
            selectFlowAnalysisRule({
                request: {
                    id: entity.id
                }
            })
        );
    }

    viewFlowAnalysisRuleDocumentation(entity: FlowAnalysisRuleEntity): void {
        this.store.dispatch(
            navigateToComponentDocumentation({
                request: {
                    backNavigation: {
                        route: ['/settings', 'flow-analysis-rules', entity.id],
                        routeBoundary: ['/documentation'],
                        context: 'Flow Analysis Rule'
                    },
                    parameters: {
                        componentType: ComponentType.FlowAnalysisRule,
                        type: entity.component.type,
                        group: entity.component.bundle.group,
                        artifact: entity.component.bundle.artifact,
                        version: entity.component.bundle.version
                    }
                }
            })
        );
    }

    enableFlowAnalysisRule(entity: FlowAnalysisRuleEntity): void {
        this.store.dispatch(
            enableFlowAnalysisRule({
                request: {
                    id: entity.id,
                    flowAnalysisRule: entity
                }
            })
        );
    }

    disableFlowAnalysisRule(entity: FlowAnalysisRuleEntity): void {
        this.store.dispatch(
            disableFlowAnalysisRule({
                request: {
                    id: entity.id,
                    flowAnalysisRule: entity
                }
            })
        );
    }

    viewStateFlowAnalysisRule(entity: FlowAnalysisRuleEntity): void {
        const canClear: boolean = entity.status.runStatus === 'DISABLED';
        this.store.dispatch(
            getComponentStateAndOpenDialog({
                request: {
                    componentUri: entity.uri,
                    componentName: entity.component.name,
                    canClear
                }
            })
        );
    }

    changeFlowAnalysisRuleVersion(entity: FlowAnalysisRuleEntity): void {
        this.store.dispatch(
            openChangeFlowAnalysisRuleVersionDialog({
                request: {
                    id: entity.id,
                    bundle: entity.component.bundle,
                    uri: entity.uri,
                    type: entity.component.type,
                    revision: entity.revision
                }
            })
        );
    }

    deleteFlowAnalysisRule(entity: FlowAnalysisRuleEntity): void {
        this.store.dispatch(
            promptFlowAnalysisRuleDeletion({
                request: {
                    flowAnalysisRule: entity
                }
            })
        );
    }

    configureFlowAnalysisRule(entity: FlowAnalysisRuleEntity): void {
        this.store.dispatch(
            navigateToEditFlowAnalysisRule({
                id: entity.id
            })
        );
    }

    ngOnDestroy(): void {
        this.store.dispatch(resetFlowAnalysisRulesState());
    }
}
