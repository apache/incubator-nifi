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
import { ManagementControllerServicesState } from '../../state/management-controller-services';
import {
    selectControllerServiceIdFromRoute,
    selectManagementControllerServicesState,
    selectService,
    selectSingleEditedService
} from '../../state/management-controller-services/management-controller-services.selectors';
import {
    loadManagementControllerServices,
    navigateToAdvancedServiceUi,
    navigateToEditService,
    navigateToManageComponentPolicies,
    openChangeMgtControllerServiceVersionDialog,
    openConfigureControllerServiceDialog,
    openDisableControllerServiceDialog,
    openEnableControllerServiceDialog,
    openNewControllerServiceDialog,
    promptControllerServiceDeletion,
    resetManagementControllerServicesState,
    selectControllerService
} from '../../state/management-controller-services/management-controller-services.actions';
import { ControllerServiceEntity } from '../../../../state/shared';
import { initialState } from '../../state/management-controller-services/management-controller-services.reducer';
import { filter, switchMap, take } from 'rxjs';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { selectCurrentUser } from '../../../../state/current-user/current-user.selectors';
import { NiFiState } from '../../../../state';
import { selectFlowConfiguration } from '../../../../state/flow-configuration/flow-configuration.selectors';
import { CurrentUser } from '../../../../state/current-user';
import { getComponentStateAndOpenDialog } from '../../../../state/component-state/component-state.actions';
import { navigateToComponentDocumentation } from '../../../../state/documentation/documentation.actions';
import { ComponentType } from '@nifi/shared';

@Component({
    selector: 'management-controller-services',
    templateUrl: './management-controller-services.component.html',
    styleUrls: ['./management-controller-services.component.scss'],
    standalone: false
})
export class ManagementControllerServices implements OnInit, OnDestroy {
    serviceState$ = this.store.select(selectManagementControllerServicesState);
    selectedServiceId$ = this.store.select(selectControllerServiceIdFromRoute);
    currentUser$ = this.store.select(selectCurrentUser);
    flowConfiguration$ = this.store.select(selectFlowConfiguration);

    constructor(private store: Store<NiFiState>) {
        this.store
            .select(selectSingleEditedService)
            .pipe(
                filter((id: string) => id != null),
                switchMap((id: string) =>
                    this.store.select(selectService(id)).pipe(
                        filter((entity) => entity != null),
                        take(1)
                    )
                ),
                takeUntilDestroyed()
            )
            .subscribe((entity) => {
                if (entity) {
                    this.store.dispatch(
                        openConfigureControllerServiceDialog({
                            request: {
                                id: entity.id,
                                controllerService: entity
                            }
                        })
                    );
                }
            });
    }

    ngOnInit(): void {
        this.store.dispatch(loadManagementControllerServices());
    }

    isInitialLoading(state: ManagementControllerServicesState): boolean {
        // using the current timestamp to detect the initial load event
        return state.loadedTimestamp == initialState.loadedTimestamp;
    }

    openNewControllerServiceDialog(): void {
        this.store.dispatch(openNewControllerServiceDialog());
    }

    refreshControllerServiceListing(): void {
        this.store.dispatch(loadManagementControllerServices());
    }

    formatScope(): string {
        return 'Controller';
    }

    definedByCurrentGroup(): boolean {
        return true;
    }

    viewControllerServiceDocumentation(entity: ControllerServiceEntity): void {
        this.store.dispatch(
            navigateToComponentDocumentation({
                request: {
                    backNavigation: {
                        route: ['/settings', 'management-controller-services', entity.id],
                        routeBoundary: ['/documentation'],
                        context: 'Controller Service'
                    },
                    parameters: {
                        componentType: ComponentType.ControllerService,
                        type: entity.component.type,
                        group: entity.component.bundle.group,
                        artifact: entity.component.bundle.artifact,
                        version: entity.component.bundle.version
                    }
                }
            })
        );
    }

    configureControllerService(entity: ControllerServiceEntity): void {
        this.store.dispatch(
            navigateToEditService({
                id: entity.id
            })
        );
    }

    navigateToManageComponentPolicies(entity: ControllerServiceEntity): void {
        this.store.dispatch(
            navigateToManageComponentPolicies({
                id: entity.id
            })
        );
    }

    openAdvancedUi(entity: ControllerServiceEntity): void {
        this.store.dispatch(
            navigateToAdvancedServiceUi({
                id: entity.id
            })
        );
    }

    enableControllerService(entity: ControllerServiceEntity): void {
        this.store.dispatch(
            openEnableControllerServiceDialog({
                request: {
                    id: entity.id,
                    controllerService: entity
                }
            })
        );
    }

    disableControllerService(entity: ControllerServiceEntity): void {
        this.store.dispatch(
            openDisableControllerServiceDialog({
                request: {
                    id: entity.id,
                    controllerService: entity
                }
            })
        );
    }

    viewStateControllerService(entity: ControllerServiceEntity): void {
        this.store.dispatch(
            getComponentStateAndOpenDialog({
                request: {
                    componentUri: entity.uri,
                    componentName: entity.component.name,
                    canClear: entity.component.state === 'DISABLED'
                }
            })
        );
    }

    changeControllerServiceVersion(entity: ControllerServiceEntity): void {
        this.store.dispatch(
            openChangeMgtControllerServiceVersionDialog({
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

    deleteControllerService(entity: ControllerServiceEntity): void {
        this.store.dispatch(
            promptControllerServiceDeletion({
                request: {
                    controllerService: entity
                }
            })
        );
    }

    canModifyParent(currentUser: CurrentUser): (entity: ControllerServiceEntity) => boolean {
        return () => currentUser.controllerPermissions.canRead && currentUser.controllerPermissions.canWrite;
    }

    selectControllerService(entity: ControllerServiceEntity): void {
        this.store.dispatch(
            selectControllerService({
                request: {
                    id: entity.id
                }
            })
        );
    }

    ngOnDestroy(): void {
        this.store.dispatch(resetManagementControllerServicesState());
    }
}
