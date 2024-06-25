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

import { Component, Inject, Input } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { FormBuilder, FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import {
    ControllerServiceEntity,
    ControllerServiceReferencingComponent,
    ControllerServiceReferencingComponentEntity
} from '../../../../state/shared';
import { MatInputModule } from '@angular/material/input';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatButtonModule } from '@angular/material/button';
import { AsyncPipe, NgTemplateOutlet } from '@angular/common';
import { MatTabsModule } from '@angular/material/tabs';
import { MatOptionModule } from '@angular/material/core';
import { MatSelectModule } from '@angular/material/select';
import { ControllerServiceApi } from '../../../../ui/common/controller-service/controller-service-api/controller-service-api.component';
import { ControllerServiceReferences } from '../../../../ui/common/controller-service/controller-service-references/controller-service-references.component';
import { NifiSpinnerDirective } from '../../../../ui/common/spinner/nifi-spinner.directive';
import { TextTip } from '../../../../ui/common/tooltips/text-tip/text-tip.component';
import { NifiTooltipDirective } from '../../../../ui/common/tooltips/nifi-tooltip.directive';
import { Store } from '@ngrx/store';
import { CloseOnEscapeDialog } from '../../../../ui/common/close-on-escape-dialog/close-on-escape-dialog.component';
import { moveControllerService } from '../../state/controller-services/controller-services.actions';
import { SelectOption } from '../../../../state/shared/index';
import { NiFiState } from 'apps/nifi/src/app/state';
import { MoveControllerServiceDialogRequest } from '../../state/controller-services';
import { ComponentEntity, ProcessGroupFlow } from '../../state/flow';
import { NgIf } from '@angular/common';

@Component({
    selector: 'move-controller-service',
    standalone: true,
    templateUrl: './move-controller-service.component.html',
    imports: [
        ReactiveFormsModule,
        MatDialogModule,
        MatInputModule,
        MatCheckboxModule,
        MatButtonModule,
        MatTabsModule,
        MatOptionModule,
        MatSelectModule,
        ControllerServiceApi,
        ControllerServiceReferences,
        AsyncPipe,
        NifiSpinnerDirective,
        NifiTooltipDirective,
        NgTemplateOutlet,
        NgIf
    ],
    styleUrls: ['./move-controller-service.component.scss']
})
export class MoveControllerService extends CloseOnEscapeDialog {
    @Input() goToReferencingComponent!: (component: ControllerServiceReferencingComponent) => void;
    protected readonly TextTip = TextTip;
    protected controllerServiceActionProcessGroups: SelectOption[] = [];

    controllerService: ControllerServiceEntity;

    moveControllerServiceForm: FormGroup;

    constructor(
        @Inject(MAT_DIALOG_DATA) public request: MoveControllerServiceDialogRequest,
        private store: Store<NiFiState>,
        private formBuilder: FormBuilder
    ) {
        super();

        this.controllerService = request.controllerService;

        // build the form
        this.moveControllerServiceForm = this.formBuilder.group({
            processGroups: new FormControl('Process Group', Validators.required)
        });

        const parentControllerServices = request.parentControllerServices.filter(
            (cs) => cs.parentGroupId != request.controllerService.parentGroupId
        );

        if (request.processGroupFlow != null) {
            const processGroups: SelectOption[] = [];
            this.loadParentOption(request.processGroupFlow, parentControllerServices, processGroups);
            this.loadChildOptions(request.processGroupFlow, request.processGroupEntity, processGroups);
            this.controllerServiceActionProcessGroups = processGroups;

            const firstEnabled = processGroups.findIndex((pg) => !pg.disabled);
            if (firstEnabled != -1) {
                this.moveControllerServiceForm.controls['processGroups'].setValue(processGroups[firstEnabled].value);
            }
        }
    }

    loadParentOption(
        processGroupFlow: ProcessGroupFlow,
        parentControllerServices: ControllerServiceEntity[],
        processGroups: SelectOption[]
    ) {
        if (processGroupFlow.breadcrumb.parentBreadcrumb != undefined) {
            if (processGroupFlow.breadcrumb.parentBreadcrumb != undefined) {
                const parentBreadcrumb = processGroupFlow.breadcrumb.parentBreadcrumb;
                if (parentBreadcrumb.permissions.canRead && parentBreadcrumb.permissions.canWrite) {
                    const option: SelectOption = {
                        text: parentBreadcrumb.breadcrumb.name + ' (Parent)',
                        value: parentBreadcrumb.breadcrumb.id
                    };

                    let errorMsg = '';
                    const descriptors = this.controllerService.component.descriptors;
                    for (const descriptor in descriptors) {
                        if (descriptors[descriptor].identifiesControllerService != undefined) {
                            const controllerId =
                                this.controllerService.component.properties[descriptors[descriptor].name];
                            if (
                                controllerId != null &&
                                !parentControllerServices.some((service) => service.id == controllerId)
                            ) {
                                errorMsg += '[' + descriptors[descriptor].name + ']';
                            }
                        }
                    }

                    if (errorMsg != '') {
                        option.description =
                            'The following properties reference controller services that would be out of scope for this ' +
                            'process group: ' +
                            errorMsg;
                        option.disabled = true;
                    } else {
                        option.disabled = false;
                    }

                    processGroups.push(option);
                }
            }
        }
    }

    loadChildOptions(
        processGroupFlow: ProcessGroupFlow,
        currentProcessGroupEntity: any,
        processGroups: SelectOption[]
    ) {
        const referencingComponents: ControllerServiceReferencingComponentEntity[] =
            this.controllerService.component.referencingComponents;
        processGroupFlow.flow.processGroups.forEach((child: ComponentEntity) => {
            if (child.permissions.canRead && child.permissions.canWrite) {
                const option: SelectOption = {
                    text: child.component.name,
                    value: child.component.id
                };

                const root = this.getProcessGroupById(currentProcessGroupEntity.component, child.component.id);
                let errorMsg = '';
                for (const component of referencingComponents) {
                    if (!this.processGroupContainsComponent(root, component.component.groupId)) {
                        errorMsg += '[' + component.component.name + ']';
                    }
                }

                if (errorMsg != '') {
                    option.description =
                        'The following components would be out of scope for this ' + 'process group: ' + errorMsg;
                    option.disabled = true;
                } else {
                    option.disabled = false;
                }

                processGroups.push(option);
            }
        });
    }

    processGroupContainsComponent(processGroup: any, groupId: string): boolean {
        if (processGroup.contents != undefined) {
            if (processGroup.id == groupId) {
                return true;
            } else {
                for (const pg of processGroup.contents.processGroups) {
                    return this.processGroupContainsComponent(pg, groupId);
                }
                return false;
            }
        }
        return false;
    }

    getProcessGroupById(root: any, processGroupId: string): any {
        if (root != undefined) {
            if (root.id == processGroupId) {
                return root;
            } else {
                if (root.contents != undefined) {
                    for (const pg of root.contents.processGroups) {
                        return this.getProcessGroupById(pg, processGroupId);
                    }
                }
                return null;
            }
        }
        return null;
    }

    submitForm() {
        if (this.moveControllerServiceForm.get('processGroups')?.value != 'Process Group') {
            this.store.dispatch(
                moveControllerService({
                    request: {
                        controllerService: this.request.controllerService,
                        data: {
                            parentGroupId: this.moveControllerServiceForm.get('processGroups')?.value,
                            revision: this.request.controllerService.revision
                        }
                    }
                })
            );
        }
    }

    override isDirty(): boolean {
        return this.moveControllerServiceForm.dirty;
    }
}
