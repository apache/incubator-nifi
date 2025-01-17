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

import { Component, forwardRef, Input } from '@angular/core';
import { ControlValueAccessor, NG_VALUE_ACCESSOR } from '@angular/forms';
import { MatButtonModule } from '@angular/material/button';
import { MatDialogModule } from '@angular/material/dialog';
import { MatTableModule } from '@angular/material/table';
import { NgTemplateOutlet } from '@angular/common';
import { ParameterContextEntity } from '../../../../state/shared';
import {
    DragDropModule,
    CdkDrag,
    CdkDragDrop,
    CdkDropList,
    moveItemInArray,
    transferArrayItem
} from '@angular/cdk/drag-drop';
import { NiFiCommon, NifiTooltipDirective, ParameterContextReferenceEntity, PipesModule, TextTip } from '@nifi/shared';
import { SortObjectByPropertyPipe } from '../../../../../../../../libs/shared/src/pipes/sort-by-property.pipe';

@Component({
    selector: 'parameter-context-inheritance',
    standalone: true,
    templateUrl: './parameter-context-inheritance.component.html',
    imports: [
        MatButtonModule,
        MatDialogModule,
        MatTableModule,
        DragDropModule,
        NgTemplateOutlet,
        NifiTooltipDirective,
        CdkDropList,
        CdkDrag,
        PipesModule
    ],
    providers: [
        {
            provide: NG_VALUE_ACCESSOR,
            useExisting: forwardRef(() => ParameterContextInheritance),
            multi: true
        },
        SortObjectByPropertyPipe
    ],
    styleUrls: ['./parameter-context-inheritance.component.scss']
})
export class ParameterContextInheritance implements ControlValueAccessor {
    @Input() set allParameterContexts(allParameterContexts: ParameterContextEntity[]) {
        this._allParameterContexts = [...allParameterContexts];
        this.processParameterContexts();
    }

    protected readonly TextTip = TextTip;

    isDisabled = false;
    isTouched = false;
    onTouched!: () => void;
    onChange!: (inheritedParameterContexts: ParameterContextReferenceEntity[]) => void;

    _allParameterContexts: ParameterContextEntity[] = [];

    availableParameterContexts: ParameterContextEntity[] = [];
    selectedParameterContexts: ParameterContextEntity[] = [];

    inheritedParameterContexts!: ParameterContextReferenceEntity[];

    constructor(
        private nifiCommon: NiFiCommon,
        private sortObjectByPropertyPipe: SortObjectByPropertyPipe
    ) {}

    private processParameterContexts(): void {
        this.availableParameterContexts = [];
        this.selectedParameterContexts = [];

        if (this._allParameterContexts && this.inheritedParameterContexts) {
            this._allParameterContexts.forEach((parameterContext) => {
                const isInherited: boolean = this.inheritedParameterContexts.some(
                    (inheritedParameterContext) => parameterContext.id == inheritedParameterContext.id
                );
                if (isInherited) {
                    this.selectedParameterContexts.push(parameterContext);
                } else {
                    this.availableParameterContexts.push(parameterContext);
                }
            });

            this.sortObjectByPropertyPipe.transform(this.availableParameterContexts, 'component.name');
        }
    }

    registerOnChange(onChange: (inheritedParameterContexts: ParameterContextReferenceEntity[]) => void): void {
        this.onChange = onChange;
    }

    registerOnTouched(onTouch: () => void): void {
        this.onTouched = onTouch;
    }

    setDisabledState(isDisabled: boolean): void {
        this.isDisabled = isDisabled;
    }

    writeValue(inheritedParameterContexts: ParameterContextReferenceEntity[]): void {
        this.inheritedParameterContexts = [...inheritedParameterContexts];
        this.processParameterContexts();
    }

    hasDescription(entity: ParameterContextEntity): boolean {
        return !this.nifiCommon.isBlank(entity.component?.description);
    }

    removeSelected(entity: ParameterContextEntity, i: number): void {
        transferArrayItem(
            this.selectedParameterContexts,
            this.availableParameterContexts,
            i,
            this.availableParameterContexts.length
        );

        this.handleChanged();
    }

    dropAvailable(event: CdkDragDrop<ParameterContextEntity[]>) {
        if (event.previousContainer !== event.container) {
            transferArrayItem(
                event.previousContainer.data,
                event.container.data,
                event.previousIndex,
                event.currentIndex
            );

            this.handleChanged();
        }
    }

    dropSelected(event: CdkDragDrop<ParameterContextEntity[]>) {
        if (event.previousContainer === event.container) {
            if (event.previousIndex !== event.currentIndex) {
                moveItemInArray(event.container.data, event.previousIndex, event.currentIndex);

                this.handleChanged();
            }
        } else {
            transferArrayItem(
                event.previousContainer.data,
                event.container.data,
                event.previousIndex,
                event.currentIndex
            );

            this.handleChanged();
        }
    }

    private handleChanged() {
        // mark the component as touched if not already
        if (!this.isTouched) {
            this.isTouched = true;
            this.onTouched();
        }

        // emit the changes
        this.onChange(this.serializeInheritedParameterContexts());

        this.sortObjectByPropertyPipe.transform(this.availableParameterContexts, 'component.name');
    }

    private serializeInheritedParameterContexts(): ParameterContextReferenceEntity[] {
        return this.selectedParameterContexts.map((parameterContext) => {
            // @ts-ignore - component will be defined since the user has permissions to inherit from the context, but it is optional as defined by the type
            const name = parameterContext.component.name;

            return {
                permissions: parameterContext.permissions,
                id: parameterContext.id,
                component: {
                    id: parameterContext.id,
                    name
                }
            };
        });
    }
}
