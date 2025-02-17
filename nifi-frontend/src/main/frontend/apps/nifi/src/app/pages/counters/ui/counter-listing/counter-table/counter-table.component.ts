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

import { AfterViewInit, Component, DestroyRef, EventEmitter, inject, Input, Output } from '@angular/core';
import { CounterEntity } from '../../../state/counter-listing';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { MatSortModule, Sort } from '@angular/material/sort';
import { FormBuilder, FormGroup, ReactiveFormsModule } from '@angular/forms';
import { debounceTime } from 'rxjs';
import { NiFiCommon } from '@nifi/shared';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { MatFormField } from '@angular/material/form-field';
import { MatInput } from '@angular/material/input';
import { MatLabel, MatOption, MatSelect } from '@angular/material/select';

@Component({
    selector: 'counter-table',
    templateUrl: './counter-table.component.html',
    imports: [
        ReactiveFormsModule,
        MatFormField,
        MatInput,
        MatSelect,
        MatOption,
        MatLabel,
        MatTableModule,
        MatSortModule
    ],
    styleUrls: ['./counter-table.component.scss']
})
export class CounterTable implements AfterViewInit {
    private _canModifyCounters = false;
    private destroyRef: DestroyRef = inject(DestroyRef);
    filterTerm = '';
    filterColumn: 'context' | 'name' = 'name';
    totalCount = 0;
    filteredCount = 0;

    displayedColumns: string[] = ['context', 'name', 'value'];
    dataSource: MatTableDataSource<CounterEntity> = new MatTableDataSource<CounterEntity>();
    filterForm: FormGroup;

    @Input() initialSortColumn: 'context' | 'name' | 'value' = 'context';
    @Input() initialSortDirection: 'asc' | 'desc' = 'asc';

    activeSort: Sort = {
        active: this.initialSortColumn,
        direction: this.initialSortDirection
    };

    @Input() set counters(counterEntities: CounterEntity[]) {
        this.dataSource.data = this.sortEntities(counterEntities, this.activeSort);

        this.dataSource.filterPredicate = (data: CounterEntity, filter: string) => {
            const { filterTerm, filterColumn } = JSON.parse(filter);
            if (filterColumn === 'name') {
                return this.nifiCommon.stringContains(data.name, filterTerm, true);
            } else {
                return this.nifiCommon.stringContains(data.context, filterTerm, true);
            }
        };
        this.totalCount = counterEntities.length;
        this.filteredCount = counterEntities.length;

        // apply any filtering to the new data
        const filterTerm = this.filterForm.get('filterTerm')?.value;
        if (filterTerm?.length > 0) {
            const filterColumn = this.filterForm.get('filterColumn')?.value;
            this.applyFilter(filterTerm, filterColumn);
        }
    }

    @Input()
    set canModifyCounters(canWrite: boolean) {
        if (canWrite) {
            this.displayedColumns = ['context', 'name', 'value', 'reset'];
        } else {
            this.displayedColumns = ['context', 'name', 'value'];
        }
        this._canModifyCounters = canWrite;
    }

    get canModifyCounters(): boolean {
        return this._canModifyCounters;
    }

    @Output() resetCounter: EventEmitter<CounterEntity> = new EventEmitter<CounterEntity>();

    constructor(
        private formBuilder: FormBuilder,
        private nifiCommon: NiFiCommon
    ) {
        this.filterForm = this.formBuilder.group({ filterTerm: '', filterColumn: 'name' });
    }

    ngAfterViewInit(): void {
        this.filterForm
            .get('filterTerm')
            ?.valueChanges.pipe(debounceTime(500), takeUntilDestroyed(this.destroyRef))
            .subscribe((filterTerm: string) => {
                const filterColumn = this.filterForm.get('filterColumn')?.value;
                this.applyFilter(filterTerm, filterColumn);
            });

        this.filterForm
            .get('filterColumn')
            ?.valueChanges.pipe(takeUntilDestroyed(this.destroyRef))
            .subscribe((filterColumn: string) => {
                const filterTerm = this.filterForm.get('filterTerm')?.value;
                this.applyFilter(filterTerm, filterColumn);
            });
    }

    applyFilter(filterTerm: string, filterColumn: string) {
        this.dataSource.filter = JSON.stringify({ filterTerm, filterColumn });
        this.filteredCount = this.dataSource.filteredData.length;
    }

    formatContext(counter: CounterEntity): string {
        return counter.context;
    }

    formatName(counter: CounterEntity): string {
        return counter.name;
    }

    formatValue(counter: CounterEntity): string {
        return counter.value;
    }

    resetClicked(counter: CounterEntity, event: MouseEvent) {
        event.stopPropagation();
        this.resetCounter.next(counter);
    }

    sortData(sort: Sort) {
        this.activeSort = sort;
        this.dataSource.data = this.sortEntities(this.dataSource.data, sort);
    }

    private sortEntities(data: CounterEntity[], sort: Sort): CounterEntity[] {
        if (!data) {
            return [];
        }
        return data.slice().sort((a, b) => {
            const isAsc = sort.direction === 'asc';
            let retVal = 0;
            switch (sort.active) {
                case 'name':
                    retVal = this.nifiCommon.compareString(this.formatName(a), this.formatName(b));
                    break;
                case 'value':
                    retVal = this.nifiCommon.compareNumber(a.valueCount, b.valueCount);
                    break;
                case 'context':
                    retVal = this.nifiCommon.compareString(this.formatContext(a), this.formatContext(b));
                    break;
                default:
                    return 0;
            }
            return retVal * (isAsc ? 1 : -1);
        });
    }
}
