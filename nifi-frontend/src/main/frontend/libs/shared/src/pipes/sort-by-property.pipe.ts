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

import { Pipe, PipeTransform } from '@angular/core';

export interface SortableBy {
    [key: string]: any; // Allows for flexibility with additional properties
}

@Pipe({
    name: 'sortObjectByProperty',
    pure: true // Set to true to ensure the pipe is only recalculated when inputs change
})
export class SortObjectByPropertyPipe implements PipeTransform {
    transform(items: SortableBy[], property: string = 'name'): any[] {
        return items.sort((a, b) =>
            this.getPropertyValue(a, property).localeCompare(this.getPropertyValue(b, property))
        );
    }

    private getPropertyValue(item: SortableBy, propertyPath: string): any {
        return propertyPath.split('.').reduce((obj, key) => (obj ? obj[key] : ''), item) ?? '';
    }
}
