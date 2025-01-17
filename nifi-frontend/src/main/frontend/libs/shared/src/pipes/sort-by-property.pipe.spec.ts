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

import { SortableBy, SortObjectByPropertyPipe } from './sort-by-property.pipe';

describe('SortObjectByPipe', () => {
    let pipe: SortObjectByPropertyPipe;

    beforeEach(() => {
        pipe = new SortObjectByPropertyPipe();
    });

    it('should sort objects alphabetically by name', () => {
        const items: SortableBy[] = [
            { name: 'Banana', value: 'Fruit' },
            { name: 'Apple', value: 'Fruit' },
            { name: 'Carrot', value: 'Vegetable' }
        ];
        const result = pipe.transform(items);
        expect(result).toEqual([
            { name: 'Apple', value: 'Fruit' },
            { name: 'Banana', value: 'Fruit' },
            { name: 'Carrot', value: 'Vegetable' }
        ]);
    });

    it('should handle case-insensitive sorting', () => {
        const items: SortableBy[] = [{ name: 'banana' }, { name: 'Apple' }, { name: 'carrot' }];
        const result = pipe.transform(items);
        expect(result).toEqual([{ name: 'Apple' }, { name: 'banana' }, { name: 'carrot' }]);
    });

    it('should return an empty array if input is empty', () => {
        const items: SortableBy[] = [];
        const result = pipe.transform(items);
        expect(result).toEqual([]);
    });

    it('should handle a single-item array', () => {
        const items: SortableBy[] = [{ name: 'Apple' }];
        const result = pipe.transform(items);
        expect(result).toEqual([{ name: 'Apple' }]);
    });

    it('should not modify the original array', () => {
        const items: SortableBy[] = [{ name: 'Banana' }, { name: 'Apple' }];
        const itemsCopy = [...items];
        pipe.transform(items);
        expect(items).not.toEqual(itemsCopy);
    });

    it('should handle null or undefined value properties', () => {
        const items: any[] = [{ name: 'Apple' }, { name: undefined }, { name: null }];
        const result = pipe.transform(items);
        expect(result).toEqual([{ name: undefined }, { name: null }, { name: 'Apple' }]);
    });

    it('should handle an array of objects with only missing name properties', () => {
        const items: SortableBy[] = [{ color: 'Blue' }, { color: 'Red' }, { color: 'Green' }];
        const result = pipe.transform(items);
        expect(result).toEqual([{ color: 'Blue' }, { color: 'Red' }, { color: 'Green' }]);
    });

    it('should sort objects by nested properties', () => {
        const items = [
            {
                id: '12345',
                component: {
                    name: 'bbb'
                }
            },
            {
                id: '98765',
                component: {
                    name: 'aaa'
                }
            }
        ];

        const sortedItems = pipe.transform(items, 'component.name');

        expect(sortedItems).toEqual([
            {
                id: '98765',
                component: {
                    name: 'aaa'
                }
            },
            {
                id: '12345',
                component: {
                    name: 'bbb'
                }
            }
        ]);
    });

    it('should handle missing nested properties gracefully', () => {
        const items = [
            {
                id: '12345',
                component: {
                    name: 'bbb'
                }
            },
            {
                id: '98765'
            }
        ];

        const sortedItems = pipe.transform(items, 'component.name');

        expect(sortedItems).toEqual([
            {
                id: '98765'
            },
            {
                id: '12345',
                component: {
                    name: 'bbb'
                }
            }
        ]);
    });
});
