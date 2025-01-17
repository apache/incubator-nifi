/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { Component } from '@angular/core';
import { By } from '@angular/platform-browser';
import { CopyDirective } from './copy.directive';
import { ComponentFixture, TestBed } from '@angular/core/testing';

@Component({
    standalone: true,
    template: `<div [copy]="copyText">test</div>`,
    imports: [CopyDirective]
})
class TestComponent {
    copyText = 'copied value';
}

describe('CopyDirective', () => {
    let component: TestComponent;
    let fixture: ComponentFixture<TestComponent>;
    let directiveDebugEl: any;

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [CopyDirective, TestComponent]
        });
        fixture = TestBed.createComponent(TestComponent);
        component = fixture.componentInstance;
        fixture.detectChanges();
        directiveDebugEl = fixture.debugElement.query(By.directive(CopyDirective));
    });

    afterEach(() => {
        jest.restoreAllMocks();
    });

    it('should create a copy button on mouse enter', () => {
        directiveDebugEl.triggerEventHandler('mouseenter', null);
        fixture.detectChanges();
        const copyButton = directiveDebugEl.nativeElement.querySelector('.copy-button');
        expect(copyButton).not.toBeNull();
        expect(copyButton?.classList).toContain('fa-copy');
    });

    it('should remove the copy button on mouse leave', () => {
        directiveDebugEl.triggerEventHandler('mouseenter', null);
        fixture.detectChanges();
        directiveDebugEl.triggerEventHandler('mouseleave', null);
        fixture.detectChanges();
        const copyButton = directiveDebugEl.nativeElement.querySelector('.copy-button');
        expect(copyButton).toBeNull();
    });

    it('should copy text to clipboard and change button appearance on click', async () => {
        // Mock the clipboard object with a writable property
        const mockClipboard = {
            writeText: jest.fn().mockResolvedValue(undefined)
        };

        // Use Object.defineProperty to define a writable property
        Object.defineProperty(navigator, 'clipboard', {
            value: mockClipboard,
            writable: true
        });

        directiveDebugEl.triggerEventHandler('mouseenter', null);
        fixture.detectChanges();
        const copyButton = directiveDebugEl.nativeElement.querySelector('.copy-button');
        copyButton?.dispatchEvent(new MouseEvent('click'));
        fixture.detectChanges();
        await fixture.whenStable();
        expect(navigator.clipboard.writeText).toHaveBeenCalledWith(component.copyText);
        expect(copyButton?.classList).toContain('copied');
        expect(copyButton?.classList).toContain('fa-check');
        expect(copyButton?.classList).toContain('success-color-default');
    });

    it('should unsubscribe from events on mouse leave', () => {
        directiveDebugEl.triggerEventHandler('mouseenter', null);
        fixture.detectChanges();
        const directiveInstance = directiveDebugEl.injector.get(CopyDirective);
        const unsubscribeSpy = jest.spyOn(directiveInstance.subscription!, 'unsubscribe');
        directiveDebugEl.triggerEventHandler('mouseleave', null);
        fixture.detectChanges();
        expect(unsubscribeSpy).toHaveBeenCalled();
        unsubscribeSpy.mockRestore();
    });
});
