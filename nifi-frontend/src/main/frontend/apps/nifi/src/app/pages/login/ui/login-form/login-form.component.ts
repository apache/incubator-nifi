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

import { Component } from '@angular/core';
import { FormBuilder, FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { Store } from '@ngrx/store';
import { login } from '../../state/access/access.actions';
import { selectLoginFailure } from '../../state/access/access.selectors';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { selectLogoutSupported } from '../../../../state/current-user/current-user.selectors';
import { NiFiState } from '../../../../state';
import { setRoutedToFullScreenError } from '../../../../state/error/error.actions';
import { logout } from '../../../../state/current-user/current-user.actions';
import { RouterLink } from '@angular/router';
import { MatFormField } from '@angular/material/form-field';
import { MatInput } from '@angular/material/input';
import { MatLabel } from '@angular/material/select';

@Component({
    selector: 'login-form',
    templateUrl: './login-form.component.html',
    imports: [RouterLink, ReactiveFormsModule, MatFormField, MatInput, MatLabel],
    styleUrls: ['./login-form.component.scss']
})
export class LoginForm {
    logoutSupported = this.store.selectSignal(selectLogoutSupported);

    loginForm: FormGroup;

    constructor(
        private formBuilder: FormBuilder,
        private store: Store<NiFiState>
    ) {
        // build the form
        this.loginForm = this.formBuilder.group({
            username: new FormControl('', Validators.required),
            password: new FormControl('', Validators.required)
        });

        this.store
            .select(selectLoginFailure)
            .pipe(takeUntilDestroyed())
            .subscribe((loginFailure) => {
                if (loginFailure) {
                    this.loginForm.get('password')?.setValue('');
                }
            });
    }

    logout(): void {
        this.store.dispatch(logout());
    }

    resetRoutedToFullScreenError(): void {
        this.store.dispatch(setRoutedToFullScreenError({ routedToFullScreenError: false }));
    }

    login() {
        const username: string = this.loginForm.get('username')?.value;
        const password: string = this.loginForm.get('password')?.value;

        this.store.dispatch(
            login({
                request: {
                    username,
                    password
                }
            })
        );
    }
}
