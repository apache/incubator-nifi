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

import { Component, OnInit } from '@angular/core';
import {
    GuardsCheckEnd,
    GuardsCheckStart,
    NavigationCancel,
    NavigationStart,
    NavigationEnd,
    Router
} from '@angular/router';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { Storage } from './service/storage.service';
import { ThemingService } from './service/theming.service';
import { MatDialog } from '@angular/material/dialog';
import { NiFiState } from './state';
import { Store } from '@ngrx/store';
import { BackNavigation } from './state/navigation';
import { popBackNavigation, pushBackNavigation } from './state/navigation/navigation.actions';
import { filter, map, tap } from 'rxjs';
import { concatLatestFrom } from '@ngrx/operators';
import { selectBackNavigation } from './state/navigation/navigation.selectors';
import { selectBannerText } from './state/banner-text/banner-text.selectors';
import { loadBannerText } from './state/banner-text/banner-text.actions';

@Component({
    selector: 'nifi',
    templateUrl: './app.component.html',
    styleUrls: ['./app.component.scss']
})
export class AppComponent implements OnInit {
    title = 'nifi';
    guardLoading = true;

    bannerText = this.store.selectSignal(selectBannerText);

    constructor(
        private router: Router,
        private storage: Storage,
        private themingService: ThemingService,
        private dialog: MatDialog,
        private store: Store<NiFiState>
    ) {
        this.router.events
            .pipe(
                takeUntilDestroyed(),
                tap((event) => {
                    if (event instanceof NavigationStart) {
                        this.dialog.openDialogs.forEach((dialog) => dialog.close('ROUTED'));
                    }
                    if (event instanceof GuardsCheckStart) {
                        this.guardLoading = true;
                    }
                    if (event instanceof GuardsCheckEnd || event instanceof NavigationCancel) {
                        this.guardLoading = false;
                    }
                }),
                filter((e) => e instanceof NavigationEnd),
                map((e) => e as NavigationEnd),
                concatLatestFrom(() => this.store.select(selectBackNavigation))
            )
            .subscribe(([event, previousBackNavigation]) => {
                const extras = this.router.getCurrentNavigation()?.extras;
                if (extras?.state?.['backNavigation']) {
                    const backNavigation: BackNavigation = extras?.state?.['backNavigation'];
                    this.store.dispatch(
                        pushBackNavigation({
                            backNavigation
                        })
                    );
                } else if (previousBackNavigation) {
                    this.store.dispatch(
                        popBackNavigation({
                            url: event.url
                        })
                    );
                }
            });

        let theme = this.storage.getItem('theme');

        // Initially check if dark mode is enabled on system
        const darkModeOn = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;

        // If dark mode is enabled then directly switch to the dark-theme
        this.themingService.toggleTheme(darkModeOn, theme);

        if (window.matchMedia) {
            // Watch for changes of the preference
            window.matchMedia('(prefers-color-scheme: dark)').addListener((e) => {
                theme = this.storage.getItem('theme');
                this.themingService.toggleTheme(e.matches, theme);
            });
        }
    }

    ngOnInit(): void {
        this.store.dispatch(loadBannerText());
    }
}
