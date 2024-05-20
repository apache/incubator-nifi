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

import { createAction, props } from '@ngrx/store';
import { BackNavigation } from './index';

/**
 * The preserveCurrentBackNavigation should be used prior to navigating where a new Back Navigation will be pushed
 * after routing completes. This is needed because by default the Back Navigation will be popped when the Navigation
 * bar is destroy. By preserve the current Back Navigation, the current Back Navigation will not be popped allowing
 * for multiple Back Navigation to be possible.
 */
export const preserveCurrentBackNavigation = createAction('[Navigation] Preserve Current Back Navigation');

export const pushBackNavigation = createAction(
    '[Navigation] Push Back Navigation',
    props<{ backNavigation: BackNavigation }>()
);

export const popBackNavigation = createAction('[Navigation] Pop Back Navigation');
