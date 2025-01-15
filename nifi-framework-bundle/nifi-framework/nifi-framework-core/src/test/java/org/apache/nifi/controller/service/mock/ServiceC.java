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
package org.apache.nifi.controller.service.mock;

import java.util.List;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ControllerService;

public class ServiceC extends AbstractControllerService {

    public static final PropertyDescriptor REQ_SERVICE_1 = new PropertyDescriptor.Builder()
            .name("S1")
            .identifiesControllerService(ControllerService.class)
            .required(true)
            .build();

    public static final PropertyDescriptor REQ_SERVICE_2 = new PropertyDescriptor.Builder()
            .name("S2")
            .identifiesControllerService(ControllerService.class)
            .required(true)
            .build();

    public static final PropertyDescriptor OPT_SERVICE = new PropertyDescriptor.Builder()
            .name("S3")
            .identifiesControllerService(ControllerService.class)
            .required(false)
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return List.of(
                REQ_SERVICE_1,
                REQ_SERVICE_2,
                OPT_SERVICE
        );
    }

}
