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
package org.apache.nifi.flow.resource;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;

/**
 * Represents an external source where the resource files might be acquired from. These external resources might be
 * various, as database drivers, different kind of configurations and so on.
 */
public interface FlowResourceProvider {
    /**
     * Initializes the External Resource Provider based on the given set of properties.
     */
    void initialize(FlowResourceProviderInitializationContext context);

    /**
     * Performs a listing of all resources that are available.
     *
     * @Return The result is a list of descriptors for the available resources.
     */
    Collection<FlowResourceDescriptor> listResources() throws IOException;

    /**
     * Fetches the resource at the given location. The location should be one of the values returned by <code>listResources()</code>.
     */
    InputStream fetchExternalResource(String location) throws IOException;
}
