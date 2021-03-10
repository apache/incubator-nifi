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

package org.apache.nifi.components.resource;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;

public class ResourceReferenceFactory {

    public static ResourceReference createResourceReference(final String value) {
        if (value == null) {
            return null;
        }

        final String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            return null;
        }

        try {
            if (trimmed.startsWith("http://") || trimmed.startsWith("https://")) {
                return new URLResourceReference(new URL(trimmed));
            }

            if (trimmed.startsWith("file:")) {
                final URL url = new URL(trimmed);
                final String filename = url.getFile();
                final File file = new File(filename);
                return new FileResourceReference(file);
            }
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Invalid URL: " + trimmed);
        }

        final File file = new File(trimmed);
        return new FileResourceReference(file);
    }
}
