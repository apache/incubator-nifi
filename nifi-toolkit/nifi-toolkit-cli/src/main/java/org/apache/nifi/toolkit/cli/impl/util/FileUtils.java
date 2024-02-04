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
package org.apache.nifi.toolkit.cli.impl.util;

import jakarta.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

public class FileUtils {
    public static File getFileContent(final Response response, final File outputFile) {
        final String contentDispositionHeader = response.getHeaderString("Content-Disposition");
        if (StringUtils.isBlank(contentDispositionHeader)) {
            throw new IllegalStateException("Content-Disposition header was blank or missing");
        }

        try (final InputStream responseInputStream = response.readEntity(InputStream.class)) {
            Files.copy(responseInputStream, outputFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            return outputFile;
        } catch (Exception e) {
            throw new IllegalStateException("Unable to write content due to: " + e.getMessage(), e);
        }
    }
}
