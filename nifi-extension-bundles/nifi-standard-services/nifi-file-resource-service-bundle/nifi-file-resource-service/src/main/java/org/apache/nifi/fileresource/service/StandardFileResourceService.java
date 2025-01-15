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
package org.apache.nifi.fileresource.service;

import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceReference;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.fileresource.service.api.FileResource;
import org.apache.nifi.fileresource.service.api.FileResourceService;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

@Tags({"file", "resource"})
@CapabilityDescription("Provides a file resource for other components. The file needs to be available locally by Nifi (e.g. local disk or mounted storage). " +
        "NiFi needs to have read permission to the file.")
@Restricted(
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.READ_FILESYSTEM,
                        explanation = "Provides operator the ability to read from any file that NiFi has access to.")
        }
)
public class StandardFileResourceService extends AbstractControllerService implements FileResourceService {

    public static final PropertyDescriptor FILE_PATH = new PropertyDescriptor.Builder()
            .name("file-path")
            .displayName("File Path")
            .description("Path to a file that can be accessed locally.")
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .defaultValue("${absolute.path}/${filename}")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            FILE_PATH
    );

    private volatile PropertyContext context;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        this.context = context;
    }

    @OnDisabled
    public void onDisabled() {
        this.context = null;
    }

    @Override
    public FileResource getFileResource(Map<String, String> attributes) {
        final ResourceReference resourceReference = context.getProperty(FILE_PATH).evaluateAttributeExpressions(attributes).asResource();

        if (resourceReference == null) {
            throw new ProcessException("Evaluated path is empty. Path expression: " + context.getProperty(FILE_PATH).getValue());
        }

        final File file = resourceReference.asFile();

        if (!file.exists() || !file.isFile()) {
            throw new ProcessException("Path does not exist or it is not a file: " + file.getAbsolutePath());
        }

        try {
            final InputStream inputStream = resourceReference.read();
            final long size = file.length();

            return new FileResource(inputStream, size);
        } catch (IOException e) {
            throw new ProcessException("File cannot be read: " + file.getAbsolutePath(), e);
        }
    }
}
