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
package org.apache.nifi.toolkit.cli.impl.command.registry.extension;

import org.apache.commons.cli.ParseException;
import org.apache.nifi.registry.client.ExtensionClient;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.extension.component.ExtensionFilterParams;
import org.apache.nifi.registry.extension.component.ExtensionMetadata;
import org.apache.nifi.registry.extension.component.ExtensionMetadataContainer;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.registry.AbstractNiFiRegistryCommand;
import org.apache.nifi.toolkit.cli.impl.result.registry.ExtensionMetadataResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public class ListExtensionsWithTags extends AbstractNiFiRegistryCommand<ExtensionMetadataResult> {

    public ListExtensionsWithTags() {
        super("list-extensions-with-tags", ExtensionMetadataResult.class);
    }

    @Override
    protected void doInitialize(Context context) {
        addOption(CommandOption.EXT_TAGS.createOption());
    }

    @Override
    public String getDescription() {
        return "Lists info for extensions with the given tags.";
    }

    @Override
    public ExtensionMetadataResult doExecute(final NiFiRegistryClient client, final Properties properties)
            throws IOException, NiFiRegistryException, ParseException {

        final String tags = getRequiredArg(properties, CommandOption.EXT_TAGS);
        final String[] splitTags = tags.split("[,]");

        final Set<String> cleanedTags = Arrays.stream(splitTags)
                .map(t -> t.trim())
                .collect(Collectors.toSet());

        if (cleanedTags.isEmpty()) {
            throw new IllegalArgumentException("Invalid tag argument");
        }

        final ExtensionClient extensionClient = client.getExtensionClient();

        final ExtensionFilterParams filterParams = new ExtensionFilterParams.Builder().addTags(cleanedTags).build();
        final ExtensionMetadataContainer metadataContainer = extensionClient.findExtensions(filterParams);

        final List<ExtensionMetadata> metadataList = new ArrayList<>(metadataContainer.getExtensions());
        return new ExtensionMetadataResult(getResultType(properties), metadataList);
    }
}
