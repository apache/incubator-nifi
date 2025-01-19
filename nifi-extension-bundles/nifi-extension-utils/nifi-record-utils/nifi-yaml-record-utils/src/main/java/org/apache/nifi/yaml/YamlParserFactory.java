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
package org.apache.nifi.yaml;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import org.apache.nifi.json.TokenParserFactory;
import org.yaml.snakeyaml.LoaderOptions;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

public class YamlParserFactory implements TokenParserFactory {

    private static final YAMLMapper yamlMapper = new YAMLMapper();

    private final YAMLFactory yamlFactory;

    /**
     * YAML Parser Factory constructor with default configuration for YAML Mapper
     */
    public YamlParserFactory() {
        yamlFactory = YAMLFactory.builder().build();
        yamlFactory.setCodec(yamlMapper);
    }

    /**
     * YAML Parser Factory constructor with configurable parsing constraints
     *
     * @param streamReadConstraints Stream Read Constraints required
     * @param allowComments Allow Comments during parsing
     */
    public YamlParserFactory(final StreamReadConstraints streamReadConstraints, final boolean allowComments) {
        Objects.requireNonNull(streamReadConstraints, "Stream Read Constraints required");

        final LoaderOptions loaderOptions = new LoaderOptions();
        loaderOptions.setCodePointLimit(streamReadConstraints.getMaxStringLength());
        loaderOptions.setProcessComments(allowComments);

        yamlFactory = YAMLFactory.builder().loaderOptions(loaderOptions).build();
        yamlFactory.setCodec(yamlMapper);
    }

    /**
     * Get Parser implementation for YAML
     *
     * @param in Input Stream to be parsed
     * @return YAML Parser
     * @throws IOException Thrown on parser creation failures
     */
    @Override
    public JsonParser getJsonParser(final InputStream in) throws IOException {
        return yamlFactory.createParser(in);
    }
}
