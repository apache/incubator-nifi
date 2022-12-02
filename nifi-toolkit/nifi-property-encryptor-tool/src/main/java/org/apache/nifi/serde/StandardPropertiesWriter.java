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
package org.apache.nifi.serde;

import org.apache.nifi.properties.ReadableProperties;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StandardPropertiesWriter implements PropertiesWriter {

    private static final String PROPERTY_REGEX = "^%s=.*$";
    private static final String PROPERTY_FORMAT = "%s=%s";

    public void writePropertiesFile(final InputStream inputStream, final OutputStream outputStream, final ReadableProperties properties) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream))) {
            String line;
            Set<String> propertyKeys = properties.getPropertyKeys();
            Set<String> remainingKeys = properties.getPropertyKeys();
            while ((line = reader.readLine()) != null) {
                for (final String key : propertyKeys) {
                    Pattern regex = Pattern.compile(String.format(PROPERTY_REGEX, key));
                    Matcher m = regex.matcher(line);
                    if (m.matches()) {
                        remainingKeys.remove(key);

                        StringBuilder updatedLine = new StringBuilder();
                        updatedLine.append(String.format(PROPERTY_FORMAT, key, properties.getProperty(key)));

                        Optional<String> similarKey = remainingKeys.stream()
                                .filter(remainingKey -> remainingKey.contains(key))
                                .findFirst();
                        if (similarKey.isPresent()) {
                            updatedLine.append(System.getProperty("line.separator"));
                            updatedLine.append(String.format(PROPERTY_FORMAT, similarKey.get(), properties.getProperty(similarKey.get())));
                        }

                        line = updatedLine.toString();
                    }
                }
                writer.write(line);
                writer.newLine();
            }
        }
    }
}