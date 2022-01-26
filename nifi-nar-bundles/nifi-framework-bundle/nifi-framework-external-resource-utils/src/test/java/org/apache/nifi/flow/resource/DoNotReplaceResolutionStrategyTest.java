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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;

@ExtendWith(MockitoExtension.class)
public class DoNotReplaceResolutionStrategyTest {
    private static final String RESOURCE_NAME = "resource.json";
    private static final String OTHER_RESOURCE_NAME = "driver.jar";
    private static final long MODIFIED_AT = System.currentTimeMillis();

    @Mock
    private File targetDirectory;

    @Mock
    private File existingResource;

    private final DoNotReplaceResolutionStrategy testSubject = new DoNotReplaceResolutionStrategy();
    private final ExternalResourceDescriptor descriptor = new ImmutableExternalResourceDescriptor(RESOURCE_NAME, MODIFIED_AT);

    @Test
    public void testEmptyFolder() {
        Mockito.when(targetDirectory.listFiles()).thenReturn(new File[]{});

        final boolean result = testSubject.shouldBeFetched(targetDirectory, descriptor);

        Assertions.assertTrue(result);
    }

    @Test
    public void testFolderWithoutMatchingFile() {
        Mockito.when(existingResource.getName()).thenReturn(OTHER_RESOURCE_NAME);
        Mockito.when(targetDirectory.listFiles()).thenReturn(new File[]{existingResource});

        final boolean result = testSubject.shouldBeFetched(targetDirectory, descriptor);

        Assertions.assertTrue(result);
    }

    @Test
    public void testFolderWithMatchingFile() {
        Mockito.when(existingResource.getName()).thenReturn(RESOURCE_NAME);
        Mockito.when(targetDirectory.listFiles()).thenReturn(new File[]{existingResource});

        final boolean result = testSubject.shouldBeFetched(targetDirectory, descriptor);

        Assertions.assertFalse(result);
    }
}