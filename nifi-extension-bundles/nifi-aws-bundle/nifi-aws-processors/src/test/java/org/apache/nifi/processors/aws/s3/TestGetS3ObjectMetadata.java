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

package org.apache.nifi.processors.aws.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Region;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.aws.testutil.AuthUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestGetS3ObjectMetadata {
    private TestRunner runner = null;
    private GetS3ObjectMetadata mockGetS3ObjectMetadata = null;
    private AmazonS3Client mockS3Client = null;

    private ObjectMetadata mockMetadata;

    @BeforeEach
    public void setUp() {
        mockS3Client = mock(AmazonS3Client.class);
        mockGetS3ObjectMetadata = new GetS3ObjectMetadata() {
            @Override
            protected AmazonS3Client createClient(final ProcessContext context, final AWSCredentialsProvider credentialsProvider, final Region region, final ClientConfiguration config,
                                                  final AwsClientBuilder.EndpointConfiguration endpointConfiguration) {
                return mockS3Client;
            }
        };
        runner = TestRunners.newTestRunner(mockGetS3ObjectMetadata);
        AuthUtils.enableAccessKey(runner, "accessKeyId", "secretKey");

        mockMetadata = mock(ObjectMetadata.class);
        Map<String, String> user = Map.of("x", "y");
        Map<String, Object> raw = Map.of("a", "b");
        when(mockMetadata.getUserMetadata()).thenReturn(user);
        when(mockMetadata.getRawMetadata()).thenReturn(raw);
    }

    private void run() {
        runner.setProperty(GetS3ObjectMetadata.BUCKET_WITH_DEFAULT_VALUE, "${s3.bucket}");
        runner.setProperty(GetS3ObjectMetadata.KEY, "${filename}");
        runner.enqueue("", Map.of("s3.bucket", "test-data", "filename", "test.txt"));

        runner.run();
    }

    /*
     * Router mode tests
     */

    @DisplayName("Validate router mode works when the file exists")
    @Test
    public void testRouterRunExists() {
        runner.setProperty(GetS3ObjectMetadata.MODE, GetS3ObjectMetadata.MODE_ROUTER.getValue());
        when(mockS3Client.getObjectMetadata(anyString(), anyString()))
                .thenReturn(mockMetadata);
        run();
        runner.assertTransferCount(GetS3ObjectMetadata.REL_FOUND, 1);
    }

    @DisplayName("Validate router mode routes to failure on S3 error")
    @Test
    public void testRouterRunHasS3Error() {
        AmazonS3Exception exception = new AmazonS3Exception("test");
        exception.setStatusCode(501);

        when(mockS3Client.getObjectMetadata(anyString(), anyString()))
                .thenThrow(exception);
        run();
        runner.assertTransferCount(GetS3ObjectMetadata.REL_FAILURE, 1);
    }

    @DisplayName("Validate router mode routes to not-found when when the file doesn't exist")
    @Test
    public void testRouterRunDoesNotExist() {
        AmazonS3Exception exception = new AmazonS3Exception("test");
        exception.setStatusCode(404);

        runner.setProperty(GetS3ObjectMetadata.MODE, GetS3ObjectMetadata.MODE_ROUTER.getValue());
        when(mockS3Client.getObjectMetadata(anyString(), anyString()))
                .thenThrow(exception);
        run();
        runner.assertTransferCount(GetS3ObjectMetadata.REL_NOT_FOUND, 1);
    }

    /*
     * Fetch to attribute tests
     */

    @DisplayName("Validate fetch metadata to attribute routes to found when the file exists")
    @Test
    public void testFetchMetadataToAttributeExists() {
        runner.setProperty(GetS3ObjectMetadata.MODE, GetS3ObjectMetadata.MODE_FETCH_METADATA.getValue());
        runner.setProperty(GetS3ObjectMetadata.METADATA_TARGET, GetS3ObjectMetadata.TARGET_ATTRIBUTE.getValue());
        when(mockS3Client.getObjectMetadata(anyString(), anyString()))
                .thenReturn(mockMetadata);
        run();
        runner.assertTransferCount(GetS3ObjectMetadata.REL_FOUND, 1);
    }

    @DisplayName("Validate fetch to attribute mode routes to failure on S3 error")
    @Test
    public void testFetchMetadataToAttributeS3Error() {
        AmazonS3Exception exception = new AmazonS3Exception("test");
        exception.setStatusCode(501);

        runner.setProperty(GetS3ObjectMetadata.MODE, GetS3ObjectMetadata.MODE_FETCH_METADATA.getValue());
        runner.setProperty(GetS3ObjectMetadata.METADATA_TARGET, GetS3ObjectMetadata.TARGET_ATTRIBUTE.getValue());
        when(mockS3Client.getObjectMetadata(anyString(), anyString()))
                .thenThrow(exception);
        run();
        runner.assertTransferCount(GetS3ObjectMetadata.REL_FAILURE, 1);
    }

    @DisplayName("Validate fetch metadata to attribute routes to not-found when when the file doesn't exist")
    @Test
    public void testFetchMetadataToAttributeNotExist() {
        AmazonS3Exception exception = new AmazonS3Exception("test");
        exception.setStatusCode(404);

        runner.setProperty(GetS3ObjectMetadata.MODE, GetS3ObjectMetadata.MODE_FETCH_METADATA.getValue());
        runner.setProperty(GetS3ObjectMetadata.METADATA_TARGET, GetS3ObjectMetadata.TARGET_ATTRIBUTE.getValue());
        runner.setProperty(GetS3ObjectMetadata.MODE, GetS3ObjectMetadata.MODE_FETCH_METADATA.getValue());
        when(mockS3Client.getObjectMetadata(anyString(), anyString()))
                .thenThrow(exception);
        run();
        runner.assertTransferCount(GetS3ObjectMetadata.REL_NOT_FOUND, 1);
    }

    /*
     * Fetch to body tests
     */

    @DisplayName("Validate fetch metadata to flowfile body routes to found when the file exists")
    @Test
    public void testFetchMetadataToBodyExists() {
        runner.setProperty(GetS3ObjectMetadata.MODE, GetS3ObjectMetadata.MODE_FETCH_METADATA.getValue());
        runner.setProperty(GetS3ObjectMetadata.METADATA_TARGET, GetS3ObjectMetadata.TARGET_FLOWFILE_BODY.getValue());
        when(mockS3Client.getObjectMetadata(anyString(), anyString()))
                .thenReturn(mockMetadata);
        run();
        runner.assertTransferCount(GetS3ObjectMetadata.REL_FOUND, 1);
        List<MockFlowFile> list = runner.getFlowFilesForRelationship(GetS3ObjectMetadata.REL_FOUND);
    }

    @DisplayName("Validate fetch metadata to flowfile body routes to not-found when when the file doesn't exist")
    @Test
    public void testFetchMetadataToBodyNotExist() {
        AmazonS3Exception exception = new AmazonS3Exception("test");
        exception.setStatusCode(404);

        runner.setProperty(GetS3ObjectMetadata.MODE, GetS3ObjectMetadata.MODE_FETCH_METADATA.getValue());
        runner.setProperty(GetS3ObjectMetadata.METADATA_TARGET, GetS3ObjectMetadata.TARGET_FLOWFILE_BODY.getValue());
        when(mockS3Client.getObjectMetadata(anyString(), anyString()))
                .thenThrow(exception);
        run();
        runner.assertTransferCount(GetS3ObjectMetadata.REL_NOT_FOUND, 1);
    }

    @DisplayName("Validate fetch to flowfile body mode routes to failure on S3 error")
    @Test
    public void testFetchMetadataToBodyS3Error() {
        AmazonS3Exception exception = new AmazonS3Exception("test");
        exception.setStatusCode(501);

        runner.setProperty(GetS3ObjectMetadata.MODE, GetS3ObjectMetadata.MODE_FETCH_METADATA.getValue());
        runner.setProperty(GetS3ObjectMetadata.METADATA_TARGET, GetS3ObjectMetadata.TARGET_FLOWFILE_BODY.getValue());
        when(mockS3Client.getObjectMetadata(anyString(), anyString()))
                .thenThrow(exception);
        run();
        runner.assertTransferCount(GetS3ObjectMetadata.REL_FAILURE, 1);
    }
}
