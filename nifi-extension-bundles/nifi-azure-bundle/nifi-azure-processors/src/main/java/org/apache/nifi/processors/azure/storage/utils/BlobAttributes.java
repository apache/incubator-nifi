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
package org.apache.nifi.processors.azure.storage.utils;

public final class BlobAttributes {

    public static final String ATTR_NAME_CONTAINER = "azure.container";
    public static final String ATTR_DESCRIPTION_CONTAINER = "The name of the Azure Blob Storage container";

    public static final String ATTR_NAME_BLOBNAME = "azure.blobname";
    public static final String ATTR_DESCRIPTION_BLOBNAME = "The name of the blob on Azure Blob Storage";

    public static final String ATTR_NAME_PRIMARY_URI = "azure.primaryUri";
    public static final String ATTR_DESCRIPTION_PRIMARY_URI = "Primary location of the blob";

    public static final String ATTR_NAME_ETAG = "azure.etag";
    public static final String ATTR_DESCRIPTION_ETAG = "ETag of the blob";

    public static final String ATTR_NAME_BLOBTYPE = "azure.blobtype";
    public static final String ATTR_DESCRIPTION_BLOBTYPE = "Type of the blob (either BlockBlob, PageBlob or AppendBlob)";

    public static final String ATTR_NAME_MIME_TYPE = "mime.type";
    public static final String ATTR_DESCRIPTION_MIME_TYPE = "MIME Type of the content";

    public static final String ATTR_NAME_LANG = "lang";
    public static final String ATTR_DESCRIPTION_LANG = "Language code for the content";

    public static final String ATTR_NAME_TIMESTAMP = "azure.timestamp";
    public static final String ATTR_DESCRIPTION_TIMESTAMP = "Timestamp of the blob";

    public static final String ATTR_NAME_LENGTH = "azure.length";
    public static final String ATTR_DESCRIPTION_LENGTH = "Length of the blob";

    public static final String ATTR_NAME_ERROR_CODE = "azure.error.code";
    public static final String ATTR_DESCRIPTION_ERROR_CODE = "Error code reported during blob operation";

    public static final String ATTR_NAME_IGNORED = "azure.ignored";
    public static final String ATTR_DESCRIPTION_IGNORED = "When Conflict Resolution Strategy is 'ignore', " +
            "this property will be true/false depending on whether the blob was ignored.";

    public static final String ATTR_NAME_USER_METADATA = "azure.usermetadata";
    public static final String ATTR_DESCRIPTION_USER_METADATA = "A human-readable form of the User Metadata of " +
            "the blob, if any was set";
}
