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
package org.apache.nifi.atlas.provenance.analyzer;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.utils.AtlasPathExtractorUtil;
import org.apache.atlas.utils.PathExtractorContext;
import org.apache.atlas.v1.model.instance.Referenceable;
import org.apache.hadoop.fs.Path;
import org.apache.nifi.atlas.provenance.AbstractNiFiProvenanceEventAnalyzer;
import org.apache.nifi.atlas.provenance.AnalysisContext;
import org.apache.nifi.atlas.provenance.DataSetRefs;
import org.apache.nifi.provenance.ProvenanceEventRecord;

import java.util.Map;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;

/**
 * Analyzes a transit URI as a GCS bucket or directory (skipping the file name).
 * <p>
 * Atlas entity hierarchy v1: gcs_virtual_dir -> gcs_bucket
 * <p>gcs_virtual_dir
 * <ul>
 *   <li>qualifiedName=gs://bucket/path@namespace (example: gs://mybucket/mydir1/mydir2@ns1)
 *   <li>name=/path (example: /mydir1/mydir2)
 * </ul>
 * <p>gcs_bucket
 * <ul>
 *   <li>qualifiedName=gs://bucket@namespace (example: gs://mybucket@ns1)
 *   <li>name=bucket (example: mybucket)
 * </ul>
 */
public class GCSDirectory extends AbstractNiFiProvenanceEventAnalyzer {

    static final String GCP_STORAGE_VIRTUAL_DIRECTORY = "gcp_storage_virtual_directory";
    static final String REL_PARENT = "parent";

    @Override
    public DataSetRefs analyze(AnalysisContext context, ProvenanceEventRecord event) {
        String transitUri = event.getTransitUri();
        if (transitUri == null) {
            return null;
        }

        String directoryUri = transitUri.substring(0, transitUri.lastIndexOf('/') + 1);

        Path path = new Path(directoryUri);

        String namespace = context.getNamespaceResolver().fromHostNames(path.toUri().getHost());

        PathExtractorContext pathExtractorContext = new PathExtractorContext(namespace, context.getAwsS3ModelVersion());
        AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo = AtlasPathExtractorUtil.getPathEntity(path, pathExtractorContext);

        Referenceable ref = convertToReferenceable(entityWithExtInfo.getEntity(), pathExtractorContext.getKnownEntities());

        return ref != null ? singleDataSetRef(event.getComponentId(), event.getEventType(), ref) : null;
    }

    @Override
    public String targetTransitUriPattern() {
        return "^gs://.+/.+$";
    }

    private Referenceable convertToReferenceable(AtlasEntity entity, Map<String, AtlasEntity> knownEntities) {
        if (entity == null) {
            return null;
        }

        Referenceable ref = createReferenceable(entity);

        if (GCP_STORAGE_VIRTUAL_DIRECTORY.equals(entity.getTypeName())) {
            final AtlasObjectId parentId = (AtlasObjectId) entity.getRelationshipAttribute(REL_PARENT);
            if (parentId != null) {
                AtlasEntity parentEntity = knownEntities.get(parentId.getUniqueAttributes().get(ATTR_QUALIFIED_NAME));
                ref.set(REL_PARENT, convertToReferenceable(parentEntity, knownEntities));
            }
        }

        return ref;
    }

    private Referenceable createReferenceable(AtlasEntity entity) {
        Referenceable ref = new Referenceable(entity.getTypeName());
        ref.setValues(entity.getAttributes());
        return ref;
    }

}
