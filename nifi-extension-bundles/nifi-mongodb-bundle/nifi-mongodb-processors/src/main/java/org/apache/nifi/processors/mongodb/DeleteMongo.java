/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.nifi.processors.mongodb;

import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.DeleteResult;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.StringUtils;
import org.bson.Document;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({ "delete", "mongo", "mongodb" })
@CapabilityDescription(
    "Executes a delete query against a MongoDB collection. The query is provided in the body of the flowfile " +
    "and the user can select whether it will delete one or many documents that match it."
)
@ReadsAttribute(
    attribute = "mongodb.delete.mode",
    description = "Configurable parameter for controlling delete mode on a per-flowfile basis. The process must be " +
            "configured to use this option. Acceptable values are 'one' and 'many.'"
)
public class DeleteMongo extends AbstractMongoProcessor {

    static final AllowableValue DELETE_ONE = new AllowableValue("one", "Delete One", "Delete only the first document that matches the query.");
    static final AllowableValue DELETE_MANY = new AllowableValue("many", "Delete Many", "Delete every document that matches the query.");
    static final AllowableValue DELETE_ATTR = new AllowableValue("attr", "Use 'mongodb.delete.mode' attribute",
            "Read the 'mongodb.delete.mode attribute and use that mode. Acceptable values are 'many' and 'one.'");

    static final AllowableValue YES_FAIL = new AllowableValue("true", "True", "Fail when no documents are deleted.");
    static final AllowableValue NO_FAIL  = new AllowableValue("false", "False", "Do not fail when nothing is deleted.");

    static final PropertyDescriptor DELETE_MODE = new PropertyDescriptor.Builder()
            .name("delete-mongo-delete-mode")
            .displayName("Delete Mode")
            .description("Choose between deleting one document by query or many documents by query.")
            .allowableValues(DELETE_ONE, DELETE_MANY, DELETE_ATTR)
            .defaultValue("one")
            .addValidator(Validator.VALID)
            .build();

    static final PropertyDescriptor FAIL_ON_NO_DELETE = new PropertyDescriptor.Builder()
            .name("delete-mongo-fail-on-no-delete")
            .displayName("Fail When Nothing Is Deleted")
            .description("Determines whether to send the flowfile to the success or failure relationship if nothing is successfully deleted.")
            .allowableValues(YES_FAIL, NO_FAIL)
            .defaultValue("true")
            .addValidator(Validator.VALID)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("All FlowFiles that are written to MongoDB are routed to this relationship").build();
    static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("All FlowFiles that cannot be written to MongoDB are routed to this relationship").build();

    private final static Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    private final static List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Stream.concat(
            getCommonPropertyDescriptors().stream(),
            Stream.of(
                    DELETE_MODE,
                    FAIL_ON_NO_DELETE
            )
    ).toList();

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    private static final List<String> ALLOWED_DELETE_VALUES;
    static {
        ALLOWED_DELETE_VALUES = new ArrayList<>();
        ALLOWED_DELETE_VALUES.add("one");
        ALLOWED_DELETE_VALUES.add("many");
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        final WriteConcern writeConcern = clientService.getWriteConcern();
        final String deleteMode = context.getProperty(DELETE_MODE).getValue();
        final String deleteAttr = flowFile.getAttribute("mongodb.delete.mode");
        final Boolean failMode  = context.getProperty(FAIL_ON_NO_DELETE).asBoolean();

        if (deleteMode.equals(DELETE_ATTR.getValue())
                && (StringUtils.isEmpty(deleteAttr) || !ALLOWED_DELETE_VALUES.contains(deleteAttr.toLowerCase()) )) {
            getLogger().error("{} is not an allowed value for mongodb.delete.mode", deleteAttr);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        try {
            final MongoCollection<Document> collection = getCollection(context, flowFile).withWriteConcern(writeConcern);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            session.exportTo(flowFile, bos);
            bos.close();

            String json = new String(bos.toByteArray());
            Document query = Document.parse(json);
            DeleteResult result;

            if (deleteMode.equals(DELETE_ONE.getValue())
                    || (deleteMode.equals(DELETE_ATTR.getValue()) && deleteAttr.toLowerCase().equals("one") )) {
                result = collection.deleteOne(query);
            } else {
                result = collection.deleteMany(query);
            }

            if (failMode && result.getDeletedCount() == 0) {
                session.transfer(flowFile, REL_FAILURE);
            } else {
                session.transfer(flowFile, REL_SUCCESS);
            }

        } catch (Exception ex) {
            getLogger().error("Could not send a delete to MongoDB, failing...", ex);
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
