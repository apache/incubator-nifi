# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from pymilvus import DataType, FieldSchema
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult

class MilvusProcessor(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        description = "This processor depends on both google-cloud-vision and pymilvus"
        version = '0.0.1-SNAPSHOT'
        tags = ['cloud', 'vision', 'milvus']
        dependencies = ['pymilvus==2.4.4']

    def __init__(self, jvm):
        pass

    def transform(self, context, flow_file):
        num_cows_field = FieldSchema(name="number of cows", dtype=DataType.INT64)
        return FlowFileTransformResult('success', contents=num_cows_field.name)
