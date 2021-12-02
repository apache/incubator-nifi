/*
 * Apache NiFi
 * Copyright 2014-2018 The Apache Software Foundation
 *
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

package org.apache.nifi.c2.protocol.api;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.io.Serializable;
import javax.validation.constraints.NotBlank;

@ApiModel
public class AgentClassManifestConfig implements Serializable {
    @NotBlank
    private String agentClassName;

    @NotBlank
    private String agentManifestId;

    @ApiModelProperty
    public String getAgentClassName() {
        return agentClassName;
    }

    public void setAgentClassName(final String agentClassName) {
        this.agentClassName = agentClassName;
    }

    @ApiModelProperty
    public String getAgentManifestId() {
        return agentManifestId;
    }

    public void setAgentManifestId(final String agentManifestId) {
        this.agentManifestId = agentManifestId;
    }
}
