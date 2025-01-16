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
package org.apache.nifi.web.api.entity;

import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "processGroupOptionEntity")
public class ProcessGroupOptionEntity extends Entity {

    private String text;
    private String value;
    private String description;
    private boolean disabled;

    /**
     * The name for this ProcessGroup.
     *
     * @return The name
     */
    @Schema(description = "The name of this ProcessGroup.")
    public String getText() {
        return text;
    }

    public void setText(String id) {
        this.text = id;
    }

    /**
     * The id for this ProcessGroup.
     *
     * @return The id
     */
    @Schema(description = "The id of this ProcessGroup.")
    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    /**
     * Text containing any conflicts for this ProcessGroup.
     *
     * @return The text
     */
    @Schema(description = "Text containing any conflicts for this ProcessGroup.")
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * Boolean showing whether this option is disabled.
     *
     * @return The disabled boolean
     */
    @Schema(description = "Boolean showing whether this option is disabled.")
    public boolean isDisabled() {
        return disabled;
    }

    public void setDisabled(boolean disabled) {
        this.disabled = disabled;
    }
}
