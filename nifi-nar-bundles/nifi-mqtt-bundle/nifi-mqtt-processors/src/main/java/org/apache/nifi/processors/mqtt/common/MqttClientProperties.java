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
package org.apache.nifi.processors.mqtt.common;

import java.net.URI;

public class MqttClientProperties {
    private URI brokerUri;
    private String clientId;

    private MqttVersion mqttVersion;

    public String getBroker() {
        return brokerUri.toString();
    }

    public MqttProtocolScheme getScheme() {
        return MqttProtocolScheme.valueOf(brokerUri.getScheme().toUpperCase());
    }

    public URI getBrokerUri() {
        return brokerUri;
    }

    public void setBrokerUri(URI brokerUri) {
        this.brokerUri = brokerUri;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public MqttVersion getMqttVersion() {
        return mqttVersion;
    }

    public void setMqttVersion(MqttVersion mqttVersion) {
        this.mqttVersion = mqttVersion;
    }
}
