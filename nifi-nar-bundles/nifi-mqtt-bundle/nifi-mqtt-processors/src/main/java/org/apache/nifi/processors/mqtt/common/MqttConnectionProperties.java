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

import org.apache.nifi.ssl.SSLContextService;

public class MqttConnectionProperties {

    private MqttVersion mqttVersion;
    private int keepAliveInterval;
    private int connectionTimeout;

    private boolean cleanSession;
    private Long sessionExpiryInterval;

    private SSLContextService sslContextService;

    private String lastWillTopic;
    private String lastWillMessage;
    private Boolean lastWillRetain;
    private Integer lastWillQOS;

    private String username;
    private String password;

    public MqttVersion getMqttVersion() {
        return mqttVersion;
    }

    public void setMqttVersion(MqttVersion mqttVersion) {
        this.mqttVersion = mqttVersion;
    }

    public int getKeepAliveInterval() {
        return keepAliveInterval;
    }

    public void setKeepAliveInterval(int keepAliveInterval) {
        this.keepAliveInterval = keepAliveInterval;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public boolean isCleanSession() {
        return cleanSession;
    }

    public void setCleanSession(boolean cleanSession) {
        this.cleanSession = cleanSession;
    }

    public Long getSessionExpiryInterval() {
        return sessionExpiryInterval;
    }

    public void setSessionExpiryInterval(Long sessionExpiryInterval) {
        this.sessionExpiryInterval = sessionExpiryInterval;
    }

    public SSLContextService getSslContextService() {
        return sslContextService;
    }

    public void setSslContextService(SSLContextService sslContextService) {
        this.sslContextService = sslContextService;
    }

    public String getLastWillTopic() {
        return lastWillTopic;
    }

    public void setLastWillTopic(String lastWillTopic) {
        this.lastWillTopic = lastWillTopic;
    }

    public String getLastWillMessage() {
        return lastWillMessage;
    }

    public void setLastWillMessage(String lastWillMessage) {
        this.lastWillMessage = lastWillMessage;
    }

    public Boolean getLastWillRetain() {
        return lastWillRetain;
    }

    public void setLastWillRetain(Boolean lastWillRetain) {
        this.lastWillRetain = lastWillRetain;
    }

    public Integer getLastWillQOS() {
        return lastWillQOS;
    }

    public void setLastWillQOS(Integer lastWillQOS) {
        this.lastWillQOS = lastWillQOS;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
