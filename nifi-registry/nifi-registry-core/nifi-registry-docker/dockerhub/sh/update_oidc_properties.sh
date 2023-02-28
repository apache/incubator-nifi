#!/bin/sh -e

#    Licensed to the Apache Software Foundation (ASF) under one or more
#    contributor license agreements.  See the NOTICE file distributed with
#    this work for additional information regarding copyright ownership.
#    The ASF licenses this file to You under the Apache License, Version 2.0
#    (the "License"); you may not use this file except in compliance with
#    the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

: "${NIFI_REGISTRY_SECURITY_USER_OIDC_DISCOVERY_URL:?"Must specify the OIDC Discovery URL."}"
export NIFI_REGISTRY_SECURITY_USER_OIDC_CONNECT_TIMEOUT="${NIFI_REGISTRY_SECURITY_USER_OIDC_CONNECT_TIMEOUT:?"Must specify the OIDC Connect Timeout."}"
export NIFI_REGISTRY_SECURITY_USER_OIDC_READ_TIMEOUT="${NIFI_REGISTRY_SECURITY_USER_OIDC_READ_TIMEOUT:?"Must specify the OIDC Read Timeout."}"
: "${NIFI_REGISTRY_SECURITY_USER_OIDC_CLIENT_ID:?"Must specify the OIDC Client ID."}"
: "${NIFI_REGISTRY_SECURITY_USER_OIDC_CLIENT_SECRET:?"Must specify the OIDC Client Secret."}"
: "${NIFI_REGISTRY_SECURITY_USER_OIDC_PREFERRED_JWSALGORITHM:?"Must specify the OIDC Preferred JWS Algorithm."}"
export NIFI_REGISTRY_SECURITY_USER_OIDC_ADDITIONAL_SCOPES="${NIFI_REGISTRY_SECURITY_USER_OIDC_ADDITIONAL_SCOPES:-}"
export NIFI_REGISTRY_SECURITY_USER_OIDC_CLAIM_IDENTIFYING_USER="${NIFI_REGISTRY_SECURITY_USER_OIDC_CLAIM_IDENTIFYING_USER:-}"
