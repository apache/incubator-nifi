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
package org.apache.nifi.database.dialect.service.api;

import org.apache.nifi.controller.ControllerService;

import java.util.Set;

/**
 * Abstraction responsible for returning SQL statements and attributes specific to database services
 */
public interface DatabaseDialectService extends ControllerService {
    /**
     * Get SQL Statement based on request properties
     *
     * @param statementRequest Statement request
     * @return Statement Response containing rendered SQL
     */
    StatementResponse getStatement(StatementRequest statementRequest);

    /**
     * Get SQL Statement Types supported in the Database Dialect Service
     *
     * @return Set of supported SQL Statement Types
     */
    Set<StatementType> getSupportedStatementTypes();
}
