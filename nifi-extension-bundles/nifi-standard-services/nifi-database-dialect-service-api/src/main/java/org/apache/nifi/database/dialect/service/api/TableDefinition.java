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

import java.util.List;
import java.util.Optional;

/**
 * Database Table definition
 *
 * @param catalog Database Catalog Name may be empty
 * @param schemaName Database Schema Name may be empty
 * @param tableName Database Table Name is required
 * @param columns Database Table Column definitions
 */
public record TableDefinition(
        Optional<String> catalog,
        Optional<String> schemaName,
        String tableName,
        List<ColumnDefinition> columns
) {
}
