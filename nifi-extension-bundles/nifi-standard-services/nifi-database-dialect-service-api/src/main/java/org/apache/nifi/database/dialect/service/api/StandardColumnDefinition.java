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

import java.sql.Types;
import java.util.Optional;

/**
 * Standard Database Table Column definition
 *
 * @param columnName Table Column Name
 * @param dataType SQL type from java.sql.Types
 * @param nullable Nullable status corresponding to java.sql.DatabaseMetaData IS_NULLABLE
 * @param defaultValue Default column value may be empty
 * @param primaryKey Primary Key status
 */
public record StandardColumnDefinition(
        String columnName,
        int dataType,
        Nullable nullable,
        Optional<String> defaultValue,
        boolean primaryKey
) implements ColumnDefinition {
    /**
     * Standard Column Definition with Column Name and defaults for other properties for queries
     *
     * @param columnName Table Column Name
     */
    public StandardColumnDefinition(final String columnName) {
        this(columnName, Types.VARCHAR, Nullable.UNKNOWN, Optional.empty(), false);
    }
}
