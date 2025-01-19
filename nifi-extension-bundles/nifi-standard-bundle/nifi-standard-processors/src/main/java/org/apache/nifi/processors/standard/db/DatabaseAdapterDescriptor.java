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
package org.apache.nifi.processors.standard.db;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.database.dialect.service.api.DatabaseDialectService;
import org.apache.nifi.processors.standard.db.impl.DatabaseDialectServiceDatabaseAdapter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;

public class DatabaseAdapterDescriptor {

    private static final List<AllowableValue> databaseTypes = new ArrayList<>();

    private static final Map<String, DatabaseAdapter> databaseAdapters = new HashMap<>();

    static {
        final ServiceLoader<DatabaseAdapter> loader = ServiceLoader.load(DatabaseAdapter.class);
        loader.forEach(databaseAdapter -> {
            final String name = databaseAdapter.getName();
            final String description = databaseAdapter.getDescription();
            final AllowableValue databaseType = new AllowableValue(name, name, description);
            databaseTypes.add(databaseType);
            databaseAdapters.put(name, databaseAdapter);
        });
    }

    public static PropertyDescriptor getDatabaseDialectServiceDescriptor(final PropertyDescriptor dependsOnPropertyDescriptor) {
        return new PropertyDescriptor.Builder()
                .name("Database Dialect Service")
                .description("Database Dialect Service for generating statements specific to a particular service or vendor.")
                .identifiesControllerService(DatabaseDialectService.class)
                .required(true)
                .dependsOn(dependsOnPropertyDescriptor, DatabaseDialectServiceDatabaseAdapter.NAME)
                .build();
    }

    public static PropertyDescriptor getDatabaseTypeDescriptor(final String propertyName) {
        return new PropertyDescriptor.Builder()
                .name(propertyName)
                .displayName("Database Type")
                .description("""
                        Database Type for generating statements specific to a particular service or vendor.
                        The Generic Type supports most cases but selecting a specific type enables optimal processing
                        or additional features.
                        """
                )
                .allowableValues(databaseTypes.toArray(new AllowableValue[0]))
                .defaultValue("Generic")
                .required(true)
                .build();
    }

    public static DatabaseAdapter getDatabaseAdapter(final String databaseType) {
        Objects.requireNonNull(databaseType, "Database Type required");
        return databaseAdapters.get(databaseType);
    }
}
