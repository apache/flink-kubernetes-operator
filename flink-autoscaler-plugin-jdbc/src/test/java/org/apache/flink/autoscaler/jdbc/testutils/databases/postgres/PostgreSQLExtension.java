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

package org.apache.flink.autoscaler.jdbc.testutils.databases.postgres;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

/** The extension of PostgreSQL. */
class PostgreSQLExtension implements BeforeAllCallback, AfterAllCallback, AfterEachCallback {

    private static final String INIT_SCRIPT = "test_schema/postgres_schema.sql";
    private static final String DATABASE_NAME = "flink_autoscaler";
    private static final String USER_NAME = "root";
    private static final String PASSWORD = "123456";
    private static final List<String> TABLES =
            List.of("t_flink_autoscaler_state_store", "t_flink_autoscaler_event_handler");

    private final PostgreSQLContainer<?> container;

    public PostgreSQLExtension(String postgresqlVersion) {
        this.container =
                new PostgreSQLContainer<>(String.format("postgres:%s", postgresqlVersion))
                        .withDatabaseName(DATABASE_NAME)
                        .withUsername(USER_NAME)
                        .withPassword(PASSWORD)
                        .withInitScript(INIT_SCRIPT)
                        .withEnv("POSTGRES_MAX_CONNECTIONS", "10");
    }

    public Connection getConnection() throws Exception {
        return DriverManager.getConnection(
                container.getJdbcUrl(), container.getUsername(), container.getPassword());
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) {
        container.start();
    }

    @Override
    public void afterAll(ExtensionContext extensionContext) {
        container.stop();
    }

    @Override
    public void afterEach(ExtensionContext extensionContext) throws Exception {
        try (var conn = getConnection();
                var st = conn.createStatement()) {
            for (var tableName : TABLES) {
                st.executeUpdate(String.format("DELETE from %s", tableName));
            }
        }
    }
}
