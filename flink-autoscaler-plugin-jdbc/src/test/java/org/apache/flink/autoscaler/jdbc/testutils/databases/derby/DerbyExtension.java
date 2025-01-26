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

package org.apache.flink.autoscaler.jdbc.testutils.databases.derby;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

/** The extension of Derby. */
public class DerbyExtension implements BeforeAllCallback, AfterAllCallback, AfterEachCallback {

    private static final List<String> TABLES =
            List.of("t_flink_autoscaler_state_store", "t_flink_autoscaler_event_handler");
    private static final String JDBC_URL = "jdbc:derby:memory:test";

    public HikariDataSource getDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(JDBC_URL);
        config.setValidationTimeout(1000);
        return new HikariDataSource(config);
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws Exception {
        DriverManager.getConnection(String.format("%s;create=true", JDBC_URL)).close();

        var stateStoreDDL =
                "CREATE TABLE t_flink_autoscaler_state_store\n"
                        + "(\n"
                        + "    id            BIGINT       NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),\n"
                        + "    update_time   TIMESTAMP    NOT NULL,\n"
                        + "    job_key       VARCHAR(191) NOT NULL,\n"
                        + "    state_type    VARCHAR(100) NOT NULL,\n"
                        + "    state_value   CLOB NOT NULL,\n"
                        + "    PRIMARY KEY (id)\n"
                        + ")\n";

        var createStateStoreIndex =
                "CREATE UNIQUE INDEX un_job_state_type_inx ON t_flink_autoscaler_state_store (job_key, state_type)";

        var eventHandlerDDL =
                "CREATE TABLE t_flink_autoscaler_event_handler\n"
                        + "(\n"
                        + "    id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),\n"
                        + "    create_time TIMESTAMP NOT NULL,\n"
                        + "    update_time TIMESTAMP NOT NULL,\n"
                        + "    job_key VARCHAR(191) NOT NULL,\n"
                        + "    reason VARCHAR(500) NOT NULL,\n"
                        + "    event_type VARCHAR(100) NOT NULL,\n"
                        + "    message CLOB NOT NULL,\n"
                        + "    event_count INTEGER NOT NULL,\n"
                        + "    event_key VARCHAR(100) NOT NULL,\n"
                        + "    PRIMARY KEY (id)\n"
                        + ")\n";

        var eventKeyIndex =
                "CREATE INDEX job_key_reason_event_key_idx ON t_flink_autoscaler_event_handler (job_key, reason, event_key)";
        var jobKeyReasonCreateTimeIndex =
                "CREATE INDEX job_key_reason_create_time_idx ON t_flink_autoscaler_event_handler (job_key, reason, create_time)";

        try (var dataSource = getDataSource();
                var conn = dataSource.getConnection();
                var st = conn.createStatement()) {
            st.execute(stateStoreDDL);
            st.execute(createStateStoreIndex);
            st.execute(eventHandlerDDL);
            st.execute(eventKeyIndex);
            st.execute(jobKeyReasonCreateTimeIndex);
        }
    }

    @Override
    public void afterAll(ExtensionContext extensionContext) throws Exception {
        try (var dataSource = getDataSource();
                var conn = dataSource.getConnection();
                var st = conn.createStatement()) {
            for (var tableName : TABLES) {
                st.executeUpdate(String.format("DROP TABLE %s", tableName));
            }
        }
        try {
            DriverManager.getConnection(String.format("%s;shutdown=true", JDBC_URL)).close();
        } catch (SQLException ignored) {
        }
    }

    @Override
    public void afterEach(ExtensionContext extensionContext) throws Exception {
        // Clean up all data
        try (var dataSource = getDataSource();
                var conn = dataSource.getConnection();
                var st = conn.createStatement()) {
            for (var tableName : TABLES) {
                st.executeUpdate(String.format("DELETE from %s", tableName));
            }
        }
    }
}
