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

package org.apache.flink.autoscaler.jdbc.state;

import org.apache.flink.autoscaler.jdbc.testutils.databases.DatabaseTest;
import org.apache.flink.autoscaler.jdbc.testutils.databases.derby.DerbyTestBase;
import org.apache.flink.autoscaler.jdbc.testutils.databases.mysql.MySQL56TestBase;
import org.apache.flink.autoscaler.jdbc.testutils.databases.mysql.MySQL57TestBase;
import org.apache.flink.autoscaler.jdbc.testutils.databases.mysql.MySQL8TestBase;
import org.apache.flink.autoscaler.jdbc.testutils.databases.postgres.PostgreSQLTestBase;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.apache.flink.autoscaler.jdbc.state.StateType.COLLECTED_METRICS;
import static org.apache.flink.autoscaler.jdbc.state.StateType.SCALING_HISTORY;
import static org.assertj.core.api.Assertions.assertThat;

/** The abstract IT case for {@link JdbcStateInteractor}. */
abstract class AbstractJdbcStateInteractorITCase implements DatabaseTest {

    @Test
    void testAllOperations() throws Exception {
        var jobKey = "jobKey";
        var value1 = "value1";
        var value2 = "value2";
        var value3 = "value3";
        try (var jdbcStateInteractor = new JdbcStateInteractor(getDataSource())) {
            assertThat(jdbcStateInteractor.queryData(jobKey)).isEmpty();

            // Test for creating data.
            jdbcStateInteractor.createData(
                    jobKey,
                    List.of(COLLECTED_METRICS, SCALING_HISTORY),
                    Map.of(COLLECTED_METRICS, value1, SCALING_HISTORY, value2));
            assertThat(jdbcStateInteractor.queryData(jobKey))
                    .isEqualTo(Map.of(COLLECTED_METRICS, value1, SCALING_HISTORY, value2));

            // Test for updating data.
            jdbcStateInteractor.updateData(
                    jobKey,
                    List.of(COLLECTED_METRICS),
                    Map.of(COLLECTED_METRICS, value3, SCALING_HISTORY, value2));
            assertThat(jdbcStateInteractor.queryData(jobKey))
                    .isEqualTo(Map.of(COLLECTED_METRICS, value3, SCALING_HISTORY, value2));

            // Test for deleting data.
            jdbcStateInteractor.deleteData(jobKey, List.of(COLLECTED_METRICS));
            assertThat(jdbcStateInteractor.queryData(jobKey))
                    .isEqualTo(Map.of(SCALING_HISTORY, value2));
            jdbcStateInteractor.deleteData(jobKey, List.of(SCALING_HISTORY));
            assertThat(jdbcStateInteractor.queryData(jobKey)).isEmpty();
        }
    }
}

/** Test {@link JdbcStateInteractor} via Derby database. */
class DerbyJdbcStateInteractorITCase extends AbstractJdbcStateInteractorITCase
        implements DerbyTestBase {}

/** Test {@link JdbcStateInteractor} via MySQL 5.6.x. */
class MySQL56JdbcStateInteractorITCase extends AbstractJdbcStateInteractorITCase
        implements MySQL56TestBase {}

/** Test {@link JdbcStateInteractor} via MySQL 5.7.x. */
class MySQL57JdbcStateInteractorITCase extends AbstractJdbcStateInteractorITCase
        implements MySQL57TestBase {}

/** Test {@link JdbcStateInteractor} via MySQL 8.x. */
class MySQL8JdbcStateInteractorITCase extends AbstractJdbcStateInteractorITCase
        implements MySQL8TestBase {}

/** Test {@link JdbcStateInteractor} via Postgre SQL. */
class PostgreSQLJdbcStateInteractorITCase extends AbstractJdbcStateInteractorITCase
        implements PostgreSQLTestBase {}
