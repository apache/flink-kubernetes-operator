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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.autoscaler.jdbc.state.StateType.COLLECTED_METRICS;
import static org.apache.flink.autoscaler.jdbc.state.StateType.SCALING_HISTORY;
import static org.apache.flink.autoscaler.jdbc.state.StateType.SCALING_TRACKING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The abstract IT case for {@link JdbcStateStore}. */
abstract class AbstractJdbcStateStoreITCase implements DatabaseTest {

    private static final String DEFAULT_JOB_KEY = "jobKey";
    private DataSource dataSource;
    private CountableJdbcStateInteractor jdbcStateInteractor;
    private JdbcStateStore jdbcStateStore;

    @BeforeEach
    void beforeEach() throws Exception {
        this.dataSource = getDataSource();
        this.jdbcStateInteractor = new CountableJdbcStateInteractor(dataSource);
        this.jdbcStateStore = new JdbcStateStore(jdbcStateInteractor);
    }

    @AfterEach
    void afterEach() throws Exception {
        if (dataSource instanceof AutoCloseable) {
            ((AutoCloseable) dataSource).close();
        }
    }

    @Test
    void testCaching() throws Exception {
        var value1 = "value1";
        var value2 = "value2";
        var value3 = "value3";

        jdbcStateInteractor.assertCountableJdbcInteractor(0, 0, 0, 0);

        // Query from database.
        jdbcStateStore.getSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS);
        jdbcStateInteractor.assertCountableJdbcInteractor(1, 0, 0, 0);

        // The rest of state types of same job key shouldn't query database.
        assertThat(jdbcStateStore.getSerializedState(DEFAULT_JOB_KEY, SCALING_HISTORY)).isEmpty();
        assertThat(jdbcStateStore.getSerializedState(DEFAULT_JOB_KEY, SCALING_TRACKING)).isEmpty();
        jdbcStateInteractor.assertCountableJdbcInteractor(1, 0, 0, 0);

        //  Putting does not go to database, unless flushing.
        jdbcStateStore.putSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS, value1);
        jdbcStateStore.putSerializedState(DEFAULT_JOB_KEY, SCALING_HISTORY, value2);
        jdbcStateInteractor.assertCountableJdbcInteractor(1, 0, 0, 0);

        // Flush together! Create counter is one.
        jdbcStateStore.flush(DEFAULT_JOB_KEY);
        jdbcStateInteractor.assertCountableJdbcInteractor(1, 0, 0, 1);

        // Get
        assertStateValueForCacheAndDatabase(COLLECTED_METRICS, value1);
        assertStateValueForCacheAndDatabase(SCALING_HISTORY, value2);
        var job2 = "job2";
        assertThat(jdbcStateStore.getSerializedState(job2, COLLECTED_METRICS)).isEmpty();
        jdbcStateInteractor.assertCountableJdbcInteractor(2, 0, 0, 1);
        assertThat(jdbcStateStore.getSerializedState(job2, SCALING_HISTORY)).isEmpty();

        // Put and flush state for job2
        jdbcStateStore.putSerializedState(job2, SCALING_TRACKING, value3);
        jdbcStateStore.flush(job2);
        jdbcStateInteractor.assertCountableJdbcInteractor(2, 0, 0, 2);

        // Build the new JdbcStateStore
        var newJdbcStore = new JdbcStateStore(jdbcStateInteractor);
        assertThat(newJdbcStore.getSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS))
                .hasValue(value1);
        assertThat(newJdbcStore.getSerializedState(DEFAULT_JOB_KEY, SCALING_HISTORY))
                .hasValue(value2);
        jdbcStateInteractor.assertCountableJdbcInteractor(3, 0, 0, 2);

        assertThat(newJdbcStore.getSerializedState(job2, SCALING_TRACKING)).hasValue(value3);
        jdbcStateInteractor.assertCountableJdbcInteractor(4, 0, 0, 2);

        // Removing the data from cache and query from database again.
        newJdbcStore.removeInfoFromCache(job2);
        assertThat(newJdbcStore.getSerializedState(job2, SCALING_TRACKING)).hasValue(value3);
        jdbcStateInteractor.assertCountableJdbcInteractor(5, 0, 0, 2);
    }

    @Test
    void testDeleting() throws Exception {
        var value1 = "value1";

        jdbcStateInteractor.assertCountableJdbcInteractor(0, 0, 0, 0);
        assertThat(jdbcStateStore.getSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS)).isEmpty();
        jdbcStateInteractor.assertCountableJdbcInteractor(1, 0, 0, 0);

        // Get from cache, and it shouldn't exist in database.
        jdbcStateStore.putSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS, value1);
        assertThat(jdbcStateStore.getSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS))
                .hasValue(value1);
        assertThat(getValueFromDatabase(DEFAULT_JOB_KEY, COLLECTED_METRICS)).isEmpty();
        jdbcStateInteractor.assertCountableJdbcInteractor(1, 0, 0, 0);

        // Deleting before flushing
        jdbcStateStore.removeSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS);
        assertThat(jdbcStateStore.getSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS)).isEmpty();
        assertThat(getValueFromDatabase(DEFAULT_JOB_KEY, COLLECTED_METRICS)).isEmpty();
        jdbcStateInteractor.assertCountableJdbcInteractor(1, 0, 0, 0);

        // Flush method shouldn't flush any data.
        jdbcStateStore.flush(DEFAULT_JOB_KEY);
        assertThat(jdbcStateStore.getSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS)).isEmpty();
        assertThat(getValueFromDatabase(DEFAULT_JOB_KEY, COLLECTED_METRICS)).isEmpty();
        jdbcStateInteractor.assertCountableJdbcInteractor(1, 0, 0, 0);

        // Put and flush data to database.
        var value2 = "value2";
        jdbcStateStore.putSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS, value2);
        jdbcStateStore.flush(DEFAULT_JOB_KEY);
        assertStateValueForCacheAndDatabase(COLLECTED_METRICS, value2);
        jdbcStateInteractor.assertCountableJdbcInteractor(1, 0, 0, 1);

        // Deleting after flushing, data is deleted in cache, but it still exists in database.
        jdbcStateStore.removeSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS);
        assertThat(jdbcStateStore.getSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS)).isEmpty();
        assertThat(getValueFromDatabase(DEFAULT_JOB_KEY, COLLECTED_METRICS)).hasValue(value2);

        // Flushing
        jdbcStateStore.flush(DEFAULT_JOB_KEY);
        assertThat(jdbcStateStore.getSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS)).isEmpty();
        assertThat(getValueFromDatabase(DEFAULT_JOB_KEY, COLLECTED_METRICS)).isEmpty();
        jdbcStateInteractor.assertCountableJdbcInteractor(1, 1, 0, 1);

        // Get from database for a new JdbcStateStore.
        var newJdbcStore = new JdbcStateStore(jdbcStateInteractor);
        assertThat(newJdbcStore.getSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS)).isEmpty();
        assertThat(getValueFromDatabase(DEFAULT_JOB_KEY, COLLECTED_METRICS)).isEmpty();
    }

    @Test
    void testErrorHandlingDuringFlush() throws Exception {
        var value1 = "value1";
        var value2 = "value2";
        jdbcStateInteractor.assertCountableJdbcInteractor(0, 0, 0, 0);
        assertThat(jdbcStateStore.getSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS)).isEmpty();

        // Modify the database directly.
        var tmpJdbcInteractor = new JdbcStateInteractor(dataSource);
        tmpJdbcInteractor.createData(
                DEFAULT_JOB_KEY, List.of(COLLECTED_METRICS), Map.of(COLLECTED_METRICS, value1));
        assertThat(getValueFromDatabase(DEFAULT_JOB_KEY, COLLECTED_METRICS)).hasValue(value1);

        // Cache cannot read data of database.
        assertThat(jdbcStateStore.getSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS)).isEmpty();
        jdbcStateInteractor.assertCountableJdbcInteractor(1, 0, 0, 0);

        // Create it with SQLException due to the data has already existed in database.
        jdbcStateStore.putSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS, value2);
        assertThatThrownBy(() -> jdbcStateStore.flush(DEFAULT_JOB_KEY))
                .hasCauseInstanceOf(SQLException.class);
        jdbcStateInteractor.assertCountableJdbcInteractor(1, 0, 0, 1);

        // Get normally.
        assertThat(jdbcStateStore.getSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS))
                .hasValue(value1);
        jdbcStateInteractor.assertCountableJdbcInteractor(2, 0, 0, 1);
    }

    @Test
    void testErrorHandlingDuringQuery() throws Exception {
        var value1 = "value1";
        final var expectedException = new RuntimeException("Database isn't stable.");

        var exceptionableJdbcStateInteractor =
                new CountableJdbcStateInteractor(dataSource) {
                    private final AtomicBoolean isFirst = new AtomicBoolean(true);

                    @Override
                    public Map<StateType, String> queryData(String jobKey) throws Exception {
                        if (isFirst.get()) {
                            isFirst.set(false);
                            throw expectedException;
                        }
                        return super.queryData(jobKey);
                    }
                };

        var exceptionableJdbcStore = new JdbcStateStore(exceptionableJdbcStateInteractor);

        // First get will fail.
        jdbcStateInteractor.assertCountableJdbcInteractor(0, 0, 0, 0);
        assertThatThrownBy(
                        () ->
                                exceptionableJdbcStore.getSerializedState(
                                        DEFAULT_JOB_KEY, COLLECTED_METRICS))
                .rootCause()
                .isSameAs(expectedException);

        // It's recovered.
        assertThat(exceptionableJdbcStore.getSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS))
                .isEmpty();
        exceptionableJdbcStore.putSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS, value1);
        exceptionableJdbcStore.flush(DEFAULT_JOB_KEY);
        assertStateValueForCacheAndDatabase(COLLECTED_METRICS, value1);
    }

    @Test
    void testDiscardAllState() throws Exception {
        var value1 = "value1";
        var value2 = "value2";
        var value3 = "value3";

        // Put and flush all state types first.
        jdbcStateStore.putSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS, value1);
        jdbcStateStore.putSerializedState(DEFAULT_JOB_KEY, SCALING_HISTORY, value2);
        jdbcStateStore.putSerializedState(DEFAULT_JOB_KEY, SCALING_TRACKING, value3);
        jdbcStateStore.flush(DEFAULT_JOB_KEY);
        jdbcStateInteractor.assertCountableJdbcInteractor(1, 0, 0, 1);

        assertStateValueForCacheAndDatabase(COLLECTED_METRICS, value1);
        assertStateValueForCacheAndDatabase(SCALING_HISTORY, value2);
        assertStateValueForCacheAndDatabase(SCALING_TRACKING, value3);

        // Clear all in cache.
        jdbcStateStore.clearAll(DEFAULT_JOB_KEY);
        assertThat(jdbcStateStore.getSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS)).isEmpty();
        assertThat(jdbcStateStore.getSerializedState(DEFAULT_JOB_KEY, SCALING_HISTORY)).isEmpty();
        assertThat(jdbcStateStore.getSerializedState(DEFAULT_JOB_KEY, SCALING_TRACKING)).isEmpty();
        assertThat(getValueFromDatabase(DEFAULT_JOB_KEY, COLLECTED_METRICS)).hasValue(value1);
        assertThat(getValueFromDatabase(DEFAULT_JOB_KEY, SCALING_HISTORY)).hasValue(value2);
        assertThat(getValueFromDatabase(DEFAULT_JOB_KEY, SCALING_TRACKING)).hasValue(value3);

        // Flush!
        jdbcStateStore.flush(DEFAULT_JOB_KEY);
        assertThat(jdbcStateStore.getSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS)).isEmpty();
        assertThat(jdbcStateStore.getSerializedState(DEFAULT_JOB_KEY, SCALING_HISTORY)).isEmpty();
        assertThat(jdbcStateStore.getSerializedState(DEFAULT_JOB_KEY, SCALING_TRACKING)).isEmpty();
        assertThat(getValueFromDatabase(DEFAULT_JOB_KEY, COLLECTED_METRICS)).isEmpty();
        assertThat(getValueFromDatabase(DEFAULT_JOB_KEY, SCALING_HISTORY)).isEmpty();
        assertThat(getValueFromDatabase(DEFAULT_JOB_KEY, SCALING_TRACKING)).isEmpty();
    }

    private void assertStateValueForCacheAndDatabase(StateType stateType, String expectedValue)
            throws Exception {
        assertThat(jdbcStateStore.getSerializedState(DEFAULT_JOB_KEY, stateType))
                .hasValue(expectedValue);
        assertThat(getValueFromDatabase(DEFAULT_JOB_KEY, stateType)).hasValue(expectedValue);
    }

    private Optional<String> getValueFromDatabase(String jobKey, StateType stateType)
            throws Exception {
        var jdbcInteractor = new JdbcStateInteractor(dataSource);
        return Optional.ofNullable(jdbcInteractor.queryData(jobKey).get(stateType));
    }
}

/** Test {@link JdbcStateStore} via Derby database. */
class DerbyJdbcStateStoreITCase extends AbstractJdbcStateStoreITCase implements DerbyTestBase {}

/** Test {@link JdbcStateStore} via MySQL 5.6.x. */
class MySQL56JdbcStateStoreITCase extends AbstractJdbcStateStoreITCase implements MySQL56TestBase {}

/** Test {@link JdbcStateStore} via MySQL 5.7.x. */
class MySQL57JdbcStateStoreITCase extends AbstractJdbcStateStoreITCase implements MySQL57TestBase {}

/** Test {@link JdbcStateStore} via MySQL 8. */
class MySQL8JdbcStateStoreITCase extends AbstractJdbcStateStoreITCase implements MySQL8TestBase {}

/** Test {@link JdbcStateStore} via Postgre SQL. */
class PostgreSQLJdbcStoreITCase extends AbstractJdbcStateStoreITCase
        implements PostgreSQLTestBase {}
