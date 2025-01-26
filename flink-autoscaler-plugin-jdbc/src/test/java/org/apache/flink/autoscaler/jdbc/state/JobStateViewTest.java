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

import org.apache.flink.autoscaler.jdbc.testutils.databases.derby.DerbyTestBase;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;

import java.util.Optional;

import static org.apache.flink.autoscaler.jdbc.state.StateType.COLLECTED_METRICS;
import static org.apache.flink.autoscaler.jdbc.state.StateType.SCALING_HISTORY;
import static org.apache.flink.autoscaler.jdbc.state.StateType.SCALING_TRACKING;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link JobStateView}. */
class JobStateViewTest implements DerbyTestBase {

    private static final String DEFAULT_JOB_KEY = "jobKey";
    private DataSource dataSource;
    private CountableJdbcStateInteractor jdbcStateInteractor;
    private JobStateView jobStateView;

    @BeforeEach
    void beforeEach() throws Exception {
        this.dataSource = getDataSource();
        this.jdbcStateInteractor = new CountableJdbcStateInteractor(dataSource);
        this.jobStateView = new JobStateView(jdbcStateInteractor, DEFAULT_JOB_KEY);
        jdbcStateInteractor.assertCountableJdbcInteractor(1, 0, 0, 0);
    }

    @AfterEach
    void afterEach() throws Exception {
        if (jdbcStateInteractor != null) {
            jdbcStateInteractor.close();
        }
    }

    @Test
    void testAllOperations() throws Exception {
        // All state types should be get together to avoid query database frequently.
        assertThat(jobStateView.get(COLLECTED_METRICS)).isNull();
        jdbcStateInteractor.assertCountableJdbcInteractor(1, 0, 0, 0);
        assertThat(jobStateView.get(SCALING_HISTORY)).isNull();
        jdbcStateInteractor.assertCountableJdbcInteractor(1, 0, 0, 0);

        // Put data to cache, and it shouldn't exist in database.
        var value1 = "value1";
        jobStateView.put(COLLECTED_METRICS, value1);
        assertThat(jobStateView.get(COLLECTED_METRICS)).isEqualTo(value1);
        assertThat(getValueFromDatabase(COLLECTED_METRICS)).isEmpty();

        var value2 = "value2";
        jobStateView.put(SCALING_HISTORY, value2);
        assertThat(jobStateView.get(SCALING_HISTORY)).isEqualTo(value2);
        assertThat(getValueFromDatabase(SCALING_HISTORY)).isEmpty();
        jdbcStateInteractor.assertCountableJdbcInteractor(1, 0, 0, 0);

        // Test creating together.
        jobStateView.flush();
        assertStateValueForCacheAndDatabase(COLLECTED_METRICS, value1);
        assertStateValueForCacheAndDatabase(SCALING_HISTORY, value2);
        jdbcStateInteractor.assertCountableJdbcInteractor(1, 0, 0, 1);

        // Test updating data to cache, and they aren't updated in database.
        var value3 = "value3";
        jobStateView.put(COLLECTED_METRICS, value3);
        assertThat(jobStateView.get(COLLECTED_METRICS)).isEqualTo(value3);
        assertThat(getValueFromDatabase(COLLECTED_METRICS)).hasValue(value1);

        var value4 = "value4";
        jobStateView.put(SCALING_HISTORY, value4);
        assertThat(jobStateView.get(SCALING_HISTORY)).isEqualTo(value4);
        assertThat(getValueFromDatabase(SCALING_HISTORY)).hasValue(value2);
        jdbcStateInteractor.assertCountableJdbcInteractor(1, 0, 0, 1);

        // Test updating together.
        jobStateView.flush();
        assertStateValueForCacheAndDatabase(COLLECTED_METRICS, value3);
        assertStateValueForCacheAndDatabase(SCALING_HISTORY, value4);
        jdbcStateInteractor.assertCountableJdbcInteractor(1, 0, 1, 1);

        // Test deleting data from cache, and they aren't deleted in database.
        jobStateView.remove(COLLECTED_METRICS);
        assertThat(jobStateView.get(COLLECTED_METRICS)).isNull();
        assertThat(getValueFromDatabase(COLLECTED_METRICS)).hasValue(value3);

        jobStateView.remove(SCALING_HISTORY);
        assertThat(jobStateView.get(SCALING_HISTORY)).isNull();
        assertThat(getValueFromDatabase(SCALING_HISTORY)).hasValue(value4);
        jdbcStateInteractor.assertCountableJdbcInteractor(1, 0, 1, 1);

        // Test updating together.
        jobStateView.flush();
        assertThat(jobStateView.get(COLLECTED_METRICS)).isNull();
        assertThat(getValueFromDatabase(COLLECTED_METRICS)).isEmpty();
        assertThat(jobStateView.get(SCALING_HISTORY)).isNull();
        assertThat(getValueFromDatabase(SCALING_HISTORY)).isEmpty();
        jdbcStateInteractor.assertCountableJdbcInteractor(1, 1, 1, 1);
    }

    @Test
    void testAvoidUnnecessaryFlushes() throws Exception {
        var value1 = "value1";
        jobStateView.put(COLLECTED_METRICS, value1);
        jobStateView.flush();
        jdbcStateInteractor.assertCountableJdbcInteractor(1, 0, 0, 1);

        // Avoid unnecessary flush for creating.
        jobStateView.flush();
        jdbcStateInteractor.assertCountableJdbcInteractor(1, 0, 0, 1);

        // Avoid unnecessary flush for deleting.
        jobStateView.clear();
        jobStateView.flush();
        jdbcStateInteractor.assertCountableJdbcInteractor(1, 1, 0, 1);
        jobStateView.flush();
        jdbcStateInteractor.assertCountableJdbcInteractor(1, 1, 0, 1);

        // Avoid unnecessary flush even if clear is called..
        jobStateView.clear();
        jobStateView.flush();
        jdbcStateInteractor.assertCountableJdbcInteractor(1, 1, 0, 1);
    }

    @Test
    void testCreateDeleteAndUpdateWorkTogether() throws Exception {
        var value1 = "value1";
        var value2 = "value2";
        var value3 = "value3";
        var value4 = "value4";
        // Create 2 state types first.
        jobStateView.put(COLLECTED_METRICS, value1);
        jobStateView.put(SCALING_HISTORY, value2);
        jobStateView.flush();
        assertStateValueForCacheAndDatabase(COLLECTED_METRICS, value1);
        assertStateValueForCacheAndDatabase(SCALING_HISTORY, value2);
        jdbcStateInteractor.assertCountableJdbcInteractor(1, 0, 0, 1);

        // Delete one, update one and create one.
        jobStateView.remove(COLLECTED_METRICS);
        jobStateView.put(SCALING_HISTORY, value3);
        jobStateView.put(SCALING_TRACKING, value4);

        assertThat(jobStateView.get(COLLECTED_METRICS)).isNull();
        assertThat(getValueFromDatabase(COLLECTED_METRICS)).hasValue(value1);
        assertThat(jobStateView.get(SCALING_HISTORY)).isEqualTo(value3);
        assertThat(getValueFromDatabase(SCALING_HISTORY)).hasValue(value2);
        assertThat(jobStateView.get(SCALING_TRACKING)).isEqualTo(value4);
        assertThat(getValueFromDatabase(SCALING_TRACKING)).isEmpty();
        jdbcStateInteractor.assertCountableJdbcInteractor(1, 0, 0, 1);

        // Flush!
        jobStateView.flush();
        assertThat(jobStateView.get(COLLECTED_METRICS)).isNull();
        assertThat(getValueFromDatabase(COLLECTED_METRICS)).isEmpty();
        assertStateValueForCacheAndDatabase(SCALING_HISTORY, value3);
        assertStateValueForCacheAndDatabase(SCALING_TRACKING, value4);
        jdbcStateInteractor.assertCountableJdbcInteractor(1, 1, 1, 2);

        // Build the new JobStateView
        var newJobStateView = new JobStateView(jdbcStateInteractor, DEFAULT_JOB_KEY);
        assertThat(newJobStateView.get(COLLECTED_METRICS)).isNull();
        assertThat(getValueFromDatabase(COLLECTED_METRICS)).isEmpty();
        assertThat(newJobStateView.get(SCALING_HISTORY)).isEqualTo(value3);
        assertThat(getValueFromDatabase(SCALING_HISTORY)).hasValue(value3);
        assertThat(newJobStateView.get(SCALING_TRACKING)).isEqualTo(value4);
        assertThat(getValueFromDatabase(SCALING_TRACKING)).hasValue(value4);
        jdbcStateInteractor.assertCountableJdbcInteractor(2, 1, 1, 2);
    }

    private void assertStateValueForCacheAndDatabase(StateType stateType, String expectedValue)
            throws Exception {
        assertThat(jobStateView.get(stateType)).isEqualTo(expectedValue);
        assertThat(getValueFromDatabase(stateType)).hasValue(expectedValue);
    }

    private Optional<String> getValueFromDatabase(StateType stateType) throws Exception {
        var jdbcInteractor = new JdbcStateInteractor(dataSource);
        return Optional.ofNullable(jdbcInteractor.queryData(DEFAULT_JOB_KEY).get(stateType));
    }
}
