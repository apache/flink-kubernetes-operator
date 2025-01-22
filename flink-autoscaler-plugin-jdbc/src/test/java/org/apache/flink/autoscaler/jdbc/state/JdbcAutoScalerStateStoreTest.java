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

import org.apache.flink.api.common.JobID;
import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.autoscaler.jdbc.testutils.databases.derby.DerbyTestBase;
import org.apache.flink.autoscaler.state.AbstractAutoScalerStateStoreTest;
import org.apache.flink.autoscaler.state.AutoScalerStateStore;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.TreeMap;

import static org.apache.flink.autoscaler.TestingAutoscalerUtils.createDefaultJobAutoScalerContext;
import static org.apache.flink.autoscaler.metrics.ScalingHistoryUtils.getTrimmedScalingHistory;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for {@link JdbcAutoScalerStateStore}.
 *
 * <p>Note: {@link #testDiscardInvalidHistory} is referred from {@link
 * KubernetesAutoScalerStateStoreTest}.
 */
class JdbcAutoScalerStateStoreTest
        extends AbstractAutoScalerStateStoreTest<JobID, JobAutoScalerContext<JobID>>
        implements DerbyTestBase {

    private JdbcStateStore jdbcStateStore;
    private JdbcAutoScalerStateStore<JobID, JobAutoScalerContext<JobID>> cachedStateStore;

    @Override
    protected void preSetup() throws Exception {
        jdbcStateStore = new JdbcStateStore(new JdbcStateInteractor(getDataSource()));
        cachedStateStore = new JdbcAutoScalerStateStore<>(jdbcStateStore);
    }

    @Override
    protected AutoScalerStateStore<JobID, JobAutoScalerContext<JobID>>
            createPhysicalAutoScalerStateStore() throws Exception {
        return new JdbcAutoScalerStateStore<>(
                new JdbcStateStore(new JdbcStateInteractor(getDataSource())));
    }

    @Override
    protected AutoScalerStateStore<JobID, JobAutoScalerContext<JobID>>
            createCachedAutoScalerStateStore() {
        return cachedStateStore;
    }

    @Override
    protected JobAutoScalerContext<JobID> createJobContext() {
        return createDefaultJobAutoScalerContext();
    }

    @Test
    void testDiscardInvalidHistory() throws Exception {
        jdbcStateStore.putSerializedState(
                ctx.getJobKey().toString(), StateType.COLLECTED_METRICS, "invalid");
        jdbcStateStore.putSerializedState(
                ctx.getJobKey().toString(), StateType.SCALING_HISTORY, "invalid2");

        var now = Instant.now();

        assertThat(
                        jdbcStateStore.getSerializedState(
                                ctx.getJobKey().toString(), StateType.COLLECTED_METRICS))
                .isPresent();
        assertThat(stateStore.getCollectedMetrics(ctx)).isEmpty();
        assertThat(
                        jdbcStateStore.getSerializedState(
                                ctx.getJobKey().toString(), StateType.COLLECTED_METRICS))
                .isEmpty();

        assertThat(
                        jdbcStateStore.getSerializedState(
                                ctx.getJobKey().toString(), StateType.SCALING_HISTORY))
                .isPresent();
        Assertions.assertEquals(new TreeMap<>(), getTrimmedScalingHistory(stateStore, ctx, now));
        assertThat(
                        jdbcStateStore.getSerializedState(
                                ctx.getJobKey().toString(), StateType.SCALING_HISTORY))
                .isEmpty();
    }
}
