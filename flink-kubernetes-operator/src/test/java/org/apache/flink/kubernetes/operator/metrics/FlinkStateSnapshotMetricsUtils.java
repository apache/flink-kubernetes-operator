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

package org.apache.flink.kubernetes.operator.metrics;

import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;
import org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus;
import org.apache.flink.metrics.Gauge;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.flink.kubernetes.operator.metrics.FlinkStateSnapshotMetrics.CHECKPOINT_GROUP_NAME;
import static org.apache.flink.kubernetes.operator.metrics.FlinkStateSnapshotMetrics.COUNTER_GROUP_NAME;
import static org.apache.flink.kubernetes.operator.metrics.FlinkStateSnapshotMetrics.SAVEPOINT_GROUP_NAME;
import static org.apache.flink.kubernetes.operator.metrics.FlinkStateSnapshotMetrics.STATE_GROUP_NAME;
import static org.assertj.core.api.Assertions.assertThat;

/** Utils for testing FlinkStateSnapshot metrics. */
public class FlinkStateSnapshotMetricsUtils {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkStateSnapshotMetricsUtils.class);

    public static void assertSnapshotMetrics(
            TestingMetricListener listener,
            String namespace,
            Map<FlinkStateSnapshotStatus.State, Integer> savepointExpectedCountMap,
            Map<FlinkStateSnapshotStatus.State, Integer> checkpointExpectedCountMap) {
        assertSnapshotMetricsForGroup(
                listener, namespace, SAVEPOINT_GROUP_NAME, savepointExpectedCountMap);
        assertSnapshotMetricsForGroup(
                listener, namespace, CHECKPOINT_GROUP_NAME, checkpointExpectedCountMap);
    }

    private static void assertSnapshotMetricsForGroup(
            TestingMetricListener listener,
            String namespace,
            String snapshotGroupName,
            Map<FlinkStateSnapshotStatus.State, Integer> expectedStateCountMap) {
        var snapshotCount = expectedStateCountMap.values().stream().mapToInt(i -> i).sum();
        var counterIdCheckpoint =
                listener.getNamespaceMetricId(
                        FlinkStateSnapshot.class, namespace, snapshotGroupName, COUNTER_GROUP_NAME);
        assertThat(listener.getGauge(counterIdCheckpoint))
                .isPresent()
                .get()
                .extracting(Gauge::getValue)
                .isEqualTo(snapshotCount);

        for (FlinkStateSnapshotStatus.State state : FlinkStateSnapshotStatus.State.values()) {
            var stateId =
                    listener.getNamespaceMetricId(
                            FlinkStateSnapshot.class,
                            namespace,
                            snapshotGroupName,
                            STATE_GROUP_NAME,
                            state.name(),
                            COUNTER_GROUP_NAME);
            var expectedStateCount = expectedStateCountMap.getOrDefault(state, 0);

            LOG.info("Asserting snapshot metrics {} = {}", stateId, expectedStateCount);
            assertThat(listener.getGauge(stateId))
                    .isPresent()
                    .get()
                    .extracting(Gauge::getValue)
                    .isEqualTo(expectedStateCount);
        }
    }
}
