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

package org.apache.flink.kubernetes.operator.autoscaler;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Test for AutoScalerInfo. */
public class AutoScalerInfoTest {

    @Test
    public void testTopologyUpdate() {
        var data = new HashMap<String, String>();
        var info = new AutoScalerInfo(data);

        var v1 = new JobVertexID();
        var v2 = new JobVertexID();
        var v3 = new JobVertexID();

        var history = new HashMap<JobVertexID, ScalingSummary>();
        history.put(v1, new ScalingSummary(1, 2, null));
        history.put(v2, new ScalingSummary(1, 2, null));

        info.addToScalingHistory(Instant.now(), history, new Configuration());

        assertEquals(history.keySet(), info.getScalingHistory().keySet());
        assertEquals(history.keySet(), new AutoScalerInfo(data).getScalingHistory().keySet());

        info.updateVertexList(List.of(v2, v3));

        // Expect v1 to be removed
        assertEquals(Set.of(v2), info.getScalingHistory().keySet());
        assertEquals(Set.of(v2), new AutoScalerInfo(data).getScalingHistory().keySet());
    }

    @Test
    public void testHistorySizeConfigs() {
        var data = new HashMap<String, String>();
        var info = new AutoScalerInfo(data);

        var v1 = new JobVertexID();

        var history = new HashMap<JobVertexID, ScalingSummary>();
        history.put(v1, new ScalingSummary(1, 2, null));

        var conf = new Configuration();
        conf.set(AutoScalerOptions.VERTEX_SCALING_HISTORY_COUNT, 2);
        conf.set(AutoScalerOptions.VERTEX_SCALING_HISTORY_AGE, Duration.ofSeconds(10));

        var now = Instant.now();

        // Verify count based expiration
        info.addToScalingHistory(now, history, conf);
        assertEquals(1, info.getScalingHistory().get(v1).size());

        info.addToScalingHistory(now.plus(Duration.ofSeconds(1)), history, conf);
        info.addToScalingHistory(now.plus(Duration.ofSeconds(2)), history, conf);

        assertEquals(2, info.getScalingHistory().get(v1).size());
        assertEquals(
                Set.of(now.plus(Duration.ofSeconds(1)), now.plus(Duration.ofSeconds(2))),
                info.getScalingHistory().get(v1).keySet());

        // Verify time based expiration
        info.addToScalingHistory(now.plus(Duration.ofSeconds(15)), history, conf);
        assertEquals(1, info.getScalingHistory().get(v1).size());
        assertEquals(
                Set.of(now.plus(Duration.ofSeconds(15))),
                info.getScalingHistory().get(v1).keySet());
        assertEquals(
                Set.of(now.plus(Duration.ofSeconds(15))),
                new AutoScalerInfo(data).getScalingHistory().get(v1).keySet());
    }

    @Test
    public void testCompressionMigration() throws JsonProcessingException {
        var jobUpdateTs = Instant.now();
        var v1 = new JobVertexID();

        var metricHistory = new TreeMap<Instant, Map<JobVertexID, Map<ScalingMetric, Double>>>();
        metricHistory.put(jobUpdateTs, Map.of(v1, Map.of(ScalingMetric.TRUE_PROCESSING_RATE, 1.)));

        var scalingHistory = new HashMap<JobVertexID, SortedMap<Instant, ScalingSummary>>();
        scalingHistory.put(v1, new TreeMap<>());
        scalingHistory
                .get(v1)
                .put(
                        jobUpdateTs,
                        new ScalingSummary(
                                1, 2, Map.of(ScalingMetric.LAG, EvaluatedScalingMetric.of(2.))));

        // Store uncompressed data in map to simulate migration
        var data = new HashMap<String, String>();
        data.put(
                AutoScalerInfo.COLLECTED_METRICS_KEY,
                AutoScalerInfo.YAML_MAPPER.writeValueAsString(metricHistory));
        data.put(AutoScalerInfo.JOB_UPDATE_TS_KEY, jobUpdateTs.toString());
        data.put(
                AutoScalerInfo.SCALING_HISTORY_KEY,
                AutoScalerInfo.YAML_MAPPER.writeValueAsString(scalingHistory));

        var info = new AutoScalerInfo(data);
        assertEquals(scalingHistory, info.getScalingHistory());
        assertEquals(metricHistory, info.getMetricHistory());

        // Override with compressed data
        var newTs = Instant.now();
        info.updateMetricHistory(newTs, metricHistory);
        info.addToScalingHistory(newTs, Map.of(), new Configuration());

        // Make sure we can still access everything
        assertEquals(scalingHistory, info.getScalingHistory());
        assertEquals(metricHistory, info.getMetricHistory());
    }
}
