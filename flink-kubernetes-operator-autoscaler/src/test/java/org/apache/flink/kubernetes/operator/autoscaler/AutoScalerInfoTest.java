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

import org.apache.flink.autoscaler.ScalingSummary;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.metrics.CollectedMetrics;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.fabric8.kubernetes.api.model.ConfigMap;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

        var conf = new Configuration();
        var now = Instant.now();

        info.addToScalingHistory(now, history, conf);

        assertEquals(history.keySet(), info.getScalingHistory(now, conf).keySet());
        assertEquals(
                history.keySet(), new AutoScalerInfo(data).getScalingHistory(now, conf).keySet());

        info.updateVertexList(List.of(v2, v3), now, conf);

        // Expect v1 to be removed
        assertEquals(Set.of(v2), info.getScalingHistory(now, conf).keySet());
        assertEquals(Set.of(v2), new AutoScalerInfo(data).getScalingHistory(now, conf).keySet());
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
        assertEquals(1, info.getScalingHistory(now, conf).get(v1).size());

        info.addToScalingHistory(now.plus(Duration.ofSeconds(1)), history, conf);
        info.addToScalingHistory(now.plus(Duration.ofSeconds(2)), history, conf);

        assertEquals(
                2, info.getScalingHistory(now.plus(Duration.ofSeconds(2)), conf).get(v1).size());
        assertEquals(
                Set.of(now.plus(Duration.ofSeconds(1)), now.plus(Duration.ofSeconds(2))),
                info.getScalingHistory(now.plus(Duration.ofSeconds(2)), conf).get(v1).keySet());

        // Verify time based expiration
        info.addToScalingHistory(now.plus(Duration.ofSeconds(15)), history, conf);
        assertEquals(
                1, info.getScalingHistory(now.plus(Duration.ofSeconds(15)), conf).get(v1).size());
        assertEquals(
                Set.of(now.plus(Duration.ofSeconds(15))),
                info.getScalingHistory(now.plus(Duration.ofSeconds(15)), conf).get(v1).keySet());
        assertEquals(
                Set.of(now.plus(Duration.ofSeconds(15))),
                new AutoScalerInfo(data)
                        .getScalingHistory(now.plus(Duration.ofSeconds(15)), conf)
                        .get(v1)
                        .keySet());
    }

    @Test
    public void testCompressionMigration() throws JsonProcessingException {
        var jobUpdateTs = Instant.now();
        var v1 = new JobVertexID();

        var metricHistory = new TreeMap<Instant, CollectedMetrics>();
        metricHistory.put(
                jobUpdateTs,
                new CollectedMetrics(
                        Map.of(v1, Map.of(ScalingMetric.TRUE_PROCESSING_RATE, 1.)), Map.of()));

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
        data.put(
                AutoScalerInfo.SCALING_HISTORY_KEY,
                AutoScalerInfo.YAML_MAPPER.writeValueAsString(scalingHistory));

        var info = new AutoScalerInfo(data);
        var conf = new Configuration();
        var now = Instant.now();

        assertEquals(scalingHistory, info.getScalingHistory(now, conf));
        assertEquals(metricHistory, info.getMetricHistory());

        // Override with compressed data
        var newTs = Instant.now();
        info.updateMetricHistory(metricHistory);
        info.addToScalingHistory(newTs, Map.of(), conf);

        // Make sure we can still access everything
        assertEquals(scalingHistory, info.getScalingHistory(newTs, conf));
        assertEquals(metricHistory, info.getMetricHistory());
    }

    @Test
    public void testMetricsTrimming() throws Exception {
        var v1 = new JobVertexID();
        Random rnd = new Random();

        var metricHistory = new TreeMap<Instant, CollectedMetrics>();
        for (int i = 0; i < 50; i++) {
            var m = new HashMap<JobVertexID, Map<ScalingMetric, Double>>();
            for (int j = 0; j < 500; j++) {
                m.put(
                        new JobVertexID(),
                        Map.of(ScalingMetric.TRUE_PROCESSING_RATE, rnd.nextDouble()));
            }
            metricHistory.put(Instant.now(), new CollectedMetrics(m, Collections.emptyMap()));
        }

        var scalingHistory = new HashMap<JobVertexID, SortedMap<Instant, ScalingSummary>>();
        scalingHistory.put(v1, new TreeMap<>());
        scalingHistory
                .get(v1)
                .put(
                        Instant.now(),
                        new ScalingSummary(
                                1, 2, Map.of(ScalingMetric.LAG, EvaluatedScalingMetric.of(2.))));

        var data = new HashMap<String, String>();
        var info = new AutoScalerInfo(data);

        info.addToScalingHistory(
                Instant.now(),
                Map.of(
                        v1,
                        new ScalingSummary(
                                1, 2, Map.of(ScalingMetric.LAG, EvaluatedScalingMetric.of(2.)))),
                new Configuration());

        info.updateMetricHistory(metricHistory);

        assertFalse(
                data.get(AutoScalerInfo.COLLECTED_METRICS_KEY).length()
                                + data.get(AutoScalerInfo.SCALING_HISTORY_KEY).length()
                        < AutoScalerInfo.MAX_CM_BYTES);

        info.trimHistoryToMaxCmSize();
        assertTrue(
                data.get(AutoScalerInfo.COLLECTED_METRICS_KEY).length()
                                + data.get(AutoScalerInfo.SCALING_HISTORY_KEY).length()
                        < AutoScalerInfo.MAX_CM_BYTES);
    }

    @Test
    public void testDiscardInvalidHistory() {
        ConfigMap configMap = new ConfigMap();
        configMap.setData(
                new HashMap<>(
                        Map.of(
                                AutoScalerInfo.COLLECTED_METRICS_KEY,
                                "invalid",
                                AutoScalerInfo.SCALING_HISTORY_KEY,
                                "invalid2")));

        var info = new AutoScalerInfo(configMap);
        var now = Instant.now();
        var conf = new Configuration();

        assertEquals(2, configMap.getData().size());

        assertEquals(new TreeMap<>(), info.getMetricHistory());
        assertNull(configMap.getData().get(AutoScalerInfo.COLLECTED_METRICS_KEY));

        assertEquals(new TreeMap<>(), info.getScalingHistory(now, conf));
        assertNull(configMap.getData().get(AutoScalerInfo.SCALING_HISTORY_KEY));
    }
}
