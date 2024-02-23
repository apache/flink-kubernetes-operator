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

package org.apache.flink.autoscaler;

import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.autoscaler.topology.VertexInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

class ScalingTrackingTest {

    Duration configuredRestartTime = Duration.ofMinutes(5);
    Duration configuredMaxRestartTime = Duration.ofMinutes(15);
    private ScalingTracking scalingTracking;
    private Configuration conf;

    @BeforeEach
    void setUp() {
        scalingTracking = new ScalingTracking();
        conf = new Configuration();
        conf.set(AutoScalerOptions.RESTART_TIME, configuredRestartTime);
        conf.set(AutoScalerOptions.PREFER_TRACKED_RESTART_TIME, true);
    }

    @Test
    void shouldReturnConfiguredRestartTime_WhenNoScalingRecords() {
        // Empty scalingTracking
        var result = scalingTracking.getMaxRestartTimeOrDefault(conf);

        assertThat(result).isEqualTo(configuredRestartTime);
    }

    @Test
    void shouldReturnConfiguredRestartTime_WhenPreferTrackedRestartTimeIsFalse() {
        conf.set(AutoScalerOptions.PREFER_TRACKED_RESTART_TIME, false);
        setUpScalingRecords(configuredRestartTime.minusSeconds(1));

        var result = scalingTracking.getMaxRestartTimeOrDefault(conf);

        assertThat(result).isEqualTo(configuredRestartTime);
    }

    @Test
    void maxRestartTimeShouldNotCapConfiguredRestartTime_WhenPreferTrackedRestartTimeIsFalse() {
        conf.set(AutoScalerOptions.PREFER_TRACKED_RESTART_TIME, false);
        var restartTime =
                configuredMaxRestartTime.plusSeconds(
                        1); // exceeds max configured, but should not be capped
        conf.set(AutoScalerOptions.RESTART_TIME, restartTime);
        setUpScalingRecords(
                configuredRestartTime.minusSeconds(1)); // should not be taken into account

        var result = scalingTracking.getMaxRestartTimeOrDefault(conf);

        assertThat(result).isEqualTo(restartTime);
    }

    @Test
    void shouldReturnMaxTrackedRestartTime_WhenNotCapped() {
        var duration = configuredMaxRestartTime.minusSeconds(1);
        setUpScalingRecords(duration);

        var result = scalingTracking.getMaxRestartTimeOrDefault(conf);

        assertThat(result).isEqualTo(duration);
    }

    @Test
    void shouldReturnConfiguredRestartTime_WhenCapped() {
        var duration = configuredMaxRestartTime.plusSeconds(1);
        setUpScalingRecords(duration);

        var result = scalingTracking.getMaxRestartTimeOrDefault(conf);

        assertThat(result).isEqualTo(configuredMaxRestartTime);
    }

    @Test
    void shouldSetRestartDuration_WhenParallelismMatches() {
        var now = Instant.now();
        var restartDuration = Duration.ofSeconds(60);
        var lastScaling = now.minusSeconds(restartDuration.getSeconds());
        addScalingRecordWithoutRestartDuration(lastScaling);
        var actualParallelisms = initActualParallelisms();
        var jobTopology = new JobTopology(createVertexInfoSet(actualParallelisms));
        var scalingHistory =
                initScalingHistoryWithTargetParallelism(lastScaling, actualParallelisms);

        boolean result =
                scalingTracking.recordRestartDurationIfTrackedAndParallelismMatches(
                        now, jobTopology, scalingHistory);

        assertThat(result).isTrue();
        assertThat(
                        scalingTracking
                                .getLatestScalingRecordEntry()
                                .get()
                                .getValue()
                                .getRestartDuration())
                .isEqualTo(restartDuration);
    }

    @Test
    void shouldNotSetRestartDuration_WhenParallelismDoesNotMatch() {
        var now = Instant.now();
        var lastScaling = now.minusSeconds(60);
        addScalingRecordWithoutRestartDuration(lastScaling);
        var actualParallelisms = initActualParallelisms();
        var jobTopology = new JobTopology(createVertexInfoSet(actualParallelisms));
        var mismatchedParallelisms = new HashMap<>(actualParallelisms);
        mismatchedParallelisms.replaceAll((key, value) -> value + 1);
        var scalingHistory =
                initScalingHistoryWithTargetParallelism(lastScaling, mismatchedParallelisms);

        boolean result =
                scalingTracking.recordRestartDurationIfTrackedAndParallelismMatches(
                        now, jobTopology, scalingHistory);

        assertThat(result).isFalse();
        assertThat(
                        scalingTracking
                                .getLatestScalingRecordEntry()
                                .get()
                                .getValue()
                                .getRestartDuration())
                .isNull();
    }

    @Test
    public void removeOldRecordsShouldNotFail_WhenNoRecords() {
        scalingTracking.removeOldRecords(Instant.now(), Duration.ofHours(1), 10);
        assertThat(scalingTracking.getScalingRecords()).isEmpty();
    }

    @Test
    public void shouldRemoveOldRecords_ByTimeSpan() {
        Instant now = Instant.now();
        scalingTracking.addScalingRecord(now.minus(Duration.ofHours(3)), new ScalingRecord());
        scalingTracking.addScalingRecord(now.minus(Duration.ofHours(2)), new ScalingRecord());
        scalingTracking.addScalingRecord(now.minus(Duration.ofHours(1)), new ScalingRecord());
        scalingTracking.removeOldRecords(now, Duration.ofHours(1), 10);
        assertThat(scalingTracking.getScalingRecords()).hasSize(1);
    }

    @Test
    public void shouldRemoveRecords_WhenExceedingMaxCount() {
        Instant now = Instant.now();
        for (int i = 0; i < 10; i++) {
            scalingTracking.addScalingRecord(now.minus(Duration.ofMinutes(i)), new ScalingRecord());
        }
        scalingTracking.removeOldRecords(now, Duration.ofHours(1), 5);
        assertThat(scalingTracking.getScalingRecords()).hasSize(5);
    }

    @Test
    public void shouldAlwaysKeepAtLeastOneLatestRecord_WhenOutOfTimeSpan() {
        Instant now = Instant.now();
        scalingTracking.addScalingRecord(
                now.minus(Duration.ofDays(1)),
                new ScalingRecord()); // This is the only record and is old
        scalingTracking.removeOldRecords(now, Duration.ofHours(1), 10);
        assertThat(scalingTracking.getScalingRecords()).hasSize(1);
    }

    private void setUpScalingRecords(Duration secondRescaleDuration) {
        scalingTracking.addScalingRecord(
                Instant.parse("2023-11-15T16:00:00.00Z"), new ScalingRecord(Duration.ofMinutes(3)));
        var secondRecordStart = Instant.parse("2023-11-15T16:20:00.00Z");
        scalingTracking.addScalingRecord(
                secondRecordStart, new ScalingRecord(secondRescaleDuration));
    }

    private void addScalingRecordWithoutRestartDuration(Instant startTime) {
        ScalingRecord record = new ScalingRecord();
        scalingTracking.addScalingRecord(startTime, record);
    }

    private Set<VertexInfo> createVertexInfoSet(Map<JobVertexID, Integer> parallelisms) {
        Set<VertexInfo> vertexInfos = new HashSet<>();
        for (Map.Entry<JobVertexID, Integer> entry : parallelisms.entrySet()) {
            vertexInfos.add(
                    new VertexInfo(entry.getKey(), Map.of(), entry.getValue(), entry.getValue()));
        }
        return vertexInfos;
    }

    private Map<JobVertexID, Integer> initActualParallelisms() {
        var parallelisms = new HashMap<JobVertexID, Integer>();
        parallelisms.put(new JobVertexID(), 2);
        parallelisms.put(new JobVertexID(), 3);
        return parallelisms;
    }

    private Map<JobVertexID, SortedMap<Instant, ScalingSummary>>
            initScalingHistoryWithTargetParallelism(
                    Instant scalingTimestamp, Map<JobVertexID, Integer> targetParallelisms) {
        var history = new HashMap<JobVertexID, SortedMap<Instant, ScalingSummary>>();
        targetParallelisms.forEach(
                (id, parallelism) -> {
                    var vertexHistory = new TreeMap<Instant, ScalingSummary>();
                    vertexHistory.put(
                            scalingTimestamp,
                            new ScalingSummary(parallelism - 1, parallelism, null));
                    history.put(id, vertexHistory);
                });
        return history;
    }
}
