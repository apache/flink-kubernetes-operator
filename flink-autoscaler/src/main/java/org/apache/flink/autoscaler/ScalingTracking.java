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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

/** Stores rescaling related information for the job. */
@Experimental
@Data
@NoArgsConstructor
@Builder
public class ScalingTracking {

    private static final Logger LOG = LoggerFactory.getLogger(ScalingTracking.class);

    /** Details related to recent rescaling operations. */
    private final TreeMap<Instant, ScalingRecord> scalingRecords = new TreeMap<>();

    public void addScalingRecord(Instant startTimestamp, ScalingRecord scalingRecord) {
        scalingRecords.put(startTimestamp, scalingRecord);
    }

    @JsonIgnore
    public Optional<Entry<Instant, ScalingRecord>> getLatestScalingRecordEntry() {
        if (!scalingRecords.isEmpty()) {
            return Optional.of(scalingRecords.lastEntry());
        } else {
            return Optional.empty();
        }
    }

    /**
     * Sets the end time for the latest scaling record if its parallelism matches the current job
     * parallelism.
     *
     * @param now The current instant to be set as the end time of the scaling record.
     * @param jobTopology The current job topology containing details of the job's parallelism.
     * @param scalingHistory The scaling history.
     * @return true if the end time is successfully set, false if the end time is already set, the
     *     latest scaling record cannot be found, or the target parallelism does not match the
     *     actual parallelism.
     */
    public boolean setEndTimeIfTrackedAndParallelismMatches(
            Instant now,
            JobTopology jobTopology,
            Map<JobVertexID, SortedMap<Instant, ScalingSummary>> scalingHistory) {
        return getLatestScalingRecordEntry()
                .map(
                        entry -> {
                            var value = entry.getValue();
                            var scalingTimestamp = entry.getKey();
                            if (value.getEndTime() == null) {
                                var targetParallelism =
                                        getTargetParallelismOfScaledVertices(
                                                scalingTimestamp, scalingHistory);
                                var actualParallelism = jobTopology.getParallelisms();

                                if (targetParallelismMatchesActual(
                                        targetParallelism, actualParallelism)) {
                                    value.setEndTime(now);
                                    LOG.debug(
                                            "Recorded restart duration of {} seconds (from {} till {})",
                                            Duration.between(scalingTimestamp, now).getSeconds(),
                                            scalingTimestamp,
                                            now);
                                    return true;
                                }
                            } else {
                                LOG.debug(
                                        "Cannot record end time because already set in the latest record: {}",
                                        value.getEndTime());
                            }
                            return false;
                        })
                .orElse(false);
    }

    private static Map<JobVertexID, Integer> getTargetParallelismOfScaledVertices(
            Instant scalingTimestamp,
            Map<JobVertexID, SortedMap<Instant, ScalingSummary>> scalingHistory) {
        return scalingHistory.entrySet().stream()
                .filter(entry -> entry.getValue().containsKey(scalingTimestamp))
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                entry ->
                                        entry.getValue()
                                                .get(scalingTimestamp)
                                                .getNewParallelism()));
    }

    private static boolean targetParallelismMatchesActual(
            Map<JobVertexID, Integer> targetParallelisms,
            Map<JobVertexID, Integer> actualParallelisms) {
        return targetParallelisms.entrySet().stream()
                .allMatch(
                        entry -> {
                            var vertexID = entry.getKey();
                            var targetParallelism = entry.getValue();
                            var actualParallelism = actualParallelisms.getOrDefault(vertexID, -1);
                            boolean isEqual = actualParallelism.equals(targetParallelism);
                            if (!isEqual) {
                                LOG.debug(
                                        "Vertex {} actual parallelism {} does not match target parallelism {}",
                                        vertexID,
                                        actualParallelism,
                                        targetParallelism);
                            }
                            return isEqual;
                        });
    }

    /**
     * Retrieves the maximum restart time based on the provided configuration and scaling records.
     * Defaults to the RESTART_TIME from configuration if the PREFER_TRACKED_RESTART_TIME option is
     * set to false, or if there are no tracking records available. Otherwise, the maximum observed
     * restart time is capped by the MAX_RESTART_TIME.
     */
    public Duration getMaxRestartTimeOrDefault(Configuration conf) {
        long maxRestartTime = -1;
        if (conf.get(AutoScalerOptions.PREFER_TRACKED_RESTART_TIME)) {
            for (Map.Entry<Instant, ScalingRecord> entry : scalingRecords.entrySet()) {
                var startTime = entry.getKey();
                var endTime = entry.getValue().getEndTime();
                if (endTime != null) {
                    var restartTime = Duration.between(startTime, endTime).toSeconds();
                    maxRestartTime = Math.max(restartTime, maxRestartTime);
                }
            }
            LOG.debug("Maximum tracked restart time: {}", maxRestartTime);
        }
        var restartTimeFromConfig = conf.get(AutoScalerOptions.RESTART_TIME);
        long maxRestartTimeFromConfig =
                conf.get(AutoScalerOptions.TRACKED_RESTART_TIME_LIMIT).toSeconds();
        return maxRestartTime == -1
                ? restartTimeFromConfig
                : Duration.ofSeconds(Math.min(maxRestartTime, maxRestartTimeFromConfig));
    }

    /**
     * Removes all but one records from the internal map that are older than the specified time span
     * and trims the number of records to the specified maximum count. Always keeps at least one
     * latest entry.
     *
     * @param keptTimeSpan Duration for how long recent records are to be kept.
     * @param keptNumRecords The maximum number of recent records to keep.
     */
    public void removeOldRecords(Instant now, Duration keptTimeSpan, int keptNumRecords) {
        var latestRecord = getLatestScalingRecordEntry();
        var cutoffTime = now.minus(keptTimeSpan);

        // Remove records older than the cutoff time
        scalingRecords.headMap(cutoffTime).clear();

        // If the map size is still larger than keptNumRecords, trim further
        while (scalingRecords.size() > keptNumRecords) {
            scalingRecords.pollFirstEntry();
        }
        latestRecord.ifPresent(record -> scalingRecords.put(record.getKey(), record.getValue()));
    }
}
