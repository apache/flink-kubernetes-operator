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

package org.apache.flink.kubernetes.operator.autoscaler.metrics;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions;
import org.apache.flink.kubernetes.operator.autoscaler.topology.JobTopology;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetric;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;

import static org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions.SOURCE_SCALING_ENABLED;

/** Utilities for computing scaling metrics based on Flink metrics. */
public class ScalingMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(ScalingMetrics.class);

    public static void computeLoadMetrics(
            Map<FlinkMetric, AggregatedMetric> flinkMetrics,
            Map<ScalingMetric, Double> scalingMetrics) {

        var busyTime = flinkMetrics.get(FlinkMetric.BUSY_TIME_PER_SEC);
        if (busyTime == null) {
            return;
        }

        if (!busyTime.getAvg().isNaN()) {
            scalingMetrics.put(ScalingMetric.LOAD_AVG, busyTime.getAvg() / 1000);
        }

        if (!busyTime.getMax().isNaN()) {
            scalingMetrics.put(ScalingMetric.LOAD_MAX, busyTime.getMax() / 1000);
        }
    }

    public static void computeDataRateMetrics(
            JobVertexID jobVertexID,
            Map<FlinkMetric, AggregatedMetric> flinkMetrics,
            Map<ScalingMetric, Double> scalingMetrics,
            JobTopology topology,
            Optional<Double> lagGrowthOpt,
            Configuration conf) {

        var source = topology.getInputs().get(jobVertexID).isEmpty();
        var sink = topology.getOutputs().get(jobVertexID).isEmpty();

        var busyTimeAggregator = conf.get(AutoScalerOptions.BUSY_TIME_AGGREGATOR);
        var busyTimeOpt = busyTimeAggregator.get(flinkMetrics.get(FlinkMetric.BUSY_TIME_PER_SEC));

        if (busyTimeOpt.isEmpty()) {
            LOG.error("Cannot compute true processing/output rate without busyTimeMsPerSecond");
            return;
        }

        var numRecordsInPerSecond = flinkMetrics.get(FlinkMetric.NUM_RECORDS_IN_PER_SEC);
        if (numRecordsInPerSecond == null) {
            numRecordsInPerSecond =
                    flinkMetrics.get(FlinkMetric.SOURCE_TASK_NUM_RECORDS_IN_PER_SEC);
        }

        var outputPerSecond = flinkMetrics.get(FlinkMetric.NUM_RECORDS_OUT_PER_SEC);

        double busyTimeMultiplier = 1000 / busyTimeOpt.get();

        if (source && !conf.getBoolean(SOURCE_SCALING_ENABLED)) {
            double sourceInputRate =
                    numRecordsInPerSecond != null ? numRecordsInPerSecond.getSum() : Double.NaN;

            double targetDataRate;
            if (!Double.isNaN(sourceInputRate) && sourceInputRate > 0) {
                targetDataRate = sourceInputRate;
            } else {
                // If source in metric is not available (maybe legacy source) we use source
                // output that should always be available
                targetDataRate =
                        flinkMetrics.get(FlinkMetric.SOURCE_TASK_NUM_RECORDS_OUT_PER_SEC).getSum();
            }
            scalingMetrics.put(ScalingMetric.TRUE_PROCESSING_RATE, Double.NaN);
            scalingMetrics.put(
                    ScalingMetric.OUTPUT_RATIO, outputPerSecond.getSum() / targetDataRate);
            var trueOutputRate = busyTimeMultiplier * outputPerSecond.getSum();
            scalingMetrics.put(ScalingMetric.TRUE_OUTPUT_RATE, trueOutputRate);
            scalingMetrics.put(ScalingMetric.TARGET_DATA_RATE, trueOutputRate);
            LOG.info(
                    "Scaling disabled for source {} using output rate {} as target",
                    jobVertexID,
                    trueOutputRate);
        } else {
            if (source) {
                if (!lagGrowthOpt.isPresent() || numRecordsInPerSecond.getSum().isNaN()) {
                    LOG.error(
                            "Cannot compute source target data rate without numRecordsInPerSecond and pendingRecords (lag) metric for {}.",
                            jobVertexID);
                    scalingMetrics.put(ScalingMetric.TARGET_DATA_RATE, Double.NaN);
                } else {
                    double sourceDataRate =
                            Math.max(0, numRecordsInPerSecond.getSum() + lagGrowthOpt.get());
                    LOG.info(
                            "Using computed source data rate {} for {}",
                            sourceDataRate,
                            jobVertexID);
                    scalingMetrics.put(ScalingMetric.SOURCE_DATA_RATE, sourceDataRate);
                }
            }

            if (!numRecordsInPerSecond.getSum().isNaN()) {
                double trueProcessingRate = busyTimeMultiplier * numRecordsInPerSecond.getSum();
                if (trueProcessingRate <= 0 || !Double.isFinite(trueProcessingRate)) {
                    trueProcessingRate = Double.NaN;
                }
                scalingMetrics.put(ScalingMetric.TRUE_PROCESSING_RATE, trueProcessingRate);
            } else {
                LOG.error("Cannot compute true processing rate without numRecordsInPerSecond");
            }

            if (!sink) {
                if (!outputPerSecond.getSum().isNaN()) {
                    scalingMetrics.put(
                            ScalingMetric.OUTPUT_RATIO,
                            outputPerSecond.getSum() / numRecordsInPerSecond.getSum());
                    scalingMetrics.put(
                            ScalingMetric.TRUE_OUTPUT_RATE,
                            busyTimeMultiplier * outputPerSecond.getSum());
                } else {
                    LOG.error(
                            "Cannot compute processing and input rate without numRecordsOutPerSecond");
                }
            }
        }
    }

    public static void computeLagMetrics(
            Map<FlinkMetric, AggregatedMetric> flinkMetrics,
            Map<ScalingMetric, Double> scalingMetrics) {
        var pendingRecords = flinkMetrics.get(FlinkMetric.PENDING_RECORDS);
        if (pendingRecords != null) {
            scalingMetrics.put(ScalingMetric.LAG, pendingRecords.getSum());
        }
    }
}
