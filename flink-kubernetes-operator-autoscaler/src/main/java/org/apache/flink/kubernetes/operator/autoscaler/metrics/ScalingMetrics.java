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

import org.apache.commons.math3.util.Precision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
            double lagGrowthRate,
            Configuration conf) {

        var isSource = topology.getInputs().get(jobVertexID).isEmpty();
        var isSink = topology.getOutputs().get(jobVertexID).isEmpty();

        double busyTimeMsPerSecond = getBusyTimeMsPerSecond(flinkMetrics, conf, jobVertexID);
        double numRecordsInPerSecond =
                getNumRecordsInPerSecond(flinkMetrics, jobVertexID, isSource);

        if (isSource) {
            double sourceDataRate = Math.max(0, numRecordsInPerSecond + lagGrowthRate);
            LOG.info("Using computed source data rate {} for {}", sourceDataRate, jobVertexID);
            scalingMetrics.put(ScalingMetric.SOURCE_DATA_RATE, sourceDataRate);
            scalingMetrics.put(ScalingMetric.CURRENT_PROCESSING_RATE, numRecordsInPerSecond);
        }

        if (!Double.isNaN(numRecordsInPerSecond)) {
            double trueProcessingRate = computeTrueRate(numRecordsInPerSecond, busyTimeMsPerSecond);
            scalingMetrics.put(ScalingMetric.TRUE_PROCESSING_RATE, trueProcessingRate);
            scalingMetrics.put(ScalingMetric.CURRENT_PROCESSING_RATE, numRecordsInPerSecond);
        } else {
            LOG.error("Cannot compute true processing rate without numRecordsInPerSecond");
        }

        if (!isSink) {
            double numRecordsOutPerSecond =
                    getNumRecordsOutPerSecond(
                            flinkMetrics, jobVertexID, isSource, numRecordsInPerSecond);
            if (!Double.isNaN(numRecordsOutPerSecond)) {
                double outputRatio =
                        computeOutputRatio(numRecordsInPerSecond, numRecordsOutPerSecond);
                scalingMetrics.put(ScalingMetric.OUTPUT_RATIO, outputRatio);
                scalingMetrics.put(
                        ScalingMetric.TRUE_OUTPUT_RATE,
                        computeTrueRate(numRecordsOutPerSecond, busyTimeMsPerSecond));
            } else {
                LOG.error(
                        "Cannot compute processing and input rate without numRecordsOutPerSecond");
            }
        }
    }

    public static void computeLagMetrics(
            Map<FlinkMetric, AggregatedMetric> flinkMetrics,
            Map<ScalingMetric, Double> scalingMetrics) {
        var pendingRecords = flinkMetrics.get(FlinkMetric.PENDING_RECORDS);
        if (pendingRecords != null) {
            scalingMetrics.put(ScalingMetric.LAG, pendingRecords.getSum());
        } else {
            scalingMetrics.put(ScalingMetric.LAG, 0.);
        }
    }

    private static double getBusyTimeMsPerSecond(
            Map<FlinkMetric, AggregatedMetric> flinkMetrics,
            Configuration conf,
            JobVertexID jobVertexId) {
        var busyTimeAggregator = conf.get(AutoScalerOptions.BUSY_TIME_AGGREGATOR);
        var busyTimeMsPerSecond =
                busyTimeAggregator.get(flinkMetrics.get(FlinkMetric.BUSY_TIME_PER_SEC));
        if (!Double.isFinite(busyTimeMsPerSecond)) {
            LOG.error(
                    "No busyTimeMsPerSecond metric available for {}. No scaling will be performed for this vertex.",
                    jobVertexId);
            excludeVertexFromScaling(conf, jobVertexId);
            // Pretend that the load is balanced because we don't know any better
            busyTimeMsPerSecond = conf.get(AutoScalerOptions.TARGET_UTILIZATION) * 1000;
        } else {
            includeVertexForScaling(conf, jobVertexId);
        }
        return busyTimeMsPerSecond;
    }

    private static double getNumRecordsInPerSecond(
            Map<FlinkMetric, AggregatedMetric> flinkMetrics,
            JobVertexID jobVertexID,
            boolean isSource) {
        var numRecordsInPerSecond = flinkMetrics.get(FlinkMetric.NUM_RECORDS_IN_PER_SEC);
        if (isSource && (numRecordsInPerSecond == null || numRecordsInPerSecond.getSum() == 0)) {
            numRecordsInPerSecond =
                    flinkMetrics.get(FlinkMetric.SOURCE_TASK_NUM_RECORDS_IN_PER_SEC);
        }
        if (isSource && (numRecordsInPerSecond == null || numRecordsInPerSecond.getSum() == 0)) {
            numRecordsInPerSecond =
                    flinkMetrics.get(FlinkMetric.SOURCE_TASK_NUM_RECORDS_OUT_PER_SEC);
        }
        if (numRecordsInPerSecond == null) {
            LOG.warn("Received null input rate for {}. Returning NaN.", jobVertexID);
            return Double.NaN;
        }
        return numRecordsInPerSecond.getSum();
    }

    private static double getNumRecordsOutPerSecond(
            Map<FlinkMetric, AggregatedMetric> flinkMetrics,
            JobVertexID jobVertexID,
            boolean isSource,
            double numRecordsInPerSecond) {
        if (numRecordsInPerSecond <= 0) {
            // If the input rate is zero, we also need to flatten the output rate.
            // Otherwise, the OUTPUT_RATIO would be outrageously large, leading to
            // a rapid scale up.
            return 0;
        }
        AggregatedMetric numRecordsOutPerSecond =
                flinkMetrics.get(FlinkMetric.NUM_RECORDS_OUT_PER_SEC);
        if (numRecordsOutPerSecond == null) {
            if (isSource) {
                numRecordsOutPerSecond =
                        flinkMetrics.get(FlinkMetric.SOURCE_TASK_NUM_RECORDS_OUT_PER_SEC);
            }
            if (numRecordsOutPerSecond == null) {
                LOG.warn("Received null output rate for {}. Returning NaN.", jobVertexID);
                return Double.NaN;
            }
        }
        return numRecordsOutPerSecond.getSum();
    }

    private static double computeOutputRatio(
            double numRecordsInPerSecond, double numRecordsOutPerSecond) {
        if (numRecordsInPerSecond <= 0) {
            return 0;
        }
        return numRecordsOutPerSecond / numRecordsInPerSecond;
    }

    private static double computeTrueRate(double rate, double busyTimeMsPerSecond) {
        if (rate <= 0 || busyTimeMsPerSecond <= 0) {
            // Nothing is coming in, we assume infinite processing power
            // until we can sample the true processing rate (i.e. data flows).
            return Double.POSITIVE_INFINITY;
        }
        return rate / (busyTimeMsPerSecond / 1000);
    }

    private static void excludeVertexFromScaling(Configuration conf, JobVertexID jobVertexId) {
        Set<String> excludedIds = new HashSet<>(conf.get(AutoScalerOptions.VERTEX_EXCLUDE_IDS));
        excludedIds.add(jobVertexId.toHexString());
        conf.set(AutoScalerOptions.VERTEX_EXCLUDE_IDS, new ArrayList<>(excludedIds));
    }

    private static void includeVertexForScaling(Configuration conf, JobVertexID jobVertexId) {
        Set<String> excludedIds = new HashSet<>(conf.get(AutoScalerOptions.VERTEX_EXCLUDE_IDS));
        excludedIds.remove(jobVertexId.toHexString());
        conf.set(AutoScalerOptions.VERTEX_EXCLUDE_IDS, new ArrayList<>(excludedIds));
    }

    public static double roundMetric(double value) {
        double rounded = Precision.round(value, 3);
        // Never round down to 0, return original value instead
        return rounded == 0 ? value : rounded;
    }
}
