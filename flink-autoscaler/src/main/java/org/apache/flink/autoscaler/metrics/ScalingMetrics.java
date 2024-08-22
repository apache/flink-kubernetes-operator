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

package org.apache.flink.autoscaler.metrics;

import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.topology.IOMetrics;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.autoscaler.utils.AutoScalerUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetric;
import org.apache.flink.runtime.rest.messages.job.metrics.Metric;

import org.apache.commons.math3.util.Precision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/** Utilities for computing scaling metrics based on Flink metrics. */
public class ScalingMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(ScalingMetrics.class);

    public static void computeLoadMetrics(
            JobVertexID jobVertexID,
            Map<FlinkMetric, AggregatedMetric> flinkMetrics,
            Map<ScalingMetric, Double> scalingMetrics,
            IOMetrics ioMetrics,
            Configuration conf) {

        scalingMetrics.put(
                ScalingMetric.LOAD,
                getBusyTimeMsPerSecond(flinkMetrics, conf, jobVertexID) / 1000.);
        scalingMetrics.put(ScalingMetric.ACCUMULATED_BUSY_TIME, ioMetrics.getAccumulatedBusyTime());
    }

    private static double getBusyTimeMsPerSecond(
            Map<FlinkMetric, AggregatedMetric> flinkMetrics,
            Configuration conf,
            JobVertexID jobVertexId) {
        var busyTimeAggregator = conf.get(AutoScalerOptions.BUSY_TIME_AGGREGATOR);
        var busyTimeMsPerSecond =
                busyTimeAggregator.get(flinkMetrics.get(FlinkMetric.BUSY_TIME_PER_SEC));
        if (!Double.isFinite(busyTimeMsPerSecond)) {
            if (AutoScalerUtils.excludeVertexFromScaling(conf, jobVertexId)) {
                // We only want to log this once
                LOG.warn(
                        "No busyTimeMsPerSecond metric available for {}. No scaling will be performed for this vertex.",
                        jobVertexId);
            }
            return Double.NaN;
        }
        return Math.max(0, busyTimeMsPerSecond);
    }

    public static void computeDataRateMetrics(
            JobVertexID jobVertexID,
            Map<FlinkMetric, AggregatedMetric> flinkMetrics,
            Map<ScalingMetric, Double> scalingMetrics,
            JobTopology topology,
            Configuration conf,
            Supplier<Double> observedTprAvg) {

        var isSource = topology.isSource(jobVertexID);
        var ioMetrics = topology.get(jobVertexID).getIoMetrics();

        double numRecordsIn =
                getNumRecordsInAccumulated(flinkMetrics, ioMetrics, jobVertexID, isSource);

        scalingMetrics.put(ScalingMetric.NUM_RECORDS_IN, numRecordsIn);
        scalingMetrics.put(ScalingMetric.NUM_RECORDS_OUT, (double) ioMetrics.getNumRecordsOut());

        if (isSource) {
            double numRecordsInPerSecond =
                    getSourceNumRecordsInPerSecond(flinkMetrics, jobVertexID);
            if (!Double.isNaN(numRecordsInPerSecond)) {
                var observedTprOpt =
                        getObservedTpr(flinkMetrics, scalingMetrics, numRecordsInPerSecond, conf)
                                .orElseGet(observedTprAvg);
                scalingMetrics.put(ScalingMetric.OBSERVED_TPR, observedTprOpt);
            }
        }
    }

    private static Optional<Double> getObservedTpr(
            Map<FlinkMetric, AggregatedMetric> flinkMetrics,
            Map<ScalingMetric, Double> scalingMetrics,
            double numRecordsInPerSecond,
            Configuration conf) {

        // If there are no incoming records we return infinity to allow scale down
        if (numRecordsInPerSecond == 0) {
            return Optional.of(Double.POSITIVE_INFINITY);
        }

        // We only measure observed tpr when we are catching up, that is when the lag is beyond the
        // configured observe threshold
        boolean catchingUp =
                scalingMetrics.getOrDefault(ScalingMetric.LAG, 0.)
                        >= conf.get(AutoScalerOptions.OBSERVE_TRUE_PROCESSING_RATE_LAG_THRESHOLD)
                                        .toSeconds()
                                * numRecordsInPerSecond;
        if (!catchingUp) {
            return Optional.empty();
        }

        double observedTpr =
                computeObservedTprWithBackpressure(
                        numRecordsInPerSecond,
                        flinkMetrics.get(FlinkMetric.BACKPRESSURE_TIME_PER_SEC).getAvg());

        return Double.isNaN(observedTpr) ? Optional.empty() : Optional.of(observedTpr);
    }

    public static double computeObservedTprWithBackpressure(
            double numRecordsInPerSecond, double backpressureMsPerSeconds) {
        if (backpressureMsPerSeconds >= 1000) {
            return Double.NaN;
        }
        double nonBackpressuredRate = (1 - (backpressureMsPerSeconds / 1000));
        return numRecordsInPerSecond / nonBackpressuredRate;
    }

    public static Map<ScalingMetric, Double> computeGlobalMetrics(
            Map<FlinkMetric, Metric> collectedJmMetrics,
            Map<FlinkMetric, AggregatedMetric> collectedTmMetrics,
            Configuration conf) {
        if (collectedTmMetrics == null) {
            return null;
        }

        var out = new HashMap<ScalingMetric, Double>();

        try {
            var numTotalTaskSlots =
                    Double.valueOf(
                            collectedJmMetrics.get(FlinkMetric.NUM_TASK_SLOTS_TOTAL).getValue());
            var numTaskSlotsAvailable =
                    Double.valueOf(
                            collectedJmMetrics
                                    .get(FlinkMetric.NUM_TASK_SLOTS_AVAILABLE)
                                    .getValue());
            out.put(ScalingMetric.NUM_TASK_SLOTS_USED, numTotalTaskSlots - numTaskSlotsAvailable);
        } catch (Exception e) {
            LOG.debug("Slot metrics and registered task managers not available");
        }

        var gcTime = collectedTmMetrics.get(FlinkMetric.TOTAL_GC_TIME_PER_SEC);
        if (gcTime != null) {
            out.put(ScalingMetric.GC_PRESSURE, gcTime.getMax() / 1000);
        }

        var heapMax = collectedTmMetrics.get(FlinkMetric.HEAP_MEMORY_MAX);
        var heapUsed = collectedTmMetrics.get(FlinkMetric.HEAP_MEMORY_USED);

        if (heapMax != null && heapUsed != null) {
            out.put(ScalingMetric.HEAP_MEMORY_USED, heapUsed.getMax());
            out.put(ScalingMetric.HEAP_MAX_USAGE_RATIO, heapUsed.getMax() / heapMax.getMax());
        }

        var managedMemory = collectedTmMetrics.get(FlinkMetric.MANAGED_MEMORY_USED);
        if (managedMemory != null) {
            out.put(ScalingMetric.MANAGED_MEMORY_USED, managedMemory.getMax());
        }

        var metaspaceMemory = collectedTmMetrics.get(FlinkMetric.METASPACE_MEMORY_USED);
        if (metaspaceMemory != null) {
            out.put(ScalingMetric.METASPACE_MEMORY_USED, metaspaceMemory.getMax());
        }

        return out;
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

    private static double getSourceNumRecordsInPerSecond(
            Map<FlinkMetric, AggregatedMetric> flinkMetrics, JobVertexID jobVertexID) {
        return getNumRecordsInInternal(flinkMetrics, null, jobVertexID, true, true);
    }

    private static double getNumRecordsInAccumulated(
            Map<FlinkMetric, AggregatedMetric> flinkMetrics,
            IOMetrics ioMetrics,
            JobVertexID jobVertexID,
            boolean isSource) {
        return getNumRecordsInInternal(flinkMetrics, ioMetrics, jobVertexID, isSource, false);
    }

    private static double getNumRecordsInInternal(
            Map<FlinkMetric, AggregatedMetric> flinkMetrics,
            IOMetrics ioMetrics,
            JobVertexID jobVertexID,
            boolean isSource,
            boolean perSecond) {
        // Generate numRecordsInPerSecond from 3 metrics:
        // 1. If available, directly use the NUM_RECORDS_IN_PER_SEC task metric.
        var numRecords =
                perSecond
                        ? null // Per second only available for sources
                        : new AggregatedMetric(
                                "n",
                                Double.NaN,
                                Double.NaN,
                                Double.NaN,
                                (double) ioMetrics.getNumRecordsIn(),
                                Double.NaN);

        // 2. If the former is unavailable and the vertex contains a source operator, use the
        // corresponding source operator metric.
        if (isSource && (numRecords == null || numRecords.getSum() == 0)) {
            var sourceTaskIn =
                    flinkMetrics.get(
                            perSecond
                                    ? FlinkMetric.SOURCE_TASK_NUM_RECORDS_IN_PER_SEC
                                    : FlinkMetric.SOURCE_TASK_NUM_RECORDS_IN);
            numRecords = sourceTaskIn != null ? sourceTaskIn : numRecords;
        }
        // 3. If the vertex contains a source operator which does not emit input metrics, use output
        // metrics instead.
        if (isSource && (numRecords == null || numRecords.getSum() == 0)) {
            var sourceTaskOut =
                    flinkMetrics.get(
                            perSecond
                                    ? FlinkMetric.SOURCE_TASK_NUM_RECORDS_OUT_PER_SEC
                                    : FlinkMetric.SOURCE_TASK_NUM_RECORDS_OUT);
            numRecords = sourceTaskOut != null ? sourceTaskOut : numRecords;
        }

        if (numRecords == null) {
            LOG.debug("Received null input rate for {}. Returning NaN.", jobVertexID);
            return Double.NaN;
        }
        return Math.max(0, numRecords.getSum());
    }

    public static double roundMetric(double value) {
        double rounded = Precision.round(value, 3);
        // Never round down to 0, return original value instead
        return rounded == 0 ? value : rounded;
    }
}
