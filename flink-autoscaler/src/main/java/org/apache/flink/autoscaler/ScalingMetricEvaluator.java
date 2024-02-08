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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.metrics.CollectedMetricHistory;
import org.apache.flink.autoscaler.metrics.CollectedMetrics;
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.MetricAggregator;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.autoscaler.utils.AutoScalerUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;

import static org.apache.flink.autoscaler.config.AutoScalerOptions.BACKLOG_PROCESSING_LAG_THRESHOLD;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.TARGET_UTILIZATION;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.TARGET_UTILIZATION_BOUNDARY;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.CATCH_UP_DATA_RATE;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.CURRENT_PROCESSING_RATE;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.GC_PRESSURE;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.HEAP_MAX_USAGE_RATIO;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.HEAP_USED;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.LAG;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.LOAD;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.MAX_PARALLELISM;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.NUM_TASK_SLOTS_USED;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.OBSERVED_TPR;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.PARALLELISM;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.SCALE_DOWN_RATE_THRESHOLD;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.SCALE_UP_RATE_THRESHOLD;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.TARGET_DATA_RATE;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.TRUE_PROCESSING_RATE;

/** Job scaling evaluator for autoscaler. */
public class ScalingMetricEvaluator {

    private static final Logger LOG = LoggerFactory.getLogger(ScalingMetricEvaluator.class);

    public EvaluatedMetrics evaluate(
            Configuration conf, CollectedMetricHistory collectedMetrics, Duration restartTime) {
        LOG.debug("Restart time used in metrics evaluation: {}", restartTime);
        var scalingOutput = new HashMap<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>>();
        var metricsHistory = collectedMetrics.getMetricHistory();
        var topology = collectedMetrics.getJobTopology();

        boolean processingBacklog = isProcessingBacklog(topology, metricsHistory, conf);

        for (var vertex : topology.getVerticesInTopologicalOrder()) {
            scalingOutput.put(
                    vertex,
                    evaluateMetrics(
                            conf,
                            scalingOutput,
                            metricsHistory,
                            topology,
                            vertex,
                            processingBacklog,
                            restartTime));
        }

        var globalMetrics = evaluateGlobalMetrics(metricsHistory);
        return new EvaluatedMetrics(scalingOutput, globalMetrics);
    }

    @VisibleForTesting
    protected static boolean isProcessingBacklog(
            JobTopology topology,
            SortedMap<Instant, CollectedMetrics> metricsHistory,
            Configuration conf) {
        var lastMetrics = metricsHistory.get(metricsHistory.lastKey()).getVertexMetrics();
        return topology.getVerticesInTopologicalOrder().stream()
                .filter(topology::isSource)
                .anyMatch(
                        vertex -> {
                            double lag = lastMetrics.get(vertex).getOrDefault(LAG, 0.0);
                            double avgProcRate =
                                    getAverage(CURRENT_PROCESSING_RATE, vertex, metricsHistory);
                            if (Double.isNaN(avgProcRate)) {
                                return false;
                            }
                            double lagSeconds = lag / avgProcRate;
                            if (lagSeconds
                                    > conf.get(BACKLOG_PROCESSING_LAG_THRESHOLD).toSeconds()) {
                                LOG.info("Currently processing backlog at source {}", vertex);
                                return true;
                            } else {
                                return false;
                            }
                        });
    }

    @Nonnull
    private Map<ScalingMetric, EvaluatedScalingMetric> evaluateMetrics(
            Configuration conf,
            HashMap<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> scalingOutput,
            SortedMap<Instant, CollectedMetrics> metricsHistory,
            JobTopology topology,
            JobVertexID vertex,
            boolean processingBacklog,
            Duration restartTime) {

        var latestVertexMetrics =
                metricsHistory.get(metricsHistory.lastKey()).getVertexMetrics().get(vertex);

        var vertexInfo = topology.get(vertex);

        double busyTimeAvg =
                computeBusyTimeAvg(conf, metricsHistory, vertex, vertexInfo.getParallelism());
        double inputRateAvg = getRate(ScalingMetric.NUM_RECORDS_IN, vertex, metricsHistory);

        var evaluatedMetrics = new HashMap<ScalingMetric, EvaluatedScalingMetric>();
        computeTargetDataRate(
                topology,
                vertex,
                conf,
                inputRateAvg,
                scalingOutput,
                metricsHistory,
                latestVertexMetrics,
                evaluatedMetrics);

        evaluatedMetrics.put(
                TRUE_PROCESSING_RATE,
                EvaluatedScalingMetric.avg(
                        computeTrueProcessingRate(
                                busyTimeAvg, inputRateAvg, metricsHistory, vertex, conf)));

        evaluatedMetrics.put(LOAD, EvaluatedScalingMetric.avg(busyTimeAvg / 1000.));

        Optional.ofNullable(latestVertexMetrics.get(LAG))
                .ifPresent(l -> evaluatedMetrics.put(LAG, EvaluatedScalingMetric.of(l)));

        evaluatedMetrics.put(PARALLELISM, EvaluatedScalingMetric.of(vertexInfo.getParallelism()));

        evaluatedMetrics.put(
                MAX_PARALLELISM, EvaluatedScalingMetric.of(vertexInfo.getMaxParallelism()));
        computeProcessingRateThresholds(evaluatedMetrics, conf, processingBacklog, restartTime);
        return evaluatedMetrics;
    }

    /**
     * Compute the average busy time for the given vertex for the current metric window. Depending
     * on the {@link MetricAggregator} chosen we use two different mechanisms:
     *
     * <ol>
     *   <li>For AVG aggregator we compute from accumulated busy time to get the most precise metric
     *   <li>or MAX/MIN aggregators we have to average over the point-in-time MAX/MIN values
     *       collected over the metric windows. These are stored in the LOAD metric.
     * </ol>
     *
     * @param conf
     * @param metricsHistory
     * @param vertex
     * @param parallelism
     * @return Average busy time in the current metric window
     */
    @VisibleForTesting
    protected static double computeBusyTimeAvg(
            Configuration conf,
            SortedMap<Instant, CollectedMetrics> metricsHistory,
            JobVertexID vertex,
            int parallelism) {
        double busyTimeAvg;
        if (conf.get(AutoScalerOptions.BUSY_TIME_AGGREGATOR) == MetricAggregator.AVG) {
            busyTimeAvg =
                    getRate(ScalingMetric.ACCUMULATED_BUSY_TIME, vertex, metricsHistory)
                            / parallelism;
        } else {
            busyTimeAvg = getAverage(LOAD, vertex, metricsHistory) * 1000;
            if (Double.isNaN(busyTimeAvg)) {
                busyTimeAvg = Double.POSITIVE_INFINITY;
            }
        }
        return busyTimeAvg;
    }

    /**
     * Compute the true processing rate for the given vertex for the current metric window. The
     * computation takes into account both observed (during catchup) and busy time based processing
     * rate and selects the right metric depending on the config.
     *
     * @param busyTimeAvg
     * @param inputRateAvg
     * @param metricsHistory
     * @param vertex
     * @param conf
     * @return Average true processing rate over metric window.
     */
    protected static double computeTrueProcessingRate(
            double busyTimeAvg,
            double inputRateAvg,
            SortedMap<Instant, CollectedMetrics> metricsHistory,
            JobVertexID vertex,
            Configuration conf) {

        var busyTimeTpr = computeTprFromBusyTime(busyTimeAvg, inputRateAvg, conf);
        var observedTprAvg =
                getAverage(
                        OBSERVED_TPR,
                        vertex,
                        metricsHistory,
                        conf.get(AutoScalerOptions.OBSERVED_TRUE_PROCESSING_RATE_MIN_OBSERVATIONS));

        var tprMetric = selectTprMetric(vertex, conf, busyTimeTpr, observedTprAvg);
        return tprMetric == OBSERVED_TPR ? observedTprAvg : busyTimeTpr;
    }

    private static double computeTprFromBusyTime(
            double busyTimeMsPerSecond, double rate, Configuration conf) {
        if (rate == 0) {
            // Nothing is coming in, we assume infinite processing power
            // until we can sample the true processing rate (i.e. data flows).
            return Double.POSITIVE_INFINITY;
        }
        // Pretend that the load is balanced because we don't know any better
        if (Double.isNaN(busyTimeMsPerSecond)) {
            busyTimeMsPerSecond = conf.get(AutoScalerOptions.TARGET_UTILIZATION) * 1000;
        }
        return rate / (busyTimeMsPerSecond / 1000);
    }

    private static ScalingMetric selectTprMetric(
            JobVertexID jobVertexID,
            Configuration conf,
            double busyTimeTprAvg,
            double observedTprAvg) {

        if (Double.isNaN(observedTprAvg)) {
            return TRUE_PROCESSING_RATE;
        }

        if (Double.isInfinite(busyTimeTprAvg) || Double.isNaN(busyTimeTprAvg)) {
            return OBSERVED_TPR;
        }

        double switchThreshold =
                conf.get(AutoScalerOptions.OBSERVED_TRUE_PROCESSING_RATE_SWITCH_THRESHOLD);
        // If we could measure the observed tpr we decide whether to switch to using it
        // instead of busy time based on the error / difference between the two
        if (busyTimeTprAvg > observedTprAvg * (1 + switchThreshold)) {
            LOG.debug(
                    "Using observed tpr {} for {} as busy time based seems too large ({})",
                    observedTprAvg,
                    jobVertexID,
                    busyTimeTprAvg);
            return OBSERVED_TPR;
        } else {
            LOG.debug("Using busy time based tpr {} for {}.", busyTimeTprAvg, jobVertexID);
            return TRUE_PROCESSING_RATE;
        }
    }

    @VisibleForTesting
    protected static void computeProcessingRateThresholds(
            Map<ScalingMetric, EvaluatedScalingMetric> metrics,
            Configuration conf,
            boolean processingBacklog,
            Duration restartTime) {

        double utilizationBoundary = conf.getDouble(TARGET_UTILIZATION_BOUNDARY);
        double targetUtilization = conf.get(TARGET_UTILIZATION);

        double upperUtilization;
        double lowerUtilization;

        if (processingBacklog) {
            // When we are processing backlog we allow max utilization and we do not trigger scale
            // down on under utilization to avoid creating more lag.
            upperUtilization = 1.0;
            lowerUtilization = 0.0;
        } else {
            upperUtilization = targetUtilization + utilizationBoundary;
            lowerUtilization = targetUtilization - utilizationBoundary;
        }

        double scaleUpThreshold =
                AutoScalerUtils.getTargetProcessingCapacity(
                        metrics, conf, upperUtilization, false, restartTime);

        double scaleDownThreshold =
                AutoScalerUtils.getTargetProcessingCapacity(
                        metrics, conf, lowerUtilization, true, restartTime);

        metrics.put(SCALE_UP_RATE_THRESHOLD, EvaluatedScalingMetric.of(scaleUpThreshold));
        metrics.put(SCALE_DOWN_RATE_THRESHOLD, EvaluatedScalingMetric.of(scaleDownThreshold));
    }

    private void computeTargetDataRate(
            JobTopology topology,
            JobVertexID vertex,
            Configuration conf,
            double inputRate,
            HashMap<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> alreadyEvaluated,
            SortedMap<Instant, CollectedMetrics> metricsHistory,
            Map<ScalingMetric, Double> latestVertexMetrics,
            Map<ScalingMetric, EvaluatedScalingMetric> out) {

        if (topology.isSource(vertex)) {
            double catchUpTargetSec = conf.get(AutoScalerOptions.CATCH_UP_DURATION).toSeconds();

            double lagRate = getRate(LAG, vertex, metricsHistory);
            double ingestionDataRate = Math.max(0, inputRate + lagRate);
            if (Double.isNaN(ingestionDataRate)) {
                throw new RuntimeException(
                        "Cannot evaluate metrics without ingestion rate information");
            }

            out.put(TARGET_DATA_RATE, EvaluatedScalingMetric.avg(ingestionDataRate));

            double lag = latestVertexMetrics.getOrDefault(LAG, 0.);
            double catchUpInputRate = catchUpTargetSec == 0 ? 0 : lag / catchUpTargetSec;
            if (catchUpInputRate > 0) {
                LOG.debug(
                        "Extra backlog processing input rate for {} is {}",
                        vertex,
                        catchUpInputRate);
            }
            out.put(CATCH_UP_DATA_RATE, EvaluatedScalingMetric.of(catchUpInputRate));
        } else {
            var inputs = topology.get(vertex).getInputs();
            double sumAvgTargetRate = 0;
            double sumCatchUpDataRate = 0;
            for (var inputVertex : inputs) {
                var inputEvaluatedMetrics = alreadyEvaluated.get(inputVertex);
                var inputTargetRate = inputEvaluatedMetrics.get(TARGET_DATA_RATE);
                var outputRateMultiplier =
                        computeOutputRatio(inputVertex, vertex, topology, metricsHistory);
                LOG.debug(
                        "Computed output ratio for edge ({} -> {}) : {}",
                        inputVertex,
                        vertex,
                        outputRateMultiplier);
                sumAvgTargetRate += inputTargetRate.getAverage() * outputRateMultiplier;
                sumCatchUpDataRate +=
                        inputEvaluatedMetrics.get(CATCH_UP_DATA_RATE).getCurrent()
                                * outputRateMultiplier;
            }
            out.put(TARGET_DATA_RATE, EvaluatedScalingMetric.avg(sumAvgTargetRate));
            out.put(CATCH_UP_DATA_RATE, EvaluatedScalingMetric.of(sumCatchUpDataRate));
        }
    }

    @VisibleForTesting
    protected static Map<ScalingMetric, EvaluatedScalingMetric> evaluateGlobalMetrics(
            SortedMap<Instant, CollectedMetrics> metricHistory) {
        var latest = metricHistory.get(metricHistory.lastKey()).getGlobalMetrics();
        var out = new HashMap<ScalingMetric, EvaluatedScalingMetric>();

        var gcPressure = latest.getOrDefault(GC_PRESSURE, Double.NaN);
        var lastHeapUsage = latest.getOrDefault(HEAP_MAX_USAGE_RATIO, Double.NaN);

        out.put(GC_PRESSURE, EvaluatedScalingMetric.of(gcPressure));
        out.put(
                HEAP_MAX_USAGE_RATIO,
                new EvaluatedScalingMetric(
                        lastHeapUsage,
                        getAverageGlobalMetric(HEAP_MAX_USAGE_RATIO, metricHistory)));

        var latestObservation = latest.getOrDefault(HEAP_USED, Double.NaN);
        double heapSizeAverage = getAverageGlobalMetric(HEAP_USED, metricHistory);
        out.put(HEAP_USED, new EvaluatedScalingMetric(latestObservation, heapSizeAverage));

        out.put(
                NUM_TASK_SLOTS_USED,
                EvaluatedScalingMetric.of(latest.getOrDefault(NUM_TASK_SLOTS_USED, Double.NaN)));

        return out;
    }

    private static double getAverageGlobalMetric(
            ScalingMetric metric, SortedMap<Instant, CollectedMetrics> metricsHistory) {
        return getAverage(metric, null, metricsHistory);
    }

    public static double getAverage(
            ScalingMetric metric,
            @Nullable JobVertexID jobVertexId,
            SortedMap<Instant, CollectedMetrics> metricsHistory) {
        return getAverage(metric, jobVertexId, metricsHistory, 1);
    }

    /**
     * Compute per second rate for the given accumulated metric over the metric window.
     *
     * @param metric
     * @param jobVertexId
     * @param metricsHistory
     * @return Per second rate or Double.NaN if we don't have at least 2 observations.
     */
    public static double getRate(
            ScalingMetric metric,
            @Nullable JobVertexID jobVertexId,
            SortedMap<Instant, CollectedMetrics> metricsHistory) {

        Instant firstTs = null;
        double first = Double.NaN;

        Instant lastTs = null;
        double last = Double.NaN;

        for (var entry : metricsHistory.entrySet()) {
            double value =
                    entry.getValue()
                            .getVertexMetrics()
                            .get(jobVertexId)
                            .getOrDefault(metric, Double.NaN);
            if (!Double.isNaN(value)) {
                if (Double.isNaN(first)) {
                    first = value;
                    firstTs = entry.getKey();
                } else {
                    last = value;
                    lastTs = entry.getKey();
                }
            }
        }
        if (Double.isNaN(last)) {
            return Double.NaN;
        }

        return 1000 * (last - first) / Duration.between(firstTs, lastTs).toMillis();
    }

    public static double getAverage(
            ScalingMetric metric,
            @Nullable JobVertexID jobVertexId,
            SortedMap<Instant, CollectedMetrics> metricsHistory,
            int minElements) {

        double sum = 0;
        int n = 0;
        boolean anyInfinite = false;
        for (var collectedMetrics : metricsHistory.values()) {
            var metrics =
                    jobVertexId != null
                            ? collectedMetrics.getVertexMetrics().get(jobVertexId)
                            : collectedMetrics.getGlobalMetrics();
            double num = metrics.getOrDefault(metric, Double.NaN);
            if (Double.isNaN(num)) {
                continue;
            }
            if (Double.isInfinite(num)) {
                anyInfinite = true;
                continue;
            }

            sum += num;
            n++;
        }
        if (n == 0) {
            return anyInfinite ? Double.POSITIVE_INFINITY : Double.NaN;
        }
        return n < minElements ? Double.NaN : sum / n;
    }

    @VisibleForTesting
    protected static double computeOutputRatio(
            JobVertexID from,
            JobVertexID to,
            JobTopology topology,
            SortedMap<Instant, CollectedMetrics> metricsHistory) {

        double numRecordsIn = getRate(ScalingMetric.NUM_RECORDS_IN, from, metricsHistory);

        double outputRatio = 0;
        // If the input rate is zero, we also need to flatten the output rate.
        // Otherwise, the OUTPUT_RATIO would be outrageously large, leading to
        // a rapid scale up.
        if (numRecordsIn > 0) {
            double numRecordsOut = computeEdgeRecordsOut(topology, metricsHistory, from, to);
            if (numRecordsOut > 0) {
                outputRatio = numRecordsOut / numRecordsIn;
            }
        }
        return outputRatio;
    }

    @VisibleForTesting
    protected static double computeEdgeRecordsOut(
            JobTopology topology,
            SortedMap<Instant, CollectedMetrics> metricsHistory,
            JobVertexID from,
            JobVertexID to) {

        var toVertexInputs = topology.get(to).getInputs();
        // Case 1: Downstream vertex has single input (from) so we can use the most reliable num
        // records in
        if (toVertexInputs.size() == 1) {
            LOG.debug(
                    "Computing edge ({}, {}) data rate for single input downstream task", from, to);
            return getRate(ScalingMetric.NUM_RECORDS_IN, to, metricsHistory);
        }

        // Case 2: Downstream vertex has only inputs from upstream vertices which don't have other
        // outputs
        double numRecordsOutFromUpstreamInputs = 0;
        for (JobVertexID input : toVertexInputs) {
            if (input.equals(from)) {
                // Exclude source edge because we only want to consider other input edges
                continue;
            }
            if (topology.get(input).getOutputs().size() == 1) {
                numRecordsOutFromUpstreamInputs +=
                        getRate(ScalingMetric.NUM_RECORDS_OUT, input, metricsHistory);
            } else {
                // Output vertex has multiple outputs, cannot use this information...
                numRecordsOutFromUpstreamInputs = Double.NaN;
                break;
            }
        }
        if (!Double.isNaN(numRecordsOutFromUpstreamInputs)) {
            LOG.debug(
                    "Computing edge ({}, {}) data rate by subtracting upstream input rates",
                    from,
                    to);
            return getRate(ScalingMetric.NUM_RECORDS_IN, to, metricsHistory)
                    - numRecordsOutFromUpstreamInputs;
        }

        // Case 3: We fall back simply to num records out, this is the least reliable
        LOG.debug(
                "Computing edge ({}, {}) data rate by falling back to from num records out",
                from,
                to);
        return getRate(ScalingMetric.NUM_RECORDS_OUT, from, metricsHistory);
    }
}
