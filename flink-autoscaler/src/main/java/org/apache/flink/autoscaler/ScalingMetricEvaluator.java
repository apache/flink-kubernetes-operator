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
import static org.apache.flink.autoscaler.metrics.ScalingMetric.GC_PRESSURE;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.HEAP_MAX_USAGE_RATIO;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.HEAP_MEMORY_USED;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.LAG;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.LOAD;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.MANAGED_MEMORY_USED;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.MAX_PARALLELISM;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.METASPACE_MEMORY_USED;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.NUM_SOURCE_PARTITIONS;
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
                            double inputRateAvg =
                                    getRate(ScalingMetric.NUM_RECORDS_IN, vertex, metricsHistory);
                            if (Double.isNaN(inputRateAvg)) {
                                return false;
                            }
                            double lagSeconds = lag / inputRateAvg;
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

        double busyTimeAvg =
                computeBusyTimeAvg(conf, metricsHistory, vertex, vertexInfo.getParallelism());
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

        evaluatedMetrics.put(
                NUM_SOURCE_PARTITIONS,
                EvaluatedScalingMetric.of(vertexInfo.getNumSourcePartitions()));

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
        if (conf.get(AutoScalerOptions.BUSY_TIME_AGGREGATOR) == MetricAggregator.AVG) {
            return getRate(ScalingMetric.ACCUMULATED_BUSY_TIME, vertex, metricsHistory)
                    / parallelism;
        } else {
            return getAverage(LOAD, vertex, metricsHistory) * 1000;
        }
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

        var busyTimeTpr = computeTprFromBusyTime(busyTimeAvg, inputRateAvg);
        var observedTprAvg =
                getAverage(
                        OBSERVED_TPR,
                        vertex,
                        metricsHistory,
                        conf.get(AutoScalerOptions.OBSERVED_TRUE_PROCESSING_RATE_MIN_OBSERVATIONS));

        var tprMetric = selectTprMetric(vertex, conf, busyTimeTpr, observedTprAvg);
        return tprMetric == OBSERVED_TPR ? observedTprAvg : busyTimeTpr;
    }

    private static double computeTprFromBusyTime(double busyMsPerSecond, double rate) {
        if (rate == 0) {
            // Nothing is coming in, we assume infinite processing power
            // until we can sample the true processing rate (i.e. data flows).
            return Double.POSITIVE_INFINITY;
        }
        return rate / (busyMsPerSecond / 1000);
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
            var inputs = topology.get(vertex).getInputs().keySet();
            double sumAvgTargetRate = 0;
            double sumCatchUpDataRate = 0;
            for (var inputVertex : inputs) {
                var inputEvaluatedMetrics = alreadyEvaluated.get(inputVertex);
                var inputTargetRate = inputEvaluatedMetrics.get(TARGET_DATA_RATE);
                var outputRatio =
                        computeEdgeOutputRatio(inputVertex, vertex, topology, metricsHistory);
                LOG.debug(
                        "Computed output ratio for edge ({} -> {}) : {}",
                        inputVertex,
                        vertex,
                        outputRatio);
                sumAvgTargetRate += inputTargetRate.getAverage() * outputRatio;
                sumCatchUpDataRate +=
                        inputEvaluatedMetrics.get(CATCH_UP_DATA_RATE).getCurrent() * outputRatio;
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
        out.put(GC_PRESSURE, EvaluatedScalingMetric.of(gcPressure));

        populateMetric(HEAP_MAX_USAGE_RATIO, metricHistory, out);
        populateMetric(HEAP_MEMORY_USED, metricHistory, out);
        populateMetric(MANAGED_MEMORY_USED, metricHistory, out);
        populateMetric(METASPACE_MEMORY_USED, metricHistory, out);

        out.put(
                NUM_TASK_SLOTS_USED,
                EvaluatedScalingMetric.of(latest.getOrDefault(NUM_TASK_SLOTS_USED, Double.NaN)));

        return out;
    }

    private static void populateMetric(
            ScalingMetric scalingMetric,
            SortedMap<Instant, CollectedMetrics> metricHistory,
            Map<ScalingMetric, EvaluatedScalingMetric> out) {
        var latestMetrics = metricHistory.get(metricHistory.lastKey()).getGlobalMetrics();

        var latestObservation = latestMetrics.getOrDefault(scalingMetric, Double.NaN);
        double value = getAverageGlobalMetric(scalingMetric, metricHistory);

        out.put(scalingMetric, new EvaluatedScalingMetric(latestObservation, value));
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

    /**
     * Compute the In/Out ratio between the (from, to) vertices. The rate estimates the number of
     * output records produced to the downstream vertex for every input received for the upstream
     * vertex. For example output ratio 2.0 means that we produce approximately 2 outputs to the
     * "to" vertex for every 1 input received in the "from" vertex.
     *
     * @param from Upstream vertex
     * @param to Downstream vertex
     * @param topology
     * @param metricsHistory
     * @return Output ratio
     */
    @VisibleForTesting
    protected static double computeEdgeOutputRatio(
            JobVertexID from,
            JobVertexID to,
            JobTopology topology,
            SortedMap<Instant, CollectedMetrics> metricsHistory) {

        double inputRate = getRate(ScalingMetric.NUM_RECORDS_IN, from, metricsHistory);

        double outputRatio = 0;
        // If the input rate is zero, we also need to flatten the output rate.
        // Otherwise, the OUTPUT_RATIO would be outrageously large, leading to
        // a rapid scale up.
        if (inputRate > 0) {
            double outputRate = computeEdgeDataRate(topology, metricsHistory, from, to);
            if (outputRate > 0) {
                outputRatio = outputRate / inputRate;
            }
        }
        return outputRatio;
    }

    /**
     * Compute how many records flow between two job vertices in the pipeline. Since Flink does not
     * expose any output / data rate metric on an edge level we have to compute this from the vertex
     * level input/output metrics.
     *
     * @param topology
     * @param metricsHistory
     * @param from Upstream vertex
     * @param to Downstream vertex
     * @return Records per second data rate between the two vertices
     */
    @VisibleForTesting
    protected static double computeEdgeDataRate(
            JobTopology topology,
            SortedMap<Instant, CollectedMetrics> metricsHistory,
            JobVertexID from,
            JobVertexID to) {

        var toVertexInputs = topology.get(to).getInputs().keySet();
        // Case 1: Downstream vertex has single input (from) so we can use the most reliable num
        // records in
        if (toVertexInputs.size() == 1) {
            LOG.debug(
                    "Computing edge ({}, {}) data rate for single input downstream task", from, to);
            return getRate(ScalingMetric.NUM_RECORDS_IN, to, metricsHistory);
        }

        // Case 2: Downstream vertex has only inputs from upstream vertices which don't have other
        // outputs
        double inputRateFromOtherVertices = 0;
        for (JobVertexID input : toVertexInputs) {
            if (input.equals(from)) {
                // Exclude source edge because we only want to consider other input edges
                continue;
            }
            if (topology.get(input).getOutputs().size() == 1) {
                inputRateFromOtherVertices +=
                        getRate(ScalingMetric.NUM_RECORDS_OUT, input, metricsHistory);
            } else {
                // Output vertex has multiple outputs, cannot use this information...
                inputRateFromOtherVertices = Double.NaN;
                break;
            }
        }
        if (!Double.isNaN(inputRateFromOtherVertices)) {
            LOG.debug(
                    "Computing edge ({}, {}) data rate by subtracting upstream input rates",
                    from,
                    to);
            return getRate(ScalingMetric.NUM_RECORDS_IN, to, metricsHistory)
                    - inputRateFromOtherVertices;
        }

        // Case 3: We fall back simply to num records out, this is the least reliable
        LOG.debug(
                "Computing edge ({}, {}) data rate by falling back to from num records out",
                from,
                to);
        return getRate(ScalingMetric.NUM_RECORDS_OUT, from, metricsHistory);
    }
}
