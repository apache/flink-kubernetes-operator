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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric;
import org.apache.flink.kubernetes.operator.autoscaler.utils.AutoScalerUtils;
import org.apache.flink.kubernetes.operator.utils.KubernetesClientUtils;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.Preconditions;

import io.fabric8.kubernetes.client.KubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

import static org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions.MAX_SCALE_DOWN_FACTOR;
import static org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions.SCALE_UP_GRACE_PERIOD;
import static org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions.SCALING_ENABLED;
import static org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions.STABILIZATION_INTERVAL;
import static org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions.TARGET_UTILIZATION;
import static org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions.VERTEX_MAX_PARALLELISM;
import static org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions.VERTEX_MIN_PARALLELISM;
import static org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric.MAX_PARALLELISM;
import static org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric.PARALLELISM;
import static org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric.SCALE_DOWN_RATE_THRESHOLD;
import static org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric.SCALE_UP_RATE_THRESHOLD;
import static org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric.TRUE_PROCESSING_RATE;

/** Class responsible for executing scaling decisions. */
public class ScalingExecutor implements Cleanup {

    public static final ConfigOption<Map<String, String>> PARALLELISM_OVERRIDES =
            ConfigOptions.key("pipeline.jobvertex-parallelism-overrides")
                    .mapType()
                    .defaultValue(Collections.emptyMap())
                    .withDescription(
                            "A parallelism override map (jobVertexId -> parallelism) which will be used to update"
                                    + " the parallelism of the corresponding job vertices of submitted JobGraphs.");

    private static final Logger LOG = LoggerFactory.getLogger(ScalingExecutor.class);

    private final KubernetesClient kubernetesClient;

    private Clock clock = Clock.system(ZoneId.systemDefault());

    public ScalingExecutor(KubernetesClient kubernetesClient) {
        this.kubernetesClient = kubernetesClient;
    }

    public boolean scaleResource(
            AbstractFlinkResource<?, ?> resource,
            AutoScalerInfo scalingInformation,
            Configuration conf,
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluatedMetrics)
            throws Exception {

        if (!conf.get(SCALING_ENABLED)) {
            return false;
        }

        if (!stabilizationPeriodPassed(resource, conf)) {
            return false;
        }

        var scalingHistory = scalingInformation.getScalingHistory();
        var scalingSummaries = computeScalingSummary(conf, evaluatedMetrics, scalingHistory);
        if (scalingSummaries.isEmpty()) {
            LOG.info("All job vertices are currently running at their target parallelism.");
            return false;
        }

        if (allVerticesWithinUtilizationTarget(evaluatedMetrics, scalingSummaries)) {
            return false;
        }

        LOG.info("Scaling vertices:");
        scalingSummaries.forEach(
                (v, s) ->
                        LOG.info(
                                "{} | Parallelism {} -> {}",
                                v,
                                s.getCurrentParallelism(),
                                s.getNewParallelism()));

        setVertexParallelismOverrides(resource, evaluatedMetrics, scalingSummaries);

        KubernetesClientUtils.replaceSpecAfterScaling(kubernetesClient, resource);
        scalingInformation.addToScalingHistory(clock.instant(), scalingSummaries);

        return true;
    }

    private boolean stabilizationPeriodPassed(
            AbstractFlinkResource<?, ?> resource, Configuration conf) {
        var now = clock.instant();
        var startTs =
                Instant.ofEpochMilli(
                        Long.parseLong(resource.getStatus().getJobStatus().getStartTime()));
        var stableTime = startTs.plus(conf.get(STABILIZATION_INTERVAL));

        if (stableTime.isAfter(now)) {
            LOG.info("Waiting until {} to stabilize before new scale operation.", stableTime);
            return false;
        }
        return true;
    }

    protected static boolean allVerticesWithinUtilizationTarget(
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluatedMetrics,
            Map<JobVertexID, ScalingSummary> scalingSummaries) {

        for (Map.Entry<JobVertexID, ScalingSummary> entry : scalingSummaries.entrySet()) {
            var vertex = entry.getKey();
            var scalingSummary = entry.getValue();
            var metrics = evaluatedMetrics.get(vertex);

            double processingRate = metrics.get(TRUE_PROCESSING_RATE).getAverage();
            double scaleUpRateThreshold = metrics.get(SCALE_UP_RATE_THRESHOLD).getCurrent();
            double scaleDownRateThreshold = metrics.get(SCALE_DOWN_RATE_THRESHOLD).getCurrent();

            if (processingRate < scaleUpRateThreshold || processingRate > scaleDownRateThreshold) {
                LOG.info(
                        "Vertex {}(pCurr={}, pNew={}) processing rate {} is outside ({}, {})",
                        vertex,
                        scalingSummary.getCurrentParallelism(),
                        scalingSummary.getNewParallelism(),
                        processingRate,
                        scaleUpRateThreshold,
                        scaleDownRateThreshold);
                return false;
            } else {
                LOG.debug(
                        "Vertex {}(pCurr={}, pNew={}) processing rate {} is within target ({}, {})",
                        vertex,
                        scalingSummary.getCurrentParallelism(),
                        scalingSummary.getNewParallelism(),
                        processingRate,
                        scaleUpRateThreshold,
                        scaleDownRateThreshold);
            }
        }
        LOG.info("All vertex processing rates are within target.");
        return true;
    }

    private Map<JobVertexID, ScalingSummary> computeScalingSummary(
            Configuration conf,
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluatedMetrics,
            Map<JobVertexID, SortedMap<Instant, ScalingSummary>> scalingHistory) {

        var out = new HashMap<JobVertexID, ScalingSummary>();
        evaluatedMetrics.forEach(
                (v, metrics) -> {
                    var currentParallelism =
                            (int) metrics.get(ScalingMetric.PARALLELISM).getCurrent();
                    var newParallelism =
                            computeScaleTargetParallelism(
                                    conf,
                                    v,
                                    metrics,
                                    scalingHistory.getOrDefault(v, Collections.emptySortedMap()));
                    if (currentParallelism != newParallelism) {
                        out.put(v, new ScalingSummary(currentParallelism, newParallelism, metrics));
                    }
                });

        return out;
    }

    protected int computeScaleTargetParallelism(
            Configuration conf,
            JobVertexID vertex,
            Map<ScalingMetric, EvaluatedScalingMetric> evaluatedMetrics,
            SortedMap<Instant, ScalingSummary> history) {

        var currentParallelism = (int) evaluatedMetrics.get(PARALLELISM).getCurrent();
        double averageTrueProcessingRate = evaluatedMetrics.get(TRUE_PROCESSING_RATE).getAverage();

        if (Double.isNaN(averageTrueProcessingRate)) {
            LOG.info(
                    "True processing rate is not available for {}, cannot compute new parallelism",
                    vertex);
            return currentParallelism;
        }

        double targetCapacity =
                AutoScalerUtils.getTargetProcessingCapacity(
                        evaluatedMetrics, conf, conf.get(TARGET_UTILIZATION), true);
        if (Double.isNaN(targetCapacity)) {
            LOG.info(
                    "Target data rate is not available for {}, cannot compute new parallelism",
                    vertex);
            return currentParallelism;
        }

        LOG.info("Target processing capacity for {} is {}", vertex, targetCapacity);
        double scaleFactor = targetCapacity / averageTrueProcessingRate;
        double minScaleFactor = 1 - conf.get(MAX_SCALE_DOWN_FACTOR);
        if (scaleFactor < minScaleFactor) {
            LOG.info(
                    "Computed scale factor of {} for {} is capped by maximum scale down factor to {}",
                    scaleFactor,
                    vertex,
                    minScaleFactor);
            scaleFactor = minScaleFactor;
        }

        int newParallelism =
                scale(
                        currentParallelism,
                        (int) evaluatedMetrics.get(MAX_PARALLELISM).getCurrent(),
                        scaleFactor,
                        conf.getInteger(VERTEX_MIN_PARALLELISM),
                        conf.getInteger(VERTEX_MAX_PARALLELISM));

        if (!history.isEmpty()) {
            if (detectImmediateScaleDownAfterScaleUp(
                    conf, history, currentParallelism, newParallelism)) {
                LOG.info(
                        "Skipping immediate scale down after scale up for {} resetting target parallelism to {}",
                        vertex,
                        currentParallelism);
                newParallelism = currentParallelism;
            }

            // currentParallelism = 2 , newParallelism = 1, minimumProcRate = 1000 r/s
            // history
            // currentParallelism 1 => 3 -> empiricalProcRate = 800
            // empiricalProcRate + upperBoundary < minimumProcRate => don't scale
        }

        return newParallelism;
    }

    private boolean detectImmediateScaleDownAfterScaleUp(
            Configuration conf,
            SortedMap<Instant, ScalingSummary> history,
            int currentParallelism,
            int newParallelism) {
        var lastScalingTs = history.lastKey();
        var lastSummary = history.get(lastScalingTs);

        boolean isScaleDown = newParallelism < currentParallelism;
        boolean lastScaleUp = lastSummary.getNewParallelism() > lastSummary.getCurrentParallelism();

        var gracePeriod = conf.get(SCALE_UP_GRACE_PERIOD);

        boolean withinConfiguredTime =
                Duration.between(lastScalingTs, clock.instant()).minus(gracePeriod).isNegative();

        return isScaleDown && lastScaleUp && withinConfiguredTime;
    }

    public static int scale(
            int parallelism,
            int numKeyGroups,
            double scaleFactor,
            int minParallelism,
            int maxParallelism) {
        Preconditions.checkArgument(
                minParallelism <= maxParallelism,
                "The minimum parallelism must not be greater than the maximum parallelism.");
        if (minParallelism > numKeyGroups) {
            LOG.warn(
                    "Specified autoscaler minimum parallelism {} is greater than the operator max parallelism {}. The min parallelism will be set to the operator max parallelism.",
                    minParallelism,
                    numKeyGroups);
        }
        if (numKeyGroups < maxParallelism && maxParallelism != Integer.MAX_VALUE) {
            LOG.warn(
                    "Specified autoscaler maximum parallelism {} is greater than the operator max parallelism {}. This means the operator max parallelism can never be reached.",
                    maxParallelism,
                    numKeyGroups);
        }

        int newParallelism =
                // Prevent integer overflow when converting from double to integer.
                // We do not have to detect underflow because doubles cannot
                // underflow.
                (int) Math.min(Math.ceil(scaleFactor * parallelism), Integer.MAX_VALUE);

        // Cap parallelism at either number of key groups or parallelism limit
        final int upperBound = Math.min(numKeyGroups, maxParallelism);

        // Apply min/max parallelism
        newParallelism = Math.min(Math.max(minParallelism, newParallelism), upperBound);

        // Try to adjust the parallelism such that it divides the number of key groups without a
        // remainder => state is evenly spread across subtasks
        for (int p = newParallelism; p <= numKeyGroups / 2 && p <= upperBound; p++) {
            if (numKeyGroups % p == 0) {
                return p;
            }
        }

        // If key group adjustment fails, use originally computed parallelism
        return newParallelism;
    }

    private void setVertexParallelismOverrides(
            AbstractFlinkResource<?, ?> resource,
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluatedMetrics,
            Map<JobVertexID, ScalingSummary> summaries) {
        var flinkConf = Configuration.fromMap(resource.getSpec().getFlinkConfiguration());
        var overrides = new HashMap<String, String>();
        evaluatedMetrics.forEach(
                (id, metrics) -> {
                    if (summaries.containsKey(id)) {
                        overrides.put(
                                id.toHexString(),
                                String.valueOf(summaries.get(id).getNewParallelism()));
                    } else {
                        overrides.put(
                                id.toHexString(),
                                String.valueOf(
                                        (int) metrics.get(ScalingMetric.PARALLELISM).getCurrent()));
                    }
                });
        flinkConf.set(PARALLELISM_OVERRIDES, overrides);

        resource.getSpec().setFlinkConfiguration(flinkConf.toMap());
    }

    @VisibleForTesting
    protected void setClock(Clock clock) {
        this.clock = Preconditions.checkNotNull(clock);
    }

    @Override
    public void cleanup(AbstractFlinkResource<?, ?> cr) {
        // No cleanup is currently necessary
    }
}
