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
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric;
import org.apache.flink.kubernetes.operator.utils.KubernetesClientUtils;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.Preconditions;

import io.fabric8.kubernetes.client.KubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

import static org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions.SCALING_ENABLED;
import static org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions.STABILIZATION_INTERVAL;
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
    private final JobVertexScaler jobVertexScaler;

    private Clock clock = Clock.system(ZoneId.systemDefault());

    public ScalingExecutor(KubernetesClient kubernetesClient) {
        this(kubernetesClient, new JobVertexScaler());
    }

    public ScalingExecutor(KubernetesClient kubernetesClient, JobVertexScaler jobVertexScaler) {
        this.kubernetesClient = kubernetesClient;
        this.jobVertexScaler = jobVertexScaler;
    }

    public boolean scaleResource(
            AbstractFlinkResource<?, ?> resource,
            AutoScalerInfo scalingInformation,
            Configuration conf,
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluatedMetrics) {

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
        scalingInformation.addToScalingHistory(clock.instant(), scalingSummaries, conf);

        return true;
    }

    private boolean stabilizationPeriodPassed(
            AbstractFlinkResource<?, ?> resource, Configuration conf) {
        var jobStatus = resource.getStatus().getJobStatus();

        if (!JobStatus.RUNNING.name().equals(jobStatus.getState())) {
            // Never consider a non-running job stable
            return false;
        }

        var startTs =
                Instant.ofEpochMilli(
                        // Use the update time which will reflect the latest job state update
                        // Do not use the start time because it doesn't tell when the job went to
                        // RUNNING
                        Long.parseLong(jobStatus.getUpdateTime()));
        var stableTime = startTs.plus(conf.get(STABILIZATION_INTERVAL));

        if (stableTime.isAfter(clock.instant())) {
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
                            jobVertexScaler.computeScaleTargetParallelism(
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
        jobVertexScaler.setClock(clock);
    }

    @Override
    public void cleanup(AbstractFlinkResource<?, ?> cr) {
        // No cleanup is currently necessary
    }
}
