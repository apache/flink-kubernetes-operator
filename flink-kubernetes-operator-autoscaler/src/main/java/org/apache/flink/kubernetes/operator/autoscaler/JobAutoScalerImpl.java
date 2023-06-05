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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.reconciler.deployment.JobAutoScaler;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions.AUTOSCALER_ENABLED;

/** Application and SessionJob autoscaler. */
public class JobAutoScalerImpl implements JobAutoScaler {

    private static final Logger LOG = LoggerFactory.getLogger(JobAutoScalerImpl.class);

    private final KubernetesClient kubernetesClient;
    private final ScalingMetricCollector metricsCollector;
    private final ScalingMetricEvaluator evaluator;
    private final ScalingExecutor scalingExecutor;
    private final EventRecorder eventRecorder;

    private final Map<ResourceID, Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>>>
            lastEvaluatedMetrics = new ConcurrentHashMap<>();

    final Map<ResourceID, Map<JobVertexID, Integer>> recommendedParallelisms =
            new ConcurrentHashMap<>();
    final Map<ResourceID, AutoscalerFlinkMetrics> flinkMetrics = new ConcurrentHashMap<>();

    public JobAutoScalerImpl(
            KubernetesClient kubernetesClient,
            ScalingMetricCollector metricsCollector,
            ScalingMetricEvaluator evaluator,
            ScalingExecutor scalingExecutor,
            EventRecorder eventRecorder) {
        this.kubernetesClient = kubernetesClient;
        this.metricsCollector = metricsCollector;
        this.evaluator = evaluator;
        this.scalingExecutor = scalingExecutor;
        this.eventRecorder = eventRecorder;
    }

    @Override
    public void cleanup(AbstractFlinkResource<?, ?> cr) {
        LOG.info("Cleaning up autoscaling meta data");
        metricsCollector.cleanup(cr);
        var resourceId = ResourceID.fromResource(cr);
        lastEvaluatedMetrics.remove(resourceId);
        flinkMetrics.remove(resourceId);
        recommendedParallelisms.remove(resourceId);
    }

    @Override
    public boolean scale(FlinkResourceContext<? extends AbstractFlinkResource<?, ?>> ctx) {

        var conf = ctx.getObserveConfig();
        var resource = ctx.getResource();
        var resourceId = ResourceID.fromResource(resource);

        try {

            if (resource.getSpec().getJob() == null || !conf.getBoolean(AUTOSCALER_ENABLED)) {
                LOG.debug("Job autoscaler is disabled");
                return false;
            }

            // Initialize metrics only if autoscaler is enabled
            var flinkMetrics = getOrInitAutoscalerFlinkMetrics(ctx, resourceId);

            if (!resource.getStatus().getJobStatus().getState().equals(JobStatus.RUNNING.name())) {
                LOG.info("Job autoscaler is waiting for RUNNING job state");
                return false;
            }

            var autoScalerInfo = AutoScalerInfo.forResource(resource, kubernetesClient);

            var collectedMetrics =
                    metricsCollector.updateMetrics(
                            resource,
                            autoScalerInfo,
                            ctx.getFlinkService(),
                            conf,
                            recommendedParallelisms);

            LOG.debug("Evaluating scaling metrics for {}", collectedMetrics);
            var evaluatedMetrics = evaluator.evaluate(conf, collectedMetrics);
            LOG.debug("Scaling metrics evaluated: {}", evaluatedMetrics);
            lastEvaluatedMetrics.put(resourceId, evaluatedMetrics);

            flinkMetrics.registerEvaluatedScalingMetrics(
                    () -> lastEvaluatedMetrics.get(resourceId));
            flinkMetrics.registerRecommendedParallelismMetrics(
                    () -> recommendedParallelisms.get(resourceId));

            if (!collectedMetrics.isFullyCollected()) {
                // We have done an upfront evaluation, but we are not ready for scaling.
                resetRecommendedParallelisms(recommendedParallelisms, evaluatedMetrics, resourceId);
                autoScalerInfo.replaceInKubernetes(kubernetesClient);
                return false;
            }

            var specAdjusted =
                    scalingExecutor.scaleResource(
                            resource,
                            autoScalerInfo,
                            conf,
                            evaluatedMetrics,
                            recommendedParallelisms);

            if (specAdjusted) {
                flinkMetrics.numScalings.inc();
            } else {
                flinkMetrics.numBalanced.inc();
            }
            autoScalerInfo.replaceInKubernetes(kubernetesClient);
            return specAdjusted;
        } catch (Throwable e) {
            LOG.error("Error while scaling resource", e);
            getOrInitAutoscalerFlinkMetrics(ctx, resourceId).numErrors.inc();
            eventRecorder.triggerEvent(
                    resource,
                    EventRecorder.Type.Warning,
                    EventRecorder.Reason.AutoscalerError,
                    EventRecorder.Component.Operator,
                    e.getMessage());
            return false;
        }
    }

    private AutoscalerFlinkMetrics getOrInitAutoscalerFlinkMetrics(
            FlinkResourceContext<? extends AbstractFlinkResource<?, ?>> ctx, ResourceID resouceId) {
        return this.flinkMetrics.computeIfAbsent(
                resouceId,
                id ->
                        new AutoscalerFlinkMetrics(
                                ctx.getResourceMetricGroup().addGroup("AutoScaler")));
    }

    private void resetRecommendedParallelisms(
            Map<ResourceID, Map<JobVertexID, Integer>> recommendedParallelisms,
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> lastEvaluatedMetrics,
            ResourceID resourceID) {
        var parallelisms = new HashMap<JobVertexID, Integer>();
        lastEvaluatedMetrics.forEach(
                (jobVertexID, map) -> {
                    parallelisms.put(
                            jobVertexID, (int) map.get(ScalingMetric.PARALLELISM).getCurrent());
                });
        recommendedParallelisms.put(resourceID, parallelisms);
        LOG.debug("Recommended parallelisms are reset to current {}", parallelisms);
    }
}
