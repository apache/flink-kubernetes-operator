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
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState;
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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions.AUTOSCALER_ENABLED;
import static org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric.PARALLELISM;
import static org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric.RECOMMENDED_PARALLELISM;

/** Application and SessionJob autoscaler. */
public class JobAutoScalerImpl implements JobAutoScaler {

    private static final Logger LOG = LoggerFactory.getLogger(JobAutoScalerImpl.class);

    private final KubernetesClient kubernetesClient;
    private final ScalingMetricCollector metricsCollector;
    private final ScalingMetricEvaluator evaluator;
    private final ScalingExecutor scalingExecutor;
    private final EventRecorder eventRecorder;

    @VisibleForTesting
    final Map<ResourceID, Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>>>
            lastEvaluatedMetrics = new ConcurrentHashMap<>();

    @VisibleForTesting
    final Map<ResourceID, AutoscalerFlinkMetrics> flinkMetrics = new ConcurrentHashMap<>();

    @VisibleForTesting final AutoscalerInfoManager infoManager;

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
        this.infoManager = new AutoscalerInfoManager(kubernetesClient);
    }

    @Override
    public void cleanup(FlinkResourceContext<?> ctx) {
        LOG.info("Cleaning up autoscaling meta data");
        var cr = ctx.getResource();
        metricsCollector.cleanup(cr);
        var resourceId = ResourceID.fromResource(cr);
        lastEvaluatedMetrics.remove(resourceId);
        flinkMetrics.remove(resourceId);
        infoManager.removeInfoFromCache(cr);
    }

    @Override
    public Map<String, String> getParallelismOverrides(FlinkResourceContext<?> ctx) {
        var conf = ctx.getObserveConfig();
        try {
            var infoOpt = infoManager.getInfo(ctx.getResource());
            if (infoOpt.isPresent()) {
                var info = infoOpt.get();
                // If autoscaler was disabled need to delete the overrides
                if (!conf.getBoolean(AUTOSCALER_ENABLED) && !info.getCurrentOverrides().isEmpty()) {
                    info.removeCurrentOverrides();
                    info.replaceInKubernetes(kubernetesClient);
                } else {
                    return info.getCurrentOverrides();
                }
            }
        } catch (Exception e) {
            LOG.error("Error while getting parallelism overrides", e);
        }
        return Map.of();
    }

    @Override
    public boolean scale(FlinkResourceContext<?> ctx) {

        var conf = ctx.getObserveConfig();
        var resource = ctx.getResource();
        var resourceId = ResourceID.fromResource(resource);
        var flinkMetrics = getOrInitAutoscalerFlinkMetrics(ctx, resourceId);
        Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluatedMetrics = null;

        try {
            if (resource.getSpec().getJob() == null || !conf.getBoolean(AUTOSCALER_ENABLED)) {
                LOG.debug("Job autoscaler is disabled");
                return false;
            }

            // Initialize metrics only if autoscaler is enabled

            var status = resource.getStatus();
            if (status.getLifecycleState() != ResourceLifecycleState.STABLE
                    || !status.getJobStatus().getState().equals(JobStatus.RUNNING.name())) {
                LOG.info("Job autoscaler is waiting for RUNNING job state");
                lastEvaluatedMetrics.remove(resourceId);
                return false;
            }

            var autoScalerInfo = infoManager.getOrCreateInfo(resource);

            var collectedMetrics =
                    metricsCollector.updateMetrics(
                            resource, autoScalerInfo, ctx.getFlinkService(), conf);

            if (collectedMetrics.getMetricHistory().isEmpty()) {
                autoScalerInfo.replaceInKubernetes(kubernetesClient);
                return false;
            }

            LOG.debug("Collected metrics: {}", collectedMetrics);
            evaluatedMetrics = evaluator.evaluate(conf, collectedMetrics);
            LOG.debug("Evaluated metrics: {}", evaluatedMetrics);
            initRecommendedParallelism(evaluatedMetrics);

            if (!collectedMetrics.isFullyCollected()) {
                // We have done an upfront evaluation, but we are not ready for scaling.
                resetRecommendedParallelism(evaluatedMetrics);
                autoScalerInfo.replaceInKubernetes(kubernetesClient);
                return false;
            }

            var specAdjusted =
                    scalingExecutor.scaleResource(resource, autoScalerInfo, conf, evaluatedMetrics);

            if (specAdjusted) {
                flinkMetrics.numScalings.inc();
            } else {
                flinkMetrics.numBalanced.inc();
            }

            autoScalerInfo.replaceInKubernetes(kubernetesClient);
            return specAdjusted;
        } catch (Throwable e) {
            LOG.error("Error while scaling resource", e);
            flinkMetrics.numErrors.inc();
            eventRecorder.triggerEvent(
                    resource,
                    EventRecorder.Type.Warning,
                    EventRecorder.Reason.AutoscalerError,
                    EventRecorder.Component.Operator,
                    e.getMessage());
            return false;
        } finally {
            if (evaluatedMetrics != null) {
                lastEvaluatedMetrics.put(resourceId, evaluatedMetrics);
                flinkMetrics.registerScalingMetrics(() -> lastEvaluatedMetrics.get(resourceId));
            }
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

    private void initRecommendedParallelism(
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluatedMetrics) {
        evaluatedMetrics.forEach(
                (jobVertexID, evaluatedScalingMetricMap) ->
                        evaluatedScalingMetricMap.put(
                                RECOMMENDED_PARALLELISM,
                                evaluatedScalingMetricMap.get(PARALLELISM)));
    }

    private void resetRecommendedParallelism(
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluatedMetrics) {
        evaluatedMetrics.forEach(
                (jobVertexID, evaluatedScalingMetricMap) ->
                        evaluatedScalingMetricMap.put(RECOMMENDED_PARALLELISM, null));
    }
}
