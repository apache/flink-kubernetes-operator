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
import org.apache.flink.autoscaler.ScalingMetricEvaluator;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState;
import org.apache.flink.kubernetes.operator.api.status.CommonStatus;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.reconciler.deployment.JobAutoScaler;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import io.javaoperatorsdk.operator.processing.event.ResourceID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.autoscaler.config.AutoScalerOptions.AUTOSCALER_ENABLED;
import static org.apache.flink.kubernetes.operator.autoscaler.AutoscalerFlinkMetrics.initRecommendedParallelism;
import static org.apache.flink.kubernetes.operator.autoscaler.AutoscalerFlinkMetrics.resetRecommendedParallelism;

/** Application and SessionJob autoscaler. */
public class JobAutoScalerImpl implements JobAutoScaler {

    private static final Logger LOG = LoggerFactory.getLogger(JobAutoScalerImpl.class);

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
            ScalingMetricCollector metricsCollector,
            ScalingMetricEvaluator evaluator,
            ScalingExecutor scalingExecutor,
            EventRecorder eventRecorder) {
        this.metricsCollector = metricsCollector;
        this.evaluator = evaluator;
        this.scalingExecutor = scalingExecutor;
        this.eventRecorder = eventRecorder;
        this.infoManager = new AutoscalerInfoManager();
    }

    @Override
    public void scale(FlinkResourceContext<?> ctx) {
        var conf = ctx.getObserveConfig();
        var resource = ctx.getResource();
        var resourceId = ResourceID.fromResource(resource);
        var autoscalerMetrics = getOrInitAutoscalerFlinkMetrics(ctx, resourceId);

        try {
            runScalingLogic(ctx, conf, resource, resourceId, autoscalerMetrics);
        } catch (Throwable e) {
            onError(ctx, resource, autoscalerMetrics, e);
        } finally {
            applyParallelismOverrides(ctx);
        }
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

    private void clearParallelismOverrides(FlinkResourceContext<?> ctx) throws Exception {
        var infoOpt = infoManager.getInfo(ctx.getResource(), ctx.getKubernetesClient());
        if (infoOpt.isPresent()) {
            var info = infoOpt.get();
            info.removeCurrentOverrides();
            info.replaceInKubernetes(ctx.getKubernetesClient());
        }
    }

    @VisibleForTesting
    protected Map<String, String> getParallelismOverrides(FlinkResourceContext<?> ctx) {
        return infoManager
                .getInfo(ctx.getResource(), ctx.getKubernetesClient())
                .map(AutoScalerInfo::getCurrentOverrides)
                .orElse(Map.of());
    }

    /**
     * If there are any parallelism overrides by the {@link JobAutoScaler} apply them to the spec.
     *
     * @param ctx Resource context
     */
    @VisibleForTesting
    protected void applyParallelismOverrides(FlinkResourceContext<?> ctx) {
        var overrides = getParallelismOverrides(ctx);
        if (overrides.isEmpty()) {
            return;
        }

        LOG.debug("Applying parallelism overrides: {}", overrides);

        var spec = ctx.getResource().getSpec();
        var conf = ctx.getDeployConfig(spec);
        var userOverrides = new HashMap<>(conf.get(PipelineOptions.PARALLELISM_OVERRIDES));
        var exclusions = conf.get(AutoScalerOptions.VERTEX_EXCLUDE_IDS);

        overrides.forEach(
                (k, v) -> {
                    // Respect user override for excluded vertices
                    if (exclusions.contains(k)) {
                        userOverrides.putIfAbsent(k, v);
                    } else {
                        userOverrides.put(k, v);
                    }
                });
        spec.getFlinkConfiguration()
                .put(
                        PipelineOptions.PARALLELISM_OVERRIDES.key(),
                        ConfigurationUtils.convertValue(userOverrides, String.class));
    }

    private void runScalingLogic(
            FlinkResourceContext<?> ctx,
            Configuration conf,
            AbstractFlinkResource<?, ?> resource,
            ResourceID resourceId,
            AutoscalerFlinkMetrics autoscalerMetrics)
            throws Exception {

        if (resource.getSpec().getJob() == null || !conf.getBoolean(AUTOSCALER_ENABLED)) {
            LOG.debug("Autoscaler is disabled");
            clearParallelismOverrides(ctx);
            return;
        }

        var status = resource.getStatus();
        if (status.getLifecycleState() != ResourceLifecycleState.STABLE
                || !status.getJobStatus().getState().equals(JobStatus.RUNNING.name())) {
            LOG.info("Autoscaler is waiting for stable, running state");
            lastEvaluatedMetrics.remove(resourceId);
            return;
        }

        var autoScalerInfo = infoManager.getOrCreateInfo(resource, ctx.getKubernetesClient());

        var collectedMetrics =
                metricsCollector.updateMetrics(
                        resource, autoScalerInfo, ctx.getFlinkService(), conf);

        if (collectedMetrics.getMetricHistory().isEmpty()) {
            autoScalerInfo.replaceInKubernetes(ctx.getKubernetesClient());
            return;
        }
        LOG.debug("Collected metrics: {}", collectedMetrics);

        var evaluatedMetrics = evaluator.evaluate(conf, collectedMetrics);
        LOG.debug("Evaluated metrics: {}", evaluatedMetrics);
        lastEvaluatedMetrics.put(resourceId, evaluatedMetrics);

        initRecommendedParallelism(evaluatedMetrics);
        autoscalerMetrics.registerScalingMetrics(
                collectedMetrics.getJobTopology().getVerticesInTopologicalOrder(),
                () -> lastEvaluatedMetrics.get(resourceId));

        if (!collectedMetrics.isFullyCollected()) {
            // We have done an upfront evaluation, but we are not ready for scaling.
            resetRecommendedParallelism(evaluatedMetrics);
            autoScalerInfo.replaceInKubernetes(ctx.getKubernetesClient());
            return;
        }

        var parallelismChanged =
                scalingExecutor.scaleResource(
                        resource,
                        autoScalerInfo,
                        conf,
                        evaluatedMetrics,
                        ctx.getKubernetesClient());

        if (parallelismChanged) {
            autoscalerMetrics.numScalings.inc();
        } else {
            autoscalerMetrics.numBalanced.inc();
        }

        autoScalerInfo.replaceInKubernetes(ctx.getKubernetesClient());
    }

    private void onError(
            FlinkResourceContext<?> ctx,
            AbstractFlinkResource<?, ? extends CommonStatus<?>> resource,
            AutoscalerFlinkMetrics autoscalerMetrics,
            Throwable e) {
        LOG.error("Error while scaling resource", e);
        autoscalerMetrics.numErrors.inc();
        eventRecorder.triggerEvent(
                resource,
                EventRecorder.Type.Warning,
                EventRecorder.Reason.AutoscalerError,
                EventRecorder.Component.Operator,
                e.getMessage(),
                ctx.getKubernetesClient());
    }

    private AutoscalerFlinkMetrics getOrInitAutoscalerFlinkMetrics(
            FlinkResourceContext<? extends AbstractFlinkResource<?, ?>> ctx, ResourceID resouceId) {
        return this.flinkMetrics.computeIfAbsent(
                resouceId,
                id ->
                        new AutoscalerFlinkMetrics(
                                ctx.getResourceMetricGroup().addGroup("AutoScaler")));
    }
}
