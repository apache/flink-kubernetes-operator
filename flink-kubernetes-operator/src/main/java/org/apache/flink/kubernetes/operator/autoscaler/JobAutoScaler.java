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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.metrics.KubernetesOperatorMetricGroup;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions.AUTOSCALER_ENABLED;

/** Application and SessionJob autoscaler. */
public class JobAutoScaler implements Cleanup {

    private static final Logger LOG = LoggerFactory.getLogger(JobAutoScaler.class);

    private final KubernetesClient kubernetesClient;
    private final FlinkConfigManager configManager;
    private final ScalingMetricCollector metricsCollector;
    private final ScalingMetricEvaluator evaluator;
    private final ScalingExecutor scalingExecutor;
    private final KubernetesOperatorMetricGroup metricGroup;

    private final Map<ResourceID, Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>>>
            lastEvaluatedMetrics = new ConcurrentHashMap<>();
    private final Map<ResourceID, Set<JobVertexID>> registeredMetrics = new ConcurrentHashMap<>();

    public JobAutoScaler(
            KubernetesClient kubernetesClient,
            FlinkConfigManager configManager,
            ScalingMetricCollector metricsCollector,
            ScalingMetricEvaluator evaluator,
            ScalingExecutor scalingExecutor,
            KubernetesOperatorMetricGroup metricGroup) {
        this.kubernetesClient = kubernetesClient;

        this.configManager = configManager;
        this.metricsCollector = metricsCollector;
        this.evaluator = evaluator;
        this.scalingExecutor = scalingExecutor;
        this.metricGroup = metricGroup;
    }

    @Override
    public void cleanup(AbstractFlinkResource<?, ?> cr) {
        LOG.info("Cleaning up autoscaling meta data");
        metricsCollector.cleanup(cr);
        scalingExecutor.cleanup(cr);
        var resourceId = ResourceID.fromResource(cr);
        lastEvaluatedMetrics.remove(resourceId);
        registeredMetrics.remove(resourceId);
    }

    public boolean scale(
            AbstractFlinkResource<?, ?> resource,
            FlinkService flinkService,
            Configuration conf,
            Context<?> context) {

        if (resource.getSpec().getJob() == null || !conf.getBoolean(AUTOSCALER_ENABLED)) {
            LOG.info("Job autoscaler is disabled");
            return false;
        }

        if (!resource.getStatus().getJobStatus().getState().equals(JobStatus.RUNNING.name())) {
            LOG.info("Job autoscaler is waiting for RUNNING job     state");
            return false;
        }

        try {
            var autoScalerInfo = AutoScalerInfo.forResource(resource, kubernetesClient);

            LOG.info("Collecting metrics for scaling");
            var collectedMetrics =
                    metricsCollector.getMetricsHistory(
                            resource, autoScalerInfo, flinkService, conf);

            if (collectedMetrics == null || collectedMetrics.getMetricHistory().isEmpty()) {
                LOG.info("No metrics were collected. Skipping scaling step");
                return false;
            }

            LOG.debug("Evaluating scaling metrics for {}", collectedMetrics);
            var evaluatedMetrics = evaluator.evaluate(conf, collectedMetrics);
            LOG.info("Scaling metrics evaluated: {}", evaluatedMetrics);
            lastEvaluatedMetrics.put(ResourceID.fromResource(resource), evaluatedMetrics);
            registerResourceScalingMetrics(resource);

            var specAdjusted =
                    scalingExecutor.scaleResource(resource, autoScalerInfo, conf, evaluatedMetrics);
            autoScalerInfo.replaceInKubernetes(kubernetesClient);
            return specAdjusted;
        } catch (Exception e) {
            LOG.error("Error while scaling resource", e);
            return false;
        }
    }

    private void registerResourceScalingMetrics(AbstractFlinkResource<?, ?> resource) {
        var resourceId = ResourceID.fromResource(resource);
        var scalerGroup =
                metricGroup
                        .createResourceNamespaceGroup(
                                configManager.getDefaultConfig(),
                                resource.getClass(),
                                resource.getMetadata().getNamespace())
                        .createResourceNamespaceGroup(
                                configManager.getDefaultConfig(), resource.getMetadata().getName())
                        .addGroup("AutoScaler");

        lastEvaluatedMetrics
                .get(resourceId)
                .forEach(
                        (jobVertexID, evaluated) -> {
                            if (!registeredMetrics
                                    .computeIfAbsent(resourceId, r -> new HashSet<>())
                                    .add(jobVertexID)) {
                                return;
                            }
                            LOG.info("Registering scaling metrics for job vertex {}", jobVertexID);
                            var jobVertexMg =
                                    scalerGroup.addGroup("jobVertexID", jobVertexID.toHexString());

                            evaluated.forEach(
                                    (sm, esm) -> {
                                        var smGroup = jobVertexMg.addGroup(sm.name());

                                        smGroup.gauge(
                                                "Current",
                                                () ->
                                                        Optional.ofNullable(
                                                                        lastEvaluatedMetrics.get(
                                                                                resourceId))
                                                                .map(m -> m.get(jobVertexID))
                                                                .map(
                                                                        metrics ->
                                                                                metrics.get(sm)
                                                                                        .getCurrent())
                                                                .orElse(null));

                                        if (sm.isCalculateAverage()) {
                                            smGroup.gauge(
                                                    "Average",
                                                    () ->
                                                            Optional.ofNullable(
                                                                            lastEvaluatedMetrics
                                                                                    .get(
                                                                                            resourceId))
                                                                    .map(m -> m.get(jobVertexID))
                                                                    .map(
                                                                            metrics ->
                                                                                    metrics.get(sm)
                                                                                            .getAverage())
                                                                    .orElse(null));
                                        }
                                    });
                        });
    }

    public static JobAutoScaler create(
            KubernetesClient kubernetesClient,
            FlinkConfigManager configManager,
            KubernetesOperatorMetricGroup metricGroup) {
        return new JobAutoScaler(
                kubernetesClient,
                configManager,
                new RestApiMetricsCollector(),
                new ScalingMetricEvaluator(),
                new ScalingExecutor(kubernetesClient),
                metricGroup);
    }
}
