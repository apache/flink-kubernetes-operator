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

package org.apache.flink.kubernetes.operator.controller;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingFlinkResourceContextFactory;
import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.autoscaler.AutoscalerFactory;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.health.CanaryResourceManager;
import org.apache.flink.kubernetes.operator.metrics.MetricManager;
import org.apache.flink.kubernetes.operator.observer.deployment.FlinkDeploymentObserverFactory;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.reconciler.deployment.ReconcilerFactory;
import org.apache.flink.kubernetes.operator.resources.ClusterResourceManager;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.FlinkResourceEventCollector;
import org.apache.flink.kubernetes.operator.utils.FlinkStateSnapshotEventCollector;
import org.apache.flink.kubernetes.operator.utils.StatusRecorder;
import org.apache.flink.kubernetes.operator.utils.ValidatorUtils;

import io.fabric8.kubernetes.api.model.Event;
import io.javaoperatorsdk.operator.api.reconciler.Cleaner;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusHandler;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceInitializer;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import lombok.Getter;
import org.junit.jupiter.api.Assertions;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.function.BiConsumer;

/** A wrapper around {@link FlinkDeploymentController} used by unit tests. */
public class TestingFlinkDeploymentController
        implements Reconciler<FlinkDeployment>,
                ErrorStatusHandler<FlinkDeployment>,
                EventSourceInitializer<FlinkDeployment>,
                Cleaner<FlinkDeployment> {

    @Getter private ReconcilerFactory reconcilerFactory;
    private FlinkDeploymentController flinkDeploymentController;
    private StatusUpdateCounter statusUpdateCounter = new StatusUpdateCounter();
    private FlinkResourceEventCollector flinkResourceEventCollector =
            new FlinkResourceEventCollector();

    private EventRecorder eventRecorder;

    @Getter private TestingFlinkResourceContextFactory contextFactory;

    @Getter private StatusRecorder<FlinkDeployment, FlinkDeploymentStatus> statusRecorder;
    @Getter private CanaryResourceManager<FlinkDeployment> canaryResourceManager;

    private Map<ResourceID, Tuple2<FlinkDeploymentSpec, Long>> currentGenerations = new HashMap<>();

    public TestingFlinkDeploymentController(
            FlinkConfigManager configManager, TestingFlinkService flinkService) {

        contextFactory =
                new TestingFlinkResourceContextFactory(
                        configManager,
                        TestUtils.createTestMetricGroup(new Configuration()),
                        flinkService,
                        eventRecorder);

        eventRecorder =
                new EventRecorder(
                        flinkResourceEventCollector, new FlinkStateSnapshotEventCollector());
        statusRecorder = new StatusRecorder<>(new MetricManager<>(), statusUpdateCounter);
        reconcilerFactory =
                new ReconcilerFactory(
                        eventRecorder,
                        statusRecorder,
                        AutoscalerFactory.create(
                                flinkService.getKubernetesClient(),
                                eventRecorder,
                                new ClusterResourceManager(
                                        Duration.ZERO, flinkService.getKubernetesClient())));
        canaryResourceManager = new CanaryResourceManager<>(configManager);
        flinkDeploymentController =
                new FlinkDeploymentController(
                        ValidatorUtils.discoverValidators(configManager),
                        contextFactory,
                        reconcilerFactory,
                        new FlinkDeploymentObserverFactory(eventRecorder),
                        statusRecorder,
                        eventRecorder,
                        canaryResourceManager);
    }

    @Override
    public UpdateControl<FlinkDeployment> reconcile(
            FlinkDeployment flinkDeployment, Context<FlinkDeployment> context) throws Exception {
        FlinkDeployment cloned = ReconciliationUtils.clone(flinkDeployment);
        updateGeneration(cloned);
        statusUpdateCounter.setCurrent(flinkDeployment);
        UpdateControl<FlinkDeployment> updateControl =
                flinkDeploymentController.reconcile(cloned, context);
        Assertions.assertTrue(updateControl.isNoUpdate());
        return updateControl;
    }

    private void updateGeneration(FlinkDeployment resource) {
        var tuple =
                currentGenerations.compute(
                        ResourceID.fromResource(resource),
                        (id, t) -> {
                            var spec = ReconciliationUtils.clone(resource.getSpec());
                            if (t == null) {
                                return Tuple2.of(spec, 0L);
                            } else {
                                if (t.f0.equals(spec)) {
                                    return t;
                                } else {
                                    return Tuple2.of(spec, t.f1 + 1);
                                }
                            }
                        });
        resource.getMetadata().setGeneration(tuple.f1);
    }

    @Override
    public DeleteControl cleanup(
            FlinkDeployment flinkDeployment, Context<FlinkDeployment> context) {
        FlinkDeployment cloned = ReconciliationUtils.clone(flinkDeployment);
        statusUpdateCounter.setCurrent(flinkDeployment);
        return flinkDeploymentController.cleanup(cloned, context);
    }

    @Override
    public ErrorStatusUpdateControl<FlinkDeployment> updateErrorStatus(
            FlinkDeployment flinkDeployment, Context<FlinkDeployment> context, Exception e) {
        FlinkDeployment cloned = ReconciliationUtils.clone(flinkDeployment);
        statusUpdateCounter.setCurrent(flinkDeployment);
        return flinkDeploymentController.updateErrorStatus(cloned, context, e);
    }

    @Override
    public Map<String, EventSource> prepareEventSources(
            EventSourceContext<FlinkDeployment> eventSourceContext) {
        throw new UnsupportedOperationException();
    }

    public Queue<Event> flinkResourceEvents() {
        return flinkResourceEventCollector.events;
    }

    private static class StatusUpdateCounter
            implements BiConsumer<FlinkDeployment, FlinkDeploymentStatus> {

        private FlinkDeployment currentResource;
        private int counter;

        @Override
        public void accept(
                FlinkDeployment flinkDeploymentStatusAbstractFlinkResource,
                FlinkDeploymentStatus flinkDeploymentStatus) {
            currentResource.setStatus(flinkDeploymentStatusAbstractFlinkResource.getStatus());
            counter++;
        }

        public void setCurrent(FlinkDeployment currentResource) {
            this.currentResource = currentResource;
        }

        public int getCount() {
            return counter;
        }
    }

    public int getInternalStatusUpdateCount() {
        return statusUpdateCounter.getCount();
    }
}
