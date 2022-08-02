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

import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.TestingFlinkServiceFactory;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.crd.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.metrics.MetricManager;
import org.apache.flink.kubernetes.operator.observer.deployment.ObserverFactory;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.reconciler.deployment.ReconcilerFactory;
import org.apache.flink.kubernetes.operator.service.FlinkServiceFactory;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.StatusRecorder;
import org.apache.flink.kubernetes.operator.utils.ValidatorUtils;

import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Cleaner;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusHandler;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceInitializer;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import org.junit.jupiter.api.Assertions;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.function.BiConsumer;

/** A wrapper around {@link FlinkDeploymentController} used by unit tests. */
public class TestingFlinkDeploymentController
        implements Reconciler<FlinkDeployment>,
                ErrorStatusHandler<FlinkDeployment>,
                EventSourceInitializer<FlinkDeployment>,
                Cleaner<FlinkDeployment> {

    private FlinkDeploymentController flinkDeploymentController;
    private StatusUpdateCounter statusUpdateCounter = new StatusUpdateCounter();
    private EventCollector eventCollector = new EventCollector();

    private EventRecorder eventRecorder;
    private StatusRecorder<FlinkDeployment, FlinkDeploymentStatus> statusRecorder;

    public TestingFlinkDeploymentController(
            FlinkConfigManager configManager,
            KubernetesClient kubernetesClient,
            TestingFlinkService flinkService) {
        FlinkServiceFactory flinkServiceFactory = new TestingFlinkServiceFactory(flinkService);

        eventRecorder = new EventRecorder(kubernetesClient, eventCollector);
        statusRecorder =
                new StatusRecorder<>(kubernetesClient, new MetricManager<>(), statusUpdateCounter);
        flinkDeploymentController =
                new FlinkDeploymentController(
                        configManager,
                        ValidatorUtils.discoverValidators(configManager),
                        new ReconcilerFactory(
                                kubernetesClient,
                                flinkServiceFactory,
                                configManager,
                                eventRecorder,
                                statusRecorder),
                        new ObserverFactory(
                                flinkServiceFactory, configManager, statusRecorder, eventRecorder),
                        statusRecorder,
                        eventRecorder);
    }

    @Override
    public UpdateControl<FlinkDeployment> reconcile(
            FlinkDeployment flinkDeployment, Context<FlinkDeployment> context) throws Exception {
        FlinkDeployment cloned = ReconciliationUtils.clone(flinkDeployment);
        statusUpdateCounter.setCurrent(flinkDeployment);
        UpdateControl<FlinkDeployment> updateControl =
                flinkDeploymentController.reconcile(cloned, context);
        Assertions.assertTrue(updateControl.isNoUpdate());
        return updateControl;
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

    private static class EventCollector implements BiConsumer<AbstractFlinkResource<?, ?>, Event> {
        private Queue<Event> events = new LinkedList<>();

        @Override
        public void accept(AbstractFlinkResource<?, ?> abstractFlinkResource, Event event) {
            events.add(event);
        }
    }

    public Queue<Event> events() {
        return eventCollector.events;
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
