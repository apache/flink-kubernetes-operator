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
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.status.FlinkSessionJobStatus;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.metrics.MetricManager;
import org.apache.flink.kubernetes.operator.observer.sessionjob.FlinkSessionJobObserver;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.reconciler.sessionjob.SessionJobReconciler;
import org.apache.flink.kubernetes.operator.service.FlinkServiceFactory;
import org.apache.flink.kubernetes.operator.utils.EventCollector;
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
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;

import java.util.Map;
import java.util.Queue;
import java.util.function.BiConsumer;

/** A wrapper around {@link FlinkSessionJobController} used by unit tests. */
public class TestingFlinkSessionJobController
        implements io.javaoperatorsdk.operator.api.reconciler.Reconciler<FlinkSessionJob>,
                ErrorStatusHandler<FlinkSessionJob>,
                EventSourceInitializer<FlinkSessionJob>,
                Cleaner<FlinkSessionJob> {

    private FlinkSessionJobController flinkSessionJobController;
    private TestingFlinkSessionJobController.StatusUpdateCounter statusUpdateCounter =
            new TestingFlinkSessionJobController.StatusUpdateCounter();
    private EventCollector eventCollector = new EventCollector();
    private EventRecorder eventRecorder;
    private StatusRecorder<FlinkSessionJob, FlinkSessionJobStatus> statusRecorder;

    public TestingFlinkSessionJobController(
            FlinkConfigManager configManager,
            KubernetesClient kubernetesClient,
            TestingFlinkService flinkService) {
        FlinkServiceFactory flinkServiceFactory = new TestingFlinkServiceFactory(flinkService);

        eventRecorder = new EventRecorder(kubernetesClient, eventCollector);

        statusRecorder =
                new StatusRecorder<>(kubernetesClient, new MetricManager<>(), statusUpdateCounter);

        flinkSessionJobController =
                new FlinkSessionJobController(
                        configManager,
                        ValidatorUtils.discoverValidators(configManager),
                        new SessionJobReconciler(
                                kubernetesClient,
                                flinkServiceFactory,
                                configManager,
                                eventRecorder,
                                statusRecorder),
                        new FlinkSessionJobObserver(
                                flinkServiceFactory, configManager, eventRecorder),
                        statusRecorder,
                        eventRecorder);
    }

    @Override
    public UpdateControl<FlinkSessionJob> reconcile(
            FlinkSessionJob flinkSessionJob, Context<FlinkSessionJob> context) throws Exception {
        FlinkSessionJob cloned = ReconciliationUtils.clone(flinkSessionJob);
        statusUpdateCounter.setCurrent(flinkSessionJob);

        UpdateControl<FlinkSessionJob> updateControl =
                flinkSessionJobController.reconcile(cloned, context);

        return updateControl;
    }

    @Override
    public ErrorStatusUpdateControl<FlinkSessionJob> updateErrorStatus(
            FlinkSessionJob flinkSessionJob, Context<FlinkSessionJob> context, Exception e) {
        FlinkSessionJob cloned = ReconciliationUtils.clone(flinkSessionJob);
        statusUpdateCounter.setCurrent(flinkSessionJob);
        return flinkSessionJobController.updateErrorStatus(cloned, context, e);
    }

    @Override
    public DeleteControl cleanup(
            FlinkSessionJob flinkSessionJob, Context<FlinkSessionJob> context) {
        FlinkSessionJob cloned = ReconciliationUtils.clone(flinkSessionJob);
        statusUpdateCounter.setCurrent(flinkSessionJob);
        return flinkSessionJobController.cleanup(cloned, context);
    }

    @Override
    public Map<String, EventSource> prepareEventSources(
            EventSourceContext<FlinkSessionJob> eventSourceContext) {
        return null;
    }

    public Queue<Event> events() {
        return eventCollector.events;
    }

    private static class StatusUpdateCounter
            implements BiConsumer<FlinkSessionJob, FlinkSessionJobStatus> {

        private FlinkSessionJob currentResource;
        private int counter;

        @Override
        public void accept(
                FlinkSessionJob flinkSessionJobStatusAbstractFlinkResource,
                FlinkSessionJobStatus flinkSessionJobStatus) {
            currentResource.setStatus(flinkSessionJobStatusAbstractFlinkResource.getStatus());
            counter++;
        }

        public void setCurrent(FlinkSessionJob currentResource) {
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
