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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.controller.observer.JobStatusObserver;
import org.apache.flink.kubernetes.operator.controller.reconciler.JobReconciler;
import org.apache.flink.kubernetes.operator.controller.reconciler.SessionReconciler;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusHandler;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceInitializer;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.RetryInfo;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/** Controller that runs the main reconcile loop for Flink deployments. */
@ControllerConfiguration
public class FlinkDeploymentController
        implements Reconciler<FlinkDeployment>,
                ErrorStatusHandler<FlinkDeployment>,
                EventSourceInitializer<FlinkDeployment> {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkDeploymentController.class);
    private static final int JOB_REFRESH_SECONDS = 5;

    private final KubernetesClient kubernetesClient;

    private final String operatorNamespace;

    private final JobStatusObserver observer = new JobStatusObserver();
    private final JobReconciler jobReconciler;
    private final SessionReconciler sessionReconciler;

    public FlinkDeploymentController(KubernetesClient kubernetesClient, String namespace) {
        this.kubernetesClient = kubernetesClient;
        this.operatorNamespace = namespace;
        this.jobReconciler = new JobReconciler(kubernetesClient);
        this.sessionReconciler = new SessionReconciler(kubernetesClient);
    }

    @Override
    public DeleteControl cleanup(FlinkDeployment flinkApp, Context context) {
        LOG.info("Cleaning up application cluster {}", flinkApp.getMetadata().getName());
        FlinkUtils.deleteCluster(flinkApp, kubernetesClient);
        return DeleteControl.defaultDelete();
    }

    @Override
    public UpdateControl<FlinkDeployment> reconcile(FlinkDeployment flinkApp, Context context) {
        LOG.info("Reconciling {}", flinkApp.getMetadata().getName());

        Configuration effectiveConfig = FlinkUtils.getEffectiveConfig(flinkApp);

        boolean success = observer.observeFlinkJobStatus(flinkApp, effectiveConfig);
        if (success) {
            try {
                success = reconcileFlinkDeployment(flinkApp, effectiveConfig);
            } catch (Exception e) {
                throw new RuntimeException(
                        "Error while reconciling deployment change for "
                                + flinkApp.getMetadata().getName(),
                        e);
            }
        }

        if (!success) {
            return UpdateControl.<FlinkDeployment>noUpdate()
                    .rescheduleAfter(JOB_REFRESH_SECONDS, TimeUnit.SECONDS);
        }

        flinkApp.getStatus().setSpec(flinkApp.getSpec());
        return UpdateControl.updateStatus(flinkApp)
                .rescheduleAfter(JOB_REFRESH_SECONDS, TimeUnit.SECONDS);
    }

    private boolean reconcileFlinkDeployment(
            FlinkDeployment flinkApp, Configuration effectiveConfig) throws Exception {
        return flinkApp.getSpec().getJob() == null
                ? sessionReconciler.reconcile(flinkApp, effectiveConfig)
                : jobReconciler.reconcile(flinkApp, effectiveConfig);
    }

    @Override
    public List<EventSource> prepareEventSources(
            EventSourceContext<FlinkDeployment> eventSourceContext) {
        // TODO: start status updated
        //        return List.of(new PerResourcePollingEventSource<>(
        //                new FlinkResourceSupplier, context.getPrimaryCache(), POLL_PERIOD,
        //                FlinkApplication.class));
        return Collections.emptyList();
    }

    @Override
    public Optional<FlinkDeployment> updateErrorStatus(
            FlinkDeployment flinkApp, RetryInfo retryInfo, RuntimeException e) {
        LOG.warn("TODO: handle error status");
        return Optional.empty();
    }
}
