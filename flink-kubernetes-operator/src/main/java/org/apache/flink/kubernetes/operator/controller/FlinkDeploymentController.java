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
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.ReconciliationStatus;
import org.apache.flink.kubernetes.operator.observer.JobStatusObserver;
import org.apache.flink.kubernetes.operator.reconciler.JobReconciler;
import org.apache.flink.kubernetes.operator.reconciler.SessionReconciler;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.kubernetes.operator.utils.IngressUtils;

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

    public static final int OBSERVE_REFRESH_SECONDS = 10;
    public static final int RECONCILE_ERROR_REFRESH_SECONDS = 5;

    private final KubernetesClient kubernetesClient;

    private final String operatorNamespace;

    private final JobStatusObserver observer;
    private final JobReconciler jobReconciler;
    private final SessionReconciler sessionReconciler;

    public FlinkDeploymentController(
            KubernetesClient kubernetesClient,
            String operatorNamespace,
            JobStatusObserver observer,
            JobReconciler jobReconciler,
            SessionReconciler sessionReconciler) {
        this.kubernetesClient = kubernetesClient;
        this.operatorNamespace = operatorNamespace;
        this.observer = observer;
        this.jobReconciler = jobReconciler;
        this.sessionReconciler = sessionReconciler;
    }

    @Override
    public DeleteControl cleanup(FlinkDeployment flinkApp, Context context) {
        LOG.info("Cleaning up application cluster {}", flinkApp.getMetadata().getName());
        FlinkUtils.deleteCluster(flinkApp, kubernetesClient);
        IngressUtils.updateIngressRules(
                flinkApp,
                FlinkUtils.getEffectiveConfig(flinkApp),
                operatorNamespace,
                kubernetesClient,
                true);
        return DeleteControl.defaultDelete();
    }

    @Override
    public UpdateControl<FlinkDeployment> reconcile(FlinkDeployment flinkApp, Context context) {
        LOG.info("Reconciling {}", flinkApp.getMetadata().getName());
        if (flinkApp.getStatus() == null) {
            flinkApp.setStatus(new FlinkDeploymentStatus());
        }

        Configuration effectiveConfig = FlinkUtils.getEffectiveConfig(flinkApp);

        boolean successfulObserve = observer.observeFlinkJobStatus(flinkApp, effectiveConfig);

        if (!successfulObserve) {
            // Cluster not accessible let's retry
            return UpdateControl.<FlinkDeployment>noUpdate()
                    .rescheduleAfter(OBSERVE_REFRESH_SECONDS, TimeUnit.SECONDS);
        }

        if (!specChanged(flinkApp)) {
            // Successfully observed the cluster after reconciliation, no need to reschedule
            return UpdateControl.updateStatus(flinkApp);
        }

        try {
            reconcileFlinkDeployment(operatorNamespace, flinkApp, effectiveConfig);
        } catch (Exception e) {
            String err = "Error while reconciling deployment change: " + e.getMessage();
            String lastErr = flinkApp.getStatus().getReconciliationStatus().getError();
            if (!err.equals(lastErr)) {
                // Log new errors on the first instance
                LOG.error("Error while reconciling deployment change", e);
                updateForReconciliationError(flinkApp, err);
                return UpdateControl.updateStatus(flinkApp)
                        .rescheduleAfter(RECONCILE_ERROR_REFRESH_SECONDS, TimeUnit.SECONDS);
            } else {
                return UpdateControl.<FlinkDeployment>noUpdate()
                        .rescheduleAfter(RECONCILE_ERROR_REFRESH_SECONDS, TimeUnit.SECONDS);
            }
        }

        // Everything went well, update status and reschedule for observation
        updateForReconciliationSuccess(flinkApp);
        return UpdateControl.updateStatus(flinkApp)
                .rescheduleAfter(OBSERVE_REFRESH_SECONDS, TimeUnit.SECONDS);
    }

    private boolean specChanged(FlinkDeployment flinkApp) {
        return !flinkApp.getSpec()
                .equals(flinkApp.getStatus().getReconciliationStatus().getLastReconciledSpec());
    }

    private void reconcileFlinkDeployment(
            String operatorNamespace, FlinkDeployment flinkApp, Configuration effectiveConfig)
            throws Exception {

        if (flinkApp.getSpec().getJob() == null) {
            sessionReconciler.reconcile(operatorNamespace, flinkApp, effectiveConfig);
        } else {
            jobReconciler.reconcile(operatorNamespace, flinkApp, effectiveConfig);
        }
    }

    private void updateForReconciliationSuccess(FlinkDeployment flinkApp) {
        ReconciliationStatus reconciliationStatus = flinkApp.getStatus().getReconciliationStatus();
        reconciliationStatus.setSuccess(true);
        reconciliationStatus.setError(null);
        reconciliationStatus.setLastReconciledSpec(flinkApp.getSpec());
    }

    private void updateForReconciliationError(FlinkDeployment flinkApp, String err) {
        ReconciliationStatus reconciliationStatus = flinkApp.getStatus().getReconciliationStatus();
        reconciliationStatus.setSuccess(false);
        reconciliationStatus.setError(err);
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
