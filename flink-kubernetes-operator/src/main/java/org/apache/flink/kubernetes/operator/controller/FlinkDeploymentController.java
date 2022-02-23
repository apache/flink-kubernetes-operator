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
import org.apache.flink.kubernetes.operator.config.DefaultConfig;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.status.ReconciliationStatus;
import org.apache.flink.kubernetes.operator.exception.InvalidDeploymentException;
import org.apache.flink.kubernetes.operator.exception.ReconciliationException;
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
@ControllerConfiguration(generationAwareEventProcessing = false)
public class FlinkDeploymentController
        implements Reconciler<FlinkDeployment>,
                ErrorStatusHandler<FlinkDeployment>,
                EventSourceInitializer<FlinkDeployment> {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkDeploymentController.class);

    public static final int REFRESH_SECONDS = 60;

    private final KubernetesClient kubernetesClient;

    private final String operatorNamespace;

    private final JobStatusObserver observer;
    private final JobReconciler jobReconciler;
    private final SessionReconciler sessionReconciler;
    private final DefaultConfig defaultConfig;

    public FlinkDeploymentController(
            DefaultConfig defaultConfig,
            KubernetesClient kubernetesClient,
            String operatorNamespace,
            JobStatusObserver observer,
            JobReconciler jobReconciler,
            SessionReconciler sessionReconciler) {
        this.defaultConfig = defaultConfig;
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
                FlinkUtils.getEffectiveConfig(flinkApp, defaultConfig.getFlinkConfig()),
                operatorNamespace,
                kubernetesClient,
                true);
        return DeleteControl.defaultDelete();
    }

    @Override
    public UpdateControl<FlinkDeployment> reconcile(FlinkDeployment flinkApp, Context context) {
        LOG.info("Reconciling {}", flinkApp.getMetadata().getName());

        Configuration effectiveConfig =
                FlinkUtils.getEffectiveConfig(flinkApp, defaultConfig.getFlinkConfig());
        try {
            boolean successfulObserve = observer.observeFlinkJobStatus(flinkApp, effectiveConfig);
            if (successfulObserve) {
                reconcileFlinkDeployment(operatorNamespace, flinkApp, effectiveConfig);
                updateForReconciliationSuccess(flinkApp);
            }
        } catch (InvalidDeploymentException ide) {
            LOG.error("Reconciliation failed", ide);
            updateForReconciliationError(flinkApp, ide.getMessage());
            return UpdateControl.updateStatus(flinkApp);
        } catch (Exception e) {
            throw new ReconciliationException(e);
        }
        return UpdateControl.updateStatus(flinkApp)
                .rescheduleAfter(REFRESH_SECONDS, TimeUnit.SECONDS);
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
        LOG.warn(
                "attempt count: {}, last attempt: {}",
                retryInfo.getAttemptCount(),
                retryInfo.isLastAttempt());

        updateForReconciliationError(
                flinkApp,
                (e instanceof ReconciliationException) ? e.getCause().toString() : e.toString());
        return Optional.of(flinkApp);
    }
}
