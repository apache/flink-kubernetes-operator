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
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.exception.DeploymentFailedException;
import org.apache.flink.kubernetes.operator.exception.ReconciliationException;
import org.apache.flink.kubernetes.operator.observer.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.observer.ObserverFactory;
import org.apache.flink.kubernetes.operator.reconciler.ReconcilerFactory;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.kubernetes.operator.utils.OperatorUtils;
import org.apache.flink.kubernetes.operator.validation.FlinkDeploymentValidator;
import org.apache.flink.util.Preconditions;

import io.fabric8.kubernetes.api.model.Event;
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

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** Controller that runs the main reconcile loop for Flink deployments. */
@ControllerConfiguration
public class FlinkDeploymentController
        implements Reconciler<FlinkDeployment>,
                ErrorStatusHandler<FlinkDeployment>,
                EventSourceInitializer<FlinkDeployment> {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkDeploymentController.class);

    private final KubernetesClient kubernetesClient;

    private final String operatorNamespace;

    private final FlinkDeploymentValidator validator;
    private final ReconcilerFactory reconcilerFactory;
    private final ObserverFactory observerFactory;
    private final DefaultConfig defaultConfig;
    private final FlinkOperatorConfiguration operatorConfiguration;

    private FlinkControllerConfig controllerConfig;

    public FlinkDeploymentController(
            DefaultConfig defaultConfig,
            FlinkOperatorConfiguration operatorConfiguration,
            KubernetesClient kubernetesClient,
            String operatorNamespace,
            FlinkDeploymentValidator validator,
            ReconcilerFactory reconcilerFactory,
            ObserverFactory observerFactory) {
        this.defaultConfig = defaultConfig;
        this.operatorConfiguration = operatorConfiguration;
        this.kubernetesClient = kubernetesClient;
        this.operatorNamespace = operatorNamespace;
        this.validator = validator;
        this.reconcilerFactory = reconcilerFactory;
        this.observerFactory = observerFactory;
    }

    @Override
    public DeleteControl cleanup(FlinkDeployment flinkApp, Context context) {
        LOG.info("Stopping cluster {}", flinkApp.getMetadata().getName());
        Configuration effectiveConfig =
                FlinkUtils.getEffectiveConfig(flinkApp, defaultConfig.getFlinkConfig());
        try {
            observerFactory.getOrCreate(flinkApp).observe(flinkApp, context, effectiveConfig);
        } catch (DeploymentFailedException dfe) {
            // ignore during cleanup
        }
        return reconcilerFactory.getOrCreate(flinkApp).cleanup(flinkApp, effectiveConfig);
    }

    @Override
    public UpdateControl<FlinkDeployment> reconcile(FlinkDeployment flinkApp, Context context) {
        FlinkDeployment originalCopy = ReconciliationUtils.clone(flinkApp);
        LOG.info("Reconciling {}", flinkApp.getMetadata().getName());

        Optional<String> validationError = validator.validate(flinkApp);
        if (validationError.isPresent()) {
            LOG.error("Reconciliation failed: " + validationError.get());
            ReconciliationUtils.updateForReconciliationError(flinkApp, validationError.get());
            return ReconciliationUtils.toUpdateControl(originalCopy, flinkApp);
        }

        Configuration effectiveConfig =
                FlinkUtils.getEffectiveConfig(flinkApp, defaultConfig.getFlinkConfig());

        try {
            observerFactory.getOrCreate(flinkApp).observe(flinkApp, context, effectiveConfig);
            reconcilerFactory.getOrCreate(flinkApp).reconcile(flinkApp, context, effectiveConfig);
        } catch (DeploymentFailedException dfe) {
            handleDeploymentFailed(flinkApp, dfe);
        } catch (Exception e) {
            throw new ReconciliationException(e);
        }

        Duration rescheduleAfter =
                flinkApp.getStatus()
                        .getJobManagerDeploymentStatus()
                        .rescheduleAfter(flinkApp, operatorConfiguration);
        return ReconciliationUtils.toUpdateControl(originalCopy, flinkApp)
                .rescheduleAfter(rescheduleAfter.toMillis());
    }

    private void handleDeploymentFailed(FlinkDeployment flinkApp, DeploymentFailedException dfe) {
        LOG.error(
                "Deployment {}/{} failed with {}",
                flinkApp.getMetadata().getNamespace(),
                flinkApp.getMetadata().getName(),
                dfe.getMessage());
        flinkApp.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.ERROR);
        ReconciliationUtils.updateForReconciliationError(flinkApp, dfe.getMessage());

        // TODO: avoid repeated event
        Event event = DeploymentFailedException.asEvent(dfe, flinkApp);
        kubernetesClient
                .v1()
                .events()
                .inNamespace(flinkApp.getMetadata().getNamespace())
                .create(event);
    }

    @Override
    public List<EventSource> prepareEventSources(EventSourceContext<FlinkDeployment> ctx) {
        Preconditions.checkNotNull(controllerConfig, "Controller config cannot be null");
        Set<String> effectiveNamespaces = controllerConfig.getEffectiveNamespaces();
        if (effectiveNamespaces.isEmpty()) {
            return List.of(OperatorUtils.createJmDepInformerEventSource(kubernetesClient));
        } else {
            return effectiveNamespaces.stream()
                    .map(ns -> OperatorUtils.createJmDepInformerEventSource(kubernetesClient, ns))
                    .collect(Collectors.toList());
        }
    }

    @Override
    public Optional<FlinkDeployment> updateErrorStatus(
            FlinkDeployment flinkApp, RetryInfo retryInfo, RuntimeException e) {
        LOG.warn(
                "attempt count: {}, last attempt: {}",
                retryInfo.getAttemptCount(),
                retryInfo.isLastAttempt());

        ReconciliationUtils.updateForReconciliationError(
                flinkApp,
                (e instanceof ReconciliationException) ? e.getCause().toString() : e.toString());
        return Optional.of(flinkApp);
    }

    public void setControllerConfig(FlinkControllerConfig config) {
        this.controllerConfig = config;
    }
}
