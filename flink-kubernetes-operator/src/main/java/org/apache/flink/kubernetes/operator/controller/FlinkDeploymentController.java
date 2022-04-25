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

import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.exception.DeploymentFailedException;
import org.apache.flink.kubernetes.operator.exception.ReconciliationException;
import org.apache.flink.kubernetes.operator.observer.deployment.ObserverFactory;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.reconciler.deployment.ReconcilerFactory;
import org.apache.flink.kubernetes.operator.utils.OperatorUtils;
import org.apache.flink.kubernetes.operator.validation.FlinkResourceValidator;
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

    private final FlinkConfigManager configManager;
    private final KubernetesClient kubernetesClient;

    private final Set<FlinkResourceValidator> validators;
    private final ReconcilerFactory reconcilerFactory;
    private final ObserverFactory observerFactory;

    private FlinkControllerConfig<FlinkDeployment> controllerConfig;

    public FlinkDeploymentController(
            FlinkConfigManager configManager,
            KubernetesClient kubernetesClient,
            Set<FlinkResourceValidator> validators,
            ReconcilerFactory reconcilerFactory,
            ObserverFactory observerFactory) {
        this.configManager = configManager;
        this.kubernetesClient = kubernetesClient;
        this.validators = validators;
        this.reconcilerFactory = reconcilerFactory;
        this.observerFactory = observerFactory;
    }

    @Override
    public DeleteControl cleanup(FlinkDeployment flinkApp, Context context) {
        LOG.info("Deleting FlinkDeployment");
        try {
            observerFactory.getOrCreate(flinkApp).observe(flinkApp, context);
        } catch (DeploymentFailedException dfe) {
            // ignore during cleanup
        }
        return reconcilerFactory.getOrCreate(flinkApp).cleanup(flinkApp, context);
    }

    @Override
    public UpdateControl<FlinkDeployment> reconcile(FlinkDeployment flinkApp, Context context) {
        LOG.info("Starting reconciliation");
        FlinkDeployment originalCopy = ReconciliationUtils.clone(flinkApp);
        try {
            observerFactory.getOrCreate(flinkApp).observe(flinkApp, context);
            Optional<String> validationError = validateDeployment(flinkApp);
            if (validationError.isPresent()) {
                LOG.error("Validation failed: " + validationError.get());
                ReconciliationUtils.updateForReconciliationError(flinkApp, validationError.get());
                return ReconciliationUtils.toUpdateControl(
                        configManager.getOperatorConfiguration(), originalCopy, flinkApp, false);
            }
            reconcilerFactory.getOrCreate(flinkApp).reconcile(flinkApp, context);
        } catch (DeploymentFailedException dfe) {
            handleDeploymentFailed(flinkApp, dfe);
        } catch (Exception e) {
            throw new ReconciliationException(e);
        }

        LOG.info("End of reconciliation");
        return ReconciliationUtils.toUpdateControl(
                configManager.getOperatorConfiguration(), originalCopy, flinkApp, true);
    }

    private void handleDeploymentFailed(FlinkDeployment flinkApp, DeploymentFailedException dfe) {
        LOG.error("Flink Deployment failed", dfe);
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
                "Attempt count: {}, last attempt: {}",
                retryInfo.getAttemptCount(),
                retryInfo.isLastAttempt());

        ReconciliationUtils.updateForReconciliationError(
                flinkApp,
                (e instanceof ReconciliationException) ? e.getCause().toString() : e.toString());
        return Optional.of(flinkApp);
    }

    public void setControllerConfig(FlinkControllerConfig<FlinkDeployment> config) {
        this.controllerConfig = config;
    }

    private Optional<String> validateDeployment(FlinkDeployment deployment) {
        Optional<String> validationError = Optional.empty();
        for (FlinkResourceValidator validator : validators) {
            if ((validationError = validator.validateDeployment(deployment)).isPresent()) {
                break;
            }
        }
        return validationError;
    }
}
