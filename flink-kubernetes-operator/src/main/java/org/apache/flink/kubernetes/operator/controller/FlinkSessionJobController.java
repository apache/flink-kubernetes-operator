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
import org.apache.flink.kubernetes.operator.crd.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.exception.ReconciliationException;
import org.apache.flink.kubernetes.operator.metrics.MetricManager;
import org.apache.flink.kubernetes.operator.observer.Observer;
import org.apache.flink.kubernetes.operator.reconciler.Reconciler;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.utils.OperatorUtils;
import org.apache.flink.kubernetes.operator.validation.FlinkResourceValidator;
import org.apache.flink.util.Preconditions;

import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.FilterWatchListDeletable;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusHandler;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceInitializer;
import io.javaoperatorsdk.operator.api.reconciler.RetryInfo;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.PrimaryResourcesRetriever;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Controller that runs the main reconcile loop for {@link FlinkSessionJob}. */
@ControllerConfiguration
public class FlinkSessionJobController
        implements io.javaoperatorsdk.operator.api.reconciler.Reconciler<FlinkSessionJob>,
                ErrorStatusHandler<FlinkSessionJob>,
                EventSourceInitializer<FlinkSessionJob> {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkSessionJobController.class);
    private static final String CLUSTER_ID_INDEX = "clusterId_index";
    private static final String ALL_NAMESPACE = "allNamespace";

    private final FlinkConfigManager configManager;
    private final KubernetesClient kubernetesClient;

    private final Set<FlinkResourceValidator> validators;
    private final Reconciler<FlinkSessionJob> reconciler;
    private final Observer<FlinkSessionJob> observer;
    private final MetricManager<FlinkSessionJob> metricManager;
    private Map<String, SharedIndexInformer<FlinkSessionJob>> informers;
    private FlinkControllerConfig<FlinkSessionJob> controllerConfig;

    public FlinkSessionJobController(
            FlinkConfigManager configManager,
            KubernetesClient kubernetesClient,
            Set<FlinkResourceValidator> validators,
            Reconciler<FlinkSessionJob> reconciler,
            Observer<FlinkSessionJob> observer,
            MetricManager<FlinkSessionJob> metricManager) {
        this.configManager = configManager;
        this.kubernetesClient = kubernetesClient;
        this.validators = validators;
        this.reconciler = reconciler;
        this.observer = observer;
        this.metricManager = metricManager;
    }

    public void init(FlinkControllerConfig<FlinkSessionJob> config) {
        this.controllerConfig = config;
        this.informers = createInformers();
    }

    @Override
    public UpdateControl<FlinkSessionJob> reconcile(
            FlinkSessionJob flinkSessionJob, Context context) {
        LOG.info("Starting reconciliation");
        FlinkSessionJob originalCopy = ReconciliationUtils.clone(flinkSessionJob);
        observer.observe(flinkSessionJob, context);
        if (!validateSessionJob(flinkSessionJob, context)) {
            metricManager.onUpdate(flinkSessionJob);
            return ReconciliationUtils.toUpdateControl(flinkSessionJob, flinkSessionJob);
        }

        try {
            // TODO refactor the reconciler interface to return UpdateControl directly
            reconciler.reconcile(flinkSessionJob, context);
        } catch (Exception e) {
            throw new ReconciliationException(e);
        }
        metricManager.onUpdate(flinkSessionJob);
        return ReconciliationUtils.toUpdateControl(originalCopy, flinkSessionJob)
                .rescheduleAfter(
                        configManager.getOperatorConfiguration().getReconcileInterval().toMillis());
    }

    @Override
    public DeleteControl cleanup(FlinkSessionJob sessionJob, Context context) {
        LOG.info("Deleting FlinkSessionJob");
        metricManager.onRemove(sessionJob);
        return reconciler.cleanup(sessionJob, context);
    }

    @Override
    public Optional<FlinkSessionJob> updateErrorStatus(
            FlinkSessionJob flinkSessionJob, RetryInfo retryInfo, RuntimeException e) {
        LOG.warn(
                "Attempt count: {}, last attempt: {}",
                retryInfo.getAttemptCount(),
                retryInfo.isLastAttempt());

        ReconciliationUtils.updateForReconciliationError(
                flinkSessionJob,
                (e instanceof ReconciliationException) ? e.getCause().toString() : e.toString());
        metricManager.onUpdate(flinkSessionJob);
        return Optional.of(flinkSessionJob);
    }

    @Override
    public List<EventSource> prepareEventSources(
            EventSourceContext<FlinkSessionJob> eventSourceContext) {
        Preconditions.checkNotNull(controllerConfig, "Controller config cannot be null");
        Set<String> effectiveNamespaces = controllerConfig.getEffectiveNamespaces();
        if (effectiveNamespaces.isEmpty()) {
            return List.of(
                    createFlinkDepInformerEventSource(
                            kubernetesClient.resources(FlinkDeployment.class).inAnyNamespace(),
                            ALL_NAMESPACE));
        } else {
            return effectiveNamespaces.stream()
                    .map(
                            name ->
                                    createFlinkDepInformerEventSource(
                                            kubernetesClient
                                                    .resources(FlinkDeployment.class)
                                                    .inNamespace(name),
                                            name))
                    .collect(Collectors.toList());
        }
    }

    private InformerEventSource<FlinkDeployment, FlinkSessionJob> createFlinkDepInformerEventSource(
            FilterWatchListDeletable<FlinkDeployment, KubernetesResourceList<FlinkDeployment>>
                    filteredClient,
            String name) {
        return new InformerEventSource<>(
                filteredClient.runnableInformer(0),
                primaryResourceRetriever(),
                sessionJob ->
                        new ResourceID(
                                sessionJob.getSpec().getDeploymentName(),
                                sessionJob.getMetadata().getNamespace()),
                false) {
            @Override
            public String name() {
                return name;
            }
        };
    }

    /**
     * Mapping the {@link FlinkDeployment} session cluster to {@link FlinkSessionJob}. It leverages
     * the informer indexer.
     *
     * @return The {@link PrimaryResourcesRetriever}.
     */
    private PrimaryResourcesRetriever<FlinkDeployment> primaryResourceRetriever() {
        return flinkDeployment -> {
            var namespace = flinkDeployment.getMetadata().getNamespace();
            var informer =
                    controllerConfig.getEffectiveNamespaces().isEmpty()
                            ? informers.get(ALL_NAMESPACE)
                            : informers.get(namespace);

            var sessionJobs =
                    informer.getIndexer()
                            .byIndex(CLUSTER_ID_INDEX, flinkDeployment.getMetadata().getName());
            var resourceIDs = new HashSet<ResourceID>();
            for (FlinkSessionJob sessionJob : sessionJobs) {
                resourceIDs.add(
                        new ResourceID(
                                sessionJob.getMetadata().getName(),
                                sessionJob.getMetadata().getNamespace()));
            }
            LOG.debug(
                    "Find the target resource {} for {} ",
                    resourceIDs,
                    flinkDeployment.getMetadata().getNamespace());
            return resourceIDs;
        };
    }

    /**
     * Create informers for session job to build indexer for cluster to session job relations.
     *
     * @return The different namespace's index informer.
     */
    private Map<String, SharedIndexInformer<FlinkSessionJob>> createInformers() {
        Set<String> effectiveNamespaces = controllerConfig.getEffectiveNamespaces();
        if (effectiveNamespaces.isEmpty()) {
            return Map.of(
                    ALL_NAMESPACE,
                    kubernetesClient
                            .resources(FlinkSessionJob.class)
                            .inAnyNamespace()
                            .withIndexers(clusterToSessionJobIndexer())
                            .inform());
        } else {
            var informers = new HashMap<String, SharedIndexInformer<FlinkSessionJob>>();
            for (String effectiveNamespace : effectiveNamespaces) {
                informers.put(
                        effectiveNamespace,
                        kubernetesClient
                                .resources(FlinkSessionJob.class)
                                .inNamespace(effectiveNamespace)
                                .withIndexers(clusterToSessionJobIndexer())
                                .inform());
            }
            return informers;
        }
    }

    private Map<String, Function<FlinkSessionJob, List<String>>> clusterToSessionJobIndexer() {
        return Map.of(
                CLUSTER_ID_INDEX, sessionJob -> List.of(sessionJob.getSpec().getDeploymentName()));
    }

    private boolean validateSessionJob(FlinkSessionJob sessionJob, Context context) {
        for (FlinkResourceValidator validator : validators) {
            Optional<String> validationError =
                    validator.validateSessionJob(
                            sessionJob,
                            OperatorUtils.getSecondaryResource(
                                    sessionJob, context, configManager.getOperatorConfiguration()));
            if (validationError.isPresent()) {
                return ReconciliationUtils.applyValidationErrorAndResetSpec(
                        sessionJob, validationError.get());
            }
        }
        return true;
    }
}
