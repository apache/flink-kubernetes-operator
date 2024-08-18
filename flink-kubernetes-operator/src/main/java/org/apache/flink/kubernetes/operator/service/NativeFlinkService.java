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

package org.apache.flink.kubernetes.operator.service;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.cli.ApplicationDeployer;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.deployment.application.cli.ApplicationClusterDeployer;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.JobSpec;
import org.apache.flink.kubernetes.operator.artifact.ArtifactManager;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.JobVertexResourceRequirements;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.job.JobResourceRequirementsBody;
import org.apache.flink.runtime.rest.messages.job.JobResourceRequirementsHeaders;
import org.apache.flink.runtime.rest.messages.job.JobResourcesRequirementsUpdateHeaders;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.EditReplacePatchable;
import io.fabric8.kubernetes.client.dsl.base.PatchContext;
import io.fabric8.kubernetes.client.dsl.base.PatchType;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.kubernetes.operator.config.FlinkConfigBuilder.FLINK_VERSION;

/**
 * Implementation of {@link FlinkService} submitting and interacting with Native Kubernetes Flink
 * clusters and jobs.
 */
public class NativeFlinkService extends AbstractFlinkService {

    private static final Logger LOG = LoggerFactory.getLogger(NativeFlinkService.class);
    private static final Deployment SCALE_TO_ZERO =
            new DeploymentBuilder().editOrNewSpec().withReplicas(0).endSpec().build();
    private static final Duration JM_SHUTDOWN_MAX_WAIT = Duration.ofMinutes(1);
    private final EventRecorder eventRecorder;

    public NativeFlinkService(
            KubernetesClient kubernetesClient,
            ArtifactManager artifactManager,
            ExecutorService executorService,
            FlinkOperatorConfiguration operatorConfig,
            EventRecorder eventRecorder) {
        super(kubernetesClient, artifactManager, executorService, operatorConfig);
        this.eventRecorder = eventRecorder;
    }

    @Override
    protected void deployApplicationCluster(JobSpec jobSpec, Configuration conf) throws Exception {
        LOG.info("Deploying application cluster");
        final ClusterClientServiceLoader clusterClientServiceLoader =
                new DefaultClusterClientServiceLoader();
        final ApplicationDeployer deployer =
                new ApplicationClusterDeployer(clusterClientServiceLoader);

        final ApplicationConfiguration applicationConfiguration =
                new ApplicationConfiguration(
                        jobSpec.getArgs() != null ? jobSpec.getArgs() : new String[0],
                        jobSpec.getEntryClass());

        deployer.run(conf, applicationConfiguration);
        LOG.info("Application cluster successfully deployed");
    }

    @Override
    public void deploySessionCluster(Configuration conf) throws Exception {
        submitClusterInternal(removeOperatorConfigs(conf));
    }

    @Override
    public CancelResult cancelJob(
            FlinkDeployment deployment, SuspendMode suspendMode, Configuration configuration)
            throws Exception {
        return cancelJob(deployment, suspendMode, configuration, false);
    }

    @Override
    protected PodList getJmPodList(String namespace, String clusterId) {
        return kubernetesClient
                .pods()
                .inNamespace(namespace)
                .withLabels(KubernetesUtils.getJobManagerSelectors(clusterId))
                .list();
    }

    protected void submitClusterInternal(Configuration conf) throws Exception {
        LOG.info("Deploying session cluster");
        final ClusterClientServiceLoader clusterClientServiceLoader =
                new DefaultClusterClientServiceLoader();
        final ClusterClientFactory<String> kubernetesClusterClientFactory =
                clusterClientServiceLoader.getClusterClientFactory(conf);
        try (final ClusterDescriptor<String> kubernetesClusterDescriptor =
                kubernetesClusterClientFactory.createClusterDescriptor(conf)) {
            kubernetesClusterDescriptor.deploySessionCluster(
                    kubernetesClusterClientFactory.getClusterSpecification(conf));
        }
        LOG.info("Session cluster successfully deployed");
    }

    @Override
    protected void deleteClusterInternal(
            String namespace,
            String clusterId,
            Configuration conf,
            DeletionPropagation deletionPropagation) {

        var jmDeployment =
                kubernetesClient
                        .apps()
                        .deployments()
                        .inNamespace(namespace)
                        .withName(KubernetesUtils.getDeploymentName(clusterId));

        var remainingTimeout = operatorConfig.getFlinkShutdownClusterTimeout();

        // We shut down the JobManager first in the (default) Foreground propagation case to have a
        // cleaner exit
        if (deletionPropagation == DeletionPropagation.FOREGROUND) {
            remainingTimeout =
                    shutdownJobManagersBlocking(
                            jmDeployment, namespace, clusterId, remainingTimeout);
        }
        deleteDeploymentBlocking("JobManager", jmDeployment, deletionPropagation, remainingTimeout);
    }

    @Override
    public boolean scale(FlinkResourceContext<?> ctx, Configuration deployConfig) throws Exception {
        var resource = ctx.getResource();
        var observeConfig = ctx.getObserveConfig();

        if (!supportsInPlaceScaling(resource, observeConfig)) {
            return false;
        }

        var newOverrides = deployConfig.get(PipelineOptions.PARALLELISM_OVERRIDES);
        var previousOverrides = observeConfig.get(PipelineOptions.PARALLELISM_OVERRIDES);
        if (newOverrides.isEmpty() && previousOverrides.isEmpty()) {
            LOG.info("No overrides defined before or after. Cannot scale in-place.");
            return false;
        }

        try (var client = getClusterClient(observeConfig)) {
            var requirements = new HashMap<>(getVertexResources(client, resource));
            var alreadyScaled = true;

            for (Map.Entry<JobVertexID, JobVertexResourceRequirements> entry :
                    requirements.entrySet()) {
                var jobId = entry.getKey().toString();
                var parallelism = entry.getValue().getParallelism();
                var overrideStr = newOverrides.get(jobId);

                if (overrideStr != null) {
                    // We set the parallelism upper bound to the target parallelism, anything higher
                    // would defeat the purpose of scaling down
                    int upperBound = Integer.parseInt(overrideStr);
                    // We only change the lower bound if the new parallelism went below it. As we
                    // cannot guarantee that new resources can be acquired, increasing the lower
                    // bound to the target could potentially cause job failure.
                    int lowerBound = Math.min(upperBound, parallelism.getLowerBound());
                    var newParallelism =
                            new JobVertexResourceRequirements.Parallelism(lowerBound, upperBound);
                    // If the requirements changed we mark this as scaling triggered
                    if (!parallelism.equals(newParallelism)) {
                        entry.setValue(new JobVertexResourceRequirements(newParallelism));
                        alreadyScaled = false;
                    }
                } else if (previousOverrides.containsKey(jobId)) {
                    LOG.info(
                            "Parallelism override for {} has been removed, falling back to regular upgrade.",
                            jobId);
                    return false;
                } else {
                    // No overrides for this vertex
                }
            }
            if (alreadyScaled) {
                LOG.info("Vertex resources requirements already match target, nothing to do...");
            } else {
                updateVertexResources(client, resource, requirements);
                eventRecorder.triggerEvent(
                        resource,
                        EventRecorder.Type.Normal,
                        EventRecorder.Reason.Scaling,
                        EventRecorder.Component.Job,
                        "In-place scaling triggered",
                        ctx.getKubernetesClient());
            }
            return true;
        } catch (Throwable t) {
            LOG.error("Error while rescaling, falling back to regular upgrade", t);
            return false;
        }
    }

    private static boolean supportsInPlaceScaling(
            AbstractFlinkResource<?, ?> resource, Configuration observeConfig) {
        if (resource.getSpec().getJob() == null
                || !observeConfig.get(
                        KubernetesOperatorConfigOptions.JOB_UPGRADE_INPLACE_SCALING_ENABLED)) {
            return false;
        }

        if (!observeConfig.get(FLINK_VERSION).isEqualOrNewer(FlinkVersion.v1_18)) {
            LOG.debug("In-place rescaling is only available starting from Flink 1.18");
            return false;
        }

        if (!observeConfig
                .get(JobManagerOptions.SCHEDULER)
                .equals(JobManagerOptions.SchedulerType.Adaptive)) {
            LOG.debug("In-place rescaling is only available with the adaptive scheduler");
            return false;
        }

        var status = resource.getStatus();
        if (ReconciliationUtils.isJobInTerminalState(status)
                || JobStatus.RECONCILING.equals(status.getJobStatus().getState())) {
            LOG.info("Job in terminal or reconciling state cannot be scaled in-place");
            return false;
        }
        return true;
    }

    @VisibleForTesting
    protected void updateVertexResources(
            RestClusterClient<String> client,
            AbstractFlinkResource<?, ?> resource,
            Map<JobVertexID, JobVertexResourceRequirements> newReqs)
            throws Exception {
        var jobParameters = new JobMessageParameters();
        jobParameters.jobPathParameter.resolve(
                JobID.fromHexString(resource.getStatus().getJobStatus().getJobId()));

        var requestBody = new JobResourceRequirementsBody(new JobResourceRequirements(newReqs));

        client.sendRequest(new JobResourcesRequirementsUpdateHeaders(), jobParameters, requestBody)
                .get(operatorConfig.getFlinkClientTimeout().toSeconds(), TimeUnit.SECONDS);
    }

    @VisibleForTesting
    protected Map<JobVertexID, JobVertexResourceRequirements> getVertexResources(
            RestClusterClient<String> client, AbstractFlinkResource<?, ?> resource)
            throws Exception {
        var jobParameters = new JobMessageParameters();
        jobParameters.jobPathParameter.resolve(
                JobID.fromHexString(resource.getStatus().getJobStatus().getJobId()));

        var currentRequirements =
                client.sendRequest(
                                new JobResourceRequirementsHeaders(),
                                jobParameters,
                                EmptyRequestBody.getInstance())
                        .get(operatorConfig.getFlinkClientTimeout().toSeconds(), TimeUnit.SECONDS);

        return currentRequirements.asJobResourceRequirements().get().getJobVertexParallelisms();
    }

    /**
     * Shut down JobManagers gracefully by scaling JM deployment to zero. This avoids race
     * conditions between JM shutdown and TM shutdown / failure handling.
     *
     * @param jmDeployment
     * @param namespace
     * @param clusterId
     * @param remainingTimeout
     * @return Remaining timeout after the operation.
     */
    private Duration shutdownJobManagersBlocking(
            EditReplacePatchable<Deployment> jmDeployment,
            String namespace,
            String clusterId,
            Duration remainingTimeout) {

        // We use only half of the shutdown timeout but at most one minute as the main point
        // here is to initiate JM shutdown before the TMs
        var jmShutdownTimeout =
                ObjectUtils.min(JM_SHUTDOWN_MAX_WAIT, remainingTimeout.dividedBy(2));
        var remaining =
                deleteBlocking(
                        "Scaling JobManager Deployment to zero",
                        () -> {
                            try {
                                jmDeployment.patch(
                                        PatchContext.of(PatchType.JSON_MERGE), SCALE_TO_ZERO);
                            } catch (Exception ignore) {
                                // Ignore all errors here as this is an optional step
                                return null;
                            }
                            return kubernetesClient
                                    .pods()
                                    .inNamespace(namespace)
                                    .withLabels(KubernetesUtils.getJobManagerSelectors(clusterId));
                        },
                        jmShutdownTimeout);
        return remainingTimeout.minus(jmShutdownTimeout).plus(remaining);
    }
}
