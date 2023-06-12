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
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
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
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.rest.messages.job.JobResourceRequirementsBody;
import org.apache.flink.runtime.rest.messages.job.JobResourceRequirementsHeaders;
import org.apache.flink.runtime.rest.messages.job.JobResourcesRequirementsUpdateHeaders;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.kubernetes.operator.config.FlinkConfigBuilder.FLINK_VERSION;

/**
 * Implementation of {@link FlinkService} submitting and interacting with Native Kubernetes Flink
 * clusters and jobs.
 */
public class NativeFlinkService extends AbstractFlinkService {

    private static final Logger LOG = LoggerFactory.getLogger(NativeFlinkService.class);
    private final EventRecorder eventRecorder;

    public NativeFlinkService(
            KubernetesClient kubernetesClient,
            FlinkConfigManager configManager,
            EventRecorder eventRecorder) {
        super(kubernetesClient, configManager);
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
    public void submitSessionCluster(Configuration conf) throws Exception {
        submitClusterInternal(removeOperatorConfigs(conf));
    }

    @Override
    public void cancelJob(
            FlinkDeployment deployment, UpgradeMode upgradeMode, Configuration configuration)
            throws Exception {
        // prior to Flink 1.15, ensure removal of orphaned config maps
        // https://issues.apache.org/jira/browse/FLINK-30004
        boolean deleteClusterAfterSavepoint =
                !deployment.getSpec().getFlinkVersion().isNewerVersionThan(FlinkVersion.v1_14);
        cancelJob(deployment, upgradeMode, configuration, deleteClusterAfterSavepoint);
    }

    @Override
    protected PodList getJmPodList(String namespace, String clusterId) {
        return kubernetesClient
                .pods()
                .inNamespace(namespace)
                .withLabels(KubernetesUtils.getJobManagerSelectors(clusterId))
                .list();
    }

    @Override
    protected PodList getTmPodList(String namespace, String clusterId) {
        // Native mode does not manage TaskManager
        return new PodList();
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
            ObjectMeta meta,
            Configuration conf,
            boolean deleteHaData,
            DeletionPropagation deletionPropagation) {

        String namespace = meta.getNamespace();
        String clusterId = meta.getName();

        LOG.info(
                "Deleting JobManager deployment {}.",
                deleteHaData ? "and HA metadata" : "while preserving HA metadata");
        kubernetesClient
                .apps()
                .deployments()
                .inNamespace(namespace)
                .withName(KubernetesUtils.getDeploymentName(clusterId))
                .withPropagationPolicy(deletionPropagation)
                .delete();

        if (deleteHaData) {
            deleteHAData(namespace, clusterId, conf);
        }
    }

    @Override
    public boolean scale(FlinkResourceContext<?> ctx) throws Exception {
        var resource = ctx.getResource();
        var spec = resource.getSpec();

        var observeConfig = ctx.getObserveConfig();

        if (spec.getJob() == null
                || !observeConfig.get(
                        KubernetesOperatorConfigOptions.JOB_UPGRADE_INPLACE_SCALING_ENABLED)) {
            return false;
        }

        if (!observeConfig.get(FLINK_VERSION).isNewerVersionThan(FlinkVersion.v1_17)) {
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
                || JobStatus.RECONCILING.name().equals(status.getJobStatus().getState())) {
            LOG.info("Job in terminal or reconciling state cannot be scaled in-place");
            return false;
        }

        var deployConfig = ctx.getDeployConfig(spec);
        var newOverrides = deployConfig.get(PipelineOptions.PARALLELISM_OVERRIDES);

        try (var client = getClusterClient(observeConfig)) {
            var currentReqs = getVertexResources(client, resource);
            var newReqs = new HashMap<>(currentReqs);
            newOverrides.forEach(
                    (vs, ps) -> {
                        int p = Integer.parseInt(ps);
                        newReqs.put(
                                JobVertexID.fromHexString(vs),
                                new JobVertexResourceRequirements(
                                        new JobVertexResourceRequirements.Parallelism(p, p)));
                    });

            if (currentReqs.equals(newReqs)) {
                LOG.info("Vertex resources requirements already match target, nothing to do...");
            } else {
                updateVertexResources(client, resource, newReqs);
                eventRecorder.triggerEvent(
                        resource,
                        EventRecorder.Type.Normal,
                        EventRecorder.Reason.Scaling,
                        EventRecorder.Component.Job,
                        "In-place scaling triggered");
            }

            return true;
        } catch (Throwable t) {
            LOG.error("Error while rescaling, falling back to regular upgrade", t);
            return false;
        }
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
                .get(
                        configManager
                                .getOperatorConfiguration()
                                .getFlinkClientTimeout()
                                .toSeconds(),
                        TimeUnit.SECONDS);
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
                        .get(
                                configManager
                                        .getOperatorConfiguration()
                                        .getFlinkClientTimeout()
                                        .toSeconds(),
                                TimeUnit.SECONDS);

        return currentRequirements.asJobResourceRequirements().get().getJobVertexParallelisms();
    }

    @Override
    public boolean scalingCompleted(FlinkResourceContext<?> ctx) {
        var conf = ctx.getObserveConfig();
        var status = ctx.getResource().getStatus();
        try (var client = ctx.getFlinkService().getClusterClient(conf)) {
            var jobId = JobID.fromHexString(status.getJobStatus().getJobId());
            var jobDetailsInfo = client.getJobDetails(jobId).get();

            Map<JobVertexID, Integer> currentParallelisms =
                    jobDetailsInfo.getJobVertexInfos().stream()
                            .collect(
                                    Collectors.toMap(
                                            JobDetailsInfo.JobVertexDetailsInfo::getJobVertexID,
                                            JobDetailsInfo.JobVertexDetailsInfo::getParallelism));

            Map<String, String> parallelismOverrides =
                    conf.get(PipelineOptions.PARALLELISM_OVERRIDES);
            for (Map.Entry<String, String> entry : parallelismOverrides.entrySet()) {
                Integer currentP =
                        currentParallelisms.get(JobVertexID.fromHexString(entry.getKey()));
                Integer expectedP = Integer.valueOf(entry.getValue());
                if (!expectedP.equals(currentP)) {
                    LOG.info(
                            "Scaling still in progress for vertex {}, {} -> {}",
                            entry.getKey(),
                            currentP,
                            expectedP);
                    return false;
                }
            }
            LOG.info("All vertexes have successfully scaled");
            status.getReconciliationStatus().setState(ReconciliationState.DEPLOYED);
            return true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
