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

package org.apache.flink.kubernetes.operator.service.minicluster;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClientFactory;
import org.apache.flink.kubernetes.kubeclient.KubernetesJobManagerSpecification;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkMiniCluster;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.spec.FlinkMiniClusterSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobSpec;
import org.apache.flink.kubernetes.operator.api.status.CommonStatus;
import org.apache.flink.kubernetes.operator.api.status.FlinkMiniClusterStatus;
import org.apache.flink.kubernetes.operator.artifact.ArtifactManager;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.controller.FlinkMiniClusterContext;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.service.AbstractFlinkService;
import org.apache.flink.kubernetes.operator.service.ClusterInfo;
import org.apache.flink.kubernetes.operator.service.SuspendMode;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.concurrent.ExecutorService;

/**
 * {@link org.apache.flink.kubernetes.operator.service.FlinkService} for a {@link FlinkMiniCluster}.
 *
 * <p>A MiniCluster exposes the same REST API as the JobManager of a distributed cluster, so job
 * status, savepoints, checkpoints and cancellation all come from {@link AbstractFlinkService}.
 *
 * <p>This class adds logic specific to how a MiniCluster is deployed: creating and deleting its
 * Deployment, Service and ConfigMap, and finding its pod. There is no session cluster, and a
 * MiniCluster cannot be scaled in place.
 */
public class MiniClusterFlinkService extends AbstractFlinkService {

    private static final Logger LOG = LoggerFactory.getLogger(MiniClusterFlinkService.class);

    public MiniClusterFlinkService(
            KubernetesClient kubernetesClient,
            ArtifactManager artifactManager,
            ExecutorService executorService,
            FlinkOperatorConfiguration operatorConfig) {
        super(kubernetesClient, artifactManager, executorService, operatorConfig);
    }

    /**
     * The MiniCluster service of a MiniCluster context.
     *
     * <p>Every context of a MiniCluster is a {@link FlinkMiniClusterContext}, which is created with
     * a {@link MiniClusterFlinkService}.
     *
     * @param ctx context of a MiniCluster
     * @return the Flink service of the context
     */
    public static MiniClusterFlinkService of(FlinkResourceContext<FlinkMiniCluster> ctx) {
        return ((FlinkMiniClusterContext) ctx).getFlinkService();
    }

    /**
     * Deploys a MiniCluster.
     *
     * <p>Similar to {@link #submitApplicationCluster} in {@link AbstractFlinkService} for an
     * application cluster.
     *
     * @param miniCluster MiniCluster resource, which owns the resources created for it
     * @param spec Spec to deploy
     * @param conf Configuration to deploy with
     * @param requireHaMetadata Whether the job must be restored from its HA metadata
     * @throws Exception If the MiniCluster could not be deployed
     */
    public void submitMiniCluster(
            FlinkMiniCluster miniCluster,
            FlinkMiniClusterSpec spec,
            Configuration conf,
            boolean requireHaMetadata)
            throws Exception {
        LOG.info(
                "Deploying MiniCluster{}",
                requireHaMetadata ? " requiring last-state from HA metadata" : "");
        // Operator options are removed from the config deployed to the MiniCluster, so are read
        // before preparing it
        var startupProbeEnabled =
                conf.get(KubernetesOperatorConfigOptions.OPERATOR_JM_STARTUP_PROBE_ENABLED);
        var deployConfig = prepareApplicationClusterDeployment(conf, requireHaMetadata);
        deployMiniCluster(
                MiniClusterPodFactory.build(spec, deployConfig, startupProbeEnabled),
                miniCluster.getMetadata(),
                deployConfig);
    }

    /**
     * Creates the Kubernetes resources of a MiniCluster.
     *
     * <p>The Deployment is created rather than replaced, so an existing Deployment with the same
     * name fails the deployment. The Service and ConfigMap are owned by the Deployment.
     *
     * <p>If the Service or ConfigMap can't be created, the Deployment that was created is deleted,
     * as Flink does for the JobManager of a FlinkDeployment. Otherwise every retry would fail
     * because the Deployment exists. Unlike Flink, a Deployment that the MiniCluster doesn't own is
     * left alone, as the deployment failing because it exists doesn't make it the MiniCluster's.
     *
     * @param resources Kubernetes resources of the MiniCluster
     * @param owner Metadata of the MiniCluster resource, which owns the Deployment
     * @param conf Configuration to deploy with, which names the namespace to deploy to
     */
    protected void deployMiniCluster(
            KubernetesJobManagerSpecification resources, ObjectMeta owner, Configuration conf)
            throws Exception {
        try (var flinkKubeClient = createFlinkKubeClient(conf)) {
            flinkKubeClient.createJobManagerComponent(resources);
        } catch (Exception e) {
            try {
                var created = getDeployment(owner.getNamespace(), owner.getName()).get();
                if (created != null && isOwnedBy(created, owner)) {
                    LOG.warn(
                            "Deleting MiniCluster Deployment {} left by the failed deployment",
                            owner.getName(),
                            e);
                    deleteMiniClusterDeployment(
                            owner.getNamespace(),
                            owner.getName(),
                            operatorConfig.getDeletionPropagation(),
                            conf);
                }
            } catch (Exception cleanupException) {
                e.addSuppressed(cleanupException);
            }
            throw e;
        }
    }

    /**
     * Creates the Flink Kubernetes client that deploys a MiniCluster.
     *
     * <p>The resources of a MiniCluster are built with the Kubernetes model shaded in Flink, so
     * they are created with Flink's client rather than the Kubernetes client of the operator.
     *
     * @param conf Configuration to deploy with
     * @return a new client, which the caller closes
     */
    @VisibleForTesting
    protected FlinkKubeClient createFlinkKubeClient(Configuration conf) {
        return FlinkKubeClientFactory.getInstance().fromConfiguration(conf, "minicluster");
    }

    /**
     * Stops the job of a MiniCluster using the given suspend mode.
     *
     * <p>The same as {@link #cancelJob(FlinkDeployment, SuspendMode, Configuration)} for an
     * application cluster.
     *
     * @param miniCluster MiniCluster resource
     * @param suspendMode How to stop the job
     * @param conf Observe configuration of the MiniCluster
     * @return Result of the cancellation, with the savepoint taken if any
     * @throws Exception If the job could not be stopped
     */
    public CancelResult cancelMiniClusterJob(
            FlinkMiniCluster miniCluster, SuspendMode suspendMode, Configuration conf)
            throws Exception {
        return cancelJob(miniCluster, suspendMode, conf, false);
    }

    /**
     * Deletes the Kubernetes resources of a MiniCluster.
     *
     * <p>The same as {@link #deleteClusterDeployment} for the JobManager Deployment of an
     * application cluster.
     *
     * @param meta Metadata of the MiniCluster resource
     * @param status Status of the MiniCluster resource
     * @param conf Configuration of the MiniCluster
     * @param deleteHaData Whether to delete the HA metadata too
     */
    public void deleteMiniCluster(
            ObjectMeta meta,
            FlinkMiniClusterStatus status,
            Configuration conf,
            boolean deleteHaData) {
        deleteCluster(meta, status, conf, deleteHaData);
    }

    /**
     * Information about the MiniCluster: its Flink version, the state size of its job, and the
     * number of TaskManagers running embedded within it.
     *
     * <p>The total CPU and memory reported for a distributed cluster are derived from the
     * JobManager and TaskManager resource configuration, which does not apply to a MiniCluster.
     * They are derived from the resources of the MiniCluster pod by the observer instead.
     */
    @Override
    public ClusterInfo getClusterInfo(Configuration conf, @Nullable String jobId) throws Exception {
        var clusterInfo = new HashMap<String, String>();
        populateFlinkVersion(conf, clusterInfo);
        populateStateSize(conf, jobId, clusterInfo);
        var taskManagerReplicas = getTaskManagersInfo(conf).getTaskManagerInfos().size();
        return new ClusterInfo(clusterInfo, taskManagerReplicas);
    }

    /**
     * Lists the pods of a MiniCluster.
     *
     * @param miniCluster MiniCluster resource
     * @return the pods of the MiniCluster
     */
    public PodList getMiniClusterPodList(FlinkMiniCluster miniCluster) {
        return getJmPodList(
                miniCluster.getMetadata().getNamespace(), miniCluster.getMetadata().getName());
    }

    @Override
    protected PodList getJmPodList(String namespace, String clusterId) {
        return kubernetesClient
                .pods()
                .inNamespace(namespace)
                .withLabels(MiniClusterPodFactory.getSelectorLabels(clusterId))
                .list();
    }

    /**
     * Deletes the cluster of a MiniCluster, unless a Deployment with its name exists that it
     * doesn't own.
     *
     * <p>A Deployment with the MiniCluster's name may already exist without belonging to the
     * MiniCluster. In this case, the Deployment will be left in place (along with any HA metadata
     * for its cluster ID, as whatever is running in the Deployment may be using it).
     *
     * <p>If no unowned Deployment exists, any HA metadata is found by cluster ID and deleted.
     */
    @Override
    protected void deleteCluster(
            ObjectMeta meta, CommonStatus<?> status, Configuration conf, boolean deleteHaData) {
        var deployment = getDeployment(meta.getNamespace(), meta.getName()).get();
        if (deployment != null && !isOwnedBy(deployment, meta)) {
            LOG.warn(
                    "Not deleting Deployment {} or its HA metadata, as it is not owned by this FlinkMiniCluster",
                    meta.getName());
            updateStatusAfterClusterDeletion(status);
            return;
        }
        super.deleteCluster(meta, status, conf, deleteHaData);
    }

    private Resource<Deployment> getDeployment(String namespace, String name) {
        return kubernetesClient.apps().deployments().inNamespace(namespace).withName(name);
    }

    private static boolean isOwnedBy(Deployment deployment, ObjectMeta owner) {
        return deployment.getMetadata().getOwnerReferences().stream()
                .anyMatch(ref -> ref.getUid() != null && ref.getUid().equals(owner.getUid()));
    }

    @Override
    protected void deleteClusterInternal(
            String namespace,
            String clusterId,
            Configuration conf,
            DeletionPropagation deletionPropagation) {
        // Wait for the Deployment to be deleted before a replacement can be created with the same
        // name. Whether the pod has also gone by then depends on the deletion propagation.
        deleteMiniClusterDeployment(namespace, clusterId, deletionPropagation, conf);
    }

    private void deleteMiniClusterDeployment(
            String namespace,
            String clusterId,
            DeletionPropagation deletionPropagation,
            Configuration conf) {
        deleteDeploymentBlockingWithForceDelete(
                "MiniCluster",
                getDeployment(namespace, clusterId),
                deletionPropagation,
                operatorConfig.getFlinkShutdownClusterTimeout(),
                MiniClusterPodFactory.getSelectorLabels(clusterId),
                conf);
    }

    @Override
    protected void deployApplicationCluster(JobSpec jobSpec, Configuration conf) {
        throw new UnsupportedOperationException(
                "A FlinkMiniCluster is deployed with submitMiniCluster");
    }

    @Override
    protected void deploySessionCluster(Configuration conf) {
        throw new UnsupportedOperationException("A FlinkMiniCluster has no session cluster");
    }

    @Override
    public CancelResult cancelJob(
            FlinkDeployment deployment, SuspendMode suspendMode, Configuration conf) {
        throw new UnsupportedOperationException(
                "The job of a FlinkMiniCluster is cancelled with cancelMiniClusterJob");
    }

    @Override
    public CancelResult cancelSessionJob(
            FlinkSessionJob sessionJob, SuspendMode suspendMode, Configuration conf) {
        throw new UnsupportedOperationException("A FlinkMiniCluster has no session jobs");
    }

    @Override
    public boolean scale(FlinkResourceContext<?> ctx, Configuration deployConfig) {
        return false;
    }
}
