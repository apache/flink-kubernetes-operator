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

package org.apache.flink.kubernetes.operator.observer.minicluster;

import org.apache.flink.kubernetes.operator.api.CrdConstants;
import org.apache.flink.kubernetes.operator.api.FlinkMiniCluster;
import org.apache.flink.kubernetes.operator.api.status.FlinkMiniClusterStatus;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.observer.AbstractClusterDeploymentObserver;
import org.apache.flink.kubernetes.operator.observer.ClusterHealthObserver;
import org.apache.flink.kubernetes.operator.observer.JobStatusObserver;
import org.apache.flink.kubernetes.operator.observer.SnapshotObserver;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.service.AbstractFlinkService;
import org.apache.flink.kubernetes.operator.service.minicluster.MiniClusterFlinkService;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentSpec;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_CLUSTER_HEALTH_CHECK_ENABLED;

/**
 * Observer for a {@link FlinkMiniCluster}.
 *
 * <p>The Deployment of the MiniCluster pod is observed into {@code status.podDeploymentStatus}.
 * Once the pod is ready, the job, its savepoints and checkpoints, its health, and the cluster info
 * are observed through the REST API of the MiniCluster.
 *
 * <p>The REST endpoint runs in the same process as the job, so it is unreachable while the pod
 * restarts. That moves the job back to {@code RECONCILING} until the endpoint is reachable again.
 * Once the pod is ready, it is only observed again if a REST call times out, as for the JobManager
 * of a {@code FlinkDeployment}. When the pod is observed, a missing Deployment, failed pod
 * creation, or a container that cannot start, is an error. A Deployment that is still there is
 * observed as being deployed, without an error.
 */
public class MiniClusterObserver extends AbstractClusterDeploymentObserver<FlinkMiniCluster> {

    private final JobStatusObserver<FlinkMiniCluster> jobStatusObserver;
    private final SnapshotObserver<FlinkMiniCluster, FlinkMiniClusterStatus> snapshotObserver;
    private final ClusterHealthObserver clusterHealthObserver;

    public MiniClusterObserver(EventRecorder eventRecorder) {
        super(eventRecorder);
        this.jobStatusObserver = new MiniClusterJobStatusObserver(eventRecorder);
        this.snapshotObserver = new SnapshotObserver<>(eventRecorder);
        this.clusterHealthObserver = new ClusterHealthObserver();
    }

    @Override
    protected void observeFlinkCluster(FlinkResourceContext<FlinkMiniCluster> ctx) {
        if (jobStatusObserver.observe(ctx)) {
            snapshotObserver.observeSavepointStatus(ctx);
            snapshotObserver.observeCheckpointStatus(ctx);
            var status = ctx.getResource().getStatus();
            if (ctx.getObserveConfig().getBoolean(OPERATOR_CLUSTER_HEALTH_CHECK_ENABLED)
                    && !ReconciliationUtils.isJobInTerminalState(status)) {
                clusterHealthObserver.observe(ctx, status.getClusterInfo());
            }
        }
    }

    @Override
    protected void observeClusterInfo(FlinkResourceContext<FlinkMiniCluster> ctx) {
        var miniCluster = ctx.getResource();
        try {
            var clusterInfo =
                    ctx.getFlinkService()
                            .getClusterInfo(
                                    ctx.getObserveConfig(),
                                    miniCluster.getStatus().getJobStatus().getJobId());
            miniCluster.getStatus().getClusterInfo().putAll(clusterInfo.getInfo());
            logger.debug("ClusterInfo: {}", miniCluster.getStatus().getClusterInfo());
        } catch (Exception e) {
            logger.warn("Exception while fetching cluster info", e);
        }
        observeResourceUsage(ctx);
    }

    /**
     * Records the CPU and memory of the MiniCluster, from its container in the Deployment, as that
     * has the resources from both {@code spec.resources} and the pod template.
     */
    private void observeResourceUsage(FlinkResourceContext<FlinkMiniCluster> ctx) {
        var resources =
                ctx.getJosdkContext()
                        .getSecondaryResource(Deployment.class)
                        .flatMap(MiniClusterObserver::getMainContainer)
                        .map(Container::getResources)
                        .orElse(null);
        var clusterInfo = ctx.getResource().getStatus().getClusterInfo();
        recordUsage(
                clusterInfo,
                AbstractFlinkService.FIELD_NAME_TOTAL_CPU,
                resources,
                CrdConstants.CPU,
                q -> String.valueOf(q.getNumericalAmount().doubleValue()));
        recordUsage(
                clusterInfo,
                AbstractFlinkService.FIELD_NAME_TOTAL_MEMORY,
                resources,
                CrdConstants.MEMORY,
                q -> String.valueOf(Quantity.getAmountInBytes(q).longValue()));
    }

    private void recordUsage(
            Map<String, String> clusterInfo,
            String field,
            @Nullable ResourceRequirements resources,
            String key,
            Function<Quantity, String> format) {
        var quantity = getLimitOrRequest(resources, key);
        if (quantity.isEmpty()) {
            clusterInfo.remove(field);
            return;
        }
        try {
            clusterInfo.put(field, format.apply(quantity.get()));
        } catch (IllegalArgumentException e) {
            logger.warn("Could not parse {} {} of the MiniCluster", key, quantity.get(), e);
            clusterInfo.remove(field);
        }
    }

    private static Optional<Container> getMainContainer(Deployment deployment) {
        return Optional.ofNullable(deployment.getSpec())
                .map(DeploymentSpec::getTemplate)
                .map(PodTemplateSpec::getSpec)
                .flatMap(
                        podSpec ->
                                podSpec.getContainers().stream()
                                        .filter(
                                                c ->
                                                        Constants.MAIN_CONTAINER_NAME.equals(
                                                                c.getName()))
                                        .findFirst());
    }

    private static Optional<Quantity> getLimitOrRequest(
            @Nullable ResourceRequirements resources, String key) {
        if (resources == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(resources.getLimits())
                .map(limits -> limits.get(key))
                .or(
                        () ->
                                Optional.ofNullable(resources.getRequests())
                                        .map(requests -> requests.get(key)));
    }

    @Override
    protected PodList getJmPodList(FlinkResourceContext<FlinkMiniCluster> ctx) {
        return MiniClusterFlinkService.of(ctx).getMiniClusterPodList(ctx.getResource());
    }

    @Override
    protected String getJmDescription() {
        return "MiniCluster";
    }

    /** If the REST API times out the pod may no longer be running, so the pod is observed again. */
    private class MiniClusterJobStatusObserver extends JobStatusObserver<FlinkMiniCluster> {
        MiniClusterJobStatusObserver(EventRecorder eventRecorder) {
            super(eventRecorder);
        }

        @Override
        public void onTimeout(FlinkResourceContext<FlinkMiniCluster> ctx) {
            observeJmDeployment(ctx);
        }
    }
}
