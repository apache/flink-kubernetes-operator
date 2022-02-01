package org.apache.flink.kubernetes.operator.controller;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.cli.ApplicationDeployer;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.deployment.application.cli.ApplicationClusterDeployer;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.Fabric8FlinkKubeClient;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.JobState;
import org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.JobStatus;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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

    public FlinkDeploymentController(KubernetesClient kubernetesClient, String namespace) {
        this.kubernetesClient = kubernetesClient;
        this.operatorNamespace = namespace;
    }

    @Override
    public DeleteControl cleanup(FlinkDeployment flinkApp, Context context) {
        LOG.info("Cleaning up application cluster {}", flinkApp.getMetadata().getName());
        kubernetesClient
                .apps()
                .deployments()
                .inNamespace(flinkApp.getMetadata().getNamespace())
                .withName(flinkApp.getMetadata().getName())
                .cascading(true)
                .delete();
        return DeleteControl.defaultDelete();
    }

    @Override
    public UpdateControl<FlinkDeployment> reconcile(FlinkDeployment flinkApp, Context context) {
        LOG.info("Reconciling {}", flinkApp.getMetadata().getName());
        final String namespace = flinkApp.getMetadata().getNamespace();
        final String clusterId = flinkApp.getMetadata().getName();

        final Configuration effectiveConfig =
                FlinkUtils.getEffectiveConfig(namespace, clusterId, flinkApp.getSpec());
        if (flinkApp.getStatus() == null
                && flinkApp.getSpec().getJob().getState().equals(JobState.RUNNING)) {
            try {
                flinkApp.setStatus(new FlinkDeploymentStatus());
                deployFlinkJob(flinkApp, effectiveConfig, Optional.empty());
            } catch (Exception e) {
                LOG.error("Error while deploying " + flinkApp.getMetadata().getName());
                return UpdateControl.<FlinkDeployment>noUpdate()
                        .rescheduleAfter(JOB_REFRESH_SECONDS, TimeUnit.SECONDS);
            }
        } else {
            boolean validStatus = updateFlinkJobStatus(flinkApp, effectiveConfig);
            if (validStatus) {
                try {
                    reconcileDeploymentChanges(flinkApp, effectiveConfig);
                } catch (Exception e) {
                    throw new RuntimeException(
                            "Could not reconcile deployment change for "
                                    + flinkApp.getMetadata().getName(),
                            e);
                }
            }
        }

        flinkApp.getStatus().setSpec(flinkApp.getSpec());

        return UpdateControl.updateStatus(flinkApp)
                .rescheduleAfter(JOB_REFRESH_SECONDS, TimeUnit.SECONDS);
    }

    private void deployFlinkJob(
            FlinkDeployment flinkApp, Configuration effectiveConfig, Optional<String> savepoint)
            throws Exception {
        LOG.info("Deploying {}", flinkApp.getMetadata().getName());
        if (savepoint.isPresent()) {
            effectiveConfig.set(SavepointConfigOptions.SAVEPOINT_PATH, savepoint.get());
        } else {
            effectiveConfig.removeConfig(SavepointConfigOptions.SAVEPOINT_PATH);
        }
        final ClusterClientServiceLoader clusterClientServiceLoader =
                new DefaultClusterClientServiceLoader();
        final ApplicationDeployer deployer =
                new ApplicationClusterDeployer(clusterClientServiceLoader);

        final ApplicationConfiguration applicationConfiguration =
                new ApplicationConfiguration(
                        flinkApp.getSpec().getJob().getArgs(),
                        flinkApp.getSpec().getJob().getEntryClass());

        deployer.run(effectiveConfig, applicationConfiguration);
        flinkApp.getStatus().setJobStatuses(null);
        LOG.info("{} deployed", flinkApp.getMetadata().getName());
    }

    private boolean updateFlinkJobStatus(FlinkDeployment flinkApp, Configuration effectiveConfig) {
        if (!flinkApp.getStatus().getSpec().getJob().getState().equals(JobState.RUNNING)) {
            return true;
        }
        LOG.info("Getting job statuses for {}", flinkApp.getMetadata().getName());
        FlinkDeploymentStatus flinkAppStatus = flinkApp.getStatus();
        try (ClusterClient<String> clusterClient =
                FlinkUtils.getRestClusterClient(effectiveConfig)) {
            Collection<JobStatusMessage> clusterJobStatuses =
                    clusterClient.listJobs().get(10, TimeUnit.SECONDS);
            flinkAppStatus.setJobStatuses(
                    mergeJobStatuses(flinkAppStatus.getJobStatuses(), clusterJobStatuses));
            if (clusterJobStatuses.isEmpty()) {
                LOG.info("No jobs found on {} yet, retrying...", flinkApp.getMetadata().getName());
                return false;
            } else {
                LOG.info("Job statuses updated for {}", flinkApp.getMetadata().getName());
                return true;
            }

        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Failed to list jobs for {}", flinkApp, e);
            } else {
                LOG.warn(
                        "Failed to list jobs for {}, retrying...",
                        flinkApp.getMetadata().getName());
            }

            return false;
        }
    }

    /**
     * Merge previous job statuses with the new ones from the flink job cluster. We match jobs by
     * their name to preserve savepoint information.
     *
     * @return
     */
    private List<JobStatus> mergeJobStatuses(
            List<JobStatus> oldJobStatuses, Collection<JobStatusMessage> clusterJobStatuses) {
        List<JobStatus> newStatuses =
                oldJobStatuses != null ? new ArrayList<>(oldJobStatuses) : new ArrayList<>();
        Map<String, JobStatus> statusMap =
                newStatuses.stream().collect(Collectors.toMap(JobStatus::getJobName, j -> j));

        clusterJobStatuses.forEach(
                js -> {
                    if (statusMap.containsKey(js.getJobName())) {
                        JobStatus oldStatus = statusMap.get(js.getJobName());
                        oldStatus.setState(js.getJobState().name());
                        oldStatus.setJobId(js.getJobId().toHexString());
                    } else {
                        newStatuses.add(FlinkUtils.convert(js));
                    }
                });

        return newStatuses;
    }

    private void reconcileDeploymentChanges(FlinkDeployment flinkApp, Configuration effectiveConfig)
            throws Exception {
        boolean specChanged = !flinkApp.getSpec().equals(flinkApp.getStatus().getSpec());

        if (specChanged) {
            JobState currentJobState = flinkApp.getStatus().getSpec().getJob().getState();
            JobState desiredJobState = flinkApp.getSpec().getJob().getState();
            UpgradeMode upgradeMode = flinkApp.getSpec().getJob().getUpgradeMode();
            if (currentJobState == JobState.RUNNING) {
                if (desiredJobState == JobState.RUNNING) {
                    upgradeFlinkJob(flinkApp, effectiveConfig);
                }
                if (desiredJobState.equals(JobState.SUSPENDED)) {
                    if (upgradeMode == UpgradeMode.STATELESS) {
                        cancelJob(flinkApp, effectiveConfig);
                    } else {
                        suspendJob(flinkApp, effectiveConfig);
                    }
                }
            }
            if (currentJobState == JobState.SUSPENDED) {
                if (desiredJobState == JobState.RUNNING) {
                    if (upgradeMode == UpgradeMode.STATELESS) {
                        deployFlinkJob(flinkApp, effectiveConfig, Optional.empty());
                    } else if (upgradeMode == UpgradeMode.SAVEPOINT) {
                        restoreFromLastSavepoint(flinkApp, effectiveConfig);
                    } else {
                        throw new UnsupportedOperationException(
                                "Only savepoint and stateless strategies are supported at the moment.");
                    }
                }
            }
        }
    }

    private void upgradeFlinkJob(FlinkDeployment flinkApp, Configuration effectiveConfig)
            throws Exception {
        LOG.info("Upgrading running job");
        Optional<String> savepoint = cancelJob(flinkApp, effectiveConfig);
        deployFlinkJob(flinkApp, effectiveConfig, savepoint);
    }

    private void restoreFromLastSavepoint(FlinkDeployment flinkApp, Configuration effectiveConfig)
            throws Exception {
        JobStatus jobStatus = flinkApp.getStatus().getJobStatuses().get(0);

        String savepointLocation = jobStatus.getSavepointLocation();
        if (savepointLocation == null) {
            throw new RuntimeException("Cannot perform stateful restore without a valid savepoint");
        }
        deployFlinkJob(flinkApp, effectiveConfig, Optional.of(savepointLocation));
    }

    private Optional<String> suspendJob(FlinkDeployment flinkApp, Configuration effectiveConfig)
            throws Exception {
        LOG.info("Suspending {}", flinkApp.getMetadata().getName());
        JobID jobID = JobID.fromHexString(flinkApp.getStatus().getJobStatuses().get(0).getJobId());
        return cancelJob(flinkApp, jobID, UpgradeMode.SAVEPOINT, effectiveConfig);
    }

    private Optional<String> cancelJob(FlinkDeployment flinkApp, Configuration effectiveConfig)
            throws Exception {
        LOG.info("Cancelling {}", flinkApp.getMetadata().getName());
        UpgradeMode upgradeMode = flinkApp.getSpec().getJob().getUpgradeMode();
        JobID jobID = JobID.fromHexString(flinkApp.getStatus().getJobStatuses().get(0).getJobId());
        return cancelJob(flinkApp, jobID, upgradeMode, effectiveConfig);
    }

    private Optional<String> cancelJob(
            FlinkDeployment flinkApp,
            JobID jobID,
            UpgradeMode upgradeMode,
            Configuration effectiveConfig)
            throws Exception {
        Optional<String> ret = Optional.empty();
        try (ClusterClient<String> clusterClient =
                FlinkUtils.getRestClusterClient(effectiveConfig)) {
            switch (upgradeMode) {
                case STATELESS:
                    clusterClient.cancel(jobID).get(1, TimeUnit.MINUTES);
                    break;
                case SAVEPOINT:
                    String savepoint =
                            clusterClient
                                    .stopWithSavepoint(jobID, false, null)
                                    .get(1, TimeUnit.MINUTES);
                    ret = Optional.of(savepoint);
                    break;
                default:
                    throw new RuntimeException("Unsupported upgrade mode " + upgradeMode);
            }
        }
        waitForClusterShutdown(effectiveConfig);
        JobStatus jobStatus =
                flinkApp.getStatus().getJobStatuses().stream()
                        .filter(j -> j.getJobId().equals(jobID.toHexString()))
                        .findFirst()
                        .get();
        jobStatus.setState("suspended");
        jobStatus.setSavepointLocation(ret.orElse(null));
        return ret;
    }

    /** We need this due to the buggy flink kube cluster client behaviour for now. */
    private void waitForClusterShutdown(Configuration effectiveConfig) throws InterruptedException {
        Fabric8FlinkKubeClient flinkKubeClient =
                new Fabric8FlinkKubeClient(
                        effectiveConfig,
                        (NamespacedKubernetesClient) kubernetesClient,
                        Executors.newSingleThreadExecutor());
        for (int i = 0; i < 60; i++) {
            if (!flinkKubeClient
                    .getRestEndpoint(effectiveConfig.get(KubernetesConfigOptions.CLUSTER_ID))
                    .isPresent()) {
                break;
            }
            LOG.info("Waiting for cluster shutdown... ({})", i);
            Thread.sleep(1000);
        }
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
