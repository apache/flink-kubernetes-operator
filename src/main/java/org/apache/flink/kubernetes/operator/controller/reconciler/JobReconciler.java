package org.apache.flink.kubernetes.operator.controller.reconciler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.cli.ApplicationDeployer;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.deployment.application.cli.ApplicationClusterDeployer;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.JobState;
import org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.JobStatus;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;

import io.fabric8.kubernetes.client.KubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Reconciler responsible for handling the job lifecycle according to the desired and current
 * states.
 */
public class JobReconciler {

    private static final Logger LOG = LoggerFactory.getLogger(JobReconciler.class);

    private final KubernetesClient kubernetesClient;

    public JobReconciler(KubernetesClient kubernetesClient) {
        this.kubernetesClient = kubernetesClient;
    }

    public boolean reconcile(FlinkDeployment flinkApp, Configuration effectiveConfig)
            throws Exception {

        if (flinkApp.getStatus() == null) {
            flinkApp.setStatus(new FlinkDeploymentStatus());
            if (!flinkApp.getSpec().getJob().getState().equals(JobState.RUNNING)) {
                throw new RuntimeException("Job must start in running state");
            }
            try {
                deployFlinkJob(flinkApp, effectiveConfig, Optional.empty());
                return true;
            } catch (Exception e) {
                LOG.error("Error while deploying " + flinkApp.getMetadata().getName());
                return false;
            }
        }

        boolean specChanged = !flinkApp.getSpec().equals(flinkApp.getStatus().getSpec());
        if (specChanged) {
            if (flinkApp.getStatus().getSpec().getJob() == null) {
                throw new RuntimeException("Cannot switch from session to job cluster");
            }
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
        return true;
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
        LOG.info("{} deployed", flinkApp.getMetadata().getName());
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
        FlinkUtils.waitForClusterShutdown(kubernetesClient, effectiveConfig);
        JobStatus jobStatus =
                flinkApp.getStatus().getJobStatuses().stream()
                        .filter(j -> j.getJobId().equals(jobID.toHexString()))
                        .findFirst()
                        .get();
        jobStatus.setState("suspended");
        jobStatus.setSavepointLocation(ret.orElse(null));
        return ret;
    }
}
