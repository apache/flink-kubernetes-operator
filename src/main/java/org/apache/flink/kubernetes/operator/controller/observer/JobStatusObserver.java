package org.apache.flink.kubernetes.operator.controller.observer;

import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.JobSpec;
import org.apache.flink.kubernetes.operator.crd.spec.JobState;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.JobStatus;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.runtime.client.JobStatusMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** Observes the actual state of the running jobs on the Flink cluster. */
public class JobStatusObserver {

    private static final Logger LOG = LoggerFactory.getLogger(JobStatusObserver.class);

    public boolean observeFlinkJobStatus(FlinkDeployment flinkApp, Configuration effectiveConfig) {
        if (flinkApp.getStatus() == null) {
            // This is the first run, nothing to observe
            return true;
        }

        JobSpec jobSpec = flinkApp.getStatus().getSpec().getJob();

        if (jobSpec == null) {
            // This is a session cluster, nothing to observe
            return true;
        }

        if (!jobSpec.getState().equals(JobState.RUNNING)) {
            // The job is not running, nothing to observe
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
}
