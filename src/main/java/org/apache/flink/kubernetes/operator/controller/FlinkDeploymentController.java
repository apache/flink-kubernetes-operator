package org.apache.flink.kubernetes.operator.controller;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.KubernetesClient;

import io.javaoperatorsdk.operator.api.reconciler.*;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import org.apache.flink.client.cli.ApplicationDeployer;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.deployment.application.cli.ApplicationClusterDeployer;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.Utils.FlinkUtils;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.JobStatus;
import org.apache.flink.runtime.client.JobStatusMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@ControllerConfiguration
public class FlinkDeploymentController implements Reconciler<FlinkDeployment>, ErrorStatusHandler<FlinkDeployment>, EventSourceInitializer<FlinkDeployment> {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkDeploymentController.class);
    private static final int POLL_PERIOD = 3000;

    private final KubernetesClient kubernetesClient;

    private final Map<String, String> savepointLocation;

    private final String operatorNamespace;

    public FlinkDeploymentController(KubernetesClient kubernetesClient, String namespace) {
        this.kubernetesClient = kubernetesClient;
        this.operatorNamespace = namespace;
        this.savepointLocation = new HashMap<>();
    }

    @Override
    public DeleteControl cleanup(FlinkDeployment flinkApp, Context context) {
        LOG.info("Cleaning up application cluster {}", flinkApp.getMetadata().getName());
        kubernetesClient.apps().deployments().inNamespace(flinkApp.getMetadata().getNamespace()).withName(
                flinkApp.getMetadata().getName()).cascading(true).delete();
        return DeleteControl.defaultDelete();
    }

    @Override
    public UpdateControl<FlinkDeployment> reconcile(FlinkDeployment flinkApp, Context context) {
        LOG.info("Reconciling application cluster {}", flinkApp.getMetadata().getName());
        final String namespace = flinkApp.getMetadata().getNamespace();
        final String clusterId = flinkApp.getMetadata().getName();
        final Deployment deployment = kubernetesClient.apps().deployments().inNamespace(namespace).withName(clusterId).get();

        final Configuration effectiveConfig;
        try {
            effectiveConfig = FlinkUtils.getEffectiveConfig(namespace, clusterId, flinkApp.getSpec());
        } catch (Exception e) {
            LOG.error("Failed to load configuration", e);
            throw new RuntimeException("Failed to load configuration", e);
        }
        if (deployment == null) {
            LOG.info("Deploying application cluster {}", flinkApp.getMetadata().getName());
            final ClusterClientServiceLoader clusterClientServiceLoader = new DefaultClusterClientServiceLoader();
            final ApplicationDeployer deployer = new ApplicationClusterDeployer(clusterClientServiceLoader);

            final ApplicationConfiguration applicationConfiguration = new ApplicationConfiguration(
                    flinkApp.getSpec().getJob().getArgs(),
                    flinkApp.getSpec().getJob().getEntryClass());
            try {
                deployer.run(effectiveConfig, applicationConfiguration);
            } catch (Exception e) {
                LOG.error("Failed to deploy {}", clusterId, e);
            }
            LOG.info("{} deployed", flinkApp.getMetadata().getName());
            return UpdateControl.<FlinkDeployment>noUpdate().rescheduleAfter(10, TimeUnit.SECONDS);
        } else {
            LOG.info("Getting job statuses for application cluster {}", flinkApp.getMetadata().getName());
            FlinkDeploymentStatus flinkAppStatus = new FlinkDeploymentStatus();
            try (ClusterClient<String> clusterClient = FlinkUtils.getRestClusterClient(effectiveConfig)) {
                final CompletableFuture<Collection<JobStatusMessage>> jobDetailsFuture = clusterClient.listJobs();
                JobStatus[] jobStatuses = jobDetailsFuture.get().stream()
                        .map(FlinkUtils::convert)
                        .toArray(size -> new JobStatus[size]);
                flinkAppStatus.setJobStatuses(jobStatuses);
                flinkApp.setStatus(flinkAppStatus);
                LOG.debug(flinkAppStatus.toString());
                if (flinkApp.getStatus().getJobStatuses().length == 0) {
                    LOG.info("Got no job status for application cluster {} retrying", flinkApp.getMetadata().getName());
                    return UpdateControl.<FlinkDeployment>noUpdate().rescheduleAfter(10, TimeUnit.SECONDS);
                } else {
                    LOG.info("Job statuses updated for application cluster {}", flinkApp.getMetadata().getName());
                    return UpdateControl.updateStatus(flinkApp);
                }
            } catch (Exception e) {
                LOG.warn("Failed to get the job statuses for application cluster {} giving up", flinkApp, e);
                return UpdateControl.noUpdate();
            }
        }
    }

    @Override
    public List<EventSource> prepareEventSources(EventSourceContext<FlinkDeployment> eventSourceContext) {
        // TODO: start status updated
//        return List.of(new PerResourcePollingEventSource<>(
//                new FlinkResourceSupplier, context.getPrimaryCache(), POLL_PERIOD,
//                FlinkApplication.class));
        return Collections.emptyList();
    }

    @Override
    public Optional<FlinkDeployment> updateErrorStatus(FlinkDeployment flinkApp, RetryInfo retryInfo, RuntimeException e) {
        LOG.warn("TODO: handle error status");
        return Optional.empty();
    }


}
