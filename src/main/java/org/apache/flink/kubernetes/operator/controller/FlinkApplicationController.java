package org.apache.flink.kubernetes.operator.controller;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.networking.v1.HTTPIngressRuleValueBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.kubernetes.api.model.networking.v1.IngressBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.IngressRule;
import io.fabric8.kubernetes.client.KubernetesClient;

import io.javaoperatorsdk.operator.api.reconciler.*;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.polling.PerResourcePollingEventSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.cli.ApplicationDeployer;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.deployment.application.cli.ApplicationClusterDeployer;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.kubernetes.operator.Utils.FlinkUtils;
import org.apache.flink.kubernetes.operator.Utils.Constants;
import org.apache.flink.kubernetes.operator.Utils.KubernetesUtils;
import org.apache.flink.kubernetes.operator.crd.FlinkApplication;
import org.apache.flink.kubernetes.operator.crd.spec.Resource;
import org.apache.flink.runtime.client.JobStatusMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.kubernetes.operator.Utils.Constants.FLINK_NATIVE_K8S_OPERATOR_NAME;

@ControllerConfiguration
public class FlinkApplicationController implements Reconciler<FlinkApplication>, ErrorStatusHandler<FlinkApplication>, EventSourceInitializer<FlinkApplication> {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkApplicationController.class);
    private static final int POLL_PERIOD = 3000;

    private final KubernetesClient kubernetesClient;

    private final Map<String, Tuple2<FlinkApplication, Configuration>> flinkApps;
    private final Map<String, String> savepointLocation;

    private final String operatorNamespace;

    public FlinkApplicationController(KubernetesClient kubernetesClient, String namespace) {
        this.kubernetesClient = kubernetesClient;
        this.operatorNamespace = namespace;

        this.flinkApps = new ConcurrentHashMap<>();
        this.savepointLocation = new HashMap<>();
    }

    @Override
    public DeleteControl cleanup(FlinkApplication flinkApp, Context context) {
        LOG.info("Cleaning up application {}", flinkApp);
        kubernetesClient.apps().deployments().inNamespace(flinkApp.getMetadata().getNamespace()).withName(flinkApp.getMetadata().getName()).cascading(true).delete();
        return DeleteControl.defaultDelete();
    }

    @Override
    public UpdateControl<FlinkApplication> reconcile(FlinkApplication flinkApp, Context context) {
        LOG.info("Reconciling application {}", flinkApp);
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

        // Create new Flink application
        if (!flinkApps.containsKey(clusterId) && deployment == null) {
            // Deploy application
            final ClusterClientServiceLoader clusterClientServiceLoader = new DefaultClusterClientServiceLoader();
            final ApplicationDeployer deployer = new ApplicationClusterDeployer(clusterClientServiceLoader);

            final ApplicationConfiguration applicationConfiguration = new ApplicationConfiguration(flinkApp.getSpec().getMainArgs(), flinkApp.getSpec().getEntryClass());
            try {
                deployer.run(effectiveConfig, applicationConfiguration);
            } catch (Exception e) {
                LOG.error("Failed to deploy cluster {}", clusterId, e);
            }

            flinkApps.put(clusterId, new Tuple2<>(flinkApp, effectiveConfig));

            updateIngress();
        } else {
            if (!flinkApps.containsKey(clusterId)) {
                LOG.info("Recovering {}", clusterId);
                flinkApps.put(clusterId, new Tuple2<>(flinkApp, effectiveConfig));
                return UpdateControl.noUpdate();
            }
            // Flink app is deleted externally
            if (deployment == null) {
                LOG.warn("{} is delete externally.", clusterId);
                flinkApps.remove(clusterId);
                return UpdateControl.noUpdate();
            }

            FlinkApplication oldFlinkApp = flinkApps.get(clusterId).f0;

            // Trigger a new savepoint
            triggerSavepoint(oldFlinkApp, flinkApp, effectiveConfig);

            // TODO support more fields updating, e.g. image, resources
        }
        return UpdateControl.updateResource(flinkApp);
    }

    @Override
    public List<EventSource> prepareEventSources(EventSourceContext<FlinkApplication> eventSourceContext) {
        // TODO: start status updated
//        return List.of(new PerResourcePollingEventSource<>(
//                new FlinkResourceSupplier, context.getPrimaryCache(), POLL_PERIOD,
//                FlinkApplication.class));
        return Collections.emptyList();
    }

    @Override
    public Optional<FlinkApplication> updateErrorStatus(FlinkApplication flinkApplication, RetryInfo retryInfo, RuntimeException e) {
        //TODO: Set error status
        return Optional.empty();
    }

    private void updateIngress() {
        final List<IngressRule> ingressRules = new ArrayList<>();
        for (Tuple2<FlinkApplication, Configuration> entry : flinkApps.values()) {
            final FlinkApplication flinkApp = entry.f0;
            final String clusterId = flinkApp.getMetadata().getName();
            final int restPort = entry.f1.getInteger(RestOptions.PORT);

            final String ingressHost = clusterId + Constants.INGRESS_SUFFIX;
            ingressRules.add(new IngressRule(ingressHost, new HTTPIngressRuleValueBuilder().addNewPath().withNewBackend().withNewService().withName(clusterId + Constants.REST_SVC_NAME_SUFFIX).withNewPort(null, restPort).endService().endBackend().withPathType("Prefix").withPath("/").endPath().build()));
        }
        final Ingress ingress = new IngressBuilder().withApiVersion(Constants.INGRESS_API_VERSION).withNewMetadata().withName(FLINK_NATIVE_K8S_OPERATOR_NAME).endMetadata().withNewSpec().withRules(ingressRules).endSpec().build();
        // Get operator deploy
        final Deployment deployment = kubernetesClient.apps().deployments().inNamespace(operatorNamespace).withName(FLINK_NATIVE_K8S_OPERATOR_NAME).get();
        if (deployment == null) {
            LOG.warn("Could not find deployment {}", FLINK_NATIVE_K8S_OPERATOR_NAME);
        } else {
            KubernetesUtils.setOwnerReference(deployment, Collections.singletonList(ingress));
        }
        kubernetesClient.resourceList(ingress).inNamespace(operatorNamespace).createOrReplace();
    }

    private void triggerSavepoint(FlinkApplication oldFlinkApp, FlinkApplication newFlinkApp, Configuration effectiveConfig) {
        final int generation = newFlinkApp.getSpec().getSavepointGeneration();
        if (generation > oldFlinkApp.getSpec().getSavepointGeneration()) {
            try (ClusterClient<String> clusterClient = FlinkUtils.getRestClusterClient(effectiveConfig)) {
                final CompletableFuture<Collection<JobStatusMessage>> jobDetailsFuture = clusterClient.listJobs();
                jobDetailsFuture.get().forEach(status -> {
                    LOG.debug("JobStatus for {}: {}", clusterClient.getClusterId(), status);
                    clusterClient.triggerSavepoint(status.getJobId(), null).thenAccept(path -> savepointLocation.put(status.getJobId().toString(), path)).join();
                });
            } catch (Exception e) {
                LOG.warn("Failed to trigger a new savepoint with generation {}", generation);
            }
        }
    }
}
