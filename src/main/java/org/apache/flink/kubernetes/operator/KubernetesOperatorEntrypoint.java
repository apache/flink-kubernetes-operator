package org.apache.flink.kubernetes.operator;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.Operator;
import io.javaoperatorsdk.operator.api.config.ConfigurationServiceOverrider;
import io.javaoperatorsdk.operator.config.runtime.DefaultConfigurationService;
import org.apache.flink.kubernetes.operator.controller.FlinkApplicationController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.takes.facets.fork.FkRegex;
import org.takes.facets.fork.TkFork;
import org.takes.http.Exit;
import org.takes.http.FtBasic;

import java.io.IOException;

/**
 * Main Class for Flink native k8s operator.
 */
public class KubernetesOperatorEntrypoint {
    private static final Logger LOG = LoggerFactory.getLogger(KubernetesOperatorEntrypoint.class);

    public static void main(String args[]) throws IOException {

        LOG.info("Starting Flink Kubernetes Operator");

        KubernetesClient client = new DefaultKubernetesClient();
        String namespace = client.getNamespace();
        if (namespace == null) {
            namespace = "default";
        }
        Operator operator = new Operator(client,
                new ConfigurationServiceOverrider(DefaultConfigurationService.instance())
                        .build());
        operator.register(new FlinkApplicationController(client, namespace));
        operator.installShutdownHook();
        operator.start();

        new FtBasic(new TkFork(new FkRegex("/health", "ALL GOOD!")), 8080).start(Exit.NEVER);
    }
}
