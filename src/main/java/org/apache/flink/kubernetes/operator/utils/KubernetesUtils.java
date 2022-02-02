package org.apache.flink.kubernetes.operator.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.networking.v1.HTTPIngressRuleValueBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.kubernetes.api.model.networking.v1.IngressBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.IngressRule;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Kubernetes related utilities. */
public class KubernetesUtils {

    public static final String REST_SVC_NAME_SUFFIX = "-rest";
    public static final String INGRESS_API_VERSION = "networking.k8s.io/v1";

    private static final Logger LOG = LoggerFactory.getLogger(KubernetesUtils.class);

    public static void deployIngress(
            FlinkDeployment flinkApp,
            Configuration effectiveConfig,
            KubernetesClient kubernetesClient) {
        if (flinkApp.getSpec().getIngressDomain() != null) {
            final List<IngressRule> ingressRules = new ArrayList<>();
            final String clusterId = flinkApp.getMetadata().getName();
            final String namespace = flinkApp.getMetadata().getNamespace();
            final int restPort = effectiveConfig.getInteger(RestOptions.PORT);
            final String ingressHost =
                    String.format("%s.%s", clusterId, flinkApp.getSpec().getIngressDomain());
            ingressRules.add(
                    new IngressRule(
                            ingressHost,
                            new HTTPIngressRuleValueBuilder()
                                    .addNewPath()
                                    .withPathType("ImplementationSpecific")
                                    .withNewBackend()
                                    .withNewService()
                                    .withName(clusterId + REST_SVC_NAME_SUFFIX)
                                    .withNewPort()
                                    .withNumber(restPort)
                                    .endPort()
                                    .endService()
                                    .endBackend()
                                    .endPath()
                                    .build()));
            final Ingress ingress =
                    new IngressBuilder()
                            .withApiVersion(INGRESS_API_VERSION)
                            .withNewMetadata()
                            .withName(clusterId)
                            .endMetadata()
                            .withNewSpec()
                            .withRules(ingressRules)
                            .endSpec()
                            .build();

            Deployment deployment =
                    kubernetesClient
                            .apps()
                            .deployments()
                            .inNamespace(flinkApp.getMetadata().getNamespace())
                            .withName(flinkApp.getMetadata().getName())
                            .get();
            KubernetesUtils.setOwnerReference(deployment, Collections.singletonList(ingress));
            LOG.info(ingress.toString());
            kubernetesClient.resourceList(ingress).inNamespace(namespace).createOrReplace();
        }
    }

    private static void setOwnerReference(HasMetadata owner, List<HasMetadata> resources) {
        final OwnerReference ownerReference =
                new OwnerReferenceBuilder()
                        .withName(owner.getMetadata().getName())
                        .withApiVersion(owner.getApiVersion())
                        .withUid(owner.getMetadata().getUid())
                        .withKind(owner.getKind())
                        .withController(true)
                        .withBlockOwnerDeletion(true)
                        .build();
        resources.forEach(
                resource ->
                        resource.getMetadata()
                                .setOwnerReferences(Collections.singletonList(ownerReference)));
    }
}
