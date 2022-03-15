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

import java.util.Collections;
import java.util.List;

/** Ingress utilities. */
public class IngressUtils {

    private static final String REST_SVC_NAME_SUFFIX = "-rest";

    private static final Logger LOG = LoggerFactory.getLogger(IngressUtils.class);

    public static void updateIngressRules(
            FlinkDeployment flinkDeployment,
            Configuration effectiveConfig,
            KubernetesClient client) {
        if (flinkDeployment.getSpec().getIngressDomain() != null) {
            final IngressRule ingressRule = fromDeployment(flinkDeployment, effectiveConfig);
            Ingress ingress =
                    new IngressBuilder()
                            .withNewMetadata()
                            .withName(flinkDeployment.getMetadata().getName())
                            .withNamespace(flinkDeployment.getMetadata().getNamespace())
                            .endMetadata()
                            .withNewSpec()
                            .withRules(ingressRule)
                            .endSpec()
                            .build();
            Deployment deployment =
                    client.apps()
                            .deployments()
                            .inNamespace(flinkDeployment.getMetadata().getNamespace())
                            .withName(flinkDeployment.getMetadata().getName())
                            .get();
            if (deployment == null) {
                LOG.warn("Could not find deployment {}", flinkDeployment.getMetadata().getName());
            } else {
                setOwnerReference(deployment, Collections.singletonList(ingress));
            }
            LOG.info("Updating ingress rules {}", ingress);
            client.resourceList(ingress)
                    .inNamespace(flinkDeployment.getMetadata().getNamespace())
                    .createOrReplace();
        }
    }

    private static IngressRule fromDeployment(
            FlinkDeployment flinkDeployment, Configuration effectiveConfig) {
        final String clusterId = flinkDeployment.getMetadata().getName();
        final int restPort = effectiveConfig.getInteger(RestOptions.PORT);
        final String ingressHost = getIngressHost(flinkDeployment, clusterId);
        return new IngressRule(
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
                        .build());
    }

    private static String getIngressHost(FlinkDeployment flinkDeployment, String clusterId) {
        return String.format(
                "%s.%s.%s",
                clusterId,
                flinkDeployment.getMetadata().getNamespace(),
                flinkDeployment.getSpec().getIngressDomain());
    }

    public static void setOwnerReference(HasMetadata owner, List<HasMetadata> resources) {
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
