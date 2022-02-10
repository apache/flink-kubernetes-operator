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

import io.fabric8.kubernetes.api.model.networking.v1.HTTPIngressRuleValueBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.kubernetes.api.model.networking.v1.IngressBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.IngressRule;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/** Ingress utilities. */
public class IngressUtils {

    private static final String INGRESS_NAME = "flink-operator";
    private static final String REST_SVC_NAME_SUFFIX = "-rest";

    private static final Logger LOG = LoggerFactory.getLogger(IngressUtils.class);

    public static void updateIngressRules(
            FlinkDeployment flinkDeployment,
            Configuration effectiveConfig,
            String operatorNamespace,
            KubernetesClient client,
            boolean remove) {
        if (flinkDeployment.getSpec().getIngressDomain() != null) {
            final IngressRule ingressRule = fromDeployment(flinkDeployment, effectiveConfig);
            getIngress(operatorNamespace, client)
                    .ifPresent(
                            ingress -> {
                                Ingress updated;
                                if (remove) {
                                    updated =
                                            new IngressBuilder(ingress)
                                                    .editSpec()
                                                    .removeFromRules(ingressRule)
                                                    .endSpec()
                                                    .build();
                                } else {
                                    updated =
                                            new IngressBuilder(ingress)
                                                    .editSpec()
                                                    .addToRules(ingressRule)
                                                    .endSpec()
                                                    .build();
                                }
                                LOG.info("Updating ingress rules {}", ingress);
                                client.resourceList(updated)
                                        .inNamespace(operatorNamespace)
                                        .createOrReplace();
                            });
        }
    }

    private static Optional<Ingress> getIngress(String operatorNamespace, KubernetesClient client) {
        return Optional.ofNullable(
                client.network()
                        .v1()
                        .ingresses()
                        .inNamespace(operatorNamespace)
                        .withName(INGRESS_NAME)
                        .get());
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
        return String.format("%s.%s", clusterId, flinkDeployment.getSpec().getIngressDomain());
    }
}
