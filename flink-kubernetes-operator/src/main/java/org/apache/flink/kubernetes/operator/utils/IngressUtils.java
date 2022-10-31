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
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.exception.ReconciliationException;
import org.apache.flink.util.Preconditions;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.networking.v1.HTTPIngressRuleValueBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.kubernetes.api.model.networking.v1.IngressBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.IngressRule;
import io.fabric8.kubernetes.api.model.networking.v1.IngressRuleBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

/** Ingress utilities. */
public class IngressUtils {

    private static final Pattern NAME_PTN =
            Pattern.compile("\\{\\{name\\}\\}", Pattern.CASE_INSENSITIVE);
    private static final Pattern NAMESPACE_PTN =
            Pattern.compile("\\{\\{namespace\\}\\}", Pattern.CASE_INSENSITIVE);
    private static final Pattern URL_PROTOCOL_REGEX =
            Pattern.compile("^https?://", Pattern.CASE_INSENSITIVE);

    private static final String REST_SVC_NAME_SUFFIX = "-rest";

    private static final Logger LOG = LoggerFactory.getLogger(IngressUtils.class);

    public static void updateIngressRules(
            ObjectMeta objectMeta,
            FlinkDeploymentSpec spec,
            Configuration effectiveConfig,
            KubernetesClient client) {

        if (spec.getIngress() != null) {
            Ingress ingress =
                    new IngressBuilder()
                            .withNewMetadata()
                            .withAnnotations(spec.getIngress().getAnnotations())
                            .withName(objectMeta.getName())
                            .withNamespace(objectMeta.getNamespace())
                            .endMetadata()
                            .withNewSpec()
                            .withIngressClassName(spec.getIngress().getClassName())
                            .withRules(getIngressRule(objectMeta, spec, effectiveConfig))
                            .endSpec()
                            .build();

            Deployment deployment =
                    client.apps()
                            .deployments()
                            .inNamespace(objectMeta.getNamespace())
                            .withName(objectMeta.getName())
                            .get();
            if (deployment == null) {
                LOG.error("Could not find deployment {}", objectMeta.getName());
            } else {
                setOwnerReference(deployment, Collections.singletonList(ingress));
            }

            LOG.info("Updating ingress rules {}", ingress);
            client.resourceList(ingress).inNamespace(objectMeta.getNamespace()).createOrReplace();
        }
    }

    private static IngressRule getIngressRule(
            ObjectMeta objectMeta, FlinkDeploymentSpec spec, Configuration effectiveConfig) {
        final String clusterId = objectMeta.getName();
        final int restPort = effectiveConfig.getInteger(RestOptions.PORT);

        URL ingressUrl =
                getIngressUrl(
                        spec.getIngress().getTemplate(),
                        objectMeta.getName(),
                        objectMeta.getNamespace());

        IngressRuleBuilder ingressRuleBuilder = new IngressRuleBuilder();
        ingressRuleBuilder.withHttp(
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

        if (!StringUtils.isBlank(ingressUrl.getHost())) {
            ingressRuleBuilder.withHost(ingressUrl.getHost());
        }

        if (!StringUtils.isBlank(ingressUrl.getPath())) {
            ingressRuleBuilder
                    .editHttp()
                    .editFirstPath()
                    .withPath(ingressUrl.getPath())
                    .endPath()
                    .endHttp();
        }
        return ingressRuleBuilder.build();
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

    public static URL getIngressUrl(String ingressTemplate, String name, String namespace) {
        String template = addProtocol(ingressTemplate);
        template = NAME_PTN.matcher(template).replaceAll(name);
        template = NAMESPACE_PTN.matcher(template).replaceAll(namespace);
        try {
            return new URL(template);
        } catch (MalformedURLException e) {
            LOG.error(e.getMessage());
            throw new ReconciliationException(
                    String.format(
                            "Unable to process the Ingress template(%s). Error: %s",
                            ingressTemplate, e.getMessage()));
        }
    }

    private static String addProtocol(String url) {
        Preconditions.checkNotNull(url);
        if (!URL_PROTOCOL_REGEX.matcher(url).find()) {
            url = "http://" + url;
        }
        return url;
    }
}
