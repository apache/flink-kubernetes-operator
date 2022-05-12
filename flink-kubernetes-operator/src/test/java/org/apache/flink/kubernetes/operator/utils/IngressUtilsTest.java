/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.IngressSpec;
import org.apache.flink.kubernetes.operator.exception.ReconciliationException;

import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.kubernetes.api.model.networking.v1.IngressRule;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test class for {@link IngressUtils}. */
@EnableKubernetesMockClient(crud = true)
public class IngressUtilsTest {

    KubernetesClient client;

    @Test
    public void testIngress() {
        FlinkDeployment appCluster = TestUtils.buildApplicationCluster();
        Configuration config =
                new FlinkConfigManager(new Configuration())
                        .getDeployConfig(appCluster.getMetadata(), appCluster.getSpec());

        // no ingress when ingressDomain is empty
        IngressUtils.updateIngressRules(
                appCluster.getMetadata(), appCluster.getSpec(), config, client);
        assertNull(
                client.network()
                        .v1()
                        .ingresses()
                        .inNamespace(appCluster.getMetadata().getNamespace())
                        .withName(appCluster.getMetadata().getName())
                        .get());

        // host based routing
        IngressSpec.IngressSpecBuilder builder = IngressSpec.builder();
        builder.template("{{name}}.{{namespace}}.example.com");
        appCluster.getSpec().setIngress(builder.build());
        IngressUtils.updateIngressRules(
                appCluster.getMetadata(), appCluster.getSpec(), config, client);
        Ingress ingress =
                client.network()
                        .v1()
                        .ingresses()
                        .inNamespace(appCluster.getMetadata().getNamespace())
                        .withName(appCluster.getMetadata().getName())
                        .get();

        List<IngressRule> rules = ingress.getSpec().getRules();
        assertEquals(1, rules.size());
        assertEquals(
                appCluster.getMetadata().getName()
                        + "."
                        + appCluster.getMetadata().getNamespace()
                        + ".example.com",
                rules.get(0).getHost());
        assertNull(rules.get(0).getHttp().getPaths().get(0).getPath());

        // path based routing
        builder.template("/{{namespace}}/{{name}}(/|$)(.*)");
        builder.className("nginx");
        builder.annotations(Map.of("nginx.ingress.kubernetes.io/rewrite-target", "/$2"));
        appCluster.getSpec().setIngress(builder.build());
        IngressUtils.updateIngressRules(
                appCluster.getMetadata(), appCluster.getSpec(), config, client);
        ingress =
                client.network()
                        .v1()
                        .ingresses()
                        .inNamespace(appCluster.getMetadata().getNamespace())
                        .withName(appCluster.getMetadata().getName())
                        .get();
        rules = ingress.getSpec().getRules();
        assertEquals(1, rules.size());
        assertNull(rules.get(0).getHost());
        assertEquals(1, rules.get(0).getHttp().getPaths().size());
        assertEquals(
                "/"
                        + appCluster.getMetadata().getNamespace()
                        + "/"
                        + appCluster.getMetadata().getName()
                        + "(/|$)(.*)",
                rules.get(0).getHttp().getPaths().get(0).getPath());
        assertEquals(
                Map.of("nginx.ingress.kubernetes.io/rewrite-target", "/$2"),
                ingress.getMetadata().getAnnotations());
        assertEquals("nginx", ingress.getSpec().getIngressClassName());

        // host + path based routing
        builder.template("example.com/{{namespace}}/{{name}}(/|$)(.*)");
        builder.className("nginx");
        appCluster.getSpec().setIngress(builder.build());
        IngressUtils.updateIngressRules(
                appCluster.getMetadata(), appCluster.getSpec(), config, client);
        ingress =
                client.network()
                        .v1()
                        .ingresses()
                        .inNamespace(appCluster.getMetadata().getNamespace())
                        .withName(appCluster.getMetadata().getName())
                        .get();
        rules = ingress.getSpec().getRules();
        assertEquals(1, rules.size());
        assertEquals(1, rules.get(0).getHttp().getPaths().size());
        assertEquals(
                "/"
                        + appCluster.getMetadata().getNamespace()
                        + "/"
                        + appCluster.getMetadata().getName()
                        + "(/|$)(.*)",
                rules.get(0).getHttp().getPaths().get(0).getPath());
        assertEquals(
                Map.of("nginx.ingress.kubernetes.io/rewrite-target", "/$2"),
                ingress.getMetadata().getAnnotations());
        assertEquals("nginx", ingress.getSpec().getIngressClassName());
    }

    @Test
    public void testIngressUrl() {
        String template = "flink.k8s.io/{{namespace}}/{{name}}";
        URL url = IngressUtils.getIngressUrl(template, "basic-ingress", "default");
        assertEquals("flink.k8s.io", url.getHost());
        assertEquals("/default/basic-ingress", url.getPath());

        template = "/{{namespace}}/{{name}}";
        url = IngressUtils.getIngressUrl(template, "basic-ingress", "default");
        assertTrue(StringUtils.isBlank(url.getHost()));
        assertEquals("/default/basic-ingress", url.getPath());

        template = "{{name}}.{{namespace}}.flink.k8s.io";
        url = IngressUtils.getIngressUrl(template, "basic-ingress", "default");

        assertEquals("basic-ingress.default.flink.k8s.io", url.getHost());
        assertTrue(StringUtils.isBlank(url.getPath()));

        assertThrows(
                ReconciliationException.class,
                () -> IngressUtils.getIngressUrl("example.com:port", "basic-ingress", "default"));
    }
}
