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
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.IngressSpec;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.controller.FlinkDeploymentContext;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.exception.ReconciliationException;

import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.kubernetes.api.model.networking.v1.IngressRule;
import io.fabric8.kubernetes.api.model.networking.v1.IngressTLS;
import io.fabric8.kubernetes.api.model.networking.v1beta1.IngressBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_MANAGE_INGRESS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test class for {@link IngressUtils}. */
@EnableKubernetesMockClient(crud = true)
class IngressUtilsTest {

    KubernetesClient client;

    TestingJosdkContext testingJosdkContext;

    private FlinkResourceContext<?> createResourceContext(FlinkDeployment appCluster) {
        testingJosdkContext = new TestingJosdkContext<>(client);
        return new FlinkDeploymentContext(
                appCluster,
                testingJosdkContext,
                null,
                new FlinkConfigManager(Configuration.fromMap(new HashMap<>())),
                null,
                null);
    }

    private FlinkResourceContext<?> createResourceContext(
            FlinkDeployment appCluster, FlinkConfigManager configManager) {
        testingJosdkContext = new TestingJosdkContext<>(client);
        return new FlinkDeploymentContext(
                appCluster, testingJosdkContext, null, configManager, null, null);
    }

    @Test
    void testIngress() {
        FlinkDeployment appCluster = TestUtils.buildApplicationCluster();
        Configuration config =
                new FlinkConfigManager(new Configuration())
                        .getDeployConfig(appCluster.getMetadata(), appCluster.getSpec());

        // no ingress when ingressDomain is empty
        IngressUtils.reconcileIngress(
                createResourceContext(appCluster), appCluster.getSpec(), config, client, null);
        if (IngressUtils.ingressInNetworkingV1(client)) {
            assertNull(
                    client.network()
                            .v1()
                            .ingresses()
                            .inNamespace(appCluster.getMetadata().getNamespace())
                            .withName(appCluster.getMetadata().getName())
                            .get());
        } else {
            assertNull(
                    client.network()
                            .v1beta1()
                            .ingresses()
                            .inNamespace(appCluster.getMetadata().getNamespace())
                            .withName(appCluster.getMetadata().getName())
                            .get());
        }

        // host based routing
        IngressSpec.IngressSpecBuilder builder = IngressSpec.builder();
        builder.template("{{name}}.{{namespace}}.example.com");
        appCluster.getSpec().setIngress(builder.build());
        IngressUtils.reconcileIngress(
                createResourceContext(appCluster), appCluster.getSpec(), config, client, null);
        Ingress ingress = null;
        io.fabric8.kubernetes.api.model.networking.v1beta1.Ingress ingressV1beta1 = null;
        if (IngressUtils.ingressInNetworkingV1(client)) {
            ingress =
                    client.network()
                            .v1()
                            .ingresses()
                            .inNamespace(appCluster.getMetadata().getNamespace())
                            .withName(appCluster.getMetadata().getName())
                            .get();
        } else {
            ingressV1beta1 =
                    client.network()
                            .v1beta1()
                            .ingresses()
                            .inNamespace(appCluster.getMetadata().getNamespace())
                            .withName(appCluster.getMetadata().getName())
                            .get();
        }

        List<IngressRule> rules = null;
        List<io.fabric8.kubernetes.api.model.networking.v1beta1.IngressRule> rulesV1beta1 = null;
        if (IngressUtils.ingressInNetworkingV1(client)) {
            rules = ingress.getSpec().getRules();
        } else {
            rulesV1beta1 = ingressV1beta1.getSpec().getRules();
        }
        assertEquals(
                1, IngressUtils.ingressInNetworkingV1(client) ? rules.size() : rulesV1beta1.size());
        assertEquals(
                appCluster.getMetadata().getName()
                        + "."
                        + appCluster.getMetadata().getNamespace()
                        + ".example.com",
                IngressUtils.ingressInNetworkingV1(client)
                        ? rules.get(0).getHost()
                        : rulesV1beta1.get(0).getHost());
        assertNull(
                IngressUtils.ingressInNetworkingV1(client)
                        ? rules.get(0).getHttp().getPaths().get(0).getPath()
                        : rulesV1beta1.get(0).getHttp().getPaths().get(0).getPath());

        // path based routing
        builder.template("/{{namespace}}/{{name}}(/|$)(.*)");
        builder.className("nginx");
        builder.annotations(Map.of("nginx.ingress.kubernetes.io/rewrite-target", "/$2"));
        appCluster.getSpec().setIngress(builder.build());
        IngressUtils.reconcileIngress(
                createResourceContext(appCluster), appCluster.getSpec(), config, client, null);
        if (IngressUtils.ingressInNetworkingV1(client)) {
            ingress =
                    client.network()
                            .v1()
                            .ingresses()
                            .inNamespace(appCluster.getMetadata().getNamespace())
                            .withName(appCluster.getMetadata().getName())
                            .get();
            rules = ingress.getSpec().getRules();
        } else {
            ingressV1beta1 =
                    client.network()
                            .v1beta1()
                            .ingresses()
                            .inNamespace(appCluster.getMetadata().getNamespace())
                            .withName(appCluster.getMetadata().getName())
                            .get();
            rulesV1beta1 = ingressV1beta1.getSpec().getRules();
        }

        assertEquals(
                1, IngressUtils.ingressInNetworkingV1(client) ? rules.size() : rulesV1beta1.size());
        assertNull(
                IngressUtils.ingressInNetworkingV1(client)
                        ? rules.get(0).getHost()
                        : rulesV1beta1.get(0).getHost());
        assertEquals(
                1,
                IngressUtils.ingressInNetworkingV1(client)
                        ? rules.get(0).getHttp().getPaths().size()
                        : rulesV1beta1.get(0).getHttp().getPaths().size());
        assertEquals(
                "/"
                        + appCluster.getMetadata().getNamespace()
                        + "/"
                        + appCluster.getMetadata().getName()
                        + "(/|$)(.*)",
                IngressUtils.ingressInNetworkingV1(client)
                        ? rules.get(0).getHttp().getPaths().get(0).getPath()
                        : rulesV1beta1.get(0).getHttp().getPaths().get(0).getPath());
        assertEquals(
                Map.of("nginx.ingress.kubernetes.io/rewrite-target", "/$2"),
                IngressUtils.ingressInNetworkingV1(client)
                        ? ingress.getMetadata().getAnnotations()
                        : ingressV1beta1.getMetadata().getAnnotations());
        assertEquals(
                "nginx",
                IngressUtils.ingressInNetworkingV1(client)
                        ? ingress.getSpec().getIngressClassName()
                        : ingressV1beta1.getSpec().getIngressClassName());

        // host + path based routing
        builder.template("example.com/{{namespace}}/{{name}}(/|$)(.*)");
        builder.className("nginx");
        appCluster.getSpec().setIngress(builder.build());
        IngressUtils.reconcileIngress(
                createResourceContext(appCluster), appCluster.getSpec(), config, client, null);
        if (IngressUtils.ingressInNetworkingV1(client)) {
            ingress =
                    client.network()
                            .v1()
                            .ingresses()
                            .inNamespace(appCluster.getMetadata().getNamespace())
                            .withName(appCluster.getMetadata().getName())
                            .get();
            rules = ingress.getSpec().getRules();
        } else {
            ingressV1beta1 =
                    client.network()
                            .v1beta1()
                            .ingresses()
                            .inNamespace(appCluster.getMetadata().getNamespace())
                            .withName(appCluster.getMetadata().getName())
                            .get();
            rulesV1beta1 = ingressV1beta1.getSpec().getRules();
        }
        assertEquals(
                1, IngressUtils.ingressInNetworkingV1(client) ? rules.size() : rulesV1beta1.size());
        assertEquals(
                1,
                IngressUtils.ingressInNetworkingV1(client)
                        ? rules.get(0).getHttp().getPaths().size()
                        : rulesV1beta1.get(0).getHttp().getPaths().size());
        assertEquals(
                "/"
                        + appCluster.getMetadata().getNamespace()
                        + "/"
                        + appCluster.getMetadata().getName()
                        + "(/|$)(.*)",
                IngressUtils.ingressInNetworkingV1(client)
                        ? rules.get(0).getHttp().getPaths().get(0).getPath()
                        : rulesV1beta1.get(0).getHttp().getPaths().get(0).getPath());
        assertEquals(
                Map.of("nginx.ingress.kubernetes.io/rewrite-target", "/$2"),
                IngressUtils.ingressInNetworkingV1(client)
                        ? ingress.getMetadata().getAnnotations()
                        : ingressV1beta1.getMetadata().getAnnotations());
        assertEquals(
                "nginx",
                IngressUtils.ingressInNetworkingV1(client)
                        ? ingress.getSpec().getIngressClassName()
                        : ingressV1beta1.getSpec().getIngressClassName());
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

    @Test
    public void testIngressTls() {
        FlinkDeployment appCluster = TestUtils.buildApplicationCluster();
        Configuration config =
                new FlinkConfigManager(new Configuration())
                        .getDeployConfig(appCluster.getMetadata(), appCluster.getSpec());

        // no tls when tls spec is empty
        IngressSpec.IngressSpecBuilder builder = IngressSpec.builder();
        builder.template("{{name}}.{{namespace}}.example.com");
        builder.tls(new ArrayList<>());
        appCluster.getSpec().setIngress(builder.build());
        IngressUtils.reconcileIngress(
                createResourceContext(appCluster), appCluster.getSpec(), config, client, null);
        Ingress ingress = null;
        io.fabric8.kubernetes.api.model.networking.v1beta1.Ingress ingressV1beta1 = null;
        if (IngressUtils.ingressInNetworkingV1(client)) {
            ingress =
                    client.network()
                            .v1()
                            .ingresses()
                            .inNamespace(appCluster.getMetadata().getNamespace())
                            .withName(appCluster.getMetadata().getName())
                            .get();
        } else {
            ingressV1beta1 =
                    client.network()
                            .v1beta1()
                            .ingresses()
                            .inNamespace(appCluster.getMetadata().getNamespace())
                            .withName(appCluster.getMetadata().getName())
                            .get();
        }
        List<IngressTLS> tls = null;
        List<io.fabric8.kubernetes.api.model.networking.v1beta1.IngressTLS> tlsV1beta1 = null;
        if (IngressUtils.ingressInNetworkingV1(client)) {
            tls = ingress.getSpec().getTls();
        } else {
            tlsV1beta1 = ingressV1beta1.getSpec().getTls();
        }
        assertEquals(
                0, IngressUtils.ingressInNetworkingV1(client) ? tls.size() : tlsV1beta1.size());

        // no tls when hosts spec is empty
        builder.template("{{name}}.{{namespace}}.example.com");
        IngressTLS ingressTlsSpecSecretOnly = new IngressTLS();
        ingressTlsSpecSecretOnly.setSecretName("secret");
        builder.tls(List.of(ingressTlsSpecSecretOnly));
        appCluster.getSpec().setIngress(builder.build());
        IngressUtils.reconcileIngress(
                createResourceContext(appCluster), appCluster.getSpec(), config, client, null);
        if (IngressUtils.ingressInNetworkingV1(client)) {
            ingress =
                    client.network()
                            .v1()
                            .ingresses()
                            .inNamespace(appCluster.getMetadata().getNamespace())
                            .withName(appCluster.getMetadata().getName())
                            .get();
        } else {
            ingressV1beta1 =
                    client.network()
                            .v1beta1()
                            .ingresses()
                            .inNamespace(appCluster.getMetadata().getNamespace())
                            .withName(appCluster.getMetadata().getName())
                            .get();
        }
        if (IngressUtils.ingressInNetworkingV1(client)) {
            tls = ingress.getSpec().getTls();
        } else {
            tlsV1beta1 = ingressV1beta1.getSpec().getTls();
        }
        assertEquals(
                1, IngressUtils.ingressInNetworkingV1(client) ? tls.size() : tlsV1beta1.size());
        assertEquals(
                "secret",
                IngressUtils.ingressInNetworkingV1(client)
                        ? tls.get(0).getSecretName()
                        : tlsV1beta1.get(0).getSecretName());

        // tls with no secretName
        builder.template("{{name}}.{{namespace}}.example.com");
        IngressTLS ingressTlsSpecHostsOnly = new IngressTLS();
        ingressTlsSpecHostsOnly.setHosts(List.of("example.com"));
        builder.tls(List.of(ingressTlsSpecHostsOnly));
        appCluster.getSpec().setIngress(builder.build());
        IngressUtils.reconcileIngress(
                createResourceContext(appCluster), appCluster.getSpec(), config, client, null);
        if (IngressUtils.ingressInNetworkingV1(client)) {
            ingress =
                    client.network()
                            .v1()
                            .ingresses()
                            .inNamespace(appCluster.getMetadata().getNamespace())
                            .withName(appCluster.getMetadata().getName())
                            .get();
        } else {
            ingressV1beta1 =
                    client.network()
                            .v1beta1()
                            .ingresses()
                            .inNamespace(appCluster.getMetadata().getNamespace())
                            .withName(appCluster.getMetadata().getName())
                            .get();
        }
        if (IngressUtils.ingressInNetworkingV1(client)) {
            tls = ingress.getSpec().getTls();
        } else {
            tlsV1beta1 = ingressV1beta1.getSpec().getTls();
        }
        assertEquals(
                1, IngressUtils.ingressInNetworkingV1(client) ? tls.size() : tlsV1beta1.size());
        assertEquals(
                "example.com",
                IngressUtils.ingressInNetworkingV1(client)
                        ? tls.get(0).getHosts().get(0)
                        : tlsV1beta1.get(0).getHosts().get(0));

        // tls with secretName and hosts
        builder.template("{{name}}.{{namespace}}.example.com");
        IngressTLS ingressTlsSpecSingleTLSWithHost =
                new IngressTLS(List.of("example.com"), "secret");
        builder.tls(List.of(ingressTlsSpecSingleTLSWithHost));
        appCluster.getSpec().setIngress(builder.build());
        IngressUtils.reconcileIngress(
                createResourceContext(appCluster), appCluster.getSpec(), config, client, null);
        if (IngressUtils.ingressInNetworkingV1(client)) {
            ingress =
                    client.network()
                            .v1()
                            .ingresses()
                            .inNamespace(appCluster.getMetadata().getNamespace())
                            .withName(appCluster.getMetadata().getName())
                            .get();
        } else {
            ingressV1beta1 =
                    client.network()
                            .v1beta1()
                            .ingresses()
                            .inNamespace(appCluster.getMetadata().getNamespace())
                            .withName(appCluster.getMetadata().getName())
                            .get();
        }
        if (IngressUtils.ingressInNetworkingV1(client)) {
            tls = ingress.getSpec().getTls();
        } else {
            tlsV1beta1 = ingressV1beta1.getSpec().getTls();
        }
        assertEquals(
                1, IngressUtils.ingressInNetworkingV1(client) ? tls.size() : tlsV1beta1.size());
        if (IngressUtils.ingressInNetworkingV1(client)) {
            assertEquals("secret", tls.get(0).getSecretName());
            assertEquals(1, tls.get(0).getHosts().size());
            assertEquals("example.com", tls.get(0).getHosts().get(0));
        } else {
            assertEquals("secret", tlsV1beta1.get(0).getSecretName());
            assertEquals(1, tlsV1beta1.get(0).getHosts().size());
            assertEquals("example.com", tlsV1beta1.get(0).getHosts().get(0));
        }

        // tls with secretName and multiple hosts
        builder.template("{{name}}.{{namespace}}.example.com");
        IngressTLS ingressTlsSpecSingleTLSWithHosts =
                new IngressTLS(List.of("example.com", "example2.com"), "secret");
        builder.tls(List.of(ingressTlsSpecSingleTLSWithHosts));
        appCluster.getSpec().setIngress(builder.build());
        IngressUtils.reconcileIngress(
                createResourceContext(appCluster), appCluster.getSpec(), config, client, null);
        if (IngressUtils.ingressInNetworkingV1(client)) {
            ingress =
                    client.network()
                            .v1()
                            .ingresses()
                            .inNamespace(appCluster.getMetadata().getNamespace())
                            .withName(appCluster.getMetadata().getName())
                            .get();
        } else {
            ingressV1beta1 =
                    client.network()
                            .v1beta1()
                            .ingresses()
                            .inNamespace(appCluster.getMetadata().getNamespace())
                            .withName(appCluster.getMetadata().getName())
                            .get();
        }
        if (IngressUtils.ingressInNetworkingV1(client)) {
            tls = ingress.getSpec().getTls();
        } else {
            tlsV1beta1 = ingressV1beta1.getSpec().getTls();
        }
        assertEquals(
                1, IngressUtils.ingressInNetworkingV1(client) ? tls.size() : tlsV1beta1.size());
        if (IngressUtils.ingressInNetworkingV1(client)) {
            assertEquals("secret", tls.get(0).getSecretName());
            assertEquals(2, tls.get(0).getHosts().size());
            assertEquals("example.com", tls.get(0).getHosts().get(0));
            assertEquals("example2.com", tls.get(0).getHosts().get(1));
        } else {
            assertEquals("secret", tlsV1beta1.get(0).getSecretName());
            assertEquals(2, tlsV1beta1.get(0).getHosts().size());
            assertEquals("example.com", tlsV1beta1.get(0).getHosts().get(0));
            assertEquals("example2.com", tlsV1beta1.get(0).getHosts().get(1));
        }

        // tls with secretName and multiple hosts and multiple tls
        builder.template("{{name}}.{{namespace}}.example.com");
        IngressTLS ingressTlsSpecMultipleTLSWithHosts1 =
                new IngressTLS(List.of("example.com", "example2.com"), "secret");
        IngressTLS ingressTlsSpecMultipleTLSWithHosts2 =
                new IngressTLS(List.of("example3.com", "example4.com"), "secret2");
        builder.tls(
                List.of(ingressTlsSpecMultipleTLSWithHosts1, ingressTlsSpecMultipleTLSWithHosts2));
        appCluster.getSpec().setIngress(builder.build());
        IngressUtils.reconcileIngress(
                createResourceContext(appCluster), appCluster.getSpec(), config, client, null);
        if (IngressUtils.ingressInNetworkingV1(client)) {
            ingress =
                    client.network()
                            .v1()
                            .ingresses()
                            .inNamespace(appCluster.getMetadata().getNamespace())
                            .withName(appCluster.getMetadata().getName())
                            .get();
        } else {
            ingressV1beta1 =
                    client.network()
                            .v1beta1()
                            .ingresses()
                            .inNamespace(appCluster.getMetadata().getNamespace())
                            .withName(appCluster.getMetadata().getName())
                            .get();
        }
        if (IngressUtils.ingressInNetworkingV1(client)) {
            tls = ingress.getSpec().getTls();
        } else {
            tlsV1beta1 = ingressV1beta1.getSpec().getTls();
        }
        assertEquals(
                2, IngressUtils.ingressInNetworkingV1(client) ? tls.size() : tlsV1beta1.size());
        if (IngressUtils.ingressInNetworkingV1(client)) {
            assertEquals("secret", tls.get(0).getSecretName());
            assertEquals(2, tls.get(0).getHosts().size());
            assertEquals("example.com", tls.get(0).getHosts().get(0));
            assertEquals("example2.com", tls.get(0).getHosts().get(1));
            assertEquals("secret2", tls.get(1).getSecretName());
            assertEquals(2, tls.get(1).getHosts().size());
            assertEquals("example3.com", tls.get(1).getHosts().get(0));
            assertEquals("example4.com", tls.get(1).getHosts().get(1));
        } else {
            assertEquals("secret", tlsV1beta1.get(0).getSecretName());
            assertEquals(2, tlsV1beta1.get(0).getHosts().size());
            assertEquals("example.com", tlsV1beta1.get(0).getHosts().get(0));
            assertEquals("example2.com", tlsV1beta1.get(0).getHosts().get(1));
            assertEquals("secret2", tlsV1beta1.get(1).getSecretName());
            assertEquals(2, tlsV1beta1.get(1).getHosts().size());
            assertEquals("example3.com", tlsV1beta1.get(1).getHosts().get(0));
            assertEquals("example4.com", tlsV1beta1.get(1).getHosts().get(1));
        }
    }

    @Test
    void testDeletesIngress() {
        FlinkDeployment appCluster = TestUtils.buildApplicationCluster();
        appCluster.getSpec().setIngress(null);
        io.fabric8.kubernetes.api.model.networking.v1beta1.Ingress ingress =
                new IngressBuilder()
                        .withNewMetadata()
                        .withName(appCluster.getMetadata().getName())
                        .withNamespace(appCluster.getMetadata().getNamespace())
                        .endMetadata()
                        .build();
        client.network().v1beta1().ingresses().resource(ingress).create();
        var context = createResourceContext(appCluster);
        testingJosdkContext.setSecondaryResources(
                Map.of(
                        io.fabric8.kubernetes.api.model.networking.v1beta1.Ingress.class,
                        List.of(ingress)));

        IngressUtils.reconcileIngress(context, appCluster.getSpec(), null, client, null);

        var ingressV1beta1 =
                client.network()
                        .v1beta1()
                        .ingresses()
                        .inNamespace(appCluster.getMetadata().getNamespace())
                        .withName(appCluster.getMetadata().getName())
                        .get();
        assertThat(ingressV1beta1).isNull();
    }

    @Test
    void skipIngressReconciliationIfFeatureFlagOff() {
        List<Event> events = new ArrayList<>();
        EventRecorder eventRecorder =
                new EventRecorder((a, event) -> events.add(event), (a, b) -> {});
        FlinkDeployment appCluster = TestUtils.buildApplicationCluster();
        FlinkConfigManager manager =
                new FlinkConfigManager(
                        Configuration.fromMap(Map.of(OPERATOR_MANAGE_INGRESS.key(), "false")));
        var context = createResourceContext(appCluster, manager);
        context.getOperatorConfig();
        Configuration config =
                new FlinkConfigManager(new Configuration())
                        .getDeployConfig(appCluster.getMetadata(), appCluster.getSpec());

        IngressSpec.IngressSpecBuilder builder = IngressSpec.builder();
        builder.template("{{name}}.{{namespace}}.example.com");
        builder.tls(new ArrayList<>());
        appCluster.getSpec().setIngress(builder.build());

        IngressUtils.reconcileIngress(context, appCluster.getSpec(), config, client, eventRecorder);

        var ingressV1beta1 =
                client.network()
                        .v1beta1()
                        .ingresses()
                        .inNamespace(appCluster.getMetadata().getNamespace())
                        .withName(appCluster.getMetadata().getName())
                        .get();
        assertThat(ingressV1beta1).isNull();
        assertThat(events).hasSize(1);
    }
}
