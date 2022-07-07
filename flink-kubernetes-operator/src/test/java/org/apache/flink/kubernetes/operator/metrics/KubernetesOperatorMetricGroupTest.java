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

package org.apache.flink.kubernetes.operator.metrics;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.Test;

import static org.apache.flink.kubernetes.operator.metrics.KubernetesOperatorMetricOptions.SCOPE_NAMING_KUBERNETES_OPERATOR;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/** @link KubernetesOperatorMetricGroup tests. */
public class KubernetesOperatorMetricGroupTest {

    @Test
    public void testGenerateScopeDefault() throws Exception {
        Configuration configuration = new Configuration();
        MetricRegistryImpl registry = new MetricRegistryImpl(fromConfiguration(configuration));
        KubernetesOperatorMetricGroup group =
                KubernetesOperatorMetricGroup.create(
                        registry,
                        configuration,
                        "default",
                        "flink-kubernetes-operator",
                        "localhost");
        assertArrayEquals(
                new String[] {
                    "localhost", "k8soperator", "default", "flink-kubernetes-operator", "system"
                },
                group.getScopeComponents());
        assertEquals(
                "localhost.k8soperator.default.flink-kubernetes-operator.system.test",
                group.getMetricIdentifier("test"));

        assertEquals(
                ImmutableMap.of(
                        "<host>",
                        "localhost",
                        "<namespace>",
                        "default",
                        "<name>",
                        "flink-kubernetes-operator"),
                group.getAllVariables());

        registry.shutdown().get();
    }

    @Test
    public void testGenerateScopeCustom() throws Exception {
        Configuration configuration = new Configuration();
        configuration.setString(SCOPE_NAMING_KUBERNETES_OPERATOR, "foo.<host>.<name>.<namespace>");
        MetricRegistryImpl registry = new MetricRegistryImpl(fromConfiguration(configuration));

        KubernetesOperatorMetricGroup group =
                KubernetesOperatorMetricGroup.create(
                        registry,
                        configuration,
                        "default",
                        "flink-kubernetes-operator",
                        "localhost");
        assertArrayEquals(
                new String[] {"foo", "localhost", "flink-kubernetes-operator", "default"},
                group.getScopeComponents());
        assertEquals(
                "foo.localhost.flink-kubernetes-operator.default.test",
                group.getMetricIdentifier("test"));

        assertEquals(
                ImmutableMap.of(
                        "<host>",
                        "localhost",
                        "<namespace>",
                        "default",
                        "<name>",
                        "flink-kubernetes-operator"),
                group.getAllVariables());

        registry.shutdown().get();
    }

    @Test
    public void testSubGroupVariables() throws Exception {
        var configuration = new Configuration();
        var registry = new MetricRegistryImpl(fromConfiguration(configuration));
        var operatorMetricGroup =
                KubernetesOperatorMetricGroup.create(
                        registry,
                        configuration,
                        "default",
                        "flink-kubernetes-operator",
                        "localhost");

        var namespaceGroup = operatorMetricGroup.createResourceNamespaceGroup(configuration, "rns");
        var resourceGroup = namespaceGroup.createResourceNamespaceGroup(configuration, "rn");

        assertEquals(
                ImmutableMap.of(
                        "<host>",
                        "localhost",
                        "<namespace>",
                        "default",
                        "<name>",
                        "flink-kubernetes-operator",
                        "<resourcens>",
                        "rns"),
                namespaceGroup.getAllVariables());
        assertEquals(
                ImmutableMap.of(
                        "<host>",
                        "localhost",
                        "<namespace>",
                        "default",
                        "<name>",
                        "flink-kubernetes-operator",
                        "<resourcens>",
                        "rns",
                        "<resourcename>",
                        "rn"),
                resourceGroup.getAllVariables());
        registry.shutdown().get();
    }

    private static MetricRegistryConfiguration fromConfiguration(Configuration configuration) {
        return MetricRegistryConfiguration.fromConfiguration(configuration, Long.MAX_VALUE);
    }
}
