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
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;

import io.fabric8.kubernetes.client.CustomResource;

import java.util.Map;

/** Base metric group for Flink Operator System metrics. */
public class KubernetesOperatorMetricGroup extends AbstractMetricGroup<AbstractMetricGroup<?>> {

    protected final String namespace;
    protected final String name;
    protected final String hostname;

    private KubernetesOperatorMetricGroup(
            MetricRegistry registry,
            String[] scope,
            String namespace,
            String name,
            String hostname) {
        super(registry, scope, null);
        this.namespace = namespace;
        this.name = name;
        this.hostname = hostname;
    }

    public KubernetesResourceNamespaceMetricGroup createResourceNamespaceGroup(
            Configuration config,
            Class<? extends CustomResource> resourceClass,
            String resourceNs) {
        var resourceType = resourceClass.getSimpleName();
        return new KubernetesResourceNamespaceMetricGroup(
                registry,
                this,
                KubernetesResourceNamespaceScopeFormat.fromConfig(config)
                        .formatScope(namespace, name, hostname, resourceNs, resourceType),
                resourceNs,
                resourceType);
    }

    public static KubernetesOperatorMetricGroup create(
            MetricRegistry metricRegistry,
            Configuration configuration,
            String namespace,
            String name,
            String hostname) {
        return new KubernetesOperatorMetricGroup(
                metricRegistry,
                KubernetesOperatorScopeFormat.fromConfig(configuration)
                        .formatScope(namespace, name, hostname),
                namespace,
                name,
                hostname);
    }

    @Override
    protected final void putVariables(Map<String, String> variables) {
        variables.put(KubernetesOperatorScopeFormat.NAMESPACE, namespace);
        variables.put(KubernetesOperatorScopeFormat.NAME, name);
        variables.put(ScopeFormat.SCOPE_HOST, hostname);
    }

    @Override
    protected final String getGroupName(CharacterFilter filter) {
        return "k8soperator";
    }

    @Override
    protected final QueryScopeInfo createQueryServiceMetricInfo(CharacterFilter filter) {
        throw new UnsupportedOperationException("Not implemented");
    }
}
