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

import java.util.Map;

/** Flink based operator metric group. */
public class KubernetesOperatorMetricGroup
        extends AbstractMetricGroup<KubernetesOperatorMetricGroup> {

    private static final String GROUP_NAME = "k8soperator";
    private final String namespace;
    private final String name;
    private final String hostname;

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
        return GROUP_NAME;
    }

    @Override
    protected final QueryScopeInfo createQueryServiceMetricInfo(CharacterFilter filter) {
        throw new UnsupportedOperationException("Not implemented");
    }
}
