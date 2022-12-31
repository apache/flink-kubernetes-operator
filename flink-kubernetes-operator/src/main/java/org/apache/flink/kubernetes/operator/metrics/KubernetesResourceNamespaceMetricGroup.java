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

import java.util.Map;

/** Base metric group for Flink Operator Resource namespace level metrics. */
public class KubernetesResourceNamespaceMetricGroup
        extends AbstractMetricGroup<KubernetesOperatorMetricGroup> {

    private final String resourceNs;
    private final String resourceType;

    protected KubernetesResourceNamespaceMetricGroup(
            MetricRegistry registry,
            KubernetesOperatorMetricGroup parent,
            String[] scope,
            String resourceNs,
            String resourceType) {
        super(registry, scope, parent);
        this.resourceNs = resourceNs;
        this.resourceType = resourceType;
    }

    public KubernetesResourceMetricGroup createResourceGroup(
            Configuration config, String resourceName) {
        return new KubernetesResourceMetricGroup(
                registry,
                this,
                KubernetesResourceScopeFormat.fromConfig(config)
                        .formatScope(
                                parent.namespace,
                                parent.name,
                                parent.hostname,
                                resourceNs,
                                resourceName,
                                resourceType),
                resourceName);
    }

    @Override
    protected final void putVariables(Map<String, String> variables) {
        variables.put(KubernetesResourceNamespaceScopeFormat.RESOURCE_NS, resourceNs);
        variables.put(KubernetesResourceNamespaceScopeFormat.RESOURCE_TYPE, resourceType);
    }

    @Override
    protected final String getGroupName(CharacterFilter filter) {
        return "namespace";
    }

    @Override
    protected final QueryScopeInfo createQueryServiceMetricInfo(CharacterFilter filter) {
        throw new UnsupportedOperationException("Not implemented");
    }
}
