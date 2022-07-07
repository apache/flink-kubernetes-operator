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

import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;

import java.util.Map;

/** Base metric group for Flink Operator Resource level metrics. */
public class KubernetesResourceMetricGroup
        extends AbstractMetricGroup<KubernetesResourceNamespaceMetricGroup> {

    private final String resourceName;

    protected KubernetesResourceMetricGroup(
            MetricRegistry registry,
            KubernetesResourceNamespaceMetricGroup parent,
            String[] scope,
            String resourceName) {
        super(registry, scope, parent);
        this.resourceName = resourceName;
    }

    @Override
    protected final void putVariables(Map<String, String> variables) {
        variables.put(KubernetesResourceScopeFormat.RESOURCE, resourceName);
    }

    @Override
    protected final String getGroupName(CharacterFilter filter) {
        return "resource";
    }

    @Override
    protected final QueryScopeInfo createQueryServiceMetricInfo(CharacterFilter filter) {
        throw new UnsupportedOperationException("Not implemented");
    }
}
