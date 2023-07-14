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
import org.apache.flink.runtime.metrics.scope.ScopeFormat;

import static org.apache.flink.kubernetes.operator.metrics.KubernetesOperatorMetricOptions.SCOPE_NAMING_KUBERNETES_OPERATOR_RESOURCE;
import static org.apache.flink.kubernetes.operator.metrics.KubernetesOperatorScopeFormat.NAME;
import static org.apache.flink.kubernetes.operator.metrics.KubernetesOperatorScopeFormat.NAMESPACE;
import static org.apache.flink.kubernetes.operator.metrics.KubernetesResourceNamespaceScopeFormat.RESOURCE_NS;
import static org.apache.flink.kubernetes.operator.metrics.KubernetesResourceNamespaceScopeFormat.RESOURCE_TYPE;

/** Format for metrics. */
public class KubernetesResourceScopeFormat extends ScopeFormat {

    public static final String RESOURCE = asVariable("resourcename");

    public KubernetesResourceScopeFormat(String format) {
        super(
                format,
                null,
                new String[] {NAMESPACE, NAME, SCOPE_HOST, RESOURCE_NS, RESOURCE, RESOURCE_TYPE});
    }

    public String[] formatScope(
            String namespace,
            String name,
            String hostname,
            String resourceNs,
            String resourceName,
            String resourceType) {
        final String[] template = copyTemplate();
        final String[] values = {namespace, name, hostname, resourceNs, resourceName, resourceType};
        return bindVariables(template, values);
    }

    public static KubernetesResourceScopeFormat fromConfig(Configuration config) {
        String format = config.getString(SCOPE_NAMING_KUBERNETES_OPERATOR_RESOURCE);
        return new KubernetesResourceScopeFormat(format);
    }
}
