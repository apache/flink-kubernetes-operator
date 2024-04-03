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
import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** FlinkStateSnapshot metrics. */
public class FlinkStateSnapshotMetrics implements CustomResourceMetrics<FlinkStateSnapshot> {

    private final KubernetesOperatorMetricGroup parentMetricGroup;
    private final Configuration configuration;
    private final Map<String, Set<String>> snapshots = new ConcurrentHashMap<>();
    public static final String COUNTER_NAME = "Count";

    public FlinkStateSnapshotMetrics(
            KubernetesOperatorMetricGroup parentMetricGroup, Configuration configuration) {
        this.parentMetricGroup = parentMetricGroup;
        this.configuration = configuration;
    }

    public void onUpdate(FlinkStateSnapshot snapshot) {
        onRemove(snapshot);
        snapshots
                .computeIfAbsent(
                        snapshot.getMetadata().getNamespace(),
                        ns -> {
                            initNamespaceSnapshotCounts(ns);
                            return ConcurrentHashMap.newKeySet();
                        })
                .add(snapshot.getMetadata().getName());
    }

    public void onRemove(FlinkStateSnapshot snapshot) {
        if (!snapshots.containsKey(snapshot.getMetadata().getNamespace())) {
            return;
        }
        snapshots
                .get(snapshot.getMetadata().getNamespace())
                .remove(snapshot.getMetadata().getName());
    }

    private void initNamespaceSnapshotCounts(String ns) {
        parentMetricGroup
                .createResourceNamespaceGroup(configuration, FlinkStateSnapshot.class, ns)
                .gauge(COUNTER_NAME, () -> snapshots.get(ns).size());
    }
}
