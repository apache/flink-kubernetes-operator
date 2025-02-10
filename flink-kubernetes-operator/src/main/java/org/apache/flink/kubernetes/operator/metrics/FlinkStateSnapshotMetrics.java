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
import org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** FlinkStateSnapshot metrics. */
public class FlinkStateSnapshotMetrics implements CustomResourceMetrics<FlinkStateSnapshot> {

    private final KubernetesOperatorMetricGroup parentMetricGroup;
    private final Configuration configuration;
    private final Map<String, Map<FlinkStateSnapshotStatus.State, Set<String>>> checkpointStatuses =
            new ConcurrentHashMap<>();
    private final Map<String, Map<FlinkStateSnapshotStatus.State, Set<String>>> savepointStatuses =
            new ConcurrentHashMap<>();
    public static final String COUNTER_GROUP_NAME = "Count";
    public static final String STATE_GROUP_NAME = "State";
    public static final String CHECKPOINT_GROUP_NAME = "Checkpoint";
    public static final String SAVEPOINT_GROUP_NAME = "Savepoint";

    public FlinkStateSnapshotMetrics(
            KubernetesOperatorMetricGroup parentMetricGroup, Configuration configuration) {
        this.parentMetricGroup = parentMetricGroup;
        this.configuration = configuration;
    }

    public void onUpdate(FlinkStateSnapshot snapshot) {
        if (snapshot.getStatus() == null || snapshot.getStatus().getState() == null) {
            return;
        }

        onRemove(snapshot);
        getSnapshotMap(snapshot)
                .computeIfAbsent(
                        snapshot.getMetadata().getNamespace(),
                        ns -> {
                            initNamespaceSnapshotStates(ns);
                            initNamespaceSnapshotCounts(ns);
                            return createSnapshotStateMap();
                        })
                .get(snapshot.getStatus().getState())
                .add(snapshot.getMetadata().getName());
    }

    public void onRemove(FlinkStateSnapshot snapshot) {
        var namespace = snapshot.getMetadata().getNamespace();
        var name = snapshot.getMetadata().getName();
        var snapshotMap = getSnapshotMap(snapshot);

        if (snapshotMap.containsKey(namespace)) {
            snapshotMap.get(namespace).values().forEach(names -> names.remove(name));
        }
    }

    private Map<String, Map<FlinkStateSnapshotStatus.State, Set<String>>> getSnapshotMap(
            FlinkStateSnapshot snapshot) {
        if (snapshot.getSpec().isSavepoint()) {
            return savepointStatuses;
        } else {
            return checkpointStatuses;
        }
    }

    private void initNamespaceSnapshotCounts(String ns) {
        var mainGroup =
                parentMetricGroup.createResourceNamespaceGroup(
                        configuration, FlinkStateSnapshot.class, ns);

        mainGroup
                .addGroup(CHECKPOINT_GROUP_NAME)
                .gauge(
                        COUNTER_GROUP_NAME,
                        () -> {
                            if (!checkpointStatuses.containsKey(ns)) {
                                return 0;
                            }
                            return checkpointStatuses.get(ns).values().stream()
                                    .mapToInt(Set::size)
                                    .sum();
                        });
        mainGroup
                .addGroup(SAVEPOINT_GROUP_NAME)
                .gauge(
                        COUNTER_GROUP_NAME,
                        () -> {
                            if (!savepointStatuses.containsKey(ns)) {
                                return 0;
                            }
                            return savepointStatuses.get(ns).values().stream()
                                    .mapToInt(Set::size)
                                    .sum();
                        });
    }

    private void initNamespaceSnapshotStates(String ns) {
        var mainGroup =
                parentMetricGroup.createResourceNamespaceGroup(
                        configuration, FlinkStateSnapshot.class, ns);

        for (var state : FlinkStateSnapshotStatus.State.values()) {
            mainGroup
                    .addGroup(CHECKPOINT_GROUP_NAME)
                    .addGroup(STATE_GROUP_NAME)
                    .addGroup(state.toString())
                    .gauge(
                            COUNTER_GROUP_NAME,
                            () ->
                                    Optional.ofNullable(checkpointStatuses.get(ns))
                                            .map(s -> s.get(state).size())
                                            .orElse(0));
            mainGroup
                    .addGroup(SAVEPOINT_GROUP_NAME)
                    .addGroup(STATE_GROUP_NAME)
                    .addGroup(state.toString())
                    .gauge(
                            COUNTER_GROUP_NAME,
                            () ->
                                    Optional.ofNullable(savepointStatuses.get(ns))
                                            .map(s -> s.get(state).size())
                                            .orElse(0));
        }
    }

    private Map<FlinkStateSnapshotStatus.State, Set<String>> createSnapshotStateMap() {
        Map<FlinkStateSnapshotStatus.State, Set<String>> statuses = new ConcurrentHashMap<>();
        for (var state : FlinkStateSnapshotStatus.State.values()) {
            statuses.put(state, ConcurrentHashMap.newKeySet());
        }
        return statuses;
    }
}
