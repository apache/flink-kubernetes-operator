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

package org.apache.flink.kubernetes.operator.reconciler;

import org.apache.flink.autoscaler.utils.DateTimeUtils;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.CrdConstants;
import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;
import org.apache.flink.kubernetes.operator.api.status.SnapshotInfo;

import io.fabric8.kubernetes.api.model.HasMetadata;
import lombok.RequiredArgsConstructor;

import javax.annotation.Nullable;

import java.time.Instant;
import java.util.Comparator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus.State.COMPLETED;
import static org.apache.flink.kubernetes.operator.api.status.SnapshotTriggerType.PERIODIC;
import static org.apache.flink.kubernetes.operator.reconciler.SnapshotType.SAVEPOINT;

/** Class used to store latest timestamps of periodic checkpoint/savepoint. */
@RequiredArgsConstructor
public class SnapshotTriggerTimestampStore {
    private final SnapshotType snapshotType;
    private final ConcurrentHashMap<String, Instant> lastTriggered = new ConcurrentHashMap<>();

    /**
     * Returns the time a periodic snapshot was last triggered for this resource. This is stored in
     * memory, on operator start it will use the latest completed FlinkStateSnapshot CR creation
     * timestamp. If none found, the return value will be the max of the resource creation timestamp
     * and the latest triggered legacy snapshot.
     *
     * @param resource Flink resource
     * @param snapshots related snapshot resources
     * @return instant of last trigger
     */
    public Instant getLastPeriodicTriggerInstant(
            AbstractFlinkResource<?, ?> resource, @Nullable Set<FlinkStateSnapshot> snapshots) {
        if (lastTriggered.containsKey(resource.getMetadata().getUid())) {
            return lastTriggered.get(resource.getMetadata().getUid());
        }

        return Optional.ofNullable(snapshots).orElse(Set.of()).stream()
                .filter(s -> s.getStatus() != null && COMPLETED.equals(s.getStatus().getState()))
                .filter(s -> (snapshotType == SAVEPOINT) == s.getSpec().isSavepoint())
                .filter(
                        s ->
                                PERIODIC.name()
                                        .equals(
                                                s.getMetadata()
                                                        .getLabels()
                                                        .get(CrdConstants.LABEL_SNAPSHOT_TYPE)))
                .map(s -> DateTimeUtils.parseKubernetes(s.getMetadata().getCreationTimestamp()))
                .max(Comparator.naturalOrder())
                .orElseGet(
                        () -> {
                            var legacyTs = getLegacyTimestamp(resource);
                            var creationTs =
                                    Instant.parse(resource.getMetadata().getCreationTimestamp());

                            if (legacyTs.compareTo(creationTs) > 0) {
                                return legacyTs;
                            } else {
                                return creationTs;
                            }
                        });
    }

    /**
     * Updates the time a periodic snapshot was last triggered for this resource.
     *
     * @param resource Kubernetes resource
     * @param instant new timestamp
     */
    public void updateLastPeriodicTriggerTimestamp(HasMetadata resource, Instant instant) {
        lastTriggered.put(resource.getMetadata().getUid(), instant);
    }

    private Instant getLegacyTimestamp(AbstractFlinkResource<?, ?> resource) {
        SnapshotInfo snapshotInfo;
        switch (snapshotType) {
            case SAVEPOINT:
                snapshotInfo = resource.getStatus().getJobStatus().getSavepointInfo();
                break;
            case CHECKPOINT:
                snapshotInfo = resource.getStatus().getJobStatus().getCheckpointInfo();
                break;
            default:
                throw new IllegalArgumentException("Unsupported snapshot type: " + snapshotType);
        }

        return Instant.ofEpochMilli(snapshotInfo.getLastPeriodicTriggerTimestamp());
    }
}
