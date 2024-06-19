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

package org.apache.flink.kubernetes.operator.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.CrdConstants;
import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;
import org.apache.flink.kubernetes.operator.api.spec.CheckpointSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkStateSnapshotReference;
import org.apache.flink.kubernetes.operator.api.spec.FlinkStateSnapshotSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobReference;
import org.apache.flink.kubernetes.operator.api.spec.SavepointSpec;
import org.apache.flink.kubernetes.operator.api.status.CheckpointType;
import org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotState;
import org.apache.flink.kubernetes.operator.api.status.SavepointFormatType;
import org.apache.flink.kubernetes.operator.api.status.SnapshotTriggerType;
import org.apache.flink.kubernetes.operator.crd.CustomResourceDefinitionWatcher;
import org.apache.flink.kubernetes.operator.reconciler.SnapshotType;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.commons.lang3.StringUtils;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.SNAPSHOT_RESOURCE_ENABLED;
import static org.apache.flink.kubernetes.operator.reconciler.SnapshotType.CHECKPOINT;
import static org.apache.flink.kubernetes.operator.reconciler.SnapshotType.SAVEPOINT;

/** Utilities class for FlinkStateSnapshot resources. */
public class FlinkStateSnapshotUtils {

    private static final DateTimeFormatter RESOURCE_NAME_DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm");

    public static String getAndValidateFlinkStateSnapshotPath(
            KubernetesClient kubernetesClient, FlinkStateSnapshotReference snapshotRef) {
        if (!StringUtils.isBlank(snapshotRef.getPath())) {
            return snapshotRef.getPath();
        }

        if (StringUtils.isBlank(snapshotRef.getName())) {
            throw new IllegalArgumentException(
                    String.format("Invalid snapshot name: %s", snapshotRef.getName()));
        }

        FlinkStateSnapshot result;
        if (snapshotRef.getName() != null) {
            result =
                    kubernetesClient
                            .resources(FlinkStateSnapshot.class)
                            .inNamespace(snapshotRef.getNamespace())
                            .withName(snapshotRef.getName())
                            .get();
        } else {
            result =
                    kubernetesClient
                            .resources(FlinkStateSnapshot.class)
                            .withName(snapshotRef.getName())
                            .get();
        }

        if (result == null) {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot find snapshot %s in namespace %s.",
                            snapshotRef.getNamespace(), snapshotRef.getName()));
        }

        if (FlinkStateSnapshotState.COMPLETED != result.getStatus().getState()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Snapshot %s/%s is not complete yet.",
                            snapshotRef.getNamespace(), snapshotRef.getName()));
        }

        var path = result.getStatus().getPath();
        if (StringUtils.isBlank(path)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Snapshot %s/%s path is incorrect: %s.",
                            snapshotRef.getNamespace(), snapshotRef.getName(), path));
        }

        return path;
    }

    protected static FlinkStateSnapshot createFlinkStateSnapshot(
            KubernetesClient kubernetesClient,
            String name,
            FlinkStateSnapshotSpec spec,
            SnapshotTriggerType triggerType) {
        var metadata = new ObjectMeta();
        metadata.setName(name);
        metadata.getLabels().put(CrdConstants.LABEL_SNAPSHOT_TYPE, triggerType.name());

        var snapshot = new FlinkStateSnapshot();
        snapshot.setSpec(spec);
        snapshot.setMetadata(metadata);

        return kubernetesClient.resource(snapshot).create();
    }

    public static FlinkStateSnapshot createUpgradeSavepointResource(
            KubernetesClient kubernetesClient,
            AbstractFlinkResource<?, ?> resource,
            String savepointDirectory,
            SavepointFormatType savepointFormatType,
            boolean disposeOnDelete) {
        var savepointSpec =
                SavepointSpec.builder()
                        .path(savepointDirectory)
                        .formatType(savepointFormatType)
                        .disposeOnDelete(disposeOnDelete)
                        .alreadyExists(true)
                        .build();

        var snapshotSpec =
                FlinkStateSnapshotSpec.builder()
                        .jobReference(JobReference.fromFlinkResource(resource))
                        .savepoint(savepointSpec)
                        .build();

        var resourceName =
                getFlinkStateSnapshotName(SAVEPOINT, SnapshotTriggerType.UPGRADE, resource);
        return createFlinkStateSnapshot(
                kubernetesClient, resourceName, snapshotSpec, SnapshotTriggerType.UPGRADE);
    }

    public static FlinkStateSnapshot createPeriodicSavepointResource(
            KubernetesClient kubernetesClient,
            AbstractFlinkResource<?, ?> resource,
            String savepointDirectory,
            SavepointFormatType savepointFormatType,
            boolean disposeOnDelete) {
        var savepointSpec =
                SavepointSpec.builder()
                        .path(savepointDirectory)
                        .formatType(savepointFormatType)
                        .disposeOnDelete(disposeOnDelete)
                        .build();

        var snapshotSpec =
                FlinkStateSnapshotSpec.builder()
                        .jobReference(JobReference.fromFlinkResource(resource))
                        .savepoint(savepointSpec)
                        .build();

        var resourceName =
                getFlinkStateSnapshotName(SAVEPOINT, SnapshotTriggerType.PERIODIC, resource);
        return createFlinkStateSnapshot(
                kubernetesClient, resourceName, snapshotSpec, SnapshotTriggerType.PERIODIC);
    }

    public static FlinkStateSnapshot createPeriodicCheckpointResource(
            KubernetesClient kubernetesClient,
            AbstractFlinkResource<?, ?> resource,
            CheckpointType checkpointType) {
        var checkpointSpec = CheckpointSpec.builder().checkpointType(checkpointType).build();

        var snapshotSpec =
                FlinkStateSnapshotSpec.builder()
                        .jobReference(JobReference.fromFlinkResource(resource))
                        .checkpoint(checkpointSpec)
                        .build();

        var resourceName =
                getFlinkStateSnapshotName(CHECKPOINT, SnapshotTriggerType.PERIODIC, resource);
        return createFlinkStateSnapshot(
                kubernetesClient, resourceName, snapshotSpec, SnapshotTriggerType.PERIODIC);
    }

    public static <CR extends AbstractFlinkResource<?, ?>>
            List<FlinkStateSnapshot> getFlinkStateSnapshotsForResource(
                    KubernetesClient kubernetesClient, CR resource) {
        return kubernetesClient.resources(FlinkStateSnapshot.class).list().getItems().stream()
                .filter(
                        s ->
                                s.getSpec()
                                        .getJobReference()
                                        .equals(JobReference.fromFlinkResource(resource)))
                .collect(Collectors.toList());
    }

    public static boolean shouldCreateSnapshotResource(
            CustomResourceDefinitionWatcher crdWatcher, Configuration configuration) {
        return configuration.get(SNAPSHOT_RESOURCE_ENABLED)
                && crdWatcher.isCrdInstalled(FlinkStateSnapshot.class);
    }

    public static String getFlinkStateSnapshotName(
            SnapshotType snapshotType,
            SnapshotTriggerType triggerType,
            AbstractFlinkResource<?, ?> referencedResource) {
        var timeStr =
                Instant.now()
                        .atZone(ZoneId.systemDefault())
                        .format(RESOURCE_NAME_DATE_TIME_FORMATTER);
        return String.format(
                "%s-%s-%s-%s-%s",
                snapshotType.name().toLowerCase(),
                triggerType.name().toLowerCase(),
                referencedResource.getMetadata().getName(),
                timeStr,
                UUID.randomUUID());
    }
}
