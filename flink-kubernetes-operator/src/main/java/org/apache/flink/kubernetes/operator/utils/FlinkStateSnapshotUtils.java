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

import org.apache.flink.autoscaler.utils.DateTimeUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.CrdConstants;
import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;
import org.apache.flink.kubernetes.operator.api.spec.CheckpointSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkStateSnapshotReference;
import org.apache.flink.kubernetes.operator.api.spec.FlinkStateSnapshotSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobReference;
import org.apache.flink.kubernetes.operator.api.spec.SavepointSpec;
import org.apache.flink.kubernetes.operator.api.status.SavepointFormatType;
import org.apache.flink.kubernetes.operator.api.status.SnapshotTriggerType;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.reconciler.SnapshotType;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;

import java.time.Instant;

import static org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus.State.ABANDONED;
import static org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus.State.COMPLETED;
import static org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus.State.IN_PROGRESS;
import static org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus.State.TRIGGER_PENDING;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.SNAPSHOT_RESOURCE_ENABLED;
import static org.apache.flink.kubernetes.operator.reconciler.SnapshotType.CHECKPOINT;
import static org.apache.flink.kubernetes.operator.reconciler.SnapshotType.SAVEPOINT;

/** Utilities class for FlinkStateSnapshot resources. */
public class FlinkStateSnapshotUtils {

    /**
     * From a snapshot reference, return its snapshot path. If a {@link FlinkStateSnapshot} is
     * referenced, it will be retrieved from Kubernetes.
     *
     * @param kubernetesClient kubernetes client
     * @param snapshotRef snapshot reference
     * @return found savepoint path
     */
    public static String getValidatedFlinkStateSnapshotPath(
            KubernetesClient kubernetesClient, FlinkStateSnapshotReference snapshotRef) {
        if (StringUtils.isNotBlank(snapshotRef.getPath())) {
            return snapshotRef.getPath();
        }

        if (StringUtils.isBlank(snapshotRef.getName())) {
            throw new IllegalArgumentException(
                    String.format("Invalid snapshot name: %s", snapshotRef.getName()));
        }

        var result =
                snapshotRef.getNamespace() == null
                        ? kubernetesClient
                                .resources(FlinkStateSnapshot.class)
                                .withName(snapshotRef.getName())
                                .get()
                        : kubernetesClient
                                .resources(FlinkStateSnapshot.class)
                                .inNamespace(snapshotRef.getNamespace())
                                .withName(snapshotRef.getName())
                                .get();

        if (result == null) {
            throw new IllegalStateException(
                    String.format(
                            "Cannot find snapshot %s in namespace %s.",
                            snapshotRef.getNamespace(), snapshotRef.getName()));
        }

        // We can return the savepoint path if it's marked as completed without waiting for the
        // reconciler to update its status.
        if (result.getSpec().isSavepoint() && result.getSpec().getSavepoint().getAlreadyExists()) {
            var path = result.getSpec().getSavepoint().getPath();
            if (!StringUtils.isBlank(path)) {
                return path;
            }
        }

        if (COMPLETED != result.getStatus().getState()) {
            throw new IllegalStateException(
                    String.format(
                            "Snapshot %s/%s is not complete yet.",
                            snapshotRef.getNamespace(), snapshotRef.getName()));
        }

        var path = result.getStatus().getPath();
        if (StringUtils.isBlank(path)) {
            throw new IllegalStateException(
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

    /**
     * Creates a checkpoint {@link FlinkStateSnapshot} resource on the Kubernetes cluster.
     *
     * @param kubernetesClient kubernetes client
     * @param resource Flink resource associated
     * @param savepointPath savepoint path if any
     * @param triggerType trigger type
     * @param savepointFormatType format type
     * @param disposeOnDelete should dispose of data on deletion
     * @return created snapshot
     */
    public static FlinkStateSnapshot createSavepointResource(
            KubernetesClient kubernetesClient,
            AbstractFlinkResource<?, ?> resource,
            @Nullable String savepointPath,
            SnapshotTriggerType triggerType,
            SavepointFormatType savepointFormatType,
            boolean disposeOnDelete) {
        var savepointSpec =
                SavepointSpec.builder()
                        .path(savepointPath)
                        .formatType(savepointFormatType)
                        .disposeOnDelete(disposeOnDelete)
                        .alreadyExists(triggerType == SnapshotTriggerType.UPGRADE)
                        .build();

        var snapshotSpec =
                FlinkStateSnapshotSpec.builder()
                        .jobReference(JobReference.fromFlinkResource(resource))
                        .savepoint(savepointSpec)
                        .build();

        var resourceName = getFlinkStateSnapshotName(SAVEPOINT, triggerType, resource);
        return createFlinkStateSnapshot(kubernetesClient, resourceName, snapshotSpec, triggerType);
    }

    /**
     * Creates a checkpoint {@link FlinkStateSnapshot} resource on the Kubernetes cluster.
     *
     * @param kubernetesClient kubernetes client
     * @param resource Flink resource associated
     * @param triggerType trigger type
     * @return created snapshot
     */
    public static FlinkStateSnapshot createCheckpointResource(
            KubernetesClient kubernetesClient,
            AbstractFlinkResource<?, ?> resource,
            SnapshotTriggerType triggerType) {
        var snapshotSpec =
                FlinkStateSnapshotSpec.builder()
                        .jobReference(JobReference.fromFlinkResource(resource))
                        .checkpoint(new CheckpointSpec())
                        .build();

        var resourceName = getFlinkStateSnapshotName(CHECKPOINT, triggerType, resource);
        return createFlinkStateSnapshot(kubernetesClient, resourceName, snapshotSpec, triggerType);
    }

    /**
     * Based on job configuration and operator configuration, decide if {@link FlinkStateSnapshot}
     * resources should be used or not. Operator configuration will disable the usage of the
     * corresponding CRD was not installed on this Kubernetes cluster.
     *
     * @param operatorConfiguration operator config
     * @param configuration job config
     * @return true if snapshot resources should be created
     */
    public static boolean isSnapshotResourceEnabled(
            FlinkOperatorConfiguration operatorConfiguration, Configuration configuration) {
        return configuration.get(SNAPSHOT_RESOURCE_ENABLED)
                && operatorConfiguration.isSnapshotResourcesEnabled();
    }

    /**
     * Return a generated name for a {@link FlinkStateSnapshot} to be created.
     *
     * @param snapshotType type of snapshot
     * @param triggerType trigger type of snapshot
     * @param referencedResource referenced resource
     * @return result name
     */
    public static String getFlinkStateSnapshotName(
            SnapshotType snapshotType,
            SnapshotTriggerType triggerType,
            AbstractFlinkResource<?, ?> referencedResource) {
        return String.format(
                "%s-%s-%s-%d",
                referencedResource.getMetadata().getName(),
                snapshotType.name().toLowerCase(),
                triggerType.name().toLowerCase(),
                System.currentTimeMillis());
    }

    /**
     * For an upgrade savepoint, create a {@link FlinkStateSnapshot} on the Kubernetes cluster and
     * return its reference if snapshot resources are enabled. In other case return a reference
     * containing only the path.
     *
     * @param conf job configuration
     * @param operatorConf operator configuration
     * @param kubernetesClient kubernetes client
     * @param flinkResource referenced Flink resource
     * @param savepointFormatType savepoint format type
     * @param savepointPath path of savepoint
     * @return reference for snapshot
     */
    public static FlinkStateSnapshotReference createReferenceForUpgradeSavepoint(
            Configuration conf,
            FlinkOperatorConfiguration operatorConf,
            KubernetesClient kubernetesClient,
            AbstractFlinkResource<?, ?> flinkResource,
            SavepointFormatType savepointFormatType,
            String savepointPath) {
        if (!isSnapshotResourceEnabled(operatorConf, conf)) {
            return FlinkStateSnapshotReference.fromPath(savepointPath);
        }

        var disposeOnDelete =
                conf.get(KubernetesOperatorConfigOptions.OPERATOR_JOB_SAVEPOINT_DISPOSE_ON_DELETE);
        var snapshot =
                createSavepointResource(
                        kubernetesClient,
                        flinkResource,
                        savepointPath,
                        SnapshotTriggerType.UPGRADE,
                        savepointFormatType,
                        disposeOnDelete);
        return FlinkStateSnapshotReference.fromResource(snapshot);
    }

    /**
     * Abandons a FlinkStateSnapshot resource if the referenced job is not found or not running.
     *
     * @param client Kubernetes client
     * @param snapshot snapshot
     * @param secondaryResource associated Flink resource
     * @param eventRecorder event recorder to trigger Kubernetes event
     * @return true if the resource was abandoned
     */
    public static boolean abandonSnapshotIfJobNotRunning(
            KubernetesClient client,
            FlinkStateSnapshot snapshot,
            @Nullable AbstractFlinkResource<?, ?> secondaryResource,
            EventRecorder eventRecorder) {
        if (secondaryResource == null) {
            var message =
                    String.format(
                            "Secondary resource %s for savepoint %s was not found",
                            snapshot.getSpec().getJobReference(), snapshot.getMetadata().getName());
            snapshotAbandoned(client, snapshot, eventRecorder, message);
            return true;
        }

        if (!ReconciliationUtils.isJobRunning(secondaryResource.getStatus())) {
            var message =
                    String.format(
                            "Secondary resource %s for savepoint %s is not running",
                            snapshot.getSpec().getJobReference(), snapshot.getMetadata().getName());
            snapshotAbandoned(client, snapshot, eventRecorder, message);
            return true;
        }

        return false;
    }

    /**
     * Sets a snapshot's state to {@link
     * org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus.State#ABANDONED}.
     *
     * @param kubernetesClient kubernetes client
     * @param eventRecorder event recorder to add event
     * @param snapshot snapshot resource
     * @param error message for the event and to add to the resource status
     */
    private static void snapshotAbandoned(
            KubernetesClient kubernetesClient,
            FlinkStateSnapshot snapshot,
            EventRecorder eventRecorder,
            String error) {
        eventRecorder.triggerSnapshotEvent(
                snapshot,
                EventRecorder.Type.Warning,
                EventRecorder.Reason.SnapshotAbandoned,
                EventRecorder.Component.Snapshot,
                error,
                kubernetesClient);

        snapshot.getStatus().setState(ABANDONED);
        snapshot.getStatus().setPath(null);
        snapshot.getStatus().setError(error);
        snapshot.getStatus().setResultTimestamp(DateTimeUtils.kubernetes(Instant.now()));
    }

    /**
     * Sets a snapshot's state to {@link
     * org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus.State#COMPLETED}.
     *
     * @param snapshot snapshot resource
     * @param location result location
     * @param setTriggerTimestamp if ture, set the trigger timestamp to current time
     */
    public static void snapshotSuccessful(
            FlinkStateSnapshot snapshot, String location, boolean setTriggerTimestamp) {
        var time = DateTimeUtils.kubernetes(Instant.now());

        snapshot.getStatus().setState(COMPLETED);
        snapshot.getStatus().setPath(location);
        snapshot.getStatus().setError(null);
        snapshot.getStatus().setResultTimestamp(time);
        if (setTriggerTimestamp) {
            snapshot.getStatus().setTriggerTimestamp(time);
        }
    }

    /**
     * Sets a snapshot's state to {@link
     * org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus.State#IN_PROGRESS}.
     *
     * @param snapshot snapshot resource
     * @param triggerId trigger ID
     */
    public static void snapshotInProgress(FlinkStateSnapshot snapshot, String triggerId) {
        snapshot.getMetadata()
                .getLabels()
                .putIfAbsent(CrdConstants.LABEL_SNAPSHOT_TYPE, SnapshotTriggerType.MANUAL.name());
        snapshot.getStatus().setState(IN_PROGRESS);
        snapshot.getStatus().setTriggerId(triggerId);
        snapshot.getStatus().setTriggerTimestamp(DateTimeUtils.kubernetes(Instant.now()));
    }

    /**
     * Sets a snapshot's state to {@link
     * org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus.State#TRIGGER_PENDING}.
     *
     * @param snapshot snapshot resource
     */
    public static void snapshotTriggerPending(FlinkStateSnapshot snapshot) {
        snapshot.getStatus().setState(TRIGGER_PENDING);
    }
}
