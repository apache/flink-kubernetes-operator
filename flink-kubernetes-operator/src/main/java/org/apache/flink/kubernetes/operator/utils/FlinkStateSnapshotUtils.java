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
import org.apache.flink.kubernetes.operator.api.spec.FlinkStateSnapshotSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobReference;
import org.apache.flink.kubernetes.operator.api.spec.SavepointSpec;
import org.apache.flink.kubernetes.operator.api.status.SavepointFormatType;
import org.apache.flink.kubernetes.operator.api.status.SnapshotTriggerType;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.reconciler.SnapshotType;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import org.apache.commons.lang3.ObjectUtils;

import javax.annotation.Nullable;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus.State.ABANDONED;
import static org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus.State.COMPLETED;
import static org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus.State.IN_PROGRESS;
import static org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus.State.TRIGGER_PENDING;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.SNAPSHOT_RESOURCE_ENABLED;
import static org.apache.flink.kubernetes.operator.reconciler.SnapshotType.CHECKPOINT;
import static org.apache.flink.kubernetes.operator.reconciler.SnapshotType.SAVEPOINT;

/** Utilities class for FlinkStateSnapshot resources. */
public class FlinkStateSnapshotUtils {

    protected static FlinkStateSnapshot createFlinkStateSnapshot(
            KubernetesClient kubernetesClient,
            String namespace,
            String name,
            FlinkStateSnapshotSpec spec,
            SnapshotTriggerType triggerType) {
        var metadata = new ObjectMeta();
        metadata.setNamespace(namespace);
        metadata.setName(name);
        metadata.getLabels().put(CrdConstants.LABEL_SNAPSHOT_TRIGGER_TYPE, triggerType.name());

        var snapshot = new FlinkStateSnapshot();
        snapshot.setSpec(spec);
        snapshot.setMetadata(metadata);

        return kubernetesClient.resource(snapshot).create();
    }

    /**
     * Extracts snapshot trigger type from a snapshot resource. If unable to do so, return {@link
     * SnapshotTriggerType#UNKNOWN}
     *
     * @param snapshot resource to check
     * @return trigger type
     */
    public static SnapshotTriggerType getSnapshotTriggerType(FlinkStateSnapshot snapshot) {
        var triggerTypeStr =
                snapshot.getMetadata().getLabels().get(CrdConstants.LABEL_SNAPSHOT_TRIGGER_TYPE);
        try {
            return SnapshotTriggerType.valueOf(triggerTypeStr);
        } catch (NullPointerException | IllegalArgumentException e) {
            return SnapshotTriggerType.UNKNOWN;
        }
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
        return createFlinkStateSnapshot(
                kubernetesClient,
                resource.getMetadata().getNamespace(),
                resourceName,
                snapshotSpec,
                triggerType);
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
        return createFlinkStateSnapshot(
                kubernetesClient,
                resource.getMetadata().getNamespace(),
                resourceName,
                snapshotSpec,
                triggerType);
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
     * For an upgrade savepoint, create a {@link FlinkStateSnapshot} on the Kubernetes cluster.
     *
     * @param conf job configuration
     * @param operatorConf operator configuration
     * @param kubernetesClient kubernetes client
     * @param flinkResource referenced Flink resource
     * @param savepointFormatType savepoint format type
     * @param savepointPath path of savepoint
     * @return State snapshot resource
     */
    public static Optional<FlinkStateSnapshot> createUpgradeSnapshotResource(
            Configuration conf,
            FlinkOperatorConfiguration operatorConf,
            KubernetesClient kubernetesClient,
            AbstractFlinkResource<?, ?> flinkResource,
            SavepointFormatType savepointFormatType,
            String savepointPath) {
        if (!isSnapshotResourceEnabled(operatorConf, conf)) {
            return Optional.empty();
        }

        var disposeOnDelete =
                conf.get(KubernetesOperatorConfigOptions.OPERATOR_JOB_SAVEPOINT_DISPOSE_ON_DELETE);
        var savepointResource =
                createSavepointResource(
                        kubernetesClient,
                        flinkResource,
                        savepointPath,
                        SnapshotTriggerType.UPGRADE,
                        savepointFormatType,
                        disposeOnDelete);
        return Optional.of(savepointResource);
    }

    /**
     * Returns a supplier of all relevant FlinkStateSnapshot resources for a given Flink resource.
     * If FlinkStateSnapshot resources are disabled, the supplier returns an empty set.
     *
     * @param ctx resource context
     * @return supplier for FlinkStateSnapshot resources
     */
    public static Supplier<Set<FlinkStateSnapshot>> getFlinkStateSnapshotsSupplier(
            FlinkResourceContext<?> ctx) {
        return () -> {
            if (FlinkStateSnapshotUtils.isSnapshotResourceEnabled(
                    ctx.getOperatorConfig(), ctx.getObserveConfig())) {
                return ObjectUtils.firstNonNull(
                        ctx.getJosdkContext().getSecondaryResources(FlinkStateSnapshot.class),
                        Set.of());
            } else {
                return Set.of();
            }
        };
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
                .putIfAbsent(
                        CrdConstants.LABEL_SNAPSHOT_TRIGGER_TYPE,
                        SnapshotTriggerType.MANUAL.name());
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

    /**
     * Creates a map of labels that can be applied to a snapshot resource based on its current spec
     * and status. As described in FLINK-36109, we should set up selectable spec fields instead of
     * labels once the Kubernetes feature is GA and widely supported.
     *
     * @param snapshot snapshot instance
     * @param secondaryResourceOpt optional referenced Flink resource
     * @return map of auto-generated labels
     */
    public static Map<String, String> getSnapshotLabels(
            FlinkStateSnapshot snapshot,
            Optional<AbstractFlinkResource<?, ?>> secondaryResourceOpt) {
        var labels = new HashMap<String, String>();
        labels.put(
                CrdConstants.LABEL_SNAPSHOT_TYPE,
                snapshot.getSpec().isSavepoint()
                        ? SnapshotType.SAVEPOINT.name()
                        : SnapshotType.CHECKPOINT.name());
        labels.put(
                CrdConstants.LABEL_SNAPSHOT_TRIGGER_TYPE,
                snapshot.getMetadata()
                        .getLabels()
                        .getOrDefault(
                                CrdConstants.LABEL_SNAPSHOT_TRIGGER_TYPE,
                                SnapshotTriggerType.MANUAL.name()));

        Optional.ofNullable(snapshot.getStatus())
                .ifPresent(
                        status ->
                                labels.put(
                                        CrdConstants.LABEL_SNAPSHOT_STATE,
                                        status.getState().name()));

        secondaryResourceOpt.ifPresent(
                secondaryResource -> {
                    labels.put(
                            CrdConstants.LABEL_SNAPSHOT_JOB_REFERENCE_KIND,
                            secondaryResource.getKind());
                    labels.put(
                            CrdConstants.LABEL_SNAPSHOT_JOB_REFERENCE_NAME,
                            secondaryResource.getMetadata().getName());
                });

        return labels;
    }

    /**
     * Extracts the namespace of the job reference from a snapshot resource. This is either
     * explicitly specified in the job reference, or it will fallback to the namespace of the
     * snapshot.
     *
     * @param snapshot snapshot resource
     * @return namespace with the job reference to be found in
     */
    public static ResourceID getSnapshotJobReferenceResourceId(FlinkStateSnapshot snapshot) {
        return new ResourceID(
                snapshot.getSpec().getJobReference().getName(),
                snapshot.getMetadata().getNamespace());
    }
}
