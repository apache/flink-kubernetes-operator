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

import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.CrdConstants;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;
import org.apache.flink.kubernetes.operator.api.spec.JobKind;
import org.apache.flink.kubernetes.operator.api.status.SnapshotTriggerType;
import org.apache.flink.kubernetes.operator.controller.FlinkDeploymentController;
import org.apache.flink.kubernetes.operator.controller.FlinkSessionJobController;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.javaoperatorsdk.operator.api.config.informer.InformerEventSourceConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.PrimaryToSecondaryMapper;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Utility class to locate secondary resources. */
public class EventSourceUtils {

    private static final String FLINK_DEPLOYMENT_IDX = FlinkDeploymentController.class.getName();
    private static final String FLINK_SESSIONJOB_IDX = FlinkSessionJobController.class.getName();
    private static final String FLINK_STATE_SNAPSHOT_IDX = FlinkStateSnapshot.class.getName();

    public static <T extends AbstractFlinkResource<?, ?>>
            InformerEventSource<FlinkStateSnapshot, T>
                    getStateSnapshotForFlinkResourceInformerEventSource(
                            EventSourceContext<T> context) {
        var labelFilters =
                Stream.of(SnapshotTriggerType.PERIODIC, SnapshotTriggerType.UPGRADE)
                        .map(Enum::name)
                        .collect(Collectors.joining(","));
        var labelSelector =
                String.format("%s in (%s)", CrdConstants.LABEL_SNAPSHOT_TRIGGER_TYPE, labelFilters);
        var configuration =
                InformerEventSourceConfiguration.from(
                                FlinkStateSnapshot.class, context.getPrimaryResourceClass())
                        .withLabelSelector(labelSelector)
                        .withSecondaryToPrimaryMapper(
                                snapshot -> {
                                    var jobRef = snapshot.getSpec().getJobReference();
                                    if (jobRef == null || jobRef.getName() == null) {
                                        return Collections.emptySet();
                                    }
                                    return Set.of(
                                            new ResourceID(
                                                    snapshot.getSpec().getJobReference().getName(),
                                                    snapshot.getMetadata().getNamespace()));
                                })
                        .withNamespacesInheritedFromController()
                        .withFollowControllerNamespacesChanges(true)
                        .build();
        return new InformerEventSource<>(configuration, context);
    }

    public static InformerEventSource<Deployment, FlinkDeployment> getDeploymentInformerEventSource(
            EventSourceContext<FlinkDeployment> context) {
        final String labelSelector =
                Map.of(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_JOB_MANAGER)
                        .entrySet()
                        .stream()
                        .map(Object::toString)
                        .collect(Collectors.joining(","));

        var configuration =
                InformerEventSourceConfiguration.from(Deployment.class, FlinkDeployment.class)
                        .withLabelSelector(labelSelector)
                        .withSecondaryToPrimaryMapper(fromLabel(Constants.LABEL_APP_KEY))
                        .withNamespacesInheritedFromController()
                        .withFollowControllerNamespacesChanges(true)
                        .build();

        return new InformerEventSource<>(configuration, context);
    }

    public static InformerEventSource<FlinkSessionJob, FlinkDeployment>
            getSessionJobInformerEventSource(EventSourceContext<FlinkDeployment> context) {

        context.getPrimaryCache()
                .addIndexer(
                        FLINK_DEPLOYMENT_IDX,
                        flinkDeployment ->
                                List.of(
                                        indexKey(
                                                flinkDeployment.getMetadata().getName(),
                                                flinkDeployment.getMetadata().getNamespace())));

        var configuration =
                InformerEventSourceConfiguration.from(FlinkSessionJob.class, FlinkDeployment.class)
                        .withSecondaryToPrimaryMapper(
                                sessionJob ->
                                        context
                                                .getPrimaryCache()
                                                .byIndex(
                                                        FLINK_DEPLOYMENT_IDX,
                                                        indexKey(
                                                                sessionJob
                                                                        .getSpec()
                                                                        .getDeploymentName(),
                                                                sessionJob
                                                                        .getMetadata()
                                                                        .getNamespace()))
                                                .stream()
                                                .map(ResourceID::fromResource)
                                                .collect(Collectors.toSet()))
                        .withNamespacesInheritedFromController()
                        .withFollowControllerNamespacesChanges(true)
                        .build();

        return new InformerEventSource<>(configuration, context);
    }

    public static InformerEventSource<FlinkDeployment, FlinkSessionJob>
            getFlinkDeploymentInformerEventSource(EventSourceContext<FlinkSessionJob> context) {
        context.getPrimaryCache()
                .addIndexer(
                        FLINK_SESSIONJOB_IDX,
                        sessionJob ->
                                List.of(
                                        indexKey(
                                                sessionJob.getSpec().getDeploymentName(),
                                                sessionJob.getMetadata().getNamespace())));

        var configuration =
                InformerEventSourceConfiguration.from(FlinkDeployment.class, FlinkSessionJob.class)
                        .withSecondaryToPrimaryMapper(
                                flinkDeployment ->
                                        context
                                                .getPrimaryCache()
                                                .byIndex(
                                                        FLINK_SESSIONJOB_IDX,
                                                        indexKey(
                                                                flinkDeployment
                                                                        .getMetadata()
                                                                        .getName(),
                                                                flinkDeployment
                                                                        .getMetadata()
                                                                        .getNamespace()))
                                                .stream()
                                                .map(ResourceID::fromResource)
                                                .collect(Collectors.toSet()))
                        .withPrimaryToSecondaryMapper(
                                (PrimaryToSecondaryMapper<FlinkSessionJob>)
                                        sessionJob ->
                                                Set.of(
                                                        new ResourceID(
                                                                sessionJob
                                                                        .getSpec()
                                                                        .getDeploymentName(),
                                                                sessionJob
                                                                        .getMetadata()
                                                                        .getNamespace())))
                        .withNamespacesInheritedFromController()
                        .withFollowControllerNamespacesChanges(true)
                        .build();
        return new InformerEventSource<>(configuration, context);
    }

    public static EventSource[] getFlinkStateSnapshotInformerEventSources(
            EventSourceContext<FlinkStateSnapshot> context) {
        context.getPrimaryCache()
                .addIndexer(
                        FLINK_STATE_SNAPSHOT_IDX,
                        savepoint -> {
                            if (savepoint.getSpec().getJobReference() == null
                                    || savepoint.getSpec().getJobReference().getName() == null) {
                                return Collections.emptyList();
                            }
                            return List.of(
                                    indexKey(
                                            savepoint.getSpec().getJobReference().toString(),
                                            savepoint.getMetadata().getNamespace()));
                        });

        var configurationFlinkSessionJob =
                InformerEventSourceConfiguration.from(
                                FlinkSessionJob.class, FlinkStateSnapshot.class)
                        .withSecondaryToPrimaryMapper(getSnapshotPrimaryMapper(context))
                        .withPrimaryToSecondaryMapper(
                                (PrimaryToSecondaryMapper<FlinkStateSnapshot>)
                                        snapshot -> {
                                            if (!JobKind.FLINK_SESSION_JOB.equals(
                                                    snapshot.getSpec()
                                                            .getJobReference()
                                                            .getKind())) {
                                                return Set.of();
                                            }
                                            return Set.of(
                                                    FlinkStateSnapshotUtils
                                                            .getSnapshotJobReferenceResourceId(
                                                                    snapshot));
                                        })
                        .withNamespacesInheritedFromController()
                        .withFollowControllerNamespacesChanges(true)
                        .build();
        var flinkSessionJobEventSource =
                new InformerEventSource<>(configurationFlinkSessionJob, context);

        var configurationFlinkDeployment =
                InformerEventSourceConfiguration.from(
                                FlinkDeployment.class, FlinkStateSnapshot.class)
                        .withSecondaryToPrimaryMapper(getSnapshotPrimaryMapper(context))
                        .withPrimaryToSecondaryMapper(
                                (PrimaryToSecondaryMapper<FlinkStateSnapshot>)
                                        snapshot -> {
                                            if (JobKind.FLINK_SESSION_JOB.equals(
                                                    snapshot.getSpec()
                                                            .getJobReference()
                                                            .getKind())) {

                                                // If FlinkSessionJob, retrieve deployment
                                                var resourceId =
                                                        FlinkStateSnapshotUtils
                                                                .getSnapshotJobReferenceResourceId(
                                                                        snapshot);
                                                var flinkSessionJob =
                                                        flinkSessionJobEventSource
                                                                .get(resourceId)
                                                                .orElseThrow();
                                                return Set.of(
                                                        new ResourceID(
                                                                flinkSessionJob
                                                                        .getSpec()
                                                                        .getDeploymentName(),
                                                                flinkSessionJob
                                                                        .getMetadata()
                                                                        .getNamespace()));
                                            }
                                            return Set.of(
                                                    FlinkStateSnapshotUtils
                                                            .getSnapshotJobReferenceResourceId(
                                                                    snapshot));
                                        })
                        .withNamespacesInheritedFromController()
                        .withFollowControllerNamespacesChanges(true)
                        .build();
        var flinkDeploymentEventSource =
                new InformerEventSource<>(configurationFlinkDeployment, context);

        return new EventSource[] {flinkSessionJobEventSource, flinkDeploymentEventSource};
    }

    private static <T extends AbstractFlinkResource<?, ?>>
            SecondaryToPrimaryMapper<T> getSnapshotPrimaryMapper(
                    EventSourceContext<FlinkStateSnapshot> ctx) {
        return flinkResource ->
                ctx
                        .getPrimaryCache()
                        .byIndex(
                                FLINK_STATE_SNAPSHOT_IDX,
                                indexKey(
                                        flinkResource.getMetadata().getName(),
                                        flinkResource.getMetadata().getNamespace()))
                        .stream()
                        .map(ResourceID::fromResource)
                        .collect(Collectors.toSet());
    }

    public static <T extends HasMetadata> SecondaryToPrimaryMapper<T> fromLabel(String nameKey) {
        return resource -> {
            final var metadata = resource.getMetadata();
            if (metadata == null) {
                return Collections.emptySet();
            } else {
                final var map = metadata.getLabels();
                if (map == null) {
                    return Collections.emptySet();
                }
                var name = map.get(nameKey);
                if (name == null) {
                    return Collections.emptySet();
                }
                var namespace = resource.getMetadata().getNamespace();
                return Set.of(new ResourceID(name, namespace));
            }
        };
    }

    private static String indexKey(String name, String namespace) {
        return name + "#" + namespace;
    }
}
