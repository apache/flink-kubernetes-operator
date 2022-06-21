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

import org.apache.flink.kubernetes.operator.controller.FlinkDeploymentController;
import org.apache.flink.kubernetes.operator.controller.FlinkSessionJobController;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.FlinkSessionJob;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.javaoperatorsdk.operator.api.config.informer.InformerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.PrimaryToSecondaryMapper;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;
import io.javaoperatorsdk.operator.processing.event.source.informer.Mappers;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Utility class to locate secondary resources. */
public class EventSourceUtils {

    private static final String FLINK_DEPLOYMENT_IDX = FlinkDeploymentController.class.getName();
    private static final String FLINK_SESSIONJOB_IDX = FlinkSessionJobController.class.getName();

    public static InformerEventSource<Deployment, FlinkDeployment> getDeploymentInformerEventSource(
            EventSourceContext<FlinkDeployment> context) {
        final String labelSelector =
                Map.of(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_JOB_MANAGER)
                        .entrySet().stream()
                        .map(Object::toString)
                        .collect(Collectors.joining(","));

        var configuration =
                InformerConfiguration.from(Deployment.class, context)
                        .withLabelSelector(labelSelector)
                        .withSecondaryToPrimaryMapper(Mappers.fromLabel(Constants.LABEL_APP_KEY))
                        .withNamespacesInheritedFromController(context)
                        .followNamespaceChanges(true)
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

        InformerConfiguration<FlinkSessionJob> configuration =
                InformerConfiguration.from(FlinkSessionJob.class, context)
                        .withSecondaryToPrimaryMapper(
                                sessionJob ->
                                        context.getPrimaryCache()
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
                        .withNamespacesInheritedFromController(context)
                        .followNamespaceChanges(true)
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

        InformerConfiguration<FlinkDeployment> configuration =
                InformerConfiguration.from(FlinkDeployment.class, context)
                        .withSecondaryToPrimaryMapper(
                                flinkDeployment ->
                                        context.getPrimaryCache()
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
                        .withNamespacesInheritedFromController(context)
                        .followNamespaceChanges(true)
                        .build();
        return new InformerEventSource<>(configuration, context);
    }

    private static String indexKey(String name, String namespace) {
        return name + "#" + namespace;
    }
}
