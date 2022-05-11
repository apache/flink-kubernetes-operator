/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.FlinkSessionJob;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.FilterWatchListDeletable;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;
import io.javaoperatorsdk.operator.processing.event.source.informer.Mappers;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/** Operator SDK related utility functions. */
public class OperatorUtils {

    private static final Logger LOG = LoggerFactory.getLogger(OperatorUtils.class);

    private static final String NAMESPACES_SPLITTER_KEY = ",";
    public static final String CLUSTER_ID_INDEX = "clusterId_index";
    public static final String ALL_NAMESPACE = "allNamespace";

    public static InformerEventSource<Deployment, HasMetadata> createJmDepInformerEventSource(
            KubernetesClient kubernetesClient, String namespace) {
        return createJmDepInformerEventSource(
                kubernetesClient.apps().deployments().inNamespace(namespace), namespace);
    }

    public static InformerEventSource<Deployment, HasMetadata> createJmDepInformerEventSource(
            KubernetesClient kubernetesClient) {
        return createJmDepInformerEventSource(
                kubernetesClient.apps().deployments().inAnyNamespace(), "all");
    }

    private static InformerEventSource<Deployment, HasMetadata> createJmDepInformerEventSource(
            FilterWatchListDeletable<Deployment, DeploymentList> filteredClient, String name) {
        SharedIndexInformer<Deployment> informer =
                filteredClient
                        .withLabel(Constants.LABEL_TYPE_KEY, Constants.LABEL_TYPE_NATIVE_TYPE)
                        .withLabel(
                                Constants.LABEL_COMPONENT_KEY,
                                Constants.LABEL_COMPONENT_JOB_MANAGER)
                        .runnableInformer(0);

        return new InformerEventSource<>(informer, Mappers.fromLabel(Constants.LABEL_APP_KEY)) {
            @Override
            public String name() {
                return name;
            }
        };
    }

    public static Set<String> getWatchedNamespaces() {
        String watchedNamespaces = EnvUtils.get(EnvUtils.ENV_WATCHED_NAMESPACES);

        if (StringUtils.isEmpty(watchedNamespaces)) {
            return Collections.emptySet();
        } else {
            return new HashSet<>(Arrays.asList(watchedNamespaces.split(NAMESPACES_SPLITTER_KEY)));
        }
    }

    public static Optional<FlinkDeployment> getSecondaryResource(
            FlinkSessionJob sessionJob,
            Context context,
            FlinkOperatorConfiguration operatorConfiguration) {
        var identifier =
                operatorConfiguration.getWatchedNamespaces().size() >= 1
                        ? sessionJob.getMetadata().getNamespace()
                        : null;
        return context.getSecondaryResource(FlinkDeployment.class, identifier);
    }

    /**
     * Create informers for session job to build indexer for cluster to session job relations.
     *
     * @return The different namespace's index informer.
     */
    public static Map<String, SharedIndexInformer<FlinkSessionJob>>
            createSessionJobInformersWithIndexer(
                    Set<String> effectiveNamespaces, KubernetesClient kubernetesClient) {
        if (effectiveNamespaces.isEmpty()) {
            return Map.of(
                    ALL_NAMESPACE,
                    kubernetesClient
                            .resources(FlinkSessionJob.class)
                            .inAnyNamespace()
                            .withIndexers(clusterToSessionJobIndexer())
                            .inform());
        } else {
            var informers = new HashMap<String, SharedIndexInformer<FlinkSessionJob>>();
            for (String effectiveNamespace : effectiveNamespaces) {
                informers.put(
                        effectiveNamespace,
                        kubernetesClient
                                .resources(FlinkSessionJob.class)
                                .inNamespace(effectiveNamespace)
                                .withIndexers(clusterToSessionJobIndexer())
                                .inform());
            }
            return informers;
        }
    }

    private static Map<String, Function<FlinkSessionJob, List<String>>>
            clusterToSessionJobIndexer() {
        return Map.of(
                CLUSTER_ID_INDEX, sessionJob -> List.of(sessionJob.getSpec().getDeploymentName()));
    }
}
