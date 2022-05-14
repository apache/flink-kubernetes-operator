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

package org.apache.flink.kubernetes.operator.informer;

import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.utils.OperatorUtils;
import org.apache.flink.util.Preconditions;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

/** The manager of the informers. */
public class InformerManager {

    private static final Logger LOG = LoggerFactory.getLogger(InformerManager.class);
    private final Set<String> watchedNamespaces;
    private final KubernetesClient kubernetesClient;
    private volatile Map<String, SharedIndexInformer<FlinkSessionJob>> sessionJobInformers;
    private volatile Map<String, SharedIndexInformer<FlinkDeployment>> flinkDepInformers;

    public InformerManager(Set<String> watchedNamespaces, KubernetesClient kubernetesClient) {
        this.watchedNamespaces = watchedNamespaces;
        this.kubernetesClient = kubernetesClient;
        LOG.info(
                "Created informer manager with watchedNamespaces: {}",
                watchedNamespaces.isEmpty()
                        ? "[" + OperatorUtils.ALL_NAMESPACE + "]"
                        : watchedNamespaces);
    }

    public SharedIndexInformer<FlinkSessionJob> getSessionJobInformer(String namespace) {
        initSessionJobInformersWithIndexer();
        var effectiveNamespace =
                watchedNamespaces.isEmpty() ? OperatorUtils.ALL_NAMESPACE : namespace;
        var informer = sessionJobInformers.get(effectiveNamespace);
        Preconditions.checkNotNull(
                informer, String.format("The informer for %s should not be null", namespace));
        return informer;
    }

    public SharedIndexInformer<FlinkDeployment> getFlinkDepInformer(String namespace) {
        initFlinkDepInformers();
        var effectiveNamespace =
                watchedNamespaces.isEmpty() ? OperatorUtils.ALL_NAMESPACE : namespace;
        var informer = flinkDepInformers.get(effectiveNamespace);
        Preconditions.checkNotNull(
                informer, String.format("The informer for %s should not be null", namespace));
        return informer;
    }

    private void initSessionJobInformersWithIndexer() {
        if (sessionJobInformers == null) {
            synchronized (this) {
                if (sessionJobInformers == null) {
                    var runnableInformers =
                            OperatorUtils.createRunnableInformer(
                                    FlinkSessionJob.class, watchedNamespaces, kubernetesClient);
                    for (Map.Entry<String, SharedIndexInformer<FlinkSessionJob>> runnableInformer :
                            runnableInformers.entrySet()) {
                        runnableInformer
                                .getValue()
                                .addIndexers(
                                        Map.of(
                                                OperatorUtils.CLUSTER_ID_INDEX,
                                                sessionJob ->
                                                        List.of(
                                                                sessionJob
                                                                        .getSpec()
                                                                        .getDeploymentName())));
                        runnableInformer.getValue().run();
                    }
                    this.sessionJobInformers = runnableInformers;
                    LOG.info("Created session job informers for {}", sessionJobInformers.keySet());
                }
            }
        }
    }

    private void initFlinkDepInformers() {
        if (flinkDepInformers == null) {
            synchronized (this) {
                if (flinkDepInformers == null) {
                    var runnableInformers =
                            OperatorUtils.createRunnableInformer(
                                    FlinkDeployment.class, watchedNamespaces, kubernetesClient);
                    for (Map.Entry<String, SharedIndexInformer<FlinkDeployment>> runnableInformer :
                            runnableInformers.entrySet()) {
                        runnableInformer.getValue().run();
                    }
                    this.flinkDepInformers = runnableInformers;
                    LOG.info(
                            "Created flink deployment informers for {}",
                            flinkDepInformers.keySet());
                }
            }
        }
    }
}
