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

package org.apache.flink.kubernetes.operator.admission.informer;

import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.util.Preconditions;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.javaoperatorsdk.operator.api.reconciler.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static io.javaoperatorsdk.operator.api.reconciler.Constants.DEFAULT_NAMESPACES_SET;

/** The manager of the informers. */
public class InformerManager {

    private static final Logger LOG = LoggerFactory.getLogger(InformerManager.class);
    private final Set<String> watchedNamespaces = ConcurrentHashMap.newKeySet();
    private final KubernetesClient kubernetesClient;
    private volatile Map<String, SharedIndexInformer<FlinkDeployment>> flinkDepInformers;
    private volatile Map<String, SharedIndexInformer<FlinkSessionJob>> flinkSessionJobInformers;

    public InformerManager(KubernetesClient kubernetesClient) {
        this.kubernetesClient = kubernetesClient;
    }

    public SharedIndexInformer<FlinkDeployment> getFlinkDepInformer(String namespace) {
        initFlinkDepInformers();
        var effectiveNamespace =
                DEFAULT_NAMESPACES_SET.equals(watchedNamespaces)
                        ? Constants.WATCH_ALL_NAMESPACES
                        : namespace;
        var informer = flinkDepInformers.get(effectiveNamespace);
        Preconditions.checkNotNull(
                informer, String.format("The informer for %s should not be null", namespace));
        return informer;
    }

    private void initFlinkDepInformers() {
        if (flinkDepInformers == null) {
            synchronized (this) {
                if (flinkDepInformers == null) {
                    var runnableInformers =
                            createRunnableInformers(
                                    FlinkDeployment.class, watchedNamespaces, kubernetesClient);
                    for (Map.Entry<String, SharedIndexInformer<FlinkDeployment>> runnableInformer :
                            runnableInformers.entrySet()) {
                        runnableInformer.getValue().run();
                    }
                    this.flinkDepInformers = runnableInformers;
                    LOG.info(
                            "Created FlinkDeployment informers for {}", flinkDepInformers.keySet());
                }
            }
        }
    }

    public SharedIndexInformer<FlinkSessionJob> getFlinkSessionJobInformer(String namespace) {
        initFlinkSessionJobInformers();
        var effectiveNamespace =
                DEFAULT_NAMESPACES_SET.equals(watchedNamespaces)
                        ? Constants.WATCH_ALL_NAMESPACES
                        : namespace;
        var informer = flinkSessionJobInformers.get(effectiveNamespace);
        Preconditions.checkNotNull(
                informer, String.format("The informer for %s should not be null", namespace));
        return informer;
    }

    private void initFlinkSessionJobInformers() {
        if (flinkSessionJobInformers == null) {
            synchronized (this) {
                if (flinkSessionJobInformers == null) {
                    var runnableInformers =
                            createRunnableInformers(
                                    FlinkSessionJob.class, watchedNamespaces, kubernetesClient);
                    for (Map.Entry<String, SharedIndexInformer<FlinkSessionJob>> runnableInformer :
                            runnableInformers.entrySet()) {
                        runnableInformer.getValue().run();
                    }
                    this.flinkSessionJobInformers = runnableInformers;
                    LOG.info(
                            "Created FlinkSessionJob informers for {}",
                            flinkSessionJobInformers.keySet());
                }
            }
        }
    }

    private static <CR extends HasMetadata>
            Map<String, SharedIndexInformer<CR>> createRunnableInformers(
                    Class<CR> resourceClass,
                    Set<String> effectiveNamespaces,
                    KubernetesClient kubernetesClient) {
        if (DEFAULT_NAMESPACES_SET.equals(effectiveNamespaces)) {
            return Map.of(
                    Constants.WATCH_ALL_NAMESPACES,
                    kubernetesClient.resources(resourceClass).inAnyNamespace().runnableInformer(0));
        } else {
            var informers = new HashMap<String, SharedIndexInformer<CR>>();
            for (String effectiveNamespace : effectiveNamespaces) {
                informers.put(
                        effectiveNamespace,
                        kubernetesClient
                                .resources(resourceClass)
                                .inNamespace(effectiveNamespace)
                                .runnableInformer(0));
            }
            return informers;
        }
    }

    public void setNamespaces(Set<String> watchedNamespaces) {
        LOG.info("Setting namespaces to {}", watchedNamespaces);
        this.watchedNamespaces.clear();
        this.watchedNamespaces.addAll(watchedNamespaces);
        if (flinkDepInformers != null) {
            synchronized (this) {
                if (flinkDepInformers != null) {
                    flinkDepInformers.forEach(
                            (key, value) -> {
                                LOG.info("Stopping FlinkDeployment informer in {})", key);
                                value.stop();
                            });
                }
                flinkDepInformers = null;
            }
        }
        if (flinkSessionJobInformers != null) {
            synchronized (this) {
                if (flinkSessionJobInformers != null) {
                    flinkSessionJobInformers.forEach(
                            (key, value) -> {
                                LOG.info("Stopping FlinkSessionJob informer in {})", key);
                                value.stop();
                            });
                }
                flinkSessionJobInformers = null;
            }
        }
    }
}
