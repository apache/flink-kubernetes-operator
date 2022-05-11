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

import org.apache.flink.kubernetes.operator.crd.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.utils.OperatorUtils;
import org.apache.flink.util.Preconditions;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

/** The manager of the informers. */
public class InformerManager {

    private static final Logger LOG = LoggerFactory.getLogger(InformerManager.class);
    private final Set<String> watchedNamespaces;
    private final KubernetesClient kubernetesClient;
    private volatile Map<String, SharedIndexInformer<FlinkSessionJob>> sessionJobInformers;

    public InformerManager(Set<String> watchedNamespaces, KubernetesClient kubernetesClient) {
        this.watchedNamespaces = watchedNamespaces;
        this.kubernetesClient = kubernetesClient;
    }

    public SharedIndexInformer<FlinkSessionJob> getSessionJobInformer(String namespace) {
        initSessionJobInformer();
        var effectiveNamespace =
                watchedNamespaces.isEmpty() ? OperatorUtils.ALL_NAMESPACE : namespace;
        var informer = sessionJobInformers.get(effectiveNamespace);
        Preconditions.checkNotNull(
                informer, String.format("The informer for %s should not be null", namespace));
        return informer;
    }

    private void initSessionJobInformer() {
        if (sessionJobInformers == null) {
            synchronized (this) {
                if (sessionJobInformers == null) {
                    this.sessionJobInformers =
                            OperatorUtils.createSessionJobInformersWithIndexer(
                                    watchedNamespaces, kubernetesClient);
                    LOG.info("Created informer for {}", sessionJobInformers.keySet());
                }
            }
        }
    }
}
