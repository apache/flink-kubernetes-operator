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

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.config.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.IndexedResourceCache;
import io.javaoperatorsdk.operator.api.reconciler.RetryInfo;
import io.javaoperatorsdk.operator.api.reconciler.dependent.managed.ManagedWorkflowAndDependentResourceContext;
import io.javaoperatorsdk.operator.processing.event.EventSourceRetriever;
import lombok.Setter;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

/**
 * JOSDK Context for unit testing.
 *
 * @param <P>
 */
public class TestingJosdkContext<P extends HasMetadata> implements Context<P> {

    @Setter private KubernetesClient kubernetesClient;

    @Setter
    private Map<Class<? extends HasMetadata>, List<? extends HasMetadata>> secondaryResources =
            new ConcurrentHashMap<>();

    public TestingJosdkContext() {}

    public TestingJosdkContext(KubernetesClient kubernetesClient) {
        this.kubernetesClient = kubernetesClient;
    }

    public TestingJosdkContext(
            KubernetesClient kubernetesClient,
            Map<Class<? extends HasMetadata>, List<? extends HasMetadata>> secondaryResources) {
        this.kubernetesClient = kubernetesClient;
        this.secondaryResources = secondaryResources;
    }

    @Override
    public Optional<RetryInfo> getRetryInfo() {
        return Optional.empty();
    }

    @Override
    public <R> Set<R> getSecondaryResources(Class<R> expectedType) {
        var res = secondaryResources.get(expectedType);
        if (res == null) {
            return Set.of();
        }
        return (Set<R>) new HashSet<>(res);
    }

    @Override
    public <R> Optional<R> getSecondaryResource(Class<R> expectedType, String eventSourceName) {
        var resources = getSecondaryResources(expectedType);
        if (resources.isEmpty()) {
            return Optional.empty();
        } else if (resources.size() == 1) {
            return Optional.of(resources.iterator().next());
        } else {
            throw new IllegalStateException("Multiple secondary resources found: " + resources);
        }
    }

    @Override
    public ControllerConfiguration<P> getControllerConfiguration() {
        return null;
    }

    @Override
    public ManagedWorkflowAndDependentResourceContext managedWorkflowAndDependentResourceContext() {
        return null;
    }

    @Override
    public EventSourceRetriever<P> eventSourceRetriever() {
        return null;
    }

    @Override
    public KubernetesClient getClient() {
        return kubernetesClient;
    }

    @Override
    public ExecutorService getWorkflowExecutorService() {
        return null;
    }

    @Override
    public P getPrimaryResource() {
        return null;
    }

    @Override
    public IndexedResourceCache<P> getPrimaryCache() {
        return null;
    }

    @Override
    public boolean isNextReconciliationImminent() {
        return false;
    }
}
