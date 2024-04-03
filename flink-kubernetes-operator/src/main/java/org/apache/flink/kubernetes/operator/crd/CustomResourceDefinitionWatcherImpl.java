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

package org.apache.flink.kubernetes.operator.crd;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.KubernetesClient;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/** Default implementation for CustomResourceDefinitionWatcher. */
@RequiredArgsConstructor
public class CustomResourceDefinitionWatcherImpl implements CustomResourceDefinitionWatcher {
    private static final Logger LOG =
            LoggerFactory.getLogger(CustomResourceDefinitionWatcherImpl.class);
    private final KubernetesClient kubernetesClient;

    private final Set<String> crds = new HashSet<>();

    @Override
    public <T extends HasMetadata> boolean isCrdInstalled(Class<T> clazz) {
        if (crds.contains(clazz.getName())) {
            return true;
        }
        try {
            kubernetesClient.resources(clazz).list().getItems();
            crds.add(clazz.getName());
            return true;
        } catch (Throwable t) {
            LOG.warn("Failed to find CRD {}", clazz.getSimpleName());
            return false;
        }
    }
}
