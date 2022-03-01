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

import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.FilterWatchListDeletable;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;
import io.javaoperatorsdk.operator.processing.event.source.informer.Mappers;

/** Operator SDK related utility functions. */
public class OperatorUtils {

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
                return super.name() + "-" + name;
            }
        };
    }
}
