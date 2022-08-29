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

package org.apache.flink.kubernetes.operator.kubeclient;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.kubeclient.Fabric8FlinkKubeClient;
import org.apache.flink.kubernetes.operator.utils.StandaloneKubernetesUtils;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.util.FlinkRuntimeException;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The Implementation of {@link FlinkStandaloneKubeClient}. */
public class Fabric8FlinkStandaloneKubeClient extends Fabric8FlinkKubeClient
        implements FlinkStandaloneKubeClient {
    private static final Logger LOG =
            LoggerFactory.getLogger(Fabric8FlinkStandaloneKubeClient.class);
    private final NamespacedKubernetesClient internalClient;

    public Fabric8FlinkStandaloneKubeClient(
            Configuration flinkConfig,
            NamespacedKubernetesClient client,
            ExecutorService executorService) {
        super(flinkConfig, client, executorService);
        internalClient = checkNotNull(client);
    }

    private void setOwnerReference(StatefulSet statefulSet, List<HasMetadata> resources) {
        final OwnerReference statefulSetOwnerReference =
                new OwnerReferenceBuilder()
                        .withName(statefulSet.getMetadata().getName())
                        .withApiVersion(statefulSet.getApiVersion())
                        .withUid(statefulSet.getMetadata().getUid())
                        .withKind(statefulSet.getKind())
                        .withController(true)
                        .withBlockOwnerDeletion(true)
                        .build();
        resources.forEach(
                resource ->
                        resource.getMetadata()
                                .setOwnerReferences(
                                        Collections.singletonList(statefulSetOwnerReference)));
    }

    @Override
    public void createJobManagerComponent(
            StandaloneKubernetesJobManagerSpecification kubernetesJMSpec) {
        final StatefulSet statefulSet = kubernetesJMSpec.getStatefulSet();
        final List<HasMetadata> accompanyingResources = kubernetesJMSpec.getAccompanyingResources();

        // create StatefulSet
        LOG.debug(
                "Start to create statefulSet with spec {}{}",
                System.lineSeparator(),
                KubernetesUtils.tryToGetPrettyPrintYaml(statefulSet));
        final StatefulSet createdStatefulSet =
                this.internalClient.apps().statefulSets().create(statefulSet);
        // Note that we should use the uid of the created StatefulSet for the OwnerReference.
        setOwnerReference(createdStatefulSet, accompanyingResources);

        this.internalClient.resourceList(accompanyingResources).createOrReplace();
    }

    @Override
    public void createTaskManagerStatefulSet(StatefulSet tmStatefulSet) {
        this.internalClient.apps().statefulSets().create(tmStatefulSet);
    }

    @Override
    public void stopAndCleanupCluster(String clusterId) {
        this.internalClient
                .apps()
                .statefulSets()
                .withName(StandaloneKubernetesUtils.getJobManagerStatefulSetName(clusterId))
                .withPropagationPolicy(DeletionPropagation.FOREGROUND)
                .delete();

        this.internalClient
                .apps()
                .statefulSets()
                .withName(StandaloneKubernetesUtils.getTaskManagerStatefulSetName(clusterId))
                .withPropagationPolicy(DeletionPropagation.FOREGROUND)
                .delete();
    }

    public static NamespacedKubernetesClient createNamespacedKubeClient(String namespace) {
        return new DefaultKubernetesClient().inNamespace(namespace);
    }

    @Override
    public PersistentVolumeClaim loadVolumeClaimTemplates(File file) {
        if (!file.exists()) {
            throw new FlinkRuntimeException(
                    String.format("Pvc template file %s does not exist.", file));
        }
        return this.internalClient.persistentVolumeClaims().load(file).get();
    }
}
