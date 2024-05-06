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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EmptyDirVolumeSource;
import io.fabric8.kubernetes.api.model.EphemeralVolumeSource;
import io.fabric8.kubernetes.api.model.HTTPGetAction;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.Probe;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentCondition;
import io.fabric8.kubernetes.api.model.apps.DeploymentStatus;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** FlinkUtilsTest. */
@EnableKubernetesMockClient(crud = true)
public class FlinkUtilsTest {

    KubernetesClient kubernetesClient;
    KubernetesMockServer mockServer;

    @Test
    public void testMergePods() {
        Container container1 = new Container();
        container1.setName("container1");
        Container container2 = new Container();
        container2.setName("container2");

        var pod1 = TestUtils.getTestPodTemplate("pod1 hostname", List.of());

        var pod2 = TestUtils.getTestPodTemplate("pod2 hostname", List.of(container1, container2));

        var mergedPod = FlinkUtils.mergePodTemplates(pod1, pod2, false);
        assertEquals(pod2.getSpec().getContainers(), mergedPod.getSpec().getContainers());
    }

    @Test
    public void testAddStartupProbe() {
        var pod = new PodTemplateSpec();
        FlinkUtils.addStartupProbe(pod);

        Probe expectedProbe = new Probe();
        expectedProbe.setPeriodSeconds(1);
        expectedProbe.setFailureThreshold(Integer.MAX_VALUE);
        expectedProbe.setHttpGet(new HTTPGetAction());
        expectedProbe.getHttpGet().setPort(new IntOrString("rest"));
        expectedProbe.getHttpGet().setPath("/config");

        assertEquals(1, pod.getSpec().getContainers().size());
        assertEquals(Constants.MAIN_CONTAINER_NAME, pod.getSpec().getContainers().get(0).getName());
        assertEquals(expectedProbe, pod.getSpec().getContainers().get(0).getStartupProbe());

        FlinkUtils.addStartupProbe(pod);

        assertEquals(1, pod.getSpec().getContainers().size());
        assertEquals(Constants.MAIN_CONTAINER_NAME, pod.getSpec().getContainers().get(0).getName());
        assertEquals(expectedProbe, pod.getSpec().getContainers().get(0).getStartupProbe());

        // Custom startup probe
        pod.getSpec().getContainers().get(0).setStartupProbe(new Probe());
        FlinkUtils.addStartupProbe(pod);

        assertEquals(1, pod.getSpec().getContainers().size());
        assertEquals(Constants.MAIN_CONTAINER_NAME, pod.getSpec().getContainers().get(0).getName());
        assertEquals(new Probe(), pod.getSpec().getContainers().get(0).getStartupProbe());

        // Test adding probe if main container was undefined
        pod.getSpec().setContainers(List.of(new Container()));
        FlinkUtils.addStartupProbe(pod);

        assertEquals(2, pod.getSpec().getContainers().size());
        assertEquals(Constants.MAIN_CONTAINER_NAME, pod.getSpec().getContainers().get(1).getName());
        assertEquals(expectedProbe, pod.getSpec().getContainers().get(1).getStartupProbe());
    }

    @Test
    public void testDeleteJobGraphInKubernetesHA() {
        final String name = "ha-configmap";
        final String clusterId = "cluster-id";
        final Map<String, String> data = new HashMap<>();
        data.put(Constants.JOB_GRAPH_STORE_KEY_PREFIX + JobID.generate(), "job-graph-data");
        data.put("leader", "localhost");
        createHAConfigMapWithData(name, kubernetesClient.getNamespace(), clusterId, data);
        assertNotNull(kubernetesClient.configMaps().withName(name).get());
        assertEquals(2, kubernetesClient.configMaps().withName(name).get().getData().size());

        FlinkUtils.deleteJobGraphInKubernetesHA(
                clusterId, kubernetesClient.getNamespace(), kubernetesClient);

        assertEquals(1, kubernetesClient.configMaps().withName(name).get().getData().size());
        assertTrue(
                kubernetesClient.configMaps().withName(name).get().getData().containsKey("leader"));
    }

    @Test
    public void kubernetesHaMetaDataCheckTest() {
        var cr = TestUtils.buildApplicationCluster();
        var confManager = new FlinkConfigManager(new Configuration());
        assertFalse(
                FlinkUtils.isKubernetesHaMetadataAvailable(
                        confManager.getDeployConfig(cr.getMetadata(), cr.getSpec()),
                        kubernetesClient));

        // Flink 1.15+
        createHAConfigMapWithData(
                cr.getMetadata().getName() + "-cluster-config-map",
                cr.getMetadata().getNamespace(),
                cr.getMetadata().getName(),
                null);
        assertFalse(
                FlinkUtils.isKubernetesHaMetadataAvailable(
                        confManager.getDeployConfig(cr.getMetadata(), cr.getSpec()),
                        kubernetesClient));

        createHAConfigMapWithData(
                cr.getMetadata().getName() + "-000000000000-config-map",
                cr.getMetadata().getNamespace(),
                cr.getMetadata().getName(),
                null);
        assertTrue(
                FlinkUtils.isKubernetesHaMetadataAvailable(
                        confManager.getDeployConfig(cr.getMetadata(), cr.getSpec()),
                        kubernetesClient));

        // Flink 1.13-1.14
        kubernetesClient.configMaps().inAnyNamespace().delete();
        assertFalse(
                FlinkUtils.isKubernetesHaMetadataAvailable(
                        confManager.getDeployConfig(cr.getMetadata(), cr.getSpec()),
                        kubernetesClient));

        createHAConfigMapWithData(
                cr.getMetadata().getName() + "-dispatcher-leader",
                cr.getMetadata().getNamespace(),
                cr.getMetadata().getName(),
                null);
        assertFalse(
                FlinkUtils.isKubernetesHaMetadataAvailable(
                        confManager.getDeployConfig(cr.getMetadata(), cr.getSpec()),
                        kubernetesClient));

        createHAConfigMapWithData(
                cr.getMetadata().getName() + "-000000000000-jobmanager-leader",
                cr.getMetadata().getNamespace(),
                cr.getMetadata().getName(),
                null);
        assertTrue(
                FlinkUtils.isKubernetesHaMetadataAvailable(
                        confManager.getDeployConfig(cr.getMetadata(), cr.getSpec()),
                        kubernetesClient));
    }

    @Test
    public void kubernetesHaMetaDataCheckpointCheckTest() {
        var cr = TestUtils.buildApplicationCluster();
        var confManager = new FlinkConfigManager(new Configuration());
        assertFalse(
                FlinkUtils.isKubernetesHaMetadataAvailableWithCheckpoint(
                        confManager.getDeployConfig(cr.getMetadata(), cr.getSpec()),
                        kubernetesClient));

        var withCheckpoint = Map.of("checkpointID-2", "p");
        var withoutCheckpoint = Map.of("counter", "2");

        // Wrong CM name
        createHAConfigMapWithData(
                cr.getMetadata().getName() + "-wrong-name",
                cr.getMetadata().getNamespace(),
                cr.getMetadata().getName(),
                withCheckpoint);
        assertFalse(
                FlinkUtils.isKubernetesHaMetadataAvailableWithCheckpoint(
                        confManager.getDeployConfig(cr.getMetadata(), cr.getSpec()),
                        kubernetesClient));

        // Missing data
        createHAConfigMapWithData(
                cr.getMetadata().getName() + "-000000000000-config-map",
                cr.getMetadata().getNamespace(),
                cr.getMetadata().getName(),
                null);
        assertFalse(
                FlinkUtils.isKubernetesHaMetadataAvailableWithCheckpoint(
                        confManager.getDeployConfig(cr.getMetadata(), cr.getSpec()),
                        kubernetesClient));

        // CM data without CP
        createHAConfigMapWithData(
                cr.getMetadata().getName() + "-000000000000-config-map",
                cr.getMetadata().getNamespace(),
                cr.getMetadata().getName(),
                withoutCheckpoint);
        assertFalse(
                FlinkUtils.isKubernetesHaMetadataAvailableWithCheckpoint(
                        confManager.getDeployConfig(cr.getMetadata(), cr.getSpec()),
                        kubernetesClient));

        // CM data with CP
        createHAConfigMapWithData(
                cr.getMetadata().getName() + "-000000000000-config-map",
                cr.getMetadata().getNamespace(),
                cr.getMetadata().getName(),
                withCheckpoint);
        assertTrue(
                FlinkUtils.isKubernetesHaMetadataAvailableWithCheckpoint(
                        confManager.getDeployConfig(cr.getMetadata(), cr.getSpec()),
                        kubernetesClient));
    }

    @Test
    public void testJmNeverStartedDetection() {
        var jmDeployment = new Deployment();
        jmDeployment.setMetadata(new ObjectMeta());
        jmDeployment.getMetadata().setCreationTimestamp("create-ts");
        jmDeployment.setStatus(new DeploymentStatus());
        var deployStatus = jmDeployment.getStatus();
        var jmNeverStartedCondition =
                new DeploymentCondition("create-ts", null, null, null, "False", "Available");
        var jmStartedButStopped =
                new DeploymentCondition("other-ts", null, null, null, "False", "Available");
        var jmAvailable =
                new DeploymentCondition("other-ts", null, null, null, "True", "Available");

        var context =
                new TestUtils.TestingContext<Deployment>() {
                    @Override
                    public <R> Optional<R> getSecondaryResource(Class<R> aClass, String name) {
                        return (Optional<R>) Optional.of(jmDeployment);
                    }
                };

        deployStatus.setConditions(Collections.emptyList());
        assertFalse(FlinkUtils.jmPodNeverStarted(context));

        deployStatus.setConditions(List.of(jmNeverStartedCondition));
        assertTrue(FlinkUtils.jmPodNeverStarted(context));

        deployStatus.setConditions(List.of(jmStartedButStopped));
        assertFalse(FlinkUtils.jmPodNeverStarted(context));

        deployStatus.setConditions(List.of(jmAvailable));
        assertFalse(FlinkUtils.jmPodNeverStarted(context));

        assertFalse(FlinkUtils.jmPodNeverStarted(TestUtils.createEmptyContext()));
    }

    @Test
    public void testDeleteJobGraphInKubernetesHAShouldNotUpdateWithEmptyConfigMap() {
        final String name = "empty-ha-configmap";
        final String clusterId = "cluster-id-2";
        mockServer
                .expect()
                .put()
                .withPath("/api/v1/namespaces/test/configmaps/" + name)
                .andReturn(HttpURLConnection.HTTP_INTERNAL_ERROR, new ConfigMapBuilder().build())
                .once();
        createHAConfigMapWithData(name, kubernetesClient.getNamespace(), clusterId, null);
        assertTrue(kubernetesClient.configMaps().withName(name).get().getData().isEmpty());
        FlinkUtils.deleteJobGraphInKubernetesHA(
                clusterId, kubernetesClient.getNamespace(), kubernetesClient);
    }

    @Test
    public void testComputeNumTms() {
        Configuration conf = new Configuration();
        conf.set(CoreOptions.DEFAULT_PARALLELISM, 2);
        conf.set(TaskManagerOptions.NUM_TASK_SLOTS, 1);

        assertEquals(2, FlinkUtils.getNumTaskManagers(conf));

        conf.set(TaskManagerOptions.NUM_TASK_SLOTS, 3);
        assertEquals(1, FlinkUtils.getNumTaskManagers(conf));

        conf.set(CoreOptions.DEFAULT_PARALLELISM, 7);
        conf.set(TaskManagerOptions.NUM_TASK_SLOTS, 2);
        assertEquals(4, FlinkUtils.getNumTaskManagers(conf));
    }

    @Test
    public void testCalculateClusterCpuUsage() {
        Configuration conf = new Configuration();
        conf.set(KubernetesConfigOptions.KUBERNETES_JOBMANAGER_REPLICAS, 2);
        conf.set(KubernetesConfigOptions.JOB_MANAGER_CPU, 2.5);
        conf.set(KubernetesConfigOptions.JOB_MANAGER_CPU_LIMIT_FACTOR, 2.0);
        conf.set(KubernetesConfigOptions.TASK_MANAGER_CPU, 3.5);
        conf.set(KubernetesConfigOptions.TASK_MANAGER_CPU_LIMIT_FACTOR, 1.5);

        assertEquals(10, FlinkUtils.calculateClusterCpuUsage(conf, 0));
        assertEquals(15.25, FlinkUtils.calculateClusterCpuUsage(conf, 1));
        assertEquals(20.5, FlinkUtils.calculateClusterCpuUsage(conf, 2));
    }

    @Test
    public void testCalculateClusterMemoryUsage() {
        Configuration conf = new Configuration();
        conf.set(HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.KUBERNETES.toString());

        conf.set(KubernetesConfigOptions.KUBERNETES_JOBMANAGER_REPLICAS, 2);
        conf.set(JobManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse("1g"));
        conf.set(KubernetesConfigOptions.JOB_MANAGER_MEMORY_LIMIT_FACTOR, 2.0);

        conf.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse("2g"));
        conf.set(KubernetesConfigOptions.TASK_MANAGER_MEMORY_LIMIT_FACTOR, 1.5);

        assertEquals(
                MemorySize.parse("4g").getBytes(), FlinkUtils.calculateClusterMemoryUsage(conf, 0));
        assertEquals(
                MemorySize.parse("7g").getBytes(), FlinkUtils.calculateClusterMemoryUsage(conf, 1));
        assertEquals(
                MemorySize.parse("10g").getBytes(),
                FlinkUtils.calculateClusterMemoryUsage(conf, 2));
    }

    @Test
    public void testMergePodUsingArrayName() {
        Container container1 = new Container();
        container1.setName("container1");
        Container container2 = new Container();
        container2.setName("container2");
        Container container3 = new Container();
        container3.setName("container3");

        Volume volume1 = new Volume();
        volume1.setName("v1");
        volume1.setEphemeral(new EphemeralVolumeSource());

        Volume volume12 = new Volume();
        volume12.setName("v1");
        volume12.setEmptyDir(new EmptyDirVolumeSource());

        Volume volume2 = new Volume();
        volume2.setName("v2");

        Volume volume3 = new Volume();
        volume3.setName("v3");

        var pod1 = TestUtils.getTestPodTemplate("pod1 hostname", List.of(container1, container2));
        pod1.getSpec().setVolumes(List.of(volume1));

        var pod2 = TestUtils.getTestPodTemplate("pod2 hostname", List.of(container1, container3));
        pod2.getSpec().setVolumes(List.of(volume12, volume2, volume3));

        var mergedPod = FlinkUtils.mergePodTemplates(pod1, pod2, true);

        Volume v1merged = new Volume();
        v1merged.setName("v1");
        v1merged.setEphemeral(new EphemeralVolumeSource());
        v1merged.setEmptyDir(new EmptyDirVolumeSource());

        assertEquals(
                List.of(container1, container2, container3), mergedPod.getSpec().getContainers());
        assertEquals(List.of(v1merged, volume2, volume3), mergedPod.getSpec().getVolumes());
    }

    private void createHAConfigMapWithData(
            String configMapName, String namespace, String clusterId, Map<String, String> data) {
        final ConfigMap kubernetesConfigMap =
                new ConfigMapBuilder()
                        .withNewMetadata()
                        .withName(configMapName)
                        .withNamespace(namespace)
                        .withLabels(
                                KubernetesUtils.getConfigMapLabels(
                                        clusterId,
                                        Constants.LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY))
                        .endMetadata()
                        .withData(data)
                        .build();

        kubernetesClient.configMaps().resource(kubernetesConfigMap).createOrReplace();
    }
}
