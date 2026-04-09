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

package org.apache.flink.kubernetes.operator.autoscaler;

import org.apache.flink.autoscaler.JobAutoScaler;
import org.apache.flink.autoscaler.event.TestAutoScalerEventHandler;
import org.apache.flink.autoscaler.realizer.TestScalingRealizer;
import org.apache.flink.autoscaler.state.TestAutoScalerStateStore;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.autoscaler.state.KubernetesAutoScalerStateStore;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.resources.ClusterResourceManager;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.FlinkResourceEventCollector;
import org.apache.flink.kubernetes.operator.utils.FlinkStateSnapshotEventCollector;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import lombok.Getter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test loading the default autoscaling implementation from the classpath. */
@EnableKubernetesMockClient(crud = true)
public class AutoscalerFactoryTest {

    @TempDir public Path temporaryFolder;

    @Getter private KubernetesClient kubernetesClient;

    @Test
    void testDefaultStateStore() throws Exception {
        assertThat(getField(createAutoscaler(), "stateStore"))
                .isInstanceOf(KubernetesAutoScalerStateStore.class);
    }

    @Test
    void testCustomStateStore() throws Exception {
        withCustomPlugins(
                () ->
                        assertThat(getField(createAutoscaler(), "stateStore"))
                                .isInstanceOf(TestAutoScalerStateStore.class));
    }

    @Test
    void testDefaultEventHandler() throws Exception {
        assertThat(getField(createAutoscaler(), "eventHandler"))
                .isInstanceOf(KubernetesAutoScalerEventHandler.class);
    }

    @Test
    void testCustomEventHandler() throws Exception {
        withCustomPlugins(
                () ->
                        assertThat(getField(createAutoscaler(), "eventHandler"))
                                .isInstanceOf(TestAutoScalerEventHandler.class));
    }

    @Test
    void testDefaultScalingRealizer() throws Exception {
        assertThat(getField(createAutoscaler(), "scalingRealizer"))
                .isInstanceOf(KubernetesScalingRealizer.class);
    }

    @Test
    void testCustomScalingRealizer() throws Exception {
        withCustomPlugins(
                () ->
                        assertThat(getField(createAutoscaler(), "scalingRealizer"))
                                .isInstanceOf(TestScalingRealizer.class));
    }

    private void withCustomPlugins(ThrowingRunnable action) throws Exception {
        Map<String, String> originalEnv = System.getenv();
        try {
            Map<String, String> systemEnv = new HashMap<>(originalEnv);
            systemEnv.put(
                    ConfigConstants.ENV_FLINK_PLUGINS_DIR,
                    TestUtils.getTestPluginsRootDir(temporaryFolder));
            TestUtils.setEnv(systemEnv);
            action.run();
        } finally {
            TestUtils.setEnv(originalEnv);
        }
    }

    private JobAutoScaler<ResourceID, KubernetesJobAutoScalerContext> createAutoscaler() {
        return AutoscalerFactory.create(
                kubernetesClient,
                new EventRecorder(
                        new FlinkResourceEventCollector(), new FlinkStateSnapshotEventCollector()),
                new ClusterResourceManager(Duration.ZERO, kubernetesClient),
                new FlinkConfigManager(Configuration.fromMap(Map.of())));
    }

    private static Object getField(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}
