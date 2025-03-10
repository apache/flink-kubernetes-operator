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

package org.apache.flink.kubernetes.kubeclient.decorators;

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.YamlParserUtils;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesJobManagerParameters;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.config.FlinkConfigBuilder;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.ConfigMap;
import org.apache.flink.kubernetes.utils.Constants;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.flink.kubernetes.kubeclient.decorators.FlinkConfMountDecorator.getFlinkConfConfigMapName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** General tests for the {@link FlinkConfMountDecorator}. */
class FlinkConfMountDecoratorTest {

    protected static final String CLUSTER_ID = "my-flink-cluster1";
    private static final String FLINK_CONF_DIR_IN_POD = "/opt/flink/flink-conf-";

    private FlinkConfMountDecorator flinkConfMountDecorator;
    private Configuration flinkConfig = new Configuration();

    @BeforeEach
    protected void onSetup() throws Exception {
        this.flinkConfig.set(KubernetesConfigOptions.FLINK_CONF_DIR, FLINK_CONF_DIR_IN_POD);
        this.flinkConfig.set(KubernetesConfigOptions.CLUSTER_ID, CLUSTER_ID);

        var clusterSpecification =
                new ClusterSpecification.ClusterSpecificationBuilder()
                        .setMasterMemoryMB(100)
                        .setTaskManagerMemoryMB(1024)
                        .setSlotsPerTaskManager(3)
                        .createClusterSpecification();

        var kubernetesJobManagerParameters =
                new KubernetesJobManagerParameters(flinkConfig, clusterSpecification);
        this.flinkConfMountDecorator = new FlinkConfMountDecorator(kubernetesJobManagerParameters);
    }

    @ParameterizedTest
    @MethodSource("testArgs")
    void testConfigMap(FlinkVersion version, String expectedConfName, boolean standardYaml)
            throws IOException {
        flinkConfig.set(FlinkConfigBuilder.FLINK_VERSION, version);
        var additionalResources = flinkConfMountDecorator.buildAccompanyingKubernetesResources();
        assertThat(additionalResources).hasSize(1);

        var resultConfigMap = (ConfigMap) additionalResources.get(0);

        assertThat(resultConfigMap.getApiVersion()).isEqualTo(Constants.API_VERSION);

        assertThat(resultConfigMap.getMetadata().getName())
                .isEqualTo(getFlinkConfConfigMapName(CLUSTER_ID));

        Map<String, String> resultDatas = resultConfigMap.getData();
        var conf = YamlParserUtils.convertToObject(resultDatas.get(expectedConfName), Map.class);
        if (standardYaml) {
            assertTrue(conf.containsKey("kubernetes"));
            assertFalse(conf.containsKey("kubernetes.cluster-id"));
        } else {
            assertFalse(conf.containsKey("kubernetes"));
            assertTrue(conf.containsKey("kubernetes.cluster-id"));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testOverlappingKeyDetection(boolean standardYaml) throws IOException {

        flinkConfig.set(
                FlinkConfigBuilder.FLINK_VERSION,
                standardYaml ? FlinkVersion.v2_0 : FlinkVersion.v1_20);
        flinkConfig.setString("k", "v");
        flinkConfig.setString("kv", "v2");

        // Non overlapping keys
        flinkConfMountDecorator.buildAccompanyingKubernetesResources();
        flinkConfig.setString("k.v", "v3");

        IllegalConfigurationException err = null;
        try {
            var additionalResources =
                    flinkConfMountDecorator.buildAccompanyingKubernetesResources();
            assertThat(additionalResources).hasSize(1);
        } catch (IllegalConfigurationException e) {
            err = e;
        }

        if (standardYaml) {
            assertThat(err)
                    .isNotNull()
                    .hasMessageContaining("Overlapping key prefixes detected (k -> k.v)");
        } else {
            assertThat(err).isNull();
        }
    }

    private static Stream<Arguments> testArgs() {
        return Stream.of(
                Arguments.arguments(FlinkVersion.v1_19, "flink-conf.yaml", false),
                Arguments.arguments(FlinkVersion.v1_20, "flink-conf.yaml", false),
                Arguments.arguments(FlinkVersion.v2_0, "config.yaml", true));
    }

    private Map<String, String> getCommonLabels() {
        Map<String, String> labels = new HashMap<>();
        labels.put(Constants.LABEL_TYPE_KEY, Constants.LABEL_TYPE_NATIVE_TYPE);
        labels.put(Constants.LABEL_APP_KEY, CLUSTER_ID);
        return labels;
    }
}
