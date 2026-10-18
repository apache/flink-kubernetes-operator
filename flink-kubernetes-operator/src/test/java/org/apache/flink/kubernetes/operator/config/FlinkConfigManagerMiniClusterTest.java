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

package org.apache.flink.kubernetes.operator.config;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.DeploymentOptionsInternal;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.api.FlinkMiniCluster;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.kubernetes.utils.Constants;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link FlinkMiniCluster} configuration built by {@link FlinkConfigManager}. */
class FlinkConfigManagerMiniClusterTest {

    private final FlinkConfigManager configManager =
            new FlinkConfigManager(
                    Configuration.fromMap(Map.of("operator.default.key", "operator-default")));

    @Test
    void testDeployConfig() {
        var miniCluster = TestUtils.buildMiniCluster();
        miniCluster.getSpec().getJob().setParallelism(3);
        miniCluster.getSpec().getFlinkConfiguration().put("user.key", "user-value");

        var conf = deployConfig(miniCluster);

        assertThat(conf.getString("operator.default.key", null)).isEqualTo("operator-default");
        assertThat(conf.getString("user.key", null)).isEqualTo("user-value");
        assertThat(conf.get(FlinkConfigBuilder.FLINK_VERSION)).isEqualTo(FlinkVersion.v2_4);
        assertThat(conf.get(KubernetesConfigOptions.CLUSTER_ID))
                .isEqualTo(TestUtils.TEST_MINI_CLUSTER_NAME);
        assertThat(conf.get(KubernetesConfigOptions.NAMESPACE)).isEqualTo(TestUtils.TEST_NAMESPACE);
        assertThat(conf.get(CoreOptions.DEFAULT_PARALLELISM)).isEqualTo(3);
        assertThat(conf.get(TaskManagerOptions.NUM_TASK_SLOTS)).isEqualTo(3);
        assertThat(conf.get(PipelineOptions.NAME)).isEqualTo(TestUtils.TEST_MINI_CLUSTER_NAME);
        assertThat(conf.get(WebOptions.CANCEL_ENABLE)).isFalse();
        assertThat(conf.get(KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE))
                .isEqualTo(KubernetesConfigOptions.ServiceExposedType.ClusterIP);
        assertThat(conf.get(DeploymentOptions.SHUTDOWN_ON_APPLICATION_FINISH)).isFalse();
        assertThat(conf.get(DeploymentOptions.SUBMIT_FAILED_JOB_ON_APPLICATION_ERROR)).isTrue();
        assertThat(conf.get(KubernetesConfigOptions.JOB_MANAGER_ANNOTATIONS))
                .containsEntry(FlinkUtils.CR_GENERATION_LABEL, "1");
    }

    @Test
    void testRestBindPortIsNotFixedInConfig() {
        // As with a FlinkDeployment, the REST bind port is only fixed when the cluster is
        // deployed, so a port that can't be deployed doesn't stop the resource being observed.
        var miniCluster = TestUtils.buildMiniCluster();
        miniCluster
                .getSpec()
                .getFlinkConfiguration()
                .put(RestOptions.BIND_PORT.key(), "50100-50200");
        miniCluster
                .getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(miniCluster.getSpec(), miniCluster);

        assertThat(deployConfig(miniCluster).get(RestOptions.BIND_PORT)).isEqualTo("50100-50200");
        assertThat(configManager.getObserveConfig(miniCluster).get(RestOptions.BIND_PORT))
                .isEqualTo("50100-50200");
    }

    @Test
    void testHighAvailabilityClusterId() {
        var miniCluster = TestUtils.buildMiniCluster();
        assertThat(deployConfig(miniCluster).get(HighAvailabilityOptions.HA_CLUSTER_ID))
                .isEqualTo(TestUtils.TEST_MINI_CLUSTER_NAME);

        miniCluster.getSpec().getFlinkConfiguration().remove(HighAvailabilityOptions.HA_MODE.key());
        assertThat(deployConfig(miniCluster).contains(HighAvailabilityOptions.HA_CLUSTER_ID))
                .isFalse();
    }

    @Test
    void testUserConfigOverridesDefaults() {
        var miniCluster = TestUtils.buildMiniCluster();
        var flinkConfiguration = miniCluster.getSpec().getFlinkConfiguration();
        flinkConfiguration.put(WebOptions.CANCEL_ENABLE.key(), "true");
        flinkConfiguration.put(
                KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE.key(), "LoadBalancer");
        flinkConfiguration.put(
                DeploymentOptions.SUBMIT_FAILED_JOB_ON_APPLICATION_ERROR.key(), "false");
        flinkConfiguration.put(PipelineOptions.NAME.key(), "my-pipeline");
        flinkConfiguration.put(TaskManagerOptions.NUM_TASK_SLOTS.key(), "5");

        var conf = deployConfig(miniCluster);

        assertThat(conf.get(WebOptions.CANCEL_ENABLE)).isTrue();
        assertThat(conf.get(KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE))
                .isEqualTo(KubernetesConfigOptions.ServiceExposedType.LoadBalancer);
        assertThat(conf.get(DeploymentOptions.SUBMIT_FAILED_JOB_ON_APPLICATION_ERROR)).isFalse();
        assertThat(conf.get(PipelineOptions.NAME)).isEqualTo("my-pipeline");
        assertThat(conf.get(TaskManagerOptions.NUM_TASK_SLOTS)).isEqualTo(5);
    }

    @Test
    void testShutdownOnApplicationFinishCannotBeEnabled() {
        // The operator relies on the MiniCluster staying up to observe the terminal state of the
        // job, as it does for an application FlinkDeployment.
        var miniCluster = TestUtils.buildMiniCluster();
        miniCluster
                .getSpec()
                .getFlinkConfiguration()
                .put(DeploymentOptions.SHUTDOWN_ON_APPLICATION_FINISH.key(), "true");

        assertThat(deployConfig(miniCluster).get(DeploymentOptions.SHUTDOWN_ON_APPLICATION_FINISH))
                .isFalse();
    }

    @Test
    void testObserveConfig() {
        var miniCluster = TestUtils.buildMiniCluster();
        assertThatThrownBy(() -> configManager.getObserveConfig(miniCluster))
                .hasMessageContaining("before first deployment");

        miniCluster.getSpec().getFlinkConfiguration().put("user.key", "deployed");
        miniCluster
                .getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(miniCluster.getSpec(), miniCluster);

        var flinkConfiguration = miniCluster.getSpec().getFlinkConfiguration();
        flinkConfiguration.put("user.key", "latest");
        flinkConfiguration.put("kubernetes.operator.test", "latest");
        flinkConfiguration.put(CheckpointingOptions.SAVEPOINT_DIRECTORY.key(), "latest-dir");

        var conf = configManager.getObserveConfig(miniCluster);

        assertThat(conf.getString("user.key", null)).isEqualTo("deployed");
        assertThat(conf.getString("kubernetes.operator.test", null)).isEqualTo("latest");
        assertThat(conf.get(CheckpointingOptions.SAVEPOINT_DIRECTORY)).isEqualTo("latest-dir");
        // Only deploy config records which generation is being deployed
        assertThat(
                        conf.getOptional(KubernetesConfigOptions.JOB_MANAGER_ANNOTATIONS)
                                .orElse(Map.of()))
                .doesNotContainKey(FlinkUtils.CR_GENERATION_LABEL);
    }

    @Test
    void testOperatorDefaultTaskSlotsPreserved() {
        var configManager =
                new FlinkConfigManager(
                        Configuration.fromMap(
                                Map.of(TaskManagerOptions.NUM_TASK_SLOTS.key(), "4")));
        var miniCluster = TestUtils.buildMiniCluster();
        miniCluster.getSpec().getJob().setParallelism(2);

        var conf = configManager.getDeployConfig(miniCluster.getMetadata(), miniCluster.getSpec());

        assertThat(conf.get(TaskManagerOptions.NUM_TASK_SLOTS)).isEqualTo(4);
    }

    @Test
    void testDefaultTaskSlotsCoverParallelismOverrides() {
        var miniCluster = TestUtils.buildMiniCluster();
        miniCluster.getSpec().getJob().setParallelism(2);
        miniCluster
                .getSpec()
                .getFlinkConfiguration()
                .put(PipelineOptions.PARALLELISM_OVERRIDES.key(), "vertex-a:4,vertex-b:1");

        var conf = deployConfig(miniCluster);

        assertThat(conf.get(TaskManagerOptions.NUM_TASK_SLOTS)).isEqualTo(4);
        assertThat(conf.get(CoreOptions.DEFAULT_PARALLELISM)).isEqualTo(2);
    }

    @Test
    void testDefaultTaskSlotsNotReducedByParallelismOverrides() {
        var miniCluster = TestUtils.buildMiniCluster();
        miniCluster.getSpec().getJob().setParallelism(3);
        miniCluster
                .getSpec()
                .getFlinkConfiguration()
                .put(PipelineOptions.PARALLELISM_OVERRIDES.key(), "vertex-a:1");

        assertThat(deployConfig(miniCluster).get(TaskManagerOptions.NUM_TASK_SLOTS)).isEqualTo(3);
    }

    @Test
    void testLogConfigurationIsWrittenToCachedTempDir() throws IOException {
        // As for a FlinkDeployment, the log configuration is written to a temporary directory for
        // the ConfigMap, so the config is cached and the directory removed when it is evicted
        var miniCluster = TestUtils.buildMiniCluster();
        miniCluster
                .getSpec()
                .setLogConfiguration(Map.of(Constants.CONFIG_FILE_LOG4J_NAME, "custom-log4j"));

        var confDir = Path.of(deployConfig(miniCluster).get(DeploymentOptionsInternal.CONF_DIR));

        assertThat(Files.readString(confDir.resolve(Constants.CONFIG_FILE_LOG4J_NAME)))
                .isEqualTo("custom-log4j");
        assertThat(deployConfig(miniCluster).get(DeploymentOptionsInternal.CONF_DIR))
                .isEqualTo(confDir.toString());
        assertThat(configManager.getCache().size()).isEqualTo(1);

        configManager.getCache().invalidateAll();
        assertThat(confDir).doesNotExist();
    }

    private Configuration deployConfig(FlinkMiniCluster miniCluster) {
        return configManager.getDeployConfig(miniCluster.getMetadata(), miniCluster.getSpec());
    }
}
