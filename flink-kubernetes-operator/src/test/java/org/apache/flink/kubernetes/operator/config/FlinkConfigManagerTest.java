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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptionsInternal;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.crd.status.ReconciliationStatus;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.Pod;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for FlinkConfigManager. */
public class FlinkConfigManagerTest {

    @Test
    public void testConfigGeneration() {
        ConfigOption<String> testConf = ConfigOptions.key("test").stringType().noDefaultValue();

        FlinkConfigManager configManager =
                new FlinkConfigManager(
                        Configuration.fromMap(
                                Map.of(
                                        KubernetesOperatorConfigOptions
                                                .OPERATOR_DYNAMIC_CONFIG_ENABLED
                                                .key(),
                                        "false")));
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        ReconciliationStatus reconciliationStatus =
                deployment.getStatus().getReconciliationStatus();

        deployment.getSpec().getFlinkConfiguration().put(testConf.key(), "reconciled");
        reconciliationStatus.serializeAndSetLastReconciledSpec(deployment.getSpec(), deployment);
        reconciliationStatus.markReconciledSpecAsStable();

        deployment.getSpec().getFlinkConfiguration().put(testConf.key(), "latest");
        assertEquals(
                "latest",
                configManager
                        .getDeployConfig(deployment.getMetadata(), deployment.getSpec())
                        .get(testConf));
        assertEquals("reconciled", configManager.getObserveConfig(deployment).get(testConf));

        deployment.getSpec().getFlinkConfiguration().put(testConf.key(), "stable");
        reconciliationStatus.serializeAndSetLastReconciledSpec(deployment.getSpec(), deployment);
        reconciliationStatus.markReconciledSpecAsStable();

        deployment.getSpec().getFlinkConfiguration().put(testConf.key(), "rolled-back");
        reconciliationStatus.serializeAndSetLastReconciledSpec(deployment.getSpec(), deployment);
        reconciliationStatus.setState(ReconciliationState.ROLLED_BACK);

        assertEquals("stable", configManager.getObserveConfig(deployment).get(testConf));

        deployment.getMetadata().setGeneration(5L);
        var deployConfig =
                configManager.getDeployConfig(deployment.getMetadata(), deployment.getSpec());
        assertEquals(
                Map.of(FlinkUtils.CR_GENERATION_LABEL, "5"),
                deployConfig.get(KubernetesConfigOptions.JOB_MANAGER_ANNOTATIONS));
    }

    @Test
    public void testConfUpdateAndCleanup() {
        Configuration config = Configuration.fromMap(Map.of("k1", "v1"));
        FlinkConfigManager configManager = new FlinkConfigManager(config);
        assertFalse(
                configManager
                        .getDefaultConfig()
                        .contains(KubernetesOperatorConfigOptions.OPERATOR_RECONCILE_INTERVAL));

        config.set(
                KubernetesOperatorConfigOptions.OPERATOR_RECONCILE_INTERVAL,
                Duration.ofSeconds(15));

        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        deployment.getSpec().setLogConfiguration(Map.of(Constants.CONFIG_FILE_LOG4J_NAME, "test"));
        deployment.getSpec().setPodTemplate(new Pod());

        ReconciliationUtils.updateStatusForDeployedSpec(deployment, config);
        Configuration deployConfig = configManager.getObserveConfig(deployment);
        assertFalse(
                deployConfig.contains(KubernetesOperatorConfigOptions.OPERATOR_RECONCILE_INTERVAL));
        assertTrue(new File(deployConfig.get(DeploymentOptionsInternal.CONF_DIR)).exists());
        assertTrue(
                new File(deployConfig.get(KubernetesConfigOptions.KUBERNETES_POD_TEMPLATE))
                        .exists());
        assertTrue(
                new File(deployConfig.get(KubernetesConfigOptions.TASK_MANAGER_POD_TEMPLATE))
                        .exists());
        assertTrue(
                new File(deployConfig.get(KubernetesConfigOptions.JOB_MANAGER_POD_TEMPLATE))
                        .exists());

        configManager.updateDefaultConfig(config);

        assertTrue(new File(deployConfig.get(DeploymentOptionsInternal.CONF_DIR)).exists());
        assertTrue(
                new File(deployConfig.get(KubernetesConfigOptions.KUBERNETES_POD_TEMPLATE))
                        .exists());
        assertTrue(
                new File(deployConfig.get(KubernetesConfigOptions.TASK_MANAGER_POD_TEMPLATE))
                        .exists());
        assertTrue(
                new File(deployConfig.get(KubernetesConfigOptions.JOB_MANAGER_POD_TEMPLATE))
                        .exists());

        configManager.getCache().invalidateAll();

        assertFalse(new File(deployConfig.get(DeploymentOptionsInternal.CONF_DIR)).exists());
        assertFalse(
                new File(deployConfig.get(KubernetesConfigOptions.KUBERNETES_POD_TEMPLATE))
                        .exists());
        assertFalse(
                new File(deployConfig.get(KubernetesConfigOptions.TASK_MANAGER_POD_TEMPLATE))
                        .exists());
        assertFalse(
                new File(deployConfig.get(KubernetesConfigOptions.JOB_MANAGER_POD_TEMPLATE))
                        .exists());

        assertEquals(
                Duration.ofSeconds(15),
                configManager
                        .getDefaultConfig()
                        .get(KubernetesOperatorConfigOptions.OPERATOR_RECONCILE_INTERVAL));
        assertEquals(
                Duration.ofSeconds(15),
                configManager.getOperatorConfiguration().getReconcileInterval());

        assertEquals(
                Duration.ofSeconds(15),
                configManager
                        .getObserveConfig(deployment)
                        .get(KubernetesOperatorConfigOptions.OPERATOR_RECONCILE_INTERVAL));
    }

    @Test
    public void testConfigOverrides(@TempDir Path confOverrideDir) throws IOException {

        assertEquals(
                0, FlinkConfigManager.loadGlobalConfiguration(Optional.empty()).keySet().size());

        Files.write(
                confOverrideDir.resolve(GlobalConfiguration.FLINK_CONF_FILENAME),
                Arrays.asList("foo: 1", "bar: 2"));
        var conf =
                FlinkConfigManager.loadGlobalConfiguration(Optional.of(confOverrideDir.toString()));
        Assertions.assertEquals(Map.of("foo", "1", "bar", "2"), conf.toMap());
    }
}
