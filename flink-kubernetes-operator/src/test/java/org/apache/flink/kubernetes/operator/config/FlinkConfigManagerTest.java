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

import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptionsInternal;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.utils.EnvUtils;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;

import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_WATCHED_NAMESPACES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for FlinkConfigManager. */
public class FlinkConfigManagerTest {

    @Test
    public void testConfigGeneration() {
        ConfigOption<String> testConf = ConfigOptions.key("test").stringType().noDefaultValue();
        ConfigOption<String> opTestConf =
                ConfigOptions.key("kubernetes.operator.test").stringType().noDefaultValue();

        FlinkConfigManager configManager =
                new FlinkConfigManager(
                        Configuration.fromMap(
                                Map.of(
                                        KubernetesOperatorConfigOptions
                                                .OPERATOR_DYNAMIC_CONFIG_ENABLED
                                                .key(),
                                        "false")));
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        var reconciliationStatus = deployment.getStatus().getReconciliationStatus();

        deployment.getSpec().getFlinkConfiguration().put(testConf.key(), "reconciled");
        deployment.getSpec().getFlinkConfiguration().put(opTestConf.key(), "reconciled");
        reconciliationStatus.serializeAndSetLastReconciledSpec(deployment.getSpec(), deployment);
        reconciliationStatus.markReconciledSpecAsStable();

        deployment.getSpec().getFlinkConfiguration().put(testConf.key(), "latest");
        deployment.getSpec().getFlinkConfiguration().put(opTestConf.key(), "latest");
        deployment
                .getSpec()
                .getFlinkConfiguration()
                .put(AutoScalerOptions.METRICS_WINDOW.key(), "1234m");

        assertEquals(
                "latest",
                configManager
                        .getDeployConfig(deployment.getMetadata(), deployment.getSpec())
                        .get(testConf));
        assertEquals(
                "latest",
                configManager
                        .getDeployConfig(deployment.getMetadata(), deployment.getSpec())
                        .get(opTestConf));
        assertEquals("reconciled", configManager.getObserveConfig(deployment).get(testConf));
        assertEquals("latest", configManager.getObserveConfig(deployment).get(opTestConf));
        assertEquals(
                Duration.ofMinutes(1234),
                configManager.getObserveConfig(deployment).get(AutoScalerOptions.METRICS_WINDOW));

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
        deployment.getSpec().setPodTemplate(new PodTemplateSpec());

        ReconciliationUtils.updateStatusForDeployedSpec(deployment, config);
        Configuration deployConfig = configManager.getObserveConfig(deployment);
        assertFalse(
                deployConfig.contains(KubernetesOperatorConfigOptions.OPERATOR_RECONCILE_INTERVAL));
        assertTrue(new File(deployConfig.get(DeploymentOptionsInternal.CONF_DIR)).exists());
        assertTrue(
                new File(deployConfig.get(KubernetesConfigOptions.TASK_MANAGER_POD_TEMPLATE))
                        .exists());
        assertTrue(
                new File(deployConfig.get(KubernetesConfigOptions.JOB_MANAGER_POD_TEMPLATE))
                        .exists());

        configManager.updateDefaultConfig(config);

        assertTrue(new File(deployConfig.get(DeploymentOptionsInternal.CONF_DIR)).exists());
        assertTrue(
                new File(deployConfig.get(KubernetesConfigOptions.TASK_MANAGER_POD_TEMPLATE))
                        .exists());
        assertTrue(
                new File(deployConfig.get(KubernetesConfigOptions.JOB_MANAGER_POD_TEMPLATE))
                        .exists());

        configManager.getCache().invalidateAll();

        assertFalse(new File(deployConfig.get(DeploymentOptionsInternal.CONF_DIR)).exists());
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
        assertEquals(Map.of("foo", "1", "bar", "2"), conf.toMap());
    }

    @Test
    public void testWatchNamespaceOverride() {
        Map<String, String> originalEnv = System.getenv();
        try {
            Map<String, String> systemEnv = new HashMap<>(originalEnv);
            // Set the env var to override the predefined config
            systemEnv.put(EnvUtils.ENV_WATCH_NAMESPACES, "ns2,ns3");
            TestUtils.setEnv(systemEnv);
            // set config to watch different namespace
            Configuration config =
                    Configuration.fromMap(Map.of(OPERATOR_WATCHED_NAMESPACES.key(), "ns1"));
            FlinkConfigManager configManager = new FlinkConfigManager(config);
            Set<String> namespaces =
                    configManager.getOperatorConfiguration().getWatchedNamespaces();
            // expect namespaces to be those defined from env var
            Assertions.assertArrayEquals(new String[] {"ns2", "ns3"}, namespaces.toArray());
        } finally {
            TestUtils.setEnv(originalEnv);
        }
    }

    @Test
    public void testWatchNamespaceOverrideWhenEmpty() {
        Map<String, String> originalEnv = System.getenv();
        try {
            Map<String, String> systemEnv = new HashMap<>(originalEnv);
            // Set the env var to override the predefined config in this case empty
            systemEnv.put(EnvUtils.ENV_WATCH_NAMESPACES, "");
            TestUtils.setEnv(systemEnv);
            // set config to watch different namespace
            Configuration config =
                    Configuration.fromMap(Map.of(OPERATOR_WATCHED_NAMESPACES.key(), "ns1"));
            FlinkConfigManager configManager = new FlinkConfigManager(config);
            Set<String> namespaces =
                    configManager.getOperatorConfiguration().getWatchedNamespaces();
            // expect namespaces to be those defined from env var
            Assertions.assertArrayEquals(new String[] {"ns1"}, namespaces.toArray());
        } finally {
            TestUtils.setEnv(originalEnv);
        }
    }

    @Test
    public void testFlinkVersionRegex() {
        String version116PlusMatch =
                KubernetesOperatorConfigOptions.VERSION_CONF_PREFIX
                        + FlinkVersion.v1_16
                        + KubernetesOperatorConfigOptions.FLINK_VERSION_GREATER_THAN_SUFFIX
                        + ".conf1";
        String version116PlusNoMatch =
                KubernetesOperatorConfigOptions.VERSION_CONF_PREFIX + "w2_15" + "-" + ".conf2";

        Matcher match = FlinkConfigManager.FLINK_VERSION_PATTERN.matcher(version116PlusMatch);
        assertTrue(match.matches());
        assertEquals(3, match.groupCount());
        assertEquals(1, Integer.parseInt(match.group("major")));
        assertEquals(16, Integer.parseInt(match.group("minor")));
        assertNotNull(match.group("gt"));
        Matcher noMatch = FlinkConfigManager.FLINK_VERSION_PATTERN.matcher(version116PlusNoMatch);
        assertFalse(noMatch.matches());
    }

    @Test
    public void testGetRelevantVersionPrefixes() {
        Map<String, String> baseConfig = new HashMap<>();
        String version116Plus =
                KubernetesOperatorConfigOptions.VERSION_CONF_PREFIX
                        + FlinkVersion.v1_16
                        + KubernetesOperatorConfigOptions.FLINK_VERSION_GREATER_THAN_SUFFIX
                        + ".";
        String version117 =
                KubernetesOperatorConfigOptions.VERSION_CONF_PREFIX + FlinkVersion.v1_17 + ".";
        String version118Plus =
                KubernetesOperatorConfigOptions.VERSION_CONF_PREFIX
                        + FlinkVersion.v1_18
                        + KubernetesOperatorConfigOptions.FLINK_VERSION_GREATER_THAN_SUFFIX
                        + ".";
        String version119 =
                KubernetesOperatorConfigOptions.VERSION_CONF_PREFIX + FlinkVersion.v1_19 + ".";
        baseConfig.put(version116Plus + "conf1", "v1");
        baseConfig.put(version117 + "conf2", "v2");
        baseConfig.put(version118Plus + "conf3", "v3");
        baseConfig.put(version119 + "conf1", "v1.1");

        List<String> relevantPrefixes =
                FlinkConfigManager.getRelevantVersionPrefixes(baseConfig, FlinkVersion.v1_19);

        System.out.println(relevantPrefixes);

        assertEquals(
                3,
                relevantPrefixes.size(),
                "Expected 3 version prefix entries, 1.16+, 1.18+ and 1.19");
        assertEquals(relevantPrefixes.get(0), version116Plus);
        assertEquals(relevantPrefixes.get(1), version118Plus);
        assertEquals(relevantPrefixes.get(2), version119);
    }

    @Test
    public void testVersionNamespaceDefaultConfs() {
        var opConf = new Configuration();
        opConf.setString("conf0", "false");
        opConf.setString(
                KubernetesOperatorConfigOptions.VERSION_CONF_PREFIX
                        + FlinkVersion.v1_16
                        + KubernetesOperatorConfigOptions.FLINK_VERSION_GREATER_THAN_SUFFIX
                        + ".conf4",
                "v1_16 greater than");
        // This entry should not appear in the final config as it is overridden by the v1_17+ entry
        opConf.setString(
                KubernetesOperatorConfigOptions.VERSION_CONF_PREFIX
                        + FlinkVersion.v1_16
                        + KubernetesOperatorConfigOptions.FLINK_VERSION_GREATER_THAN_SUFFIX
                        + ".conf5",
                "v1_16 greater than");
        opConf.setString(
                KubernetesOperatorConfigOptions.VERSION_CONF_PREFIX + FlinkVersion.v1_17 + ".conf1",
                "v1");
        opConf.setString(
                KubernetesOperatorConfigOptions.VERSION_CONF_PREFIX + FlinkVersion.v1_17 + ".conf0",
                "true");
        // This overrides the v1_16+ entry.
        opConf.setString(
                KubernetesOperatorConfigOptions.VERSION_CONF_PREFIX
                        + FlinkVersion.v1_17
                        + KubernetesOperatorConfigOptions.FLINK_VERSION_GREATER_THAN_SUFFIX
                        + ".conf5",
                "v1_17 greater than");

        opConf.setString(
                KubernetesOperatorConfigOptions.VERSION_CONF_PREFIX + FlinkVersion.v1_18 + ".conf2",
                "v2");
        opConf.setString(
                KubernetesOperatorConfigOptions.VERSION_CONF_PREFIX
                        + FlinkVersion.v1_18
                        + KubernetesOperatorConfigOptions.FLINK_VERSION_GREATER_THAN_SUFFIX
                        + ".conf6",
                "v6");

        opConf.setString(KubernetesOperatorConfigOptions.NAMESPACE_CONF_PREFIX + "ns1.conf1", "vn");
        opConf.setString(KubernetesOperatorConfigOptions.NAMESPACE_CONF_PREFIX + "ns1.conf3", "v3");
        var configManager = new FlinkConfigManager(opConf);

        var v17Conf = configManager.getDefaultConfig("ns1", FlinkVersion.v1_17).toMap();
        var v18Conf = configManager.getDefaultConfig("ns1", FlinkVersion.v1_18).toMap();
        var controlConf = configManager.getDefaultConfig("control", FlinkVersion.v1_16).toMap();

        assertEquals("v1", v17Conf.get("conf1"));
        assertEquals("true", v17Conf.get("conf0"));

        assertEquals("v1_16 greater than", v18Conf.get("conf4"));
        assertEquals("v1_17 greater than", v18Conf.get("conf5"));
        assertEquals("v2", v18Conf.get("conf2"));
        assertEquals("v6", v18Conf.get("conf6"));
        assertEquals("false", v18Conf.get("conf0"));

        // Namespace defaults
        assertEquals("vn", v18Conf.get("conf1"));
        assertEquals("v3", v18Conf.get("conf3"));
        assertEquals("v3", v17Conf.get("conf3"));

        assertFalse(controlConf.containsKey("conf1"));
        assertFalse(controlConf.containsKey("conf2"));
        assertFalse(controlConf.containsKey("conf3"));
        assertEquals("false", controlConf.get("conf0"));

        var deployment = TestUtils.buildApplicationCluster(FlinkVersion.v1_18);
        deployment.getMetadata().setNamespace("ns1");
        deployment
                .getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(deployment.getSpec(), deployment);
        var observeConfig = configManager.getObserveConfig(deployment).toMap();
        assertEquals("vn", observeConfig.get("conf1"));
        assertEquals("v2", observeConfig.get("conf2"));
        assertEquals("v3", observeConfig.get("conf3"));
        assertEquals("false", observeConfig.get("conf0"));
    }
}
