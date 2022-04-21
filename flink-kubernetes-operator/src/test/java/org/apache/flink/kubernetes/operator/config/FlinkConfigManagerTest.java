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
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.crd.status.ReconciliationStatus;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/** Test for FlinkConfigManager. */
@EnableKubernetesMockClient(crud = true)
public class FlinkConfigManagerTest {

    private KubernetesClient kubernetesClient;

    @BeforeEach
    public void setup() {
        Configuration conf = new Configuration();
        conf.set(KubernetesOperatorConfigOptions.OPERATOR_DYNAMIC_CONFIG_ENABLED, false);
        updateConfigMap(conf);
    }

    private void updateConfigMap(Configuration configuration) {
        ConfigMap cm = new ConfigMap();
        ObjectMeta objectMeta = new ObjectMeta();
        objectMeta.setNamespace("default");
        objectMeta.setName(FlinkConfigManager.OP_CM_NAME);
        cm.setMetadata(objectMeta);
        cm.setData(Map.of(GlobalConfiguration.FLINK_CONF_FILENAME, writeAsYaml(configuration)));
        kubernetesClient
                .configMaps()
                .inNamespace("default")
                .withName(FlinkConfigManager.OP_CM_NAME)
                .createOrReplace(cm);
    }

    @Test
    public void testConfigGeneration() {
        ConfigOption<String> testConf = ConfigOptions.key("test").stringType().noDefaultValue();

        FlinkConfigManager configManager = new FlinkConfigManager(kubernetesClient);
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        ReconciliationStatus reconciliationStatus =
                deployment.getStatus().getReconciliationStatus();

        deployment.getSpec().getFlinkConfiguration().put(testConf.key(), "reconciled");
        reconciliationStatus.serializeAndSetLastReconciledSpec(deployment.getSpec());
        reconciliationStatus.markReconciledSpecAsStable();

        deployment.getSpec().getFlinkConfiguration().put(testConf.key(), "latest");
        assertEquals(
                "latest",
                configManager
                        .getDeployConfig(deployment.getMetadata(), deployment.getSpec())
                        .get(testConf));
        assertEquals("reconciled", configManager.getObserveConfig(deployment).get(testConf));

        deployment.getSpec().getFlinkConfiguration().put(testConf.key(), "stable");
        reconciliationStatus.serializeAndSetLastReconciledSpec(deployment.getSpec());
        reconciliationStatus.markReconciledSpecAsStable();

        deployment.getSpec().getFlinkConfiguration().put(testConf.key(), "rolled-back");
        reconciliationStatus.serializeAndSetLastReconciledSpec(deployment.getSpec());
        reconciliationStatus.setState(ReconciliationState.ROLLED_BACK);

        assertEquals("stable", configManager.getObserveConfig(deployment).get(testConf));
    }

    @Test
    public void testConfUpdate() {
        Configuration config = new Configuration();
        updateConfigMap(config);
        FlinkConfigManager configManager = new FlinkConfigManager(kubernetesClient);
        assertFalse(
                configManager
                        .getDefaultConfig()
                        .contains(
                                KubernetesOperatorConfigOptions
                                        .OPERATOR_RECONCILER_RESCHEDULE_INTERVAL));

        config.set(
                KubernetesOperatorConfigOptions.OPERATOR_RECONCILER_RESCHEDULE_INTERVAL,
                Duration.ofSeconds(15));
        updateConfigMap(config);

        configManager.getConfigUpdater().run();

        assertEquals(
                Duration.ofSeconds(15),
                configManager
                        .getDefaultConfig()
                        .get(
                                KubernetesOperatorConfigOptions
                                        .OPERATOR_RECONCILER_RESCHEDULE_INTERVAL));
        assertEquals(
                Duration.ofSeconds(15),
                configManager.getOperatorConfiguration().getReconcileInterval());
    }

    private static String writeAsYaml(Configuration configuration) {
        StringBuilder stringBuilder = new StringBuilder();
        configuration.toMap().forEach((k, v) -> stringBuilder.append(k + ": " + v + "\n"));
        return stringBuilder.toString();
    }
}
