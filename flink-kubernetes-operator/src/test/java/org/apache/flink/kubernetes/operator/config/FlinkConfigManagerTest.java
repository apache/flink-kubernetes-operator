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
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.crd.status.ReconciliationStatus;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

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
        FlinkConfigManager configManager = new FlinkConfigManager(config);
        assertFalse(
                configManager
                        .getDefaultConfig()
                        .contains(
                                KubernetesOperatorConfigOptions
                                        .OPERATOR_RECONCILER_RESCHEDULE_INTERVAL));

        config.set(
                KubernetesOperatorConfigOptions.OPERATOR_RECONCILER_RESCHEDULE_INTERVAL,
                Duration.ofSeconds(15));

        configManager.updateDefaultConfig(config);

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
}
