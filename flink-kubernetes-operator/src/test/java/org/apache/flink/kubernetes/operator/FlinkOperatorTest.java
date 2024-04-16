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

package org.apache.flink.kubernetes.operator;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;

import io.fabric8.kubernetes.client.Config;
import io.javaoperatorsdk.operator.RegisteredController;
import io.javaoperatorsdk.operator.api.config.ControllerConfiguration;
import io.javaoperatorsdk.operator.processing.event.rate.LinearRateLimiter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ThreadPoolExecutor;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @link FlinkOperator unit tests. Since at the time of writing this the JOSDK does not support
 *     overriding the configuration multiple times (has a singleton @link
 *     ConfigurationServiceProvider) we write multiple tests as a single function, please provide
 *     ample comments.
 */
public class FlinkOperatorTest {
    @BeforeAll
    public static void setAutoTryKubeConfig() {
        System.setProperty(Config.KUBERNETES_AUTH_TRYKUBECONFIG_SYSTEM_PROPERTY, "false");
    }

    @Test
    public void testConfigurationPassedToJOSDK() {
        var testParallelism = 42;
        var testSelector = "flink=enabled";
        var testLeaseName = "test-lease";

        var operatorConfig = new Configuration();

        // We need to set this property so the operator can configure the lease namespace
        System.setProperty(Config.KUBERNETES_NAMESPACE_SYSTEM_PROPERTY, "test_namespace");

        operatorConfig.setInteger(
                KubernetesOperatorConfigOptions.OPERATOR_RECONCILE_PARALLELISM, testParallelism);
        operatorConfig.set(KubernetesOperatorConfigOptions.OPERATOR_LABEL_SELECTOR, testSelector);
        operatorConfig.set(KubernetesOperatorConfigOptions.OPERATOR_STOP_ON_INFORMER_ERROR, false);
        operatorConfig.set(KubernetesOperatorConfigOptions.OPERATOR_LEADER_ELECTION_ENABLED, true);
        operatorConfig.set(
                KubernetesOperatorConfigOptions.OPERATOR_LEADER_ELECTION_LEASE_NAME, testLeaseName);

        var testOperator = new FlinkOperator(operatorConfig);
        testOperator.registerDeploymentController();
        testOperator.registerSessionJobController();

        var configService = testOperator.getOperator().getConfigurationService();

        // Test parallelism being passed
        var executorService = configService.getExecutorService();
        Assertions.assertInstanceOf(ThreadPoolExecutor.class, executorService);
        ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) executorService;
        Assertions.assertEquals(threadPoolExecutor.getMaximumPoolSize(), testParallelism);

        // Test label selector being passed
        // We have a label selector for each controller
        var labelSelectors =
                testOperator.registeredControllers.stream()
                        .map(RegisteredController::getConfiguration)
                        .map(ControllerConfiguration::getLabelSelector);

        labelSelectors.forEach(selector -> Assertions.assertEquals(testSelector, selector));
        Assertions.assertFalse(configService.stopOnInformerErrorDuringStartup());

        testOperator.registeredControllers.stream()
                .map(RegisteredController::getConfiguration)
                .map(ControllerConfiguration::getRateLimiter)
                .forEach(rl -> Assertions.assertTrue(((LinearRateLimiter) rl).isActivated()));

        var leaderElectionConfiguration = configService.getLeaderElectionConfiguration().get();

        Assertions.assertEquals(testLeaseName, leaderElectionConfiguration.getLeaseName());
        Assertions.assertFalse(leaderElectionConfiguration.getLeaseNamespace().isPresent());
        Assertions.assertFalse(leaderElectionConfiguration.getIdentity().isPresent());
    }

    @Test
    public void testLeaderElectionConfig() {
        var operatorConfig = new Configuration();
        operatorConfig.set(KubernetesOperatorConfigOptions.OPERATOR_LEADER_ELECTION_ENABLED, true);

        try {
            new FlinkOperator(operatorConfig);
        } catch (IllegalConfigurationException ice) {
            assertTrue(
                    ice.getMessage()
                            .startsWith(
                                    "kubernetes.operator.leader-election.lease-name must be defined"));
        }

        var flinkOperator = new FlinkOperator(new Configuration());

        assertTrue(
                flinkOperator
                        .getOperator()
                        .getConfigurationService()
                        .getLeaderElectionConfiguration()
                        .isEmpty());
    }
}
