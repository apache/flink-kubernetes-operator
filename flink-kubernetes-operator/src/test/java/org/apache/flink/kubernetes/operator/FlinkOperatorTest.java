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
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;

import io.javaoperatorsdk.operator.RegisteredController;
import io.javaoperatorsdk.operator.api.config.ConfigurationServiceProvider;
import io.javaoperatorsdk.operator.api.config.ControllerConfiguration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * @link FlinkOperator unit tests. Since at the time of writing this the JOSDK does not support
 *     overriding the configuration multiple times (has a singleton @link
 *     ConfigurationServiceProvider) we write multiple tests as a single function, please provide
 *     ample comments.
 */
public class FlinkOperatorTest {

    @Test
    public void testConfigurationPassedToJOSDK() {
        var testParallelism = 42;
        var testSelector = "flink=enabled";
        var operatorConfig = new Configuration();

        operatorConfig.setInteger(
                KubernetesOperatorConfigOptions.OPERATOR_RECONCILE_PARALLELISM, testParallelism);
        operatorConfig.set(KubernetesOperatorConfigOptions.OPERATOR_LABEL_SELECTOR, testSelector);

        var testOperator = new FlinkOperator(operatorConfig);
        testOperator.registerDeploymentController();
        testOperator.registerSessionJobController();

        // Test parallelism being passed
        var executorService = ConfigurationServiceProvider.instance().getExecutorService();
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

        // TODO: Overriding operator configuration twice in JOSDK v3 yields IllegalStateException
        var secondParallelism = 420;
        var secondConfig = new Configuration();

        secondConfig.setInteger(
                KubernetesOperatorConfigOptions.OPERATOR_RECONCILE_PARALLELISM, secondParallelism);

        Assertions.assertThrows(IllegalStateException.class, () -> new FlinkOperator(secondConfig));
    }
}
