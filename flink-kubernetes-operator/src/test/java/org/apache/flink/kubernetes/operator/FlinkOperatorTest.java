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

import io.javaoperatorsdk.operator.api.config.ConfigurationServiceProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

/** @link FlinkOperator unit tests. */
public class FlinkOperatorTest {

    @Test
    public void testExecutorServiceUsesReconciliationMaxParallelismFromConfig() {
        checkExecutorServiceThreadCount(Optional.of(42), 42);
        // TODO: cannot override the operator configs twice in java-operator-sdk v3
        Assertions.assertThrows(
                IllegalStateException.class,
                () -> checkExecutorServiceThreadCount(Optional.of(-1), 1));
    }

    private void checkExecutorServiceThreadCount(
            Optional<Integer> parallelism, int expectedThreadCount) {
        var es = getExecutorForParallelismConfig(parallelism);
        Assertions.assertInstanceOf(ThreadPoolExecutor.class, es);
        ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) es;
        Assertions.assertEquals(expectedThreadCount, threadPoolExecutor.getMaximumPoolSize());
    }

    private ExecutorService getExecutorForParallelismConfig(Optional<Integer> parallelism) {
        var operatorConfig = new Configuration();
        parallelism.ifPresent(
                p ->
                        operatorConfig.setInteger(
                                KubernetesOperatorConfigOptions.OPERATOR_RECONCILER_MAX_PARALLELISM,
                                p));
        new FlinkOperator(operatorConfig);
        return ConfigurationServiceProvider.instance().getExecutorService();
    }
}
