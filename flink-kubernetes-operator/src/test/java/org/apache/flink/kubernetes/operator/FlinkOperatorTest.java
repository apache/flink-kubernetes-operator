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
import org.apache.flink.kubernetes.operator.config.DefaultConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import static java.util.Optional.empty;

/** @link FlinkOperator unit tests. */
public class FlinkOperatorTest {

    @Test
    public void testExecutorServiceDefaultsToMaxParallelism() {
        checkExecutorServiceThreadCount(empty(), Integer.MAX_VALUE);
    }

    @Test
    public void testExecutorServiceUsesReconciliationMaxParallelismFromConfig() {
        checkExecutorServiceThreadCount(Optional.of(42), 42);
    }

    @Test
    public void testExecutorServiceUsesMaxParallelismForMinusOneReconciliationMaxParallelism() {
        checkExecutorServiceThreadCount(Optional.of(-1), Integer.MAX_VALUE);
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
                p -> operatorConfig.setInteger("operator.reconciler.max.parallelism", p));

        var flinkConfig = new Configuration();
        var config = new DefaultConfig(operatorConfig, flinkConfig);

        FlinkOperator flinkOperator = new FlinkOperator(config);
        return flinkOperator.getOperator().getConfigurationService().getExecutorService();
    }
}
