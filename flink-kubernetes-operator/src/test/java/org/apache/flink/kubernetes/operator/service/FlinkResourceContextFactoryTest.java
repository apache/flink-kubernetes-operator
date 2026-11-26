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

package org.apache.flink.kubernetes.operator.service;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link FlinkResourceContextFactory}. */
public class FlinkResourceContextFactoryTest {

    @Test
    public void testUnboundedReconcileParallelismDoesNotCrashFactory() {
        var conf = new Configuration();
        conf.set(KubernetesOperatorConfigOptions.OPERATOR_RECONCILE_PARALLELISM, -1);
        var configManager = new FlinkConfigManager(conf);

        assertDoesNotThrow(
                () ->
                        new FlinkResourceContextFactory(
                                configManager,
                                TestUtils.createTestMetricGroup(new Configuration()),
                                null));
    }

    @Test
    public void testFixedReconcileParallelismConstructsFactory() {
        var conf = new Configuration();
        conf.set(KubernetesOperatorConfigOptions.OPERATOR_RECONCILE_PARALLELISM, 4);
        var configManager = new FlinkConfigManager(conf);

        assertDoesNotThrow(
                () ->
                        new FlinkResourceContextFactory(
                                configManager,
                                TestUtils.createTestMetricGroup(new Configuration()),
                                null));
    }

    @Test
    public void testCloseStopsClientExecutor() {
        try (var configManager = new FlinkConfigManager(new Configuration());
                var metricGroup = TestUtils.createTestMetricGroup(new Configuration())) {
            var contextFactory = new FlinkResourceContextFactory(configManager, metricGroup, null);
            assertFalse(contextFactory.clientExecutorService.isShutdown());

            contextFactory.close();
            assertTrue(contextFactory.clientExecutorService.isShutdown());
        }
    }

    @Test
    public void testExceptionCacheIsSeparateForResourcesOfDifferentKindsWithTheSameName() {
        try (var configManager = new FlinkConfigManager(new Configuration());
                var metricGroup = TestUtils.createTestMetricGroup(new Configuration())) {
            final var contextFactory =
                    new FlinkResourceContextFactory(configManager, metricGroup, null);
            final var deployment =
                    TestUtils.buildApplicationCluster("same-name", TestUtils.TEST_NAMESPACE);
            final var miniCluster =
                    TestUtils.buildMiniCluster(
                            "same-name",
                            TestUtils.TEST_NAMESPACE,
                            FlinkVersion.v2_4,
                            JobState.RUNNING);

            final var deploymentEntry =
                    contextFactory.getResourceContext(deployment, null).getExceptionCacheEntry();
            final var miniClusterEntry =
                    contextFactory.getResourceContext(miniCluster, null).getExceptionCacheEntry();
            assertThat(miniClusterEntry).isNotSameAs(deploymentEntry);

            contextFactory.cleanup(miniCluster);
            assertThat(contextFactory.getResourceContext(deployment, null).getExceptionCacheEntry())
                    .isSameAs(deploymentEntry);
        }
    }
}
