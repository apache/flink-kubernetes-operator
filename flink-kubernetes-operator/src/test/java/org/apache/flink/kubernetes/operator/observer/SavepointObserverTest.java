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

package org.apache.flink.kubernetes.operator.observer;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.TestingStatusHelper;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.crd.status.Savepoint;
import org.apache.flink.kubernetes.operator.crd.status.SavepointInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;

/** Tests for {@link SavepointObserver}. */
public class SavepointObserverTest {

    private final FlinkConfigManager configManager = new FlinkConfigManager(new Configuration());
    private final TestingFlinkService flinkService = new TestingFlinkService();

    @Test
    public void testBasicObserve() {
        SavepointObserver observer =
                new SavepointObserver(flinkService, configManager, new TestingStatusHelper<>());
        SavepointInfo spInfo = new SavepointInfo();
        Assertions.assertTrue(spInfo.getSavepointHistory().isEmpty());

        Savepoint sp = new Savepoint(1, "sp1");
        observer.updateSavepointHistory(spInfo, sp, configManager.getDefaultConfig());

        Assertions.assertNotNull(spInfo.getSavepointHistory());
        Assertions.assertIterableEquals(
                Collections.singletonList(sp), spInfo.getSavepointHistory());
    }

    @Test
    public void testAgeBasedDispose() {
        Configuration conf = new Configuration();
        conf.set(
                KubernetesOperatorConfigOptions.OPERATOR_SAVEPOINT_HISTORY_MAX_AGE,
                Duration.ofMillis(5));

        SavepointObserver observer =
                new SavepointObserver(flinkService, configManager, new TestingStatusHelper<>());
        SavepointInfo spInfo = new SavepointInfo();

        Savepoint sp1 = new Savepoint(1, "sp1");
        observer.updateSavepointHistory(spInfo, sp1, conf);
        Assertions.assertIterableEquals(
                Collections.singletonList(sp1), spInfo.getSavepointHistory());
        Assertions.assertIterableEquals(
                Collections.emptyList(), flinkService.getDisposedSavepoints());

        Savepoint sp2 = new Savepoint(2, "sp2");
        observer.updateSavepointHistory(spInfo, sp2, conf);
        Assertions.assertIterableEquals(
                Collections.singletonList(sp2), spInfo.getSavepointHistory());
        Assertions.assertIterableEquals(
                Collections.singletonList(sp1.getLocation()), flinkService.getDisposedSavepoints());
    }
}
