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

package org.apache.flink.kubernetes.operator.controller;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingFlinkResourceContextFactory;
import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.api.FlinkBlueGreenDeployment;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentStatus;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;

import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import lombok.AllArgsConstructor;
import lombok.Getter;

/** A wrapper around {@link FlinkBlueGreenDeploymentController} used by unit tests. */
public class TestingFlinkBlueGreenDeploymentController
        implements Reconciler<FlinkBlueGreenDeployment> {

    @Getter private TestingFlinkResourceContextFactory contextFactory;

    public final FlinkBlueGreenDeploymentController flinkBlueGreenDeploymentController;

    public TestingFlinkBlueGreenDeploymentController(
            FlinkConfigManager configManager, TestingFlinkService flinkService) {
        contextFactory =
                new TestingFlinkResourceContextFactory(
                        configManager,
                        TestUtils.createTestMetricGroup(new Configuration()),
                        flinkService,
                        null);

        flinkBlueGreenDeploymentController = new FlinkBlueGreenDeploymentController(contextFactory);
        flinkBlueGreenDeploymentController.minimumAbortGracePeriodMs = 1000;
    }

    @Override
    public UpdateControl<FlinkBlueGreenDeployment> reconcile(
            FlinkBlueGreenDeployment flinkBlueGreenDeployment,
            Context<FlinkBlueGreenDeployment> context)
            throws Exception {
        var cloned = ReconciliationUtils.clone(flinkBlueGreenDeployment);
        return flinkBlueGreenDeploymentController.reconcile(cloned, context);
    }

    @Override
    public ErrorStatusUpdateControl<FlinkBlueGreenDeployment> updateErrorStatus(
            FlinkBlueGreenDeployment flinkBlueGreenDeployment,
            Context<FlinkBlueGreenDeployment> context,
            Exception e) {
        return null;
    }

    /** A simple DTO to handle common reconciliation results for tests. */
    @AllArgsConstructor
    public static class BlueGreenReconciliationResult {
        public UpdateControl<FlinkBlueGreenDeployment> updateControl;

        public FlinkBlueGreenDeployment deployment;

        public FlinkBlueGreenDeploymentStatus reconciledStatus;
    }
}
