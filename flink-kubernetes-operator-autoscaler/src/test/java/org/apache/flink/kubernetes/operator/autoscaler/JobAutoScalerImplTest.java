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

package org.apache.flink.kubernetes.operator.autoscaler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.OperatorTestBase;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import lombok.Getter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions.AUTOSCALER_ENABLED;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for JobAutoScalerImpl. */
@EnableKubernetesMockClient(crud = true)
public class JobAutoScalerImplTest extends OperatorTestBase {

    @Getter private KubernetesClient kubernetesClient;

    private FlinkDeployment app;

    @BeforeEach
    public void setup() {
        app = TestUtils.buildApplicationCluster();
        app.getMetadata().setGeneration(1L);
        app.getStatus().getJobStatus().setJobId(new JobID().toHexString());
        kubernetesClient.resource(app).createOrReplace();

        var defaultConf = new Configuration();
        defaultConf.set(AUTOSCALER_ENABLED, true);
        configManager = new FlinkConfigManager(defaultConf);
        ReconciliationUtils.updateStatusForDeployedSpec(
                app, configManager.getDeployConfig(app.getMetadata(), app.getSpec()));
        app.getStatus().getReconciliationStatus().markReconciledSpecAsStable();
    }

    @Test
    void testErrorReporting() {
        var autoscaler = new JobAutoScalerImpl(kubernetesClient, null, null, null, eventRecorder);
        FlinkResourceContext<FlinkDeployment> resourceContext = getResourceContext(app);
        ResourceID resourceId = ResourceID.fromResource(app);

        autoscaler.scale(resourceContext);
        Assertions.assertEquals(1, autoscaler.flinkMetrics.get(resourceId).numErrors.getCount());

        autoscaler.scale(resourceContext);
        Assertions.assertEquals(2, autoscaler.flinkMetrics.get(resourceId).numErrors.getCount());

        assertEquals(0, autoscaler.flinkMetrics.get(resourceId).numScalings.getCount());
    }
}
