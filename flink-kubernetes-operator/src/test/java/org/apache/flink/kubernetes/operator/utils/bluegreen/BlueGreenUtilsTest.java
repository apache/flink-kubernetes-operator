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

package org.apache.flink.kubernetes.operator.utils.bluegreen;

import org.apache.flink.kubernetes.operator.api.FlinkBlueGreenDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.bluegreen.BlueGreenDeploymentType;
import org.apache.flink.kubernetes.operator.api.spec.ConfigObjectNode;
import org.apache.flink.kubernetes.operator.api.spec.FlinkBlueGreenDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentTemplateSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobSpec;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.utils.SpecUtils;
import org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenContext;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for {@link BlueGreenUtils}. */
public class BlueGreenUtilsTest {

    private static final String TEST_NAMESPACE = "test-namespace";

    @Test
    public void testPrepareFlinkDeploymentWithoutNameReplacement() {
        String parentDeploymentName = "my-app";
        FlinkBlueGreenDeployment bgDeployment =
                buildBlueGreenDeployment(parentDeploymentName, TEST_NAMESPACE);

        // Add configuration that contains the deployment name in values
        Map<String, String> flinkConfig =
                bgDeployment.getSpec().getTemplate().getSpec().getFlinkConfiguration().asFlatMap();
        flinkConfig.put(
                "high-availability.storageDir",
                "s3://" + parentDeploymentName + "/highavailability");
        flinkConfig.put("metrics.scope.jm", parentDeploymentName + ".jm");
        bgDeployment.getSpec().getTemplate().getSpec().setFlinkConfiguration(flinkConfig);

        BlueGreenContext context = createContext(bgDeployment);

        // Test: Prepare a BLUE deployment
        FlinkDeployment blueDeployment =
                BlueGreenUtils.prepareFlinkDeployment(
                        context,
                        BlueGreenDeploymentType.BLUE,
                        null,
                        true,
                        bgDeployment.getMetadata());

        // Verify child deployment name is correctly set in metadata
        String expectedChildName = parentDeploymentName + "-blue";
        assertEquals(expectedChildName, blueDeployment.getMetadata().getName());

        // Verify configuration values that contain the parent name are NOT replaced
        Map<String, String> resultFlinkConfig =
                blueDeployment.getSpec().getFlinkConfiguration().asFlatMap();
        assertEquals(
                "s3://" + parentDeploymentName + "/highavailability",
                resultFlinkConfig.get("high-availability.storageDir"));
        assertEquals(parentDeploymentName + ".jm", resultFlinkConfig.get("metrics.scope.jm"));
    }

    private static FlinkBlueGreenDeployment buildBlueGreenDeployment(
            String name, String namespace) {
        var deployment = new FlinkBlueGreenDeployment();
        deployment.setMetadata(
                new ObjectMetaBuilder()
                        .withName(name)
                        .withNamespace(namespace)
                        .withUid(UUID.randomUUID().toString())
                        .build());

        var flinkDeploymentSpec =
                FlinkDeploymentSpec.builder()
                        .flinkConfiguration(new ConfigObjectNode())
                        .job(JobSpec.builder().upgradeMode(UpgradeMode.STATELESS).build())
                        .build();

        var bgDeploymentSpec =
                new FlinkBlueGreenDeploymentSpec(
                        new HashMap<>(),
                        FlinkDeploymentTemplateSpec.builder().spec(flinkDeploymentSpec).build());

        deployment.setSpec(bgDeploymentSpec);
        return deployment;
    }

    private BlueGreenContext createContext(FlinkBlueGreenDeployment bgDeployment) {
        FlinkBlueGreenDeploymentStatus status = new FlinkBlueGreenDeploymentStatus();
        status.setLastReconciledSpec(SpecUtils.writeSpecAsJSON(bgDeployment.getSpec(), "spec"));
        bgDeployment.setStatus(status);

        return new BlueGreenContext(bgDeployment, status, null, null, null);
    }
}
