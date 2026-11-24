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

package org.apache.flink.kubernetes.operator.api;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.kubernetes.operator.api.spec.FlinkMiniClusterSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.api.utils.BaseTestUtils;
import org.apache.flink.kubernetes.operator.api.utils.SpecUtils;

import io.fabric8.kubernetes.api.model.ConditionBuilder;
import io.fabric8.kubernetes.client.utils.Serialization;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link FlinkMiniCluster} custom resource. */
class FlinkMiniClusterTest {

    @Test
    void testYamlRoundTrip() {
        var original = BaseTestUtils.buildMiniCluster();
        original.getSpec().getJob().setArgs(new String[] {"--input", "/data"});
        original.getSpec().setLogConfiguration(Map.of("log4j-console.properties", "x"));
        original.getStatus().setPodDeploymentStatus(JobManagerDeploymentStatus.READY);
        original.getStatus().getJobStatus().setState(JobStatus.RUNNING);
        original.getStatus()
                .setConditions(
                        List.of(
                                new ConditionBuilder()
                                        .withType("Running")
                                        .withStatus("True")
                                        .withReason("Running")
                                        .build()));

        var yaml = Serialization.asYaml(original);
        var deserialized = Serialization.unmarshal(yaml, FlinkMiniCluster.class);

        assertThat(deserialized.getKind()).isEqualTo(CrdConstants.KIND_FLINK_MINI_CLUSTER);
        assertThat(deserialized.getApiVersion())
                .isEqualTo(CrdConstants.API_GROUP + "/" + CrdConstants.API_VERSION);
        assertThat(deserialized.getSpec()).isEqualTo(original.getSpec());
        assertThat(deserialized.getStatus()).isEqualTo(original.getStatus());
    }

    @Test
    void testStatusSerializesPodDeploymentStatusOnly() {
        var miniCluster = BaseTestUtils.buildMiniCluster();
        miniCluster.getStatus().setPodDeploymentStatus(JobManagerDeploymentStatus.READY);

        var yaml = Serialization.asYaml(miniCluster.getStatus());

        assertThat(yaml).contains("podDeploymentStatus: \"READY\"");
        assertThat(yaml).doesNotContain("jobManagerDeploymentStatus");
        // getKubernetesDeploymentStatus() is derived from podDeploymentStatus and must not be
        // serialized
        assertThat(yaml).doesNotContainPattern("(?m)^kubernetesDeploymentStatus:");
    }

    @Test
    void testUnknownFieldsAreIgnored() {
        var yaml =
                "apiVersion: flink.apache.org/v1beta1\n"
                        + "kind: FlinkMiniCluster\n"
                        + "metadata:\n"
                        + "  name: test\n"
                        + "spec:\n"
                        + "  image: flink:2.4\n"
                        + "  futureField: value\n"
                        + "status:\n"
                        + "  podDeploymentStatus: DEPLOYING\n"
                        + "  futureStatusField: value\n";

        var miniCluster = Serialization.unmarshal(yaml, FlinkMiniCluster.class);

        assertThat(miniCluster.getSpec().getImage()).isEqualTo("flink:2.4");
        assertThat(miniCluster.getStatus().getPodDeploymentStatus())
                .isEqualTo(JobManagerDeploymentStatus.DEPLOYING);
    }

    @Test
    void testLastReconciledSpecRoundTrip() {
        var miniCluster = BaseTestUtils.buildMiniCluster();
        var reconciliationStatus = miniCluster.getStatus().getReconciliationStatus();
        reconciliationStatus.setState(ReconciliationState.DEPLOYED);
        reconciliationStatus.setLastReconciledSpec(
                SpecUtils.writeSpecWithMeta(miniCluster.getSpec(), miniCluster));

        FlinkMiniClusterSpec deserialized = reconciliationStatus.deserializeLastReconciledSpec();

        assertThat(deserialized).isEqualTo(miniCluster.getSpec());
        assertThat(deserialized.getJob().getState()).isEqualTo(JobState.RUNNING);
    }
}
