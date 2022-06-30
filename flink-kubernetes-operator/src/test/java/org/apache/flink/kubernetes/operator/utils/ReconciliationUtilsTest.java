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

package org.apache.flink.kubernetes.operator.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.crd.spec.JobState;
import org.apache.flink.kubernetes.operator.crd.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Test for {@link org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils}. */
public class ReconciliationUtilsTest {

    FlinkOperatorConfiguration operatorConfiguration =
            FlinkOperatorConfiguration.fromConfiguration(new Configuration());

    @Test
    public void testRescheduleUpgradeImmediately() {
        FlinkDeployment app = TestUtils.buildApplicationCluster();
        app.getSpec().getJob().setState(JobState.RUNNING);
        app.getStatus().getReconciliationStatus().setState(ReconciliationState.DEPLOYED);
        ReconciliationUtils.updateStatusForDeployedSpec(app, new Configuration());
        FlinkDeployment previous = ReconciliationUtils.clone(app);
        FlinkDeployment current = ReconciliationUtils.clone(app);
        ReconciliationUtils.updateStatusBeforeDeploymentAttempt(current, new Configuration());

        UpdateControl<FlinkDeployment> updateControl =
                ReconciliationUtils.toUpdateControl(operatorConfiguration, current, previous, true);

        assertFalse(updateControl.isUpdateResource());
        assertFalse(updateControl.isUpdateStatus());
        assertEquals(0, updateControl.getScheduleDelay().get());

        updateControl =
                ReconciliationUtils.toUpdateControl(operatorConfiguration, current, current, true);

        assertFalse(updateControl.isUpdateResource());
        assertFalse(updateControl.isUpdateStatus());
        assertNotEquals(0, updateControl.getScheduleDelay().get());
    }

    @Test
    public void testSpecSerializationWithVersion() throws JsonProcessingException {
        FlinkDeployment app = TestUtils.buildApplicationCluster();
        app.getMetadata().setGeneration(12L);
        String serialized = ReconciliationUtils.writeSpecWithMeta(app.getSpec(), app);
        ObjectNode node = (ObjectNode) new ObjectMapper().readTree(serialized);

        ObjectNode internalMeta =
                (ObjectNode) node.get(ReconciliationUtils.INTERNAL_METADATA_JSON_KEY);
        assertEquals("flink.apache.org/v1beta1", internalMeta.get("apiVersion").asText());
        assertEquals(12L, internalMeta.get("metadata").get("generation").asLong());
        assertEquals(
                app.getSpec(),
                ReconciliationUtils.deserializeSpecWithMeta(serialized, FlinkDeploymentSpec.class)
                        .f0);

        // test backward compatibility
        String oldSerialized =
                "{\"job\":{\"jarURI\":\"local:///opt/flink/examples/streaming/StateMachineExample.jar\",\"parallelism\":2,\"entryClass\":null,\"args\":[],\"state\":\"running\",\"savepointTriggerNonce\":null,\"initialSavepointPath\":null,\"upgradeMode\":\"stateless\",\"allowNonRestoredState\":null},\"restartNonce\":null,\"flinkConfiguration\":{\"taskmanager.numberOfTaskSlots\":\"2\"},\"image\":\"flink:1.15\",\"imagePullPolicy\":null,\"serviceAccount\":\"flink\",\"flinkVersion\":\"v1_15\",\"ingress\":null,\"podTemplate\":null,\"jobManager\":{\"resource\":{\"cpu\":1.0,\"memory\":\"2048m\"},\"replicas\":1,\"podTemplate\":null},\"taskManager\":{\"resource\":{\"cpu\":1.0,\"memory\":\"2048m\"},\"podTemplate\":null},\"logConfiguration\":null,\"apiVersion\":\"v1beta1\"}";

        var migrated =
                ReconciliationUtils.deserializeSpecWithMeta(
                        oldSerialized, FlinkDeploymentSpec.class);
        assertEquals(
                "local:///opt/flink/examples/streaming/StateMachineExample.jar",
                migrated.f0.getJob().getJarURI());
        assertNull(migrated.f1);
    }
}
