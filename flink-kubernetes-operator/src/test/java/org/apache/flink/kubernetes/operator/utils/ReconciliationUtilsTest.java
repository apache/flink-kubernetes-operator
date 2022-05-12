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
import org.apache.flink.kubernetes.operator.crd.CrdConstants;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.crd.spec.JobState;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/** Test for {@link org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils}. */
public class ReconciliationUtilsTest {

    FlinkOperatorConfiguration operatorConfiguration =
            FlinkOperatorConfiguration.fromConfiguration(
                    new Configuration(), Collections.emptySet());

    @Test
    public void testRescheduleUpgradeImmediately() {
        FlinkDeployment app = TestUtils.buildApplicationCluster();
        app.getSpec().getJob().setState(JobState.RUNNING);
        FlinkDeployment previous = ReconciliationUtils.clone(app);
        FlinkDeployment current = ReconciliationUtils.clone(app);
        current.getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(ReconciliationUtils.clone(current.getSpec()));
        ReconciliationUtils.updateForSpecReconciliationSuccess(current, JobState.SUSPENDED);

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
        String serialized = ReconciliationUtils.writeSpecWithCurrentVersion(app.getSpec());
        ObjectNode node = (ObjectNode) new ObjectMapper().readTree(serialized);
        assertEquals(CrdConstants.API_VERSION, node.get("apiVersion").asText());
        assertEquals(
                app.getSpec(),
                ReconciliationUtils.deserializedSpecWithVersion(
                        serialized, FlinkDeploymentSpec.class));
    }
}
