/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.operator.api.utils;

import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Test for {@link SpecUtils}. */
public class SpecUtilsTest {

    @Test
    public void testSpecSerializationWithVersion() throws JsonProcessingException {
        FlinkDeployment app = BaseTestUtils.buildApplicationCluster();
        String serialized = SpecUtils.writeSpecWithMeta(app.getSpec(), app);
        ObjectNode node = (ObjectNode) new ObjectMapper().readTree(serialized);

        ObjectNode internalMeta = (ObjectNode) node.get(SpecUtils.INTERNAL_METADATA_JSON_KEY);
        assertEquals("flink.apache.org/v1beta1", internalMeta.get("apiVersion").asText());
        assertEquals(
                app.getSpec(),
                SpecUtils.deserializeSpecWithMeta(serialized, FlinkDeploymentSpec.class).getSpec());

        // test backward compatibility
        String oldSerialized =
                "{\"job\":{\"jarURI\":\"local:///opt/flink/examples/streaming/StateMachineExample.jar\",\"parallelism\":2,\"entryClass\":null,\"args\":[],\"state\":\"running\",\"savepointTriggerNonce\":null,\"initialSavepointPath\":null,\"upgradeMode\":\"stateless\",\"allowNonRestoredState\":null},\"restartNonce\":null,\"flinkConfiguration\":{\"taskmanager.numberOfTaskSlots\":\"2\"},\"image\":\"flink:1.15\",\"imagePullPolicy\":null,\"serviceAccount\":\"flink\",\"flinkVersion\":\"v1_15\",\"ingress\":null,\"podTemplate\":null,\"jobManager\":{\"resource\":{\"cpu\":1.0,\"memory\":\"2048m\"},\"replicas\":1,\"podTemplate\":null},\"taskManager\":{\"resource\":{\"cpu\":1.0,\"memory\":\"2048m\"},\"podTemplate\":null},\"logConfiguration\":null,\"apiVersion\":\"v1beta1\"}";

        var migrated = SpecUtils.deserializeSpecWithMeta(oldSerialized, FlinkDeploymentSpec.class);
        assertEquals(
                "local:///opt/flink/examples/streaming/StateMachineExample.jar",
                migrated.getSpec().getJob().getJarURI());
        assertNull(migrated.getMeta());
    }

    @Test
    public void testSpecSerializationWithoutGeneration() throws JsonProcessingException {
        // with regards to ReconcialiationMetadata & SpecWithMeta
        FlinkDeployment app = BaseTestUtils.buildApplicationCluster();
        app.getMetadata().setGeneration(12L);
        String serialized = SpecUtils.writeSpecWithMeta(app.getSpec(), app);
        ObjectNode node = (ObjectNode) new ObjectMapper().readTree(serialized);

        ObjectNode internalMeta = (ObjectNode) node.get(SpecUtils.INTERNAL_METADATA_JSON_KEY);
        assertEquals("flink.apache.org/v1beta1", internalMeta.get("apiVersion").asText());
        assertEquals(
                app.getSpec(),
                SpecUtils.deserializeSpecWithMeta(serialized, FlinkDeploymentSpec.class).getSpec());
        assertNull(app.getStatus().getObservedGeneration());

        // test backward compatibility
        String oldSerialized =
                "{\"apiVersion\":\"flink.apache.org/v1beta1\",\"metadata\":{\"generation\":5},\"firstDeployment\":false}";
        var migrated = SpecUtils.deserializeSpecWithMeta(oldSerialized, FlinkDeploymentSpec.class);
        assertNull(migrated.getMeta());
    }
}
