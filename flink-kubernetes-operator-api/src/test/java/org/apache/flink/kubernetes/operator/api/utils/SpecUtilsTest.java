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
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Test for {@link SpecUtils}. */
class SpecUtilsTest {

    private static final ObjectMapper yamlObjectMapper = new ObjectMapper(new YAMLFactory());

    @Test
    void testSpecSerializationWithVersion() throws JsonProcessingException {
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
    void testSpecSerializationWithoutGeneration() throws JsonProcessingException {
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

    @Test
    void convertsStringMapToJsonNode() {
        var map = Map.of("k1", "v1", "k2", "v2", "k3.nested", "v3");
        var node = SpecUtils.mapToJsonNode(map);

        assertThat(node).hasSize(3);
        assertThat(node.get("k1").asText()).isEqualTo("v1");
        assertThat(node.get("k2").asText()).isEqualTo("v2");
        assertThat(node.get("k3.nested").asText()).isEqualTo("v3");
    }

    @Test
    void convertsJsonNodeToMap() throws JsonProcessingException {
        var node =
                yamlObjectMapper.readTree("k1: v1 \n" + "k2: v2 \n" + "k3:\n" + "  nested: v3\n");

        var map = SpecUtils.toStringMap(node);
        assertThat(map).hasSize(3);
        assertThat(map.get("k1")).isEqualTo("v1");
        assertThat(map.get("k2")).isEqualTo("v2");
        assertThat(map.get("k3.nested")).isEqualTo("v3");
    }

    @Test
    void addConfigPropertyToSpec() {
        var spec = new FlinkDeploymentSpec();

        SpecUtils.addConfigProperty(spec, "k1", "v1");

        assertThat(spec.getFlinkConfiguration().get("k1").asText()).isEqualTo("v1");
    }

    @Test
    void addConfigPropertiesToSpec() {
        var spec = new FlinkDeploymentSpec();

        SpecUtils.addConfigProperties(spec, Map.of("k1", "v1", "k2", "v2"));

        assertThat(spec.getFlinkConfiguration().get("k1").asText()).isEqualTo("v1");
        assertThat(spec.getFlinkConfiguration().get("k2").asText()).isEqualTo("v2");
    }

    @Test
    void removeConfigPropertiesFromSpec() {
        var spec = new FlinkDeploymentSpec();
        SpecUtils.addConfigProperties(spec, Map.of("k1", "v1", "k2", "v2"));

        SpecUtils.removeConfigProperties(spec, "k1");

        assertThat(spec.getFlinkConfiguration().get("k1")).isNull();
        assertThat(spec.getFlinkConfiguration().get("k2").asText()).isEqualTo("v2");
    }
}
